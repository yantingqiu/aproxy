// Package replication provides PostgreSQL logical replication streaming.
package replication

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"go.uber.org/zap"
)

// PGStreamer streams change events from PostgreSQL logical replication
type PGStreamer struct {
	config    *ServerConfig
	logger    *zap.Logger
	eventChan chan<- *ChangeEvent

	conn      *pgconn.PgConn
	mu        sync.Mutex

	ctx       context.Context
	cancel    context.CancelFunc
	running   atomic.Bool
	wg        sync.WaitGroup

	// Replication state
	clientXLogPos pglogrepl.LSN
	standbyMessageTimeout time.Duration
	nextStandbyMessageDeadline time.Time

	// Table information cache
	tableCache map[uint32]*TableInfo
	tableMu    sync.RWMutex
}

// TableInfo contains cached table information
type TableInfo struct {
	ID       uint32
	Schema   string
	Name     string
	Columns  []ColumnInfo
}

// ColumnInfo contains column information
type ColumnInfo struct {
	Name     string
	DataType uint32
	TypeName string
	Nullable bool
}

// NewPGStreamer creates a new PostgreSQL logical replication streamer
func NewPGStreamer(config *ServerConfig, logger *zap.Logger, eventChan chan<- *ChangeEvent) (*PGStreamer, error) {
	ctx, cancel := context.WithCancel(context.Background())

	s := &PGStreamer{
		config:                 config,
		logger:                 logger,
		eventChan:              eventChan,
		ctx:                    ctx,
		cancel:                 cancel,
		standbyMessageTimeout:  10 * time.Second,
		tableCache:             make(map[uint32]*TableInfo),
	}

	// Start the streamer
	if err := s.Start(); err != nil {
		cancel()
		return nil, err
	}

	return s, nil
}

// Start starts the PostgreSQL logical replication streamer
func (s *PGStreamer) Start() error {
	if !s.running.CompareAndSwap(false, true) {
		return fmt.Errorf("streamer already running")
	}

	// Connect to PostgreSQL with replication protocol
	if err := s.connect(); err != nil {
		s.running.Store(false)
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	// Start replication
	s.wg.Add(1)
	go s.streamLoop()

	s.logger.Info("PostgreSQL logical replication streamer started",
		zap.String("host", s.config.PGHost),
		zap.Int("port", s.config.PGPort),
		zap.String("database", s.config.PGDatabase),
		zap.String("slot", s.config.PGSlotName),
	)

	return nil
}

// Stop stops the streamer
func (s *PGStreamer) Stop() error {
	if !s.running.Load() {
		return nil
	}

	s.cancel()
	s.wg.Wait()

	s.mu.Lock()
	if s.conn != nil {
		s.conn.Close(context.Background())
		s.conn = nil
	}
	s.mu.Unlock()

	s.running.Store(false)
	s.logger.Info("PostgreSQL logical replication streamer stopped")
	return nil
}

// connect establishes a connection to PostgreSQL with replication protocol
func (s *PGStreamer) connect() error {
	connString := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s replication=database",
		s.config.PGHost,
		s.config.PGPort,
		s.config.PGDatabase,
		s.config.PGUser,
		s.config.PGPassword,
	)

	conn, err := pgconn.Connect(s.ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()

	// Create replication slot if not exists
	if err := s.ensureReplicationSlot(); err != nil {
		conn.Close(s.ctx)
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	// Create publication if not exists
	if err := s.ensurePublication(); err != nil {
		conn.Close(s.ctx)
		return fmt.Errorf("failed to create publication: %w", err)
	}

	return nil
}

// ensureReplicationSlot creates the replication slot if it doesn't exist
func (s *PGStreamer) ensureReplicationSlot() error {
	// Try to create the slot - it will fail if it already exists
	_, err := pglogrepl.CreateReplicationSlot(
		s.ctx,
		s.conn,
		s.config.PGSlotName,
		"pgoutput",
		pglogrepl.CreateReplicationSlotOptions{
			Temporary: false,
		},
	)
	if err != nil {
		// Check if slot already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			// Slot already exists, that's fine
			s.logger.Debug("Replication slot already exists", zap.String("slot", s.config.PGSlotName))
			return nil
		}
		return err
	}

	s.logger.Info("Created replication slot", zap.String("slot", s.config.PGSlotName))
	return nil
}

// ensurePublication creates the publication if it doesn't exist
func (s *PGStreamer) ensurePublication() error {
	// Create a regular connection to check/create publication
	connString := fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s",
		s.config.PGHost,
		s.config.PGPort,
		s.config.PGDatabase,
		s.config.PGUser,
		s.config.PGPassword,
	)

	conn, err := pgconn.Connect(s.ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect for publication: %w", err)
	}
	defer conn.Close(s.ctx)

	// Check if publication exists
	checkSQL := fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname = '%s'", s.config.PGPublicationName)
	result := conn.Exec(s.ctx, checkSQL)
	_, err = result.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check publication: %w", err)
	}

	// Create publication for all tables
	createSQL := fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", s.config.PGPublicationName)
	result = conn.Exec(s.ctx, createSQL)
	_, err = result.ReadAll()
	if err != nil {
		// Check if publication already exists
		if pgErr, ok := err.(*pgconn.PgError); ok && pgErr.Code == "42710" {
			s.logger.Debug("Publication already exists", zap.String("publication", s.config.PGPublicationName))
			return nil
		}
		return fmt.Errorf("failed to create publication: %w", err)
	}

	s.logger.Info("Created publication", zap.String("publication", s.config.PGPublicationName))
	return nil
}

// streamLoop is the main loop for streaming replication events
func (s *PGStreamer) streamLoop() {
	defer s.wg.Done()

	// Start replication
	err := pglogrepl.StartReplication(
		s.ctx,
		s.conn,
		s.config.PGSlotName,
		s.clientXLogPos,
		pglogrepl.StartReplicationOptions{
			PluginArgs: []string{
				"proto_version '1'",
				fmt.Sprintf("publication_names '%s'", s.config.PGPublicationName),
			},
		},
	)
	if err != nil {
		s.logger.Error("Failed to start replication", zap.Error(err))
		return
	}

	s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Send standby status update if needed
		if time.Now().After(s.nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(
				s.ctx,
				s.conn,
				pglogrepl.StandbyStatusUpdate{
					WALWritePosition: s.clientXLogPos,
				},
			)
			if err != nil {
				s.logger.Error("Failed to send standby status", zap.Error(err))
				return
			}
			s.nextStandbyMessageDeadline = time.Now().Add(s.standbyMessageTimeout)
		}

		// Receive message with timeout
		ctx, cancel := context.WithDeadline(s.ctx, s.nextStandbyMessageDeadline)
		rawMsg, err := s.conn.ReceiveMessage(ctx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			if s.ctx.Err() != nil {
				return
			}
			s.logger.Error("Failed to receive message", zap.Error(err))
			return
		}

		// Handle the message
		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			s.logger.Error("Received error from PostgreSQL",
				zap.String("severity", errMsg.Severity),
				zap.String("message", errMsg.Message),
			)
			continue
		}

		copyData, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			continue
		}

		switch copyData.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			s.handleKeepalive(copyData.Data[1:])

		case pglogrepl.XLogDataByteID:
			s.handleXLogData(copyData.Data[1:])
		}
	}
}

// handleKeepalive handles primary keepalive messages
func (s *PGStreamer) handleKeepalive(data []byte) {
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data)
	if err != nil {
		s.logger.Error("Failed to parse keepalive", zap.Error(err))
		return
	}

	if pkm.ReplyRequested {
		s.nextStandbyMessageDeadline = time.Time{}
	}
}

// handleXLogData handles XLog data messages
func (s *PGStreamer) handleXLogData(data []byte) {
	xld, err := pglogrepl.ParseXLogData(data)
	if err != nil {
		s.logger.Error("Failed to parse XLog data", zap.Error(err))
		return
	}

	// Parse logical replication message
	s.parseLogicalReplicationMessage(xld.WALData)

	// Update position
	s.clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
}

// parseLogicalReplicationMessage parses a logical replication message
func (s *PGStreamer) parseLogicalReplicationMessage(data []byte) {
	if len(data) == 0 {
		return
	}

	msgType := data[0]
	switch msgType {
	case 'R': // Relation
		s.handleRelationMessage(data)
	case 'B': // Begin
		s.handleBeginMessage(data)
	case 'C': // Commit
		s.handleCommitMessage(data)
	case 'I': // Insert
		s.handleInsertMessage(data)
	case 'U': // Update
		s.handleUpdateMessage(data)
	case 'D': // Delete
		s.handleDeleteMessage(data)
	case 'T': // Truncate
		// Handle truncate if needed
	case 'O': // Origin
		// Handle origin if needed
	case 'Y': // Type
		// Handle type if needed
	}
}

// handleRelationMessage handles relation (table) messages
func (s *PGStreamer) handleRelationMessage(data []byte) {
	rel, err := pglogrepl.Parse(data)
	if err != nil {
		s.logger.Error("Failed to parse relation message", zap.Error(err))
		return
	}

	relMsg, ok := rel.(*pglogrepl.RelationMessage)
	if !ok {
		return
	}

	// Cache table information
	tableInfo := &TableInfo{
		ID:      relMsg.RelationID,
		Schema:  relMsg.Namespace,
		Name:    relMsg.RelationName,
		Columns: make([]ColumnInfo, len(relMsg.Columns)),
	}

	for i, col := range relMsg.Columns {
		tableInfo.Columns[i] = ColumnInfo{
			Name:     col.Name,
			DataType: col.DataType,
		}
	}

	s.tableMu.Lock()
	s.tableCache[relMsg.RelationID] = tableInfo
	s.tableMu.Unlock()

	s.logger.Debug("Cached table info",
		zap.String("schema", tableInfo.Schema),
		zap.String("table", tableInfo.Name),
	)
}

// handleBeginMessage handles transaction begin messages
func (s *PGStreamer) handleBeginMessage(data []byte) {
	event := &ChangeEvent{
		Type:      ChangeTypeBegin,
		Timestamp: time.Now(),
	}
	s.sendEvent(event)
}

// handleCommitMessage handles transaction commit messages
func (s *PGStreamer) handleCommitMessage(data []byte) {
	event := &ChangeEvent{
		Type:      ChangeTypeCommit,
		Timestamp: time.Now(),
	}
	s.sendEvent(event)
}

// handleInsertMessage handles insert messages
func (s *PGStreamer) handleInsertMessage(data []byte) {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		s.logger.Error("Failed to parse insert message", zap.Error(err))
		return
	}

	insertMsg, ok := msg.(*pglogrepl.InsertMessage)
	if !ok {
		return
	}

	tableInfo := s.getTableInfo(insertMsg.RelationID)
	if tableInfo == nil {
		s.logger.Warn("Unknown table in insert", zap.Uint32("relation_id", insertMsg.RelationID))
		return
	}

	event := &ChangeEvent{
		Type:      ChangeTypeInsert,
		Timestamp: time.Now(),
		Schema:    tableInfo.Schema,
		Table:     tableInfo.Name,
		TableID:   uint64(insertMsg.RelationID),
		Columns:   s.convertColumns(tableInfo),
		NewValues: s.decodeTupleData(insertMsg.Tuple, tableInfo),
	}
	s.sendEvent(event)
}

// handleUpdateMessage handles update messages
func (s *PGStreamer) handleUpdateMessage(data []byte) {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		s.logger.Error("Failed to parse update message", zap.Error(err))
		return
	}

	updateMsg, ok := msg.(*pglogrepl.UpdateMessage)
	if !ok {
		return
	}

	tableInfo := s.getTableInfo(updateMsg.RelationID)
	if tableInfo == nil {
		s.logger.Warn("Unknown table in update", zap.Uint32("relation_id", updateMsg.RelationID))
		return
	}

	event := &ChangeEvent{
		Type:      ChangeTypeUpdate,
		Timestamp: time.Now(),
		Schema:    tableInfo.Schema,
		Table:     tableInfo.Name,
		TableID:   uint64(updateMsg.RelationID),
		Columns:   s.convertColumns(tableInfo),
		NewValues: s.decodeTupleData(updateMsg.NewTuple, tableInfo),
	}

	if updateMsg.OldTuple != nil {
		event.OldValues = s.decodeTupleData(updateMsg.OldTuple, tableInfo)
	}

	s.sendEvent(event)
}

// handleDeleteMessage handles delete messages
func (s *PGStreamer) handleDeleteMessage(data []byte) {
	msg, err := pglogrepl.Parse(data)
	if err != nil {
		s.logger.Error("Failed to parse delete message", zap.Error(err))
		return
	}

	deleteMsg, ok := msg.(*pglogrepl.DeleteMessage)
	if !ok {
		return
	}

	tableInfo := s.getTableInfo(deleteMsg.RelationID)
	if tableInfo == nil {
		s.logger.Warn("Unknown table in delete", zap.Uint32("relation_id", deleteMsg.RelationID))
		return
	}

	event := &ChangeEvent{
		Type:      ChangeTypeDelete,
		Timestamp: time.Now(),
		Schema:    tableInfo.Schema,
		Table:     tableInfo.Name,
		TableID:   uint64(deleteMsg.RelationID),
		Columns:   s.convertColumns(tableInfo),
		OldValues: s.decodeTupleData(deleteMsg.OldTuple, tableInfo),
	}
	s.sendEvent(event)
}

// getTableInfo retrieves cached table information
func (s *PGStreamer) getTableInfo(relationID uint32) *TableInfo {
	s.tableMu.RLock()
	defer s.tableMu.RUnlock()
	return s.tableCache[relationID]
}

// convertColumns converts table columns to the format expected by MySQL
func (s *PGStreamer) convertColumns(tableInfo *TableInfo) []Column {
	columns := make([]Column, len(tableInfo.Columns))
	for i, col := range tableInfo.Columns {
		columns[i] = Column{
			Name:     col.Name,
			Type:     s.pgTypeToMySQLType(col.DataType),
			Nullable: col.Nullable,
		}
	}
	return columns
}

// pgTypeToMySQLType converts PostgreSQL data types to MySQL types
func (s *PGStreamer) pgTypeToMySQLType(pgType uint32) uint8 {
	// Simplified mapping - would need full implementation
	// Reference: https://www.postgresql.org/docs/current/catalog-pg-type.html
	// Reference: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnType
	switch pgType {
	case 16: // bool
		return 1 // MYSQL_TYPE_TINY
	case 21: // int2
		return 2 // MYSQL_TYPE_SHORT
	case 23: // int4
		return 3 // MYSQL_TYPE_LONG
	case 20: // int8
		return 8 // MYSQL_TYPE_LONGLONG
	case 700: // float4
		return 4 // MYSQL_TYPE_FLOAT
	case 701: // float8
		return 5 // MYSQL_TYPE_DOUBLE
	case 1082: // date
		return 10 // MYSQL_TYPE_DATE
	case 1083: // time
		return 11 // MYSQL_TYPE_TIME
	case 1114, 1184: // timestamp, timestamptz
		return 12 // MYSQL_TYPE_DATETIME
	case 1700: // numeric
		return 246 // MYSQL_TYPE_NEWDECIMAL
	case 25, 1043: // text, varchar
		return 253 // MYSQL_TYPE_VAR_STRING
	default:
		return 253 // Default to VARCHAR
	}
}

// decodeTupleData decodes tuple data from PostgreSQL format
func (s *PGStreamer) decodeTupleData(tuple *pglogrepl.TupleData, tableInfo *TableInfo) []interface{} {
	if tuple == nil {
		return nil
	}

	values := make([]interface{}, len(tuple.Columns))
	for i, col := range tuple.Columns {
		switch col.DataType {
		case 'n': // null
			values[i] = nil
		case 'u': // unchanged
			values[i] = nil // or use a special marker
		case 't': // text
			values[i] = string(col.Data)
		default:
			values[i] = col.Data
		}
	}
	return values
}

// sendEvent sends an event to the event channel
func (s *PGStreamer) sendEvent(event *ChangeEvent) {
	select {
	case s.eventChan <- event:
	case <-s.ctx.Done():
	default:
		s.logger.Warn("Event channel full, dropping event")
	}
}
