package replication

import (
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestDefaultServerConfig(t *testing.T) {
	config := DefaultServerConfig()

	if config.Enabled != false {
		t.Errorf("Expected Enabled=false, got %v", config.Enabled)
	}
	if config.ServerID != 1 {
		t.Errorf("Expected ServerID=1, got %d", config.ServerID)
	}
	if config.PGHost != "localhost" {
		t.Errorf("Expected PGHost=localhost, got %s", config.PGHost)
	}
	if config.PGPort != 5432 {
		t.Errorf("Expected PGPort=5432, got %d", config.PGPort)
	}
	if config.BinlogFilename != "mysql-bin.000001" {
		t.Errorf("Expected BinlogFilename=mysql-bin.000001, got %s", config.BinlogFilename)
	}
	if config.BinlogPosition != 4 {
		t.Errorf("Expected BinlogPosition=4, got %d", config.BinlogPosition)
	}
}

func TestGenerateUUID(t *testing.T) {
	uuid1 := generateUUID()
	uuid2 := generateUUID()

	// UUIDs should be non-empty
	if uuid1 == "" {
		t.Error("Expected non-empty UUID")
	}

	// UUIDs should have proper format (8-4-4-4-12)
	parts := countHyphens(uuid1)
	if parts != 4 {
		t.Errorf("Expected UUID with 4 hyphens, got %d", parts)
	}

	// UUIDs should be unique (technically could collide but very unlikely)
	if uuid1 == uuid2 {
		t.Error("Expected different UUIDs")
	}
}

func countHyphens(s string) int {
	count := 0
	for _, c := range s {
		if c == '-' {
			count++
		}
	}
	return count
}

func TestConvertToMySQLEvents(t *testing.T) {
	config := &ServerConfig{
		Enabled:  true,
		ServerID: 1,
	}

	// Create a server manually without starting it
	s := &Server{
		config:   config,
		serverID: config.ServerID,
		position: mysql.Position{
			Name: "binlog.000001",
			Pos:  4,
		},
	}

	tests := []struct {
		name          string
		event         *ChangeEvent
		expectedTypes []replication.EventType
	}{
		{
			name: "BEGIN event",
			event: &ChangeEvent{
				Type:      ChangeTypeBegin,
				Timestamp: time.Now(),
			},
			expectedTypes: []replication.EventType{replication.QUERY_EVENT},
		},
		{
			name: "COMMIT event",
			event: &ChangeEvent{
				Type:      ChangeTypeCommit,
				Timestamp: time.Now(),
			},
			expectedTypes: []replication.EventType{replication.XID_EVENT},
		},
		{
			name: "INSERT event",
			event: &ChangeEvent{
				Type:      ChangeTypeInsert,
				Timestamp: time.Now(),
				Schema:    "public",
				Table:     "users",
				TableID:   1,
				Columns: []Column{
					{Name: "id", Type: 8, Nullable: false},
					{Name: "name", Type: 253, Nullable: true},
				},
				NewValues: []interface{}{1, "test"},
			},
			expectedTypes: []replication.EventType{
				replication.TABLE_MAP_EVENT,
				replication.WRITE_ROWS_EVENTv2,
			},
		},
		{
			name: "UPDATE event",
			event: &ChangeEvent{
				Type:      ChangeTypeUpdate,
				Timestamp: time.Now(),
				Schema:    "public",
				Table:     "users",
				TableID:   1,
				Columns: []Column{
					{Name: "id", Type: 8, Nullable: false},
					{Name: "name", Type: 253, Nullable: true},
				},
				OldValues: []interface{}{1, "old"},
				NewValues: []interface{}{1, "new"},
			},
			expectedTypes: []replication.EventType{
				replication.TABLE_MAP_EVENT,
				replication.UPDATE_ROWS_EVENTv2,
			},
		},
		{
			name: "DELETE event",
			event: &ChangeEvent{
				Type:      ChangeTypeDelete,
				Timestamp: time.Now(),
				Schema:    "public",
				Table:     "users",
				TableID:   1,
				Columns: []Column{
					{Name: "id", Type: 8, Nullable: false},
					{Name: "name", Type: 253, Nullable: true},
				},
				OldValues: []interface{}{1, "test"},
			},
			expectedTypes: []replication.EventType{
				replication.TABLE_MAP_EVENT,
				replication.DELETE_ROWS_EVENTv2,
			},
		},
		{
			name: "DDL event",
			event: &ChangeEvent{
				Type:      ChangeTypeDDL,
				Timestamp: time.Now(),
			},
			expectedTypes: []replication.EventType{replication.QUERY_EVENT},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			events := s.convertToMySQLEvents(tt.event)

			if len(events) != len(tt.expectedTypes) {
				t.Errorf("Expected %d events, got %d", len(tt.expectedTypes), len(events))
				return
			}

			for i, expectedType := range tt.expectedTypes {
				if events[i].Header.EventType != expectedType {
					t.Errorf("Event %d: expected type %v, got %v",
						i, expectedType, events[i].Header.EventType)
				}
			}
		})
	}
}

func TestCreateQueryEvent(t *testing.T) {
	s := &Server{
		serverID: 1,
		position: mysql.Position{
			Name: "binlog.000001",
			Pos:  100,
		},
	}

	event := s.createQueryEvent("BEGIN")

	if event.Header.EventType != replication.QUERY_EVENT {
		t.Errorf("Expected QUERY_EVENT, got %v", event.Header.EventType)
	}

	if event.Header.ServerID != 1 {
		t.Errorf("Expected ServerID=1, got %d", event.Header.ServerID)
	}

	queryEvent, ok := event.Event.(*replication.QueryEvent)
	if !ok {
		t.Fatal("Expected QueryEvent type")
	}

	if string(queryEvent.Query) != "BEGIN" {
		t.Errorf("Expected query 'BEGIN', got '%s'", string(queryEvent.Query))
	}
}

func TestCreateXIDEvent(t *testing.T) {
	s := &Server{
		serverID: 1,
		position: mysql.Position{
			Name: "binlog.000001",
			Pos:  100,
		},
	}

	event := s.createXIDEvent()

	if event.Header.EventType != replication.XID_EVENT {
		t.Errorf("Expected XID_EVENT, got %v", event.Header.EventType)
	}

	if event.Header.ServerID != 1 {
		t.Errorf("Expected ServerID=1, got %d", event.Header.ServerID)
	}

	xidEvent, ok := event.Event.(*replication.XIDEvent)
	if !ok {
		t.Fatal("Expected XIDEvent type")
	}

	if xidEvent.XID == 0 {
		t.Error("Expected non-zero XID")
	}
}

func TestCreateTableMapEvent(t *testing.T) {
	s := &Server{
		serverID: 1,
		position: mysql.Position{
			Name: "binlog.000001",
			Pos:  100,
		},
	}

	changeEvent := &ChangeEvent{
		Schema:  "public",
		Table:   "users",
		TableID: 42,
		Columns: []Column{
			{Name: "id", Type: 8, Nullable: false},
			{Name: "name", Type: 253, Nullable: true},
		},
	}

	event := s.createTableMapEvent(changeEvent)

	if event.Header.EventType != replication.TABLE_MAP_EVENT {
		t.Errorf("Expected TABLE_MAP_EVENT, got %v", event.Header.EventType)
	}

	tableMapEvent, ok := event.Event.(*replication.TableMapEvent)
	if !ok {
		t.Fatal("Expected TableMapEvent type")
	}

	if string(tableMapEvent.Schema) != "public" {
		t.Errorf("Expected schema 'public', got '%s'", string(tableMapEvent.Schema))
	}

	if string(tableMapEvent.Table) != "users" {
		t.Errorf("Expected table 'users', got '%s'", string(tableMapEvent.Table))
	}

	if tableMapEvent.TableID != 42 {
		t.Errorf("Expected TableID=42, got %d", tableMapEvent.TableID)
	}

	if tableMapEvent.ColumnCount != 2 {
		t.Errorf("Expected ColumnCount=2, got %d", tableMapEvent.ColumnCount)
	}
}

func TestCreateRowsEvent(t *testing.T) {
	s := &Server{
		serverID: 1,
		position: mysql.Position{
			Name: "binlog.000001",
			Pos:  100,
		},
	}

	tests := []struct {
		name         string
		changeType   ChangeType
		expectedType replication.EventType
	}{
		{
			name:         "INSERT",
			changeType:   ChangeTypeInsert,
			expectedType: replication.WRITE_ROWS_EVENTv2,
		},
		{
			name:         "UPDATE",
			changeType:   ChangeTypeUpdate,
			expectedType: replication.UPDATE_ROWS_EVENTv2,
		},
		{
			name:         "DELETE",
			changeType:   ChangeTypeDelete,
			expectedType: replication.DELETE_ROWS_EVENTv2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changeEvent := &ChangeEvent{
				Type:      tt.changeType,
				TableID:   42,
				OldValues: []interface{}{1, "old"},
				NewValues: []interface{}{1, "new"},
			}

			event := s.createRowsEvent(changeEvent)

			if event.Header.EventType != tt.expectedType {
				t.Errorf("Expected %v, got %v", tt.expectedType, event.Header.EventType)
			}

			rowsEvent, ok := event.Event.(*replication.RowsEvent)
			if !ok {
				t.Fatal("Expected RowsEvent type")
			}

			if rowsEvent.TableID != 42 {
				t.Errorf("Expected TableID=42, got %d", rowsEvent.TableID)
			}
		})
	}
}

func TestEncodeBinlogEvent(t *testing.T) {
	// Test with RawData
	eventWithRawData := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.QUERY_EVENT,
			ServerID:  1,
			LogPos:    100,
		},
		RawData: []byte("raw data test"),
	}

	data, err := encodeBinlogEvent(eventWithRawData)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if string(data) != "raw data test" {
		t.Errorf("Expected raw data, got %s", string(data))
	}

	// Test without RawData (should return header)
	eventWithoutRawData := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			Timestamp: uint32(time.Now().Unix()),
			EventType: replication.QUERY_EVENT,
			ServerID:  1,
			LogPos:    100,
			Flags:     0,
		},
		RawData: nil,
	}

	data, err = encodeBinlogEvent(eventWithoutRawData)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if len(data) != 19 { // Standard header size
		t.Errorf("Expected 19 byte header, got %d bytes", len(data))
	}
}

func TestChangeType(t *testing.T) {
	tests := []struct {
		changeType ChangeType
		expected   ChangeType
	}{
		{ChangeTypeInsert, ChangeType(0)},
		{ChangeTypeUpdate, ChangeType(1)},
		{ChangeTypeDelete, ChangeType(2)},
		{ChangeTypeBegin, ChangeType(3)},
		{ChangeTypeCommit, ChangeType(4)},
		{ChangeTypeDDL, ChangeType(5)},
	}

	for _, tt := range tests {
		if tt.changeType != tt.expected {
			t.Errorf("Expected %d, got %d", tt.expected, tt.changeType)
		}
	}
}

func TestDumpClientSequenceNumber(t *testing.T) {
	client := &DumpClient{
		seqNum: 1,
	}

	// Verify initial sequence number
	if client.seqNum != 1 {
		t.Errorf("Expected seqNum=1, got %d", client.seqNum)
	}

	// Simulate sequence number increment
	client.mu.Lock()
	seq := client.seqNum
	client.seqNum++
	client.mu.Unlock()

	if seq != 1 {
		t.Errorf("Expected seq=1, got %d", seq)
	}

	if client.seqNum != 2 {
		t.Errorf("Expected seqNum=2 after increment, got %d", client.seqNum)
	}
}
