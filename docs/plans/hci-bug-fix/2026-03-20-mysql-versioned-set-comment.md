# MySQL Versioned SET Comment Handling Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make aproxy recognize MySQL versioned comments that wrap `SET` statements so these session initialization commands are handled locally instead of being forwarded to PostgreSQL.

**Architecture:** Keep the fix narrowly scoped to the protocol routing path. Add a small normalization helper in `pkg/protocol/mysql` that unwraps only versioned `SET` comments, use the normalized SQL for statement classification and local `SET` handling, and preserve the existing `SET` parser in `pkg/mapper` unchanged except for regression coverage.

**Tech Stack:** Go 1.25, `testing`, `stretchr/testify`, existing `pkg/protocol/mysql` routing, existing `pkg/mapper` `SET` parser

---

### Task 1: Lock the normalization behavior with focused tests

**Files:**
- Create: `pkg/protocol/mysql/versioned_set_test.go`
- Reference: `pkg/protocol/mysql/handler.go:198-206`

**Step 1: Write the failing normalization tests**

Create `pkg/protocol/mysql/versioned_set_test.go` with table-driven coverage for the normalization helper.

Test cases should include:

```go
func TestNormalizeVersionedSetComment(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "plain set remains unchanged",
			input:    "SET NAMES utf8mb4",
			expected: "SET NAMES utf8mb4",
		},
		{
			name:     "versioned character_set_client unwraps",
			input:    "/*!40101 SET character_set_client = utf8 */",
			expected: "SET character_set_client = utf8",
		},
		{
			name:     "versioned set names unwraps",
			input:    "  /*!40101   SET NAMES utf8mb4 */ ",
			expected: "SET NAMES utf8mb4",
		},
		{
			name:     "non set versioned comment stays unchanged",
			input:    "/*!40101 CREATE TABLE t (id int) */",
			expected: "/*!40101 CREATE TABLE t (id int) */",
		},
		{
			name:     "malformed versioned comment stays trimmed",
			input:    "  /*!40101 SET autocommit = 1 ",
			expected: "/*!40101 SET autocommit = 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, normalizeVersionedSetComment(tt.input))
		})
	}
}
```

**Step 2: Run the targeted test to confirm it fails**

Run:

```powershell
go test .\pkg\protocol\mysql -run TestNormalizeVersionedSetComment -v
```

Expected before the fix:

- Build fails because `normalizeVersionedSetComment` does not exist yet.

**Step 3: Implement the minimal helper**

Create a new file `pkg/protocol/mysql/versioned_set.go` containing a narrow helper:

```go
func normalizeVersionedSetComment(sql string) string {
	trimmed := strings.TrimSpace(sql)
	if !strings.HasPrefix(trimmed, "/*!") || !strings.HasSuffix(trimmed, "*/") {
		return trimmed
	}

	body := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(trimmed, "/*!"), "*/"))
	if body == "" {
		return trimmed
	}

	versionEnd := 0
	for versionEnd < len(body) && body[versionEnd] >= '0' && body[versionEnd] <= '9' {
		versionEnd++
	}
	if versionEnd == 0 {
		return trimmed
	}

	inner := strings.TrimSpace(body[versionEnd:])
	if strings.HasPrefix(strings.ToUpper(inner), "SET ") {
		return inner
	}

	return trimmed
}
```

Keep the helper intentionally narrow. Do not add support for other versioned comment statement types.

**Step 4: Re-run the targeted test**

Run:

```powershell
go test .\pkg\protocol\mysql -run TestNormalizeVersionedSetComment -v
```

Expected:

- All helper cases pass.

### Task 2: Route normalized versioned `SET` statements through the existing local handler

**Files:**
- Modify: `pkg/protocol/mysql/handler.go:198-206`
- Test: `pkg/protocol/mysql/versioned_set_test.go`

**Step 1: Write a dispatch-focused failing test**

Add a second test in `pkg/protocol/mysql/versioned_set_test.go` that proves normalization produces a query string which the existing rewriter classifies as `SET`:

```go
func TestNormalizeVersionedSetComment_ProducesSetStatementForRouting(t *testing.T) {
	rewriter := sqlrewrite.NewRewriter(true)
	normalized := normalizeVersionedSetComment("/*!40101 SET character_set_client = utf8 */")

	assert.Equal(t, "SET character_set_client = utf8", normalized)
	assert.True(t, rewriter.IsSetStatement(normalized))
}
```

If importing `pkg/sqlrewrite` creates a package cycle, move this assertion to a handler-focused test file in `pkg/protocol/mysql` that checks the normalized string directly and keep the routing integration verification in Task 3.

**Step 2: Update the dispatch path**

In `pkg/protocol/mysql/handler.go`, normalize the incoming query once before statement classification:

```go
normalizedQuery := normalizeVersionedSetComment(query)

if ch.handler.rewriter.IsShowStatement(normalizedQuery) {
	return ch.handleShowCommand(ctx, normalizedQuery)
}

if ch.handler.rewriter.IsSetStatement(normalizedQuery) {
	return ch.handleSetCommand(ctx, normalizedQuery)
}

if ch.handler.rewriter.IsUseStatement(normalizedQuery) {
	return ch.handleUseCommand(ctx, normalizedQuery)
}
```

Do not overwrite the original `query` variable globally. Limit normalization to the dispatch decisions and downstream handler input.

**Step 3: Run the protocol package tests**

Run:

```powershell
go test .\pkg\protocol\mysql -v
```

Expected:

- The new normalization tests pass.
- Existing `pkg/protocol/mysql` tests continue to pass.

### Task 3: Confirm existing `SET` parsing still works with unwrapped SQL

**Files:**
- Modify: `pkg/mapper/show_test.go:11-80`
- Reference: `pkg/mapper/show.go:459-506`

**Step 1: Add regression tests for unwrapped session assignments**

Extend `pkg/mapper/show_test.go` with coverage that mirrors the unwrapped versioned inputs the protocol layer will now supply:

```go
func TestShowEmulatorHandleSetCommand_VersionedSetEquivalentAssignments(t *testing.T) {
	se := NewShowEmulator()
	testCases := []struct {
		name         string
		sql          string
		expectedKey  string
		expectedValue string
	}{
		{
			name:          "character_set_client assignment",
			sql:           "SET character_set_client = utf8",
			expectedKey:   "character_set_client",
			expectedValue: "utf8",
		},
		{
			name:          "user variable assignment",
			sql:           "SET @saved_cs_client = utf8",
			expectedKey:   "@saved_cs_client",
			expectedValue: "utf8",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sessionVars := make(map[string]interface{})
			require.NoError(t, se.HandleSetCommand(context.Background(), tc.sql, sessionVars))
			assert.Equal(t, tc.expectedValue, sessionVars[tc.expectedKey])
		})
	}
}
```

**Step 2: Run the focused mapper tests**

Run:

```powershell
go test .\pkg\mapper -run TestShowEmulatorHandleSetCommand -v
```

Expected:

- Existing `SET NAMES` tests still pass.
- New assignment regression tests pass.

### Task 4: Run cross-package verification and inspect the diff

**Files:**
- Verify: `pkg/protocol/mysql/versioned_set.go`
- Verify: `pkg/protocol/mysql/versioned_set_test.go`
- Verify: `pkg/protocol/mysql/handler.go`
- Verify: `pkg/mapper/show_test.go`

**Step 1: Run the combined targeted verification**

Run:

```powershell
go test .\pkg\protocol\mysql .\pkg\mapper -v
```

Expected:

- Both packages pass.
- No unrelated failures appear in the targeted scope.

**Step 2: Inspect the final diff**

Run:

```powershell
git --no-pager diff -- pkg/protocol/mysql/handler.go pkg/protocol/mysql/versioned_set.go pkg/protocol/mysql/versioned_set_test.go pkg/mapper/show_test.go
```

Expected:

- Only the narrow versioned `SET` normalization logic and associated tests changed.

**Step 3: Optional end-to-end check with the reproducing client or dump flow**

Replay the client initialization or import scenario that originally produced:

```sql
/*!40101 SET character_set_client = utf8 */
```

Expected:

- The statement is consumed locally.
- PostgreSQL no longer reports syntax errors for this initialization command.