# Create Table Dedicated DDL Generator Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Route MySQL `CREATE TABLE` statements through a dedicated PostgreSQL DDL generator that drops MySQL-only table options, preserves table and column comments via `COMMENT ON ...`, and keeps the public rewrite API unchanged.

**Architecture:** Keep parsing and AST normalization in the existing `ASTRewriter` and `ASTVisitor`, but stop using generic restore plus `PostProcess()` as the primary `CREATE TABLE` conversion path. Introduce a dedicated generator file in `pkg/sqlrewrite` that extracts table metadata from `*ast.CreateTableStmt`, emits one clean PostgreSQL `CREATE TABLE`, appends trailing `COMMENT ON TABLE` and `COMMENT ON COLUMN` statements, and fails closed on unsupported structured options.

**Tech Stack:** Go 1.25.3, TiDB parser AST (`github.com/pingcap/tidb/pkg/parser/ast`), `testing`, `stretchr/testify`, existing `pkg/sqlrewrite` rewrite pipeline

---

### Task 1: Lock the dedicated `CREATE TABLE` contract with failing generator tests

**Files:**
- Create: `pkg/sqlrewrite/create_table_generator_test.go`
- Reference: `pkg/sqlrewrite/pg_generator.go:13-146`
- Reference: `pkg/sqlrewrite/ast_visitor.go:291-349`

**Step 1: Write the failing generator tests**

Create `pkg/sqlrewrite/create_table_generator_test.go` with a parser helper and focused tests against `PGGenerator.ConvertCreateTable()`.

```go
package sqlrewrite

import (
	"testing"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func parseCreateTableStmt(t *testing.T, sql string) *ast.CreateTableStmt {
	t.Helper()

	p := parser.New()
	stmts, _, err := p.Parse(sql, "", "")
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	stmt, ok := stmts[0].(*ast.CreateTableStmt)
	require.True(t, ok, "expected CreateTableStmt")
	return stmt
}

func TestPGGenerator_ConvertCreateTable_PreservesCommentsAsTrailingStatements(t *testing.T) {
	g := NewPGGenerator()
	stmt := parseCreateTableStmt(t, `CREATE TABLE \
\	\	\	\	\	\	\	\	\	\	\	\	\	t_vm_sdk_snap_cluster_count (\
\	\	\	\	\	\	\	\	\	\	\	\	\	id bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键ID',\
\	\	\	\	\	\	\	\	\	\	\	\	\	start_time int(10) NOT NULL COMMENT '开始时间戳',\
\	\	\	\	\	\	\	\	\	\	\	\	\	updated_at datetime DEFAULT NULL COMMENT '更新时间',\
\	\	\	\	\	\	\	\	\	\	\	\	\	PRIMARY KEY (id)\
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='sdk集群数量表'`)

	result, err := g.ConvertCreateTable(stmt)
	require.NoError(t, err)
	assert.Contains(t, result, `CREATE TABLE "t_vm_sdk_snap_cluster_count"`)
	assert.Contains(t, result, `COMMENT ON TABLE "t_vm_sdk_snap_cluster_count" IS 'sdk集群数量表'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t_vm_sdk_snap_cluster_count"."id" IS '自增主键ID'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t_vm_sdk_snap_cluster_count"."updated_at" IS '更新时间'`)
}

func TestPGGenerator_ConvertCreateTable_DropsMySQLOnlyTableOptions(t *testing.T) {
	g := NewPGGenerator()
	stmt := parseCreateTableStmt(t, `CREATE TABLE t1 (id bigint unsigned NOT NULL COMMENT 'id', payload text COLLATE utf8mb4_bin NOT NULL COMMENT '正文', PRIMARY KEY (id)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='表注释'`)

	result, err := g.ConvertCreateTable(stmt)
	require.NoError(t, err)
	assert.NotContains(t, result, `ENGINE=`)
	assert.NotContains(t, result, `CHARSET`)
	assert.NotContains(t, result, `COLLATE=utf8mb4_bin`)
	assert.NotContains(t, result, `ROW_FORMAT`)
	assert.NotContains(t, result, `COMMENT '正文'`)
	assert.NotContains(t, result, `COMMENT='表注释'`)
	assert.Contains(t, result, `COMMENT ON TABLE "t1" IS '表注释'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t1"."payload" IS '正文'`)
}

func TestPGGenerator_ConvertCreateTable_EscapesCommentLiterals(t *testing.T) {
	g := NewPGGenerator()
	stmt := parseCreateTableStmt(t, `CREATE TABLE t2 (name varchar(32) COMMENT 'O''Brien') COMMENT='owner''s table'`)

	result, err := g.ConvertCreateTable(stmt)
	require.NoError(t, err)
	assert.Contains(t, result, `COMMENT ON TABLE "t2" IS 'owner''s table'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t2"."name" IS 'O''Brien'`)
}
```

**Step 2: Run the targeted tests to verify they fail**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestPGGenerator_ConvertCreateTable -v
```

Expected before the fix:

- `TestPGGenerator_ConvertCreateTable_PreservesCommentsAsTrailingStatements` fails because the current implementation only returns basic restored SQL
- `TestPGGenerator_ConvertCreateTable_DropsMySQLOnlyTableOptions` fails because `ROW_FORMAT`, inline comments, or charset/collation fragments leak through

**Step 3: Commit the red checkpoint**

```powershell
git add pkg\sqlrewrite\create_table_generator_test.go
git commit -m "test: lock create table dedicated generator contract"
```

### Task 2: Add the internal result model and quoting helpers

**Files:**
- Create: `pkg/sqlrewrite/create_table_generator.go`
- Modify: `pkg/sqlrewrite/pg_generator.go:13-146`
- Test: `pkg/sqlrewrite/create_table_generator_test.go`

**Step 1: Add helper-focused failing tests**

Extend `pkg/sqlrewrite/create_table_generator_test.go` with small helper tests before writing the implementation.

```go
func TestJoinDDLStatements(t *testing.T) {
	result := joinDDLStatements(
		`CREATE TABLE "t1" ("id" BIGINT PRIMARY KEY)`,
		`COMMENT ON TABLE "t1" IS 'hello'`,
		`COMMENT ON COLUMN "t1"."id" IS 'identifier'`,
	)

	assert.Equal(t, "CREATE TABLE \"t1\" (\"id\" BIGINT PRIMARY KEY);\nCOMMENT ON TABLE \"t1\" IS 'hello';\nCOMMENT ON COLUMN \"t1\".\"id\" IS 'identifier';", result)
}

func TestEscapePGLiteral(t *testing.T) {
	assert.Equal(t, "owner''s table", escapePGLiteral("owner's table"))
}
```

**Step 2: Run the helper tests to verify they fail**

Run:

```powershell
go test .\pkg\sqlrewrite -run "TestJoinDDLStatements|TestEscapePGLiteral" -v
```

Expected before the fix:

- build fails because the helper functions do not exist yet

**Step 3: Write the minimal helper implementation**

Create `pkg/sqlrewrite/create_table_generator.go` and add the internal result type plus shared helpers.

```go
package sqlrewrite

import "strings"

type createTableRewriteResult struct {
	createTableSQL string
	trailingSQLs   []string
}

func (r createTableRewriteResult) SQL() string {
	parts := make([]string, 0, 1+len(r.trailingSQLs))
	parts = append(parts, r.createTableSQL)
	parts = append(parts, r.trailingSQLs...)
	return joinDDLStatements(parts...)
}

func joinDDLStatements(parts ...string) string {
	clean := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		clean = append(clean, strings.TrimSuffix(part, ";")+";")
	}
	return strings.Join(clean, "\n")
}

func escapePGLiteral(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
```

Update `pkg/sqlrewrite/pg_generator.go` so `ConvertCreateTable()` delegates to a helper in the new file instead of returning basic restore output directly.

```go
func (g *PGGenerator) ConvertCreateTable(node *ast.CreateTableStmt) (string, error) {
	result, err := g.buildCreateTable(node)
	if err != nil {
		return "", err
	}
	return result.SQL(), nil
}
```

**Step 4: Re-run the helper tests**

Run:

```powershell
go test .\pkg\sqlrewrite -run "TestJoinDDLStatements|TestEscapePGLiteral" -v
```

Expected:

- helper tests pass
- `TestPGGenerator_ConvertCreateTable_*` still fail because `buildCreateTable()` is not implemented yet

**Step 5: Commit the helper checkpoint**

```powershell
git add pkg\sqlrewrite\create_table_generator.go pkg\sqlrewrite\create_table_generator_test.go pkg\sqlrewrite\pg_generator.go
git commit -m "refactor: add create table ddl result helpers"
```

### Task 3: Implement AST-driven comment extraction and table option filtering

**Files:**
- Modify: `pkg/sqlrewrite/create_table_generator.go`
- Modify: `pkg/sqlrewrite/pg_generator.go:177-223`
- Test: `pkg/sqlrewrite/create_table_generator_test.go`

**Step 1: Add one more failing test for fail-closed behavior**

Extend `pkg/sqlrewrite/create_table_generator_test.go` with a negative test using an unknown table option type so the dedicated path cannot silently pass unsupported structured input.

```go
func TestPGGenerator_ConvertCreateTable_RejectsUnknownTableOption(t *testing.T) {
	g := NewPGGenerator()
	stmt := parseCreateTableStmt(t, `CREATE TABLE t3 (id bigint PRIMARY KEY)`)
	stmt.Options = append(stmt.Options, &ast.TableOption{Tp: ast.TableOptionType(99999)})

	_, err := g.ConvertCreateTable(stmt)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported CREATE TABLE option")
}
```

**Step 2: Run the dedicated generator tests to verify the new failure**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestPGGenerator_ConvertCreateTable -v
```

Expected before the implementation:

- comment preservation tests still fail
- unknown option test fails because the current path does not classify options structurally

**Step 3: Implement the minimal AST-based generator**

In `pkg/sqlrewrite/create_table_generator.go`, implement `buildCreateTable()` using the AST rather than string scanning.

Use this structure:

```go
type tableCommentTarget struct {
	tableName   string
	columnName  string
	commentText string
}

func (g *PGGenerator) buildCreateTable(node *ast.CreateTableStmt) (createTableRewriteResult, error) {
	if node == nil {
		return createTableRewriteResult{}, fmt.Errorf("nil CreateTableStmt")
	}

	tableName := g.restoreTableName(node.Table)
	tableComment, keptTableOptions, err := g.extractTableCommentAndFilterOptions(node.Options)
	if err != nil {
		return createTableRewriteResult{}, err
	}
	node.Options = keptTableOptions

	columnComments, err := g.extractColumnComments(node, tableName)
	if err != nil {
		return createTableRewriteResult{}, err
	}

	createSQL, err := g.Generate(node)
	if err != nil {
		return createTableRewriteResult{}, err
	}
	createSQL = g.postProcessCreateTableBase(createSQL)

	trailing := make([]string, 0, 1+len(columnComments))
	if tableComment != "" {
		trailing = append(trailing, fmt.Sprintf("COMMENT ON TABLE %s IS '%s'", tableName, escapePGLiteral(tableComment)))
	}
	for _, columnComment := range columnComments {
		trailing = append(trailing, fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnComment.columnName, escapePGLiteral(columnComment.commentText)))
	}

	return createTableRewriteResult{
		createTableSQL: createSQL,
		trailingSQLs:   trailing,
	}, nil
}
```

Implement three focused helpers instead of one giant function:

- `extractTableCommentAndFilterOptions(options []*ast.TableOption) (comment string, kept []*ast.TableOption, err error)`
- `extractColumnComments(node *ast.CreateTableStmt, tableName string) ([]tableCommentTarget, error)`
- `postProcessCreateTableBase(sql string) string`

`extractTableCommentAndFilterOptions` should:

- preserve `TableOptionComment`
- silently drop `TableOptionEngine`, `TableOptionCharset`, `TableOptionCollate`, `TableOptionCheckSum`, `TableOptionCompression`, `TableOptionKeyBlockSize`, `TableOptionDelayKeyWrite`, `TableOptionRowFormat`, `TableOptionStatsPersistent`, `TableOptionStatsAutoRecalc`, `TableOptionPackKeys`, `TableOptionStatsSamplePages`
- return an error for any default case

`extractColumnComments` should:

- scan `col.Options`
- pull out `ast.ColumnOptionComment`
- keep the remaining options untouched
- mutate `col.Options` to the filtered slice before generation

`postProcessCreateTableBase` should reuse only the safe normalization steps needed for base DDL, for example:

```go
func (g *PGGenerator) postProcessCreateTableBase(sql string) string {
	sql = strings.ReplaceAll(sql, "`", "\"")
	sql = g.convertTypes(sql)
	sql = strings.ReplaceAll(sql, " SIGNED", "")
	sql = strings.ReplaceAll(sql, "CURRENT_TIMESTAMP()", "CURRENT_TIMESTAMP")
	sql = strings.ReplaceAll(sql, "CURRENT_DATE()", "CURRENT_DATE")
	sql = strings.ReplaceAll(sql, "CURRENT_TIME()", "CURRENT_TIME")
	sql = g.convertAutoIncrement(sql)
	sql = g.removeUnsupportedTypeLengths(sql)
	sql = strings.ReplaceAll(sql, " ZEROFILL", "")
	return sql
}
```

Do not call `removeTableOptions()` from the dedicated path.

**Step 4: Re-run the dedicated generator tests**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestPGGenerator_ConvertCreateTable -v
```

Expected:

- all direct generator tests pass
- output contains trailing `COMMENT ON ...` statements
- no leaked `ROW_FORMAT`, `ENGINE`, or inline comment syntax remains

**Step 5: Commit the dedicated generator implementation**

```powershell
git add pkg\sqlrewrite\create_table_generator.go pkg\sqlrewrite\create_table_generator_test.go pkg\sqlrewrite\pg_generator.go
git commit -m "feat: add dedicated create table ddl generator"
```

### Task 4: Route `CreateTableStmt` through the dedicated generator and lock the public rewrite path

**Files:**
- Modify: `pkg/sqlrewrite/ast_rewriter_v3.go:1-81`
- Create: `pkg/sqlrewrite/create_table_rewriter_test.go`
- Reference: `pkg/sqlrewrite/rewriter.go:44-75`

**Step 1: Write the failing rewrite-path tests**

Create `pkg/sqlrewrite/create_table_rewriter_test.go` with end-to-end assertions through `ASTRewriter.Rewrite()` and `Rewriter.Rewrite()`.

```go
package sqlrewrite

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestASTRewriter_CreateTable_UsesDedicatedGenerator(t *testing.T) {
	rewriter := NewASTRewriter()
	input := `CREATE TABLE t_vm_sdk_snap_cluster_count (
		id int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增主键ID',
		start_time int(10) NOT NULL COMMENT '开始时间戳',
		end_time int(10) DEFAULT NULL COMMENT '结束时间戳',
		snap_count int(10) NOT NULL COMMENT '快照数量',
		created_at datetime NOT NULL COMMENT '创建时间',
		updated_at datetime DEFAULT NULL COMMENT '更新时间',
		PRIMARY KEY (id)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC COMMENT='sdk集群数量表'`

	result, err := rewriter.Rewrite(input)
	require.NoError(t, err)
	assert.Contains(t, result, `CREATE TABLE "t_vm_sdk_snap_cluster_count"`)
	assert.Contains(t, result, `COMMENT ON TABLE "t_vm_sdk_snap_cluster_count" IS 'sdk集群数量表'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t_vm_sdk_snap_cluster_count"."id" IS '自增主键ID'`)
	assert.NotContains(t, result, `ROW_FORMAT`)
	assert.NotContains(t, result, `ENGINE=`)
	assert.NotContains(t, result, `DEFAULT CHARSET`)
	assert.NotContains(t, result, `COLLATE=utf8mb4_bin`)
	assert.NotContains(t, result, `COMMENT='sdk集群数量表'`)
}

func TestRewriter_CreateTable_PreservesCommentsThroughPublicAPI(t *testing.T) {
	publicRewriter := NewRewriter(true)
	result, err := publicRewriter.Rewrite(`CREATE TABLE t1 (id bigint unsigned NOT NULL COMMENT '主键', PRIMARY KEY (id)) COMMENT='表说明'`)
	require.NoError(t, err)
	assert.Contains(t, result, `COMMENT ON TABLE "t1" IS '表说明'`)
	assert.Contains(t, result, `COMMENT ON COLUMN "t1"."id" IS '主键'`)
	assert.NotContains(t, result, `COMMENT='表说明'`)
}
```

**Step 2: Run the rewrite-path tests to verify they fail**

Run:

```powershell
go test .\pkg\sqlrewrite -run "TestASTRewriter_CreateTable|TestRewriter_CreateTable" -v
```

Expected before the dispatch change:

- tests fail because `ASTRewriter.Rewrite()` still sends `CreateTableStmt` through the generic generator + `PostProcess()` path

**Step 3: Implement the dispatch change in `ASTRewriter.Rewrite()`**

Update `pkg/sqlrewrite/ast_rewriter_v3.go` to special-case `*ast.CreateTableStmt`.

```go
import (
	"fmt"
	"strings"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
)

// ...

	switch createStmt := stmt.(type) {
	case *ast.CreateTableStmt:
		pgSQL, err := r.generator.ConvertCreateTable(createStmt)
		if err != nil {
			return "", fmt.Errorf("CREATE TABLE generation failed: %w", err)
		}
		return pgSQL, nil
	default:
		pgSQL, paramCount, err := r.generator.GenerateWithPlaceholders(stmt)
		if err != nil {
			return "", fmt.Errorf("SQL generation failed: %w", err)
		}
		pgSQL = r.generator.PostProcess(pgSQL)
		_ = paramCount
		return pgSQL, nil
	}
```

Do not silently fall back to the old `CREATE TABLE` post-processing path from inside this switch.

**Step 4: Re-run the rewrite-path tests**

Run:

```powershell
go test .\pkg\sqlrewrite -run "TestASTRewriter_CreateTable|TestRewriter_CreateTable" -v
```

Expected:

- the end-to-end `CREATE TABLE` tests pass
- the reported `ROW_FORMAT` case now returns PostgreSQL-compatible output with trailing comments

**Step 5: Commit the routing change**

```powershell
git add pkg\sqlrewrite\ast_rewriter_v3.go pkg\sqlrewrite\create_table_rewriter_test.go
git commit -m "feat: route create table through dedicated ddl generator"
```

### Task 5: Verify fail-closed behavior and run the package regression suite

**Files:**
- Modify: `pkg/sqlrewrite/create_table_generator_test.go`
- Modify: `pkg/sqlrewrite/create_table_rewriter_test.go`
- Reference: `pkg/sqlrewrite/rewriter.go:44-75`

**Step 1: Add one final regression for multi-statement shape**

Extend `pkg/sqlrewrite/create_table_rewriter_test.go` to make the final outward-facing format explicit.

```go
func TestASTRewriter_CreateTable_EmitsCreateBeforeComments(t *testing.T) {
	rewriter := NewASTRewriter()
	result, err := rewriter.Rewrite(`CREATE TABLE t_ordered (id bigint COMMENT '主键') COMMENT='表注释'`)
	require.NoError(t, err)

	createIdx := strings.Index(result, `CREATE TABLE "t_ordered"`)
	tableCommentIdx := strings.Index(result, `COMMENT ON TABLE "t_ordered" IS '表注释'`)
	columnCommentIdx := strings.Index(result, `COMMENT ON COLUMN "t_ordered"."id" IS '主键'`)

	assert.NotEqual(t, -1, createIdx)
	assert.NotEqual(t, -1, tableCommentIdx)
	assert.NotEqual(t, -1, columnCommentIdx)
	assert.Less(t, createIdx, tableCommentIdx)
	assert.Less(t, tableCommentIdx, columnCommentIdx)
}
```

Remember to add `strings` to the test file imports.

**Step 2: Run the focused regression suite**

Run:

```powershell
go test .\pkg\sqlrewrite -run "TestPGGenerator_ConvertCreateTable|TestASTRewriter_CreateTable|TestRewriter_CreateTable" -v -count=1
```

Expected:

- all dedicated `CREATE TABLE` tests pass
- no leaked MySQL-only options remain in generated output

**Step 3: Run the full `pkg/sqlrewrite` package tests**

Run:

```powershell
go test .\pkg\sqlrewrite -count=1
```

Expected:

- the package passes cleanly with the new dedicated DDL path enabled

**Step 4: If the package fails, fix only directly related regressions**

Allowed follow-up scope:

- dedicated `CREATE TABLE` generator logic
- `ASTRewriter.Rewrite()` dispatch for `CreateTableStmt`
- helper functions introduced in `pkg/sqlrewrite/create_table_generator.go`

Do not widen scope into unrelated fallback rewrites or protocol handling.

**Step 5: Commit the verified regression coverage**

```powershell
git add pkg\sqlrewrite\create_table_generator_test.go pkg\sqlrewrite\create_table_rewriter_test.go pkg\sqlrewrite\create_table_generator.go pkg\sqlrewrite\ast_rewriter_v3.go pkg\sqlrewrite\pg_generator.go
git commit -m "test: cover dedicated create table rewrite path"
```

## Verification Notes

- Run this plan in a dedicated worktree before touching implementation.
- Keep the old `removeTableOptions()` path intact for generic fallback behavior, but do not use it from the dedicated `CREATE TABLE` path.
- If multi-statement output breaks a downstream caller, treat that as an explicit integration bug to solve after the dedicated generator is working, not as a reason to revert to string cleanup.

## Expected Outcome

After completing this plan, aproxy should rewrite the reported MySQL `CREATE TABLE` statement into a PostgreSQL-native multi-statement DDL sequence:

1. clean `CREATE TABLE`
2. optional `COMMENT ON TABLE`
3. optional `COMMENT ON COLUMN`

The output should no longer contain `ROW_FORMAT`, `ENGINE`, `DEFAULT CHARSET`, or inline MySQL `COMMENT` syntax, and future `CREATE TABLE` option handling will have a clear AST-driven extension point.