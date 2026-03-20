# Create Table Dedicated DDL Generator Design

## Problem

aproxy currently rewrites MySQL `CREATE TABLE` statements by taking the generic AST restore output and then applying string-based cleanup in `PGGenerator.PostProcess()`.

This works for some simple cases, but it is not a stable model for DDL compatibility. The current `removeTableOptions()` path removes only a subset of MySQL-specific table options, which leaves unsupported PostgreSQL input behind. The visible symptom in the reported case is that `ROW_FORMAT=DYNAMIC` survives the rewrite and PostgreSQL fails with a syntax error near `ROW_FORMAT`.

The same path also has adjacent correctness risks:

- MySQL table `COMMENT='...'` is not modeled structurally
- column `COMMENT '...'` is not converted into PostgreSQL metadata statements
- MySQL storage options such as `ENGINE`, `CHARSET`, `COLLATE`, and `ROW_FORMAT` are treated as text fragments instead of typed DDL input
- future MySQL-only options will keep leaking unless more string patches are added

The design goal is to stop treating `CREATE TABLE` as a generic statement with trailing cleanup and instead introduce a dedicated PostgreSQL DDL generation path for this statement class.

## Considered Approaches

### 1. Extend string cleanup in `removeTableOptions()`

Add more removal rules for `ROW_FORMAT`, table `COMMENT`, column `COMMENT`, and similar MySQL-only syntax.

Pros:

- smallest short-term code change
- quick bug fix for the current reported statement

Cons:

- preserves the current fragile architecture
- keeps DDL compatibility dependent on restore output formatting
- future MySQL options will keep requiring ad hoc string rules
- makes preserving comments correctly much harder

### 2. Filter more aggressively in the AST visitor and keep string cleanup as fallback

Teach the AST visitor to drop unsupported table options and maybe collect comment metadata, while still relying on generic restore plus post-processing.

Pros:

- better than pure string cleanup
- reduces some accidental leakage of MySQL-only syntax

Cons:

- still splits `CREATE TABLE` compatibility responsibilities across too many layers
- still depends on post-processing for final correctness
- still does not naturally model PostgreSQL `COMMENT ON ...`

### 3. Add a dedicated `CREATE TABLE` PostgreSQL generator

Handle `CreateTableStmt` through a dedicated generation path that emits PostgreSQL-native DDL and explicit `COMMENT ON TABLE` and `COMMENT ON COLUMN` statements.

Pros:

- fixes the root architectural problem for DDL conversion
- makes comment preservation natural instead of incidental
- gives each MySQL table option a clear policy: preserve, drop, or reject
- reduces reliance on fragile string cleanup for one of the most syntax-sensitive statement types

Cons:

- larger implementation scope than a quick patch
- requires focused test coverage for dedicated DDL generation

## Chosen Design

Use approach 3.

`CREATE TABLE` should no longer rely on the generic `GenerateWithPlaceholders()` plus `PostProcess()` path as its primary conversion mechanism. Instead, aproxy should recognize `*ast.CreateTableStmt` and route it through a dedicated generator that produces PostgreSQL-compatible DDL directly.

The dedicated generator will:

1. emit a PostgreSQL `CREATE TABLE` statement containing only supported columns and constraints
2. collect MySQL table and column comments during generation
3. emit trailing `COMMENT ON TABLE` and `COMMENT ON COLUMN` statements for preserved metadata
4. silently drop MySQL storage-layer table options that have no PostgreSQL equivalent and are safe to ignore
5. return a clear error for any future `CREATE TABLE` option that cannot be safely ignored and cannot be mapped correctly

The existing `PostProcess()` table-option cleanup remains only as a defensive fallback for non-dedicated paths. It is no longer the primary compatibility mechanism for `CREATE TABLE`.

## Architecture

### Current flow

Today the relevant path is:

1. parse SQL with TiDB parser
2. traverse AST with `ASTVisitor`
3. restore SQL generically with `PGGenerator.GenerateWithPlaceholders()`
4. fix up MySQL leftovers in `PGGenerator.PostProcess()`

This path is acceptable for many DML rewrites, but it is a weak fit for DDL because `CREATE TABLE` contains many syntax constructs that are storage-engine specific, metadata specific, or structurally different between MySQL and PostgreSQL.

### New flow

After this design, the `CREATE TABLE` path becomes:

1. parse SQL with TiDB parser
2. traverse AST with `ASTVisitor` for structural normalization such as index filtering and type preparation
3. detect `*ast.CreateTableStmt`
4. call a dedicated `ConvertCreateTable()` implementation
5. generate:
   - one PostgreSQL `CREATE TABLE` statement
   - zero or more `COMMENT ON TABLE` statements
   - zero or more `COMMENT ON COLUMN` statements
6. combine them into the outward-facing rewrite result while keeping the public API unchanged

The public `Rewrite()` method can continue returning a single string so existing callers do not need to change.

## Components

### AST rewriter dispatch

The AST rewriter keeps its current responsibilities for parsing and general AST traversal, but it gains one new routing decision: when the parsed statement is `*ast.CreateTableStmt`, it uses the dedicated `CREATE TABLE` generator instead of the generic generation pipeline.

This keeps the special handling local to one statement family instead of spreading partial DDL logic across unrelated post-processing helpers.

### AST visitor

The visitor remains responsible for structural normalization that is naturally AST-based:

- filtering unsupported inline `INDEX` and `KEY` constraints
- preparing type mapping behavior
- preserving enough column and option information for the dedicated generator

The visitor should not try to emit PostgreSQL comment statements itself. That is generation responsibility, not transformation responsibility.

### Dedicated `CREATE TABLE` generator

`ConvertCreateTable()` becomes the primary engine for PostgreSQL DDL emission.

It reads the normalized `CreateTableStmt` and constructs:

- PostgreSQL column definitions
- PostgreSQL-compatible constraints
- comment metadata extracted from table and column options
- a list of MySQL-only options that should be dropped or rejected

The generator should work from structured AST data, not by scanning already-restored SQL.

### Fallback post-processing

`PostProcess()` remains in place for generic non-DDL rewrites and as a defensive fallback. It should not be relied on to make `CREATE TABLE` valid.

## Data Model and Output Shape

Internally, the dedicated generator should not operate on a single output string only. It should first build a structured result, for example:

- `createTableSQL`
- `trailingSQLs`
- `warnings`

The external `Rewrite()` API can still join these into one multi-statement SQL string for compatibility with current callers.

This keeps the internal model extensible. If future DDL conversion also needs trailing statements such as index creation or ownership changes, the structure already exists.

## Comment Preservation Strategy

### Table comments

MySQL table option `COMMENT='...'` is preserved as PostgreSQL metadata:

```sql
COMMENT ON TABLE "table_name" IS '...';
```

It is not emitted inline inside `CREATE TABLE`.

### Column comments

MySQL column option `COMMENT '...'` is preserved as PostgreSQL metadata:

```sql
COMMENT ON COLUMN "table_name"."column_name" IS '...';
```

It is not emitted inline inside the column definition.

### Escaping and identifiers

Comment text must use PostgreSQL single-quoted literal escaping. In particular, embedded single quotes must be doubled.

Table names, schema-qualified names, and column names must use the same PostgreSQL identifier quoting rules as the rest of the generator so that `COMMENT ON ...` points to the same object names as the preceding `CREATE TABLE`.

### Ordering

The output order is:

1. `CREATE TABLE ...`
2. optional `COMMENT ON TABLE ...`
3. optional `COMMENT ON COLUMN ...`

This guarantees the commented objects already exist when metadata statements run.

## Table Option Policy

Each MySQL table option handled by the dedicated generator must have an explicit policy.

### Drop silently

These are storage-engine or physical-layout details with no PostgreSQL equivalent and are safe to ignore for this compatibility layer:

- `ENGINE`
- `ROW_FORMAT`
- `KEY_BLOCK_SIZE`
- `COMPRESSION`
- `DELAY_KEY_WRITE`
- `PACK_KEYS`
- `CHECKSUM`
- `STATS_AUTO_RECALC`
- `STATS_PERSISTENT`
- `STATS_SAMPLE_PAGES`
- `CHARSET`
- `CHARACTER SET`
- `COLLATE`

### Preserve semantically

- table `COMMENT` becomes `COMMENT ON TABLE`
- column `COMMENT` becomes `COMMENT ON COLUMN`
- existing type conversion and `AUTO_INCREMENT` handling continue through the current normalization rules, but their final SQL emission is owned by the dedicated generator

### Reject explicitly

If future work uncovers `CREATE TABLE` options that materially affect semantics and cannot be mapped or ignored safely, the generator should return a clear error instead of dropping them silently or passing them through.

This prevents silent behavior drift.

## Error Handling

The `CREATE TABLE` dedicated path should be fail-closed.

If aproxy recognizes a `CreateTableStmt` and enters the dedicated generator, it should not silently fall back to the old string-cleanup path on unsupported structured input. Falling back would reintroduce the same class of bugs this design is meant to eliminate.

Recommended behavior:

- safe-to-ignore options: drop them and continue
- supported convertible metadata: emit equivalent PostgreSQL statements
- unsupported or unsafe structured features: return a clear rewrite error

Comment generation errors are hard failures because the agreed requirement is to preserve table and column comments whenever possible.

## Testing Strategy

### Unit tests for dedicated DDL generation

Add focused tests for the dedicated `CREATE TABLE` generator covering at least:

- simple `CREATE TABLE`
- table comment only
- column comment only
- table comment plus column comments
- `ENGINE + CHARSET + COLLATE + ROW_FORMAT`
- schema-qualified table names
- comment text containing single quotes
- comment text containing non-ASCII text such as Chinese comments

### End-to-end rewrite tests

Add rewrite pipeline tests asserting that the final SQL:

- contains `CREATE TABLE`
- contains `COMMENT ON TABLE` when table comments exist
- contains `COMMENT ON COLUMN` when column comments exist
- does not contain leaked MySQL options such as `ROW_FORMAT`, `ENGINE`, or `CHARSET`

### Regression case from the reported failure

The reported statement with column comments, table comment, `ENGINE`, `DEFAULT CHARSET`, `COLLATE`, and `ROW_FORMAT` must be captured as a fixed regression case.

This should remain in the suite permanently because it exercises exactly the cross-section of syntax that exposed the architectural weakness.

### Unsupported-option failure tests

Add negative tests for any structured option category that the dedicated generator chooses to reject. The expected behavior should be an explicit rewrite error, not malformed SQL.

## Compatibility and Rollout

The public API can remain unchanged, which keeps the rollout narrow. The primary compatibility impact is internal: callers that previously received one slightly dirty `CREATE TABLE` string will instead receive one clean multi-statement PostgreSQL DDL string with trailing comment statements.

This is a behavior improvement, but it should still be verified in any path that assumes only one statement is returned by the rewriter.

If multi-statement rewrite output is not acceptable somewhere in the execution path, that becomes an explicit integration concern to solve during implementation rather than a reason to keep the current fragile architecture.

## Expected Outcome

After this change, aproxy should rewrite MySQL `CREATE TABLE` statements into PostgreSQL-native DDL instead of cleaning up generic SQL text after the fact. The immediate bug around `ROW_FORMAT` disappears, table and column comments are preserved through `COMMENT ON` statements, and future `CREATE TABLE` compatibility work gains a clear structured extension point.