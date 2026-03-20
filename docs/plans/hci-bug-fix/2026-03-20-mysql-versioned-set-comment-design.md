# MySQL Versioned SET Comment Handling Design

## Problem

MySQL clients and dump tools can send session initialization statements wrapped in MySQL versioned comments, for example:

```sql
/*!40101 SET character_set_client = utf8 */
```

In MySQL, the server executes the body of this comment when the version gate matches. In aproxy, the current routing logic only recognizes statements that begin directly with `SET `. As a result, versioned comment wrappers prevent these session statements from entering the existing local `SET` handling path.

The wrapped statement then falls through into the normal SQL execution path and reaches PostgreSQL, which does not support MySQL session variables such as `character_set_client`. This causes syntax failures during connection initialization or dump replay.

## Considered Approaches

### 1. Unwrap versioned `SET` comments inside `HandleSetCommand`

Teach `pkg/mapper/show.go` to recognize `/*!...*/` and strip the wrapper before parsing.

Pros:
- Minimal mechanical change
- Reuses the existing `SET` parser directly

Cons:
- Mixes two responsibilities: MySQL comment normalization and `SET` parsing
- Makes future debugging harder because routing and parsing concerns become coupled

### 2. Normalize versioned `SET` comments before statement classification

Add a small helper near the protocol routing layer that takes raw SQL, strips leading and trailing whitespace, unwraps MySQL versioned comments only when the inner statement is `SET ...`, and returns normalized SQL for routing and parsing.

Pros:
- Keeps routing concerns separate from `SET` parsing
- Solves the current issue and similar session initialization statements with the same pattern
- Limits behavior change to a narrow, testable boundary

Cons:
- Requires introducing a new normalization step and using both raw and normalized SQL deliberately

### 3. Implement a general versioned comment compatibility layer

Build infrastructure to parse and reinterpret all `/*!<version> ... */` statements regardless of the inner SQL type.

Pros:
- Most comprehensive long-term compatibility model

Cons:
- Too broad for the current bug
- High risk of accidental behavior changes outside `SET`
- Unnecessary scope expansion

## Chosen Design

Use approach 2.

Add a small normalization helper in `pkg/protocol/mysql` that accepts raw SQL and returns a normalized SQL string for dispatch decisions. The helper will:

1. Trim surrounding whitespace.
2. Detect MySQL versioned comment syntax in the form `/*!<digits> ... */`.
3. Extract the inner SQL body.
4. If the inner SQL begins with `SET ` after trimming, return the inner SQL.
5. Otherwise, return the original trimmed SQL unchanged.

Only routing and parsing will use the normalized SQL. Logging, error reporting, and any user-visible diagnostics will continue to use the original raw SQL where appropriate.

## Components

### Protocol normalization helper

Add a helper in `pkg/protocol/mysql` such as `normalizeVersionedSetComment(sql string) string`.

Responsibilities:
- Recognize the narrow `/*!<version> SET ... */` pattern
- Return unwrapped `SET ...` when matched
- Preserve non-target input without trying to reinterpret it

Non-responsibilities:
- Parsing arbitrary MySQL comments
- Handling non-`SET` versioned comments
- Executing SQL

### Query routing

Update the dispatch path in `pkg/protocol/mysql/handler.go` so that `IsSetStatement()` and `handleSetCommand()` receive normalized SQL. This allows the existing local `SET` path to consume the command before it can be forwarded to PostgreSQL.

### Existing `SET` parser

Keep `pkg/mapper/show.go` focused on `SET` parsing. It should continue to parse standard `SET NAMES`, assignment syntax, and session variable updates without gaining any awareness of versioned comment wrappers.

## Data Flow

After the change, the relevant path will be:

1. Receive `rawSQL` from the MySQL client.
2. Compute `normalizedSQL` through the versioned comment helper.
3. Route statement type checks using `normalizedSQL`.
4. If it is a `SET` statement, call `handleSetCommand(normalizedSQL)`.
5. Store session variables locally and return success without sending the statement to PostgreSQL.

For a query such as:

```sql
/*!40101 SET character_set_client = utf8 */
```

the normalized query becomes:

```sql
SET character_set_client = utf8
```

and the existing local `SET` handler processes it successfully.

## Error Handling

- Use normalized SQL only for routing and `SET` parsing.
- Preserve raw SQL for error text and diagnostics when useful.
- If the input is not a recognized versioned `SET` comment, do not guess. Leave it unchanged and preserve current behavior.
- If the unwrapped body is malformed `SET` syntax, let the current `SET` parser return its existing error style.

## Compatibility Boundaries

This design intentionally supports only a narrow subset:

- Supported: versioned comments whose inner body is a single `SET ...` statement
- Not supported: versioned comments wrapping `CREATE`, `INSERT`, `ALTER`, or multiple statements
- Not supported: nested comments or malformed comment bodies beyond the simple wrapper form

This keeps the fix aligned to the current bug and avoids introducing an implicit general compatibility layer.

## Verification

Verification should cover three layers:

1. Normalization unit tests for versioned and non-versioned inputs
2. Existing `SET` parser regression tests to confirm unwrapped SQL still behaves correctly
3. Routing-level tests in `pkg/protocol/mysql` to confirm wrapped `SET` statements are consumed locally rather than reaching PostgreSQL

## Expected Outcome

After the fix, aproxy should treat MySQL versioned `SET` initialization statements the same way it treats equivalent plain `SET` statements. Session setup commands such as `/*!40101 SET character_set_client = utf8 */` should no longer cause PostgreSQL syntax errors during client initialization or dump replay.