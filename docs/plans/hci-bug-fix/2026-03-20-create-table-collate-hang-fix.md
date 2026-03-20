# Create Table Collate Hang Fix Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 修复 aproxy 在重写带列级 `COLLATE`、`COMMENT`、`ROW_FORMAT` 的 MySQL `CREATE TABLE` 语句时出现卡死或生成无效 PostgreSQL SQL 的问题，并用可重复的回归测试锁定行为。

**Architecture:** 保持修复范围集中在 `pkg\sqlrewrite\pg_generator.go` 的 `PostProcess()` / `removeTableOptions()` 路径。先用低层单元测试锁定 `removeTableOptions()` 的死循环与遗漏清理，再补一条端到端 `Rewriter.Rewrite()` 回归测试，最后用最小扫描逻辑分别处理列级和表级 MySQL 选项，避免继续依赖会重复命中自身标记的字符串替换策略。

**Tech Stack:** Go 1.25, `testing`, `time`, `stretchr/testify`, PingCAP TiDB parser, existing `pkg/sqlrewrite` rewrite pipeline

---

## Problem Summary

用户提供的 SQL 在通过 aproxy 执行时，客户端阻塞在 `CREATE TABLE` 阶段，不返回成功也不返回错误。链路上真正阻塞的位置不是 MySQL 驱动，也不是 PostgreSQL 执行，而是 proxy 侧 SQL 重写。

相关调用链：

- `pkg\protocol\mysql\handler.go:274` 调用 `rewriter.Rewrite(query)`
- `pkg\sqlrewrite\rewriter.go:44-85` 调用 AST 重写器
- `pkg\sqlrewrite\ast_rewriter_v3.go:78` 在 `GenerateWithPlaceholders()` 后进入 `generator.PostProcess(pgSQL)`
- `pkg\sqlrewrite\pg_generator.go:1027-1199` 的 `removeTableOptions()` 在处理 `COLLATE` 时卡死

## Validated Root Cause

本次排查已经验证过下列事实：

1. `parser.Parse()` 正常返回，不是 TiDB parser 卡死。
2. `ASTVisitor.Accept()` 正常返回，不是 AST 访问阶段卡死。
3. `GenerateWithPlaceholders()` 正常返回，不是 SQL 生成阶段卡死。
4. `PostProcess()` 对包含列级 `COLLATE` 的 `CREATE TABLE` 不返回。

直接根因在 `pkg\sqlrewrite\pg_generator.go:1159-1195`：

```go
for {
	idx := strings.Index(strings.ToUpper(result), "COLLATE")
	if idx == -1 {
		break
	}

	// Skip spaces after COLLATE
	i := idx + 7
	for i < len(result) && result[i] == ' ' {
		i++
	}

	// Should have = after COLLATE
	if i >= len(result) || result[i] != '=' {
		result = result[:idx] + "xCOLLATE" + result[idx+7:]
		continue
	}
}
```

这里的循环只正确处理表级 `COLLATE=xxx`。当输入是列级 `COLLATE utf8mb4_bin` 时：

- 条件 `result[i] != '='` 成立
- 代码把 `COLLATE` 改成 `xCOLLATE`
- 下一轮 `strings.Index(strings.ToUpper(result), "COLLATE")` 仍然能命中 `xCOLLATE` 里的 `COLLATE`
- 结果字符串持续增长，循环无法结束

这就是客户端“卡死”的根因。

## Secondary Failures After the Hang Is Fixed

即使先把死循环修掉，当前 `PostProcess()` 仍然会生成 PostgreSQL 无法执行的 SQL：

- 列级 `COMMENT '...'` 仍会残留
- 表级 `COMMENT='...'` 仍会残留
- `ROW_FORMAT=DYNAMIC` 仍会残留
- 列级 `COLLATE utf8mb4_bin` 即使不再卡死，也仍是不合法的 PostgreSQL 方言

所以修复不能只做“避免死循环”，还必须把这些 MySQL 特有 DDL 选项从输出里移除或安全跳过。

## Reproduction and Test Strategy

最小复现已经收敛到：

```sql
CREATE TABLE `t1` (
	`id` bigint unsigned NOT NULL COMMENT 'id',
	`disks` text COLLATE utf8mb4_bin NOT NULL,
	PRIMARY KEY (`id`)
)
```

行为特征：

- `TEXT` 单独存在时，重写正常
- `TEXT COMMENT 'x'` 时，重写正常
- `TEXT COLLATE utf8mb4_bin` 时，重写挂住

后续回归测试要同时覆盖三层：

1. `removeTableOptions()` 的低层行为
2. `Rewriter.Rewrite()` 的端到端行为
3. `pkg\sqlrewrite` 包级回归，确保没有破坏其他重写路径

## Constraints and Guardrails

- 不要触碰当前脏工作树里与本问题无关的文件。
- 保持修复范围局限在 `pkg\sqlrewrite`，不要顺手改动协议层或 PostgreSQL 连接层。
- 使用 TDD：先写失败测试，再写最小修复。
- 优先修复根因，不要只在调用方加超时或吞掉错误。
- 不要继续使用会让搜索命中自身标记的“占位再恢复”写法处理 `COLLATE`。

## Preconditions for Verification

- 本地 Go toolchain 可以运行 `go test`。
- `pkg\sqlrewrite` 测试可以独立运行，不依赖数据库。
- 运行挂起回归测试时，必须显式给 `go test` 设置较小的 `-timeout`，防止失控卡死。

### Task 1: 用低层测试锁定 `removeTableOptions()` 的挂起与遗漏清理

**Files:**
- Create: `pkg\sqlrewrite\pg_generator_test.go`
- Reference: `pkg\sqlrewrite\pg_generator.go:1027-1199`

**Step 1: Write the failing tests**

新建 `pkg\sqlrewrite\pg_generator_test.go`，先写直接针对 `removeTableOptions()` 的测试。第一个测试要检测“不挂住”，第二个测试要检测“输出里不再残留 MySQL 特有选项”。

```go
package sqlrewrite

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPGGenerator_RemoveTableOptions_DoesNotHangOnColumnCollate(t *testing.T) {
	g := NewPGGenerator()
	input := `CREATE TABLE "t1" ("id" DECIMAL(20,0) NOT NULL COMMENT 'id',"disks" TEXT COLLATE utf8mb4_bin NOT NULL,PRIMARY KEY("id"))`

	done := make(chan string, 1)
	go func() {
		done <- g.removeTableOptions(input)
	}()

	select {
	case result := <-done:
		assert.NotContains(t, result, "COLLATE utf8mb4_bin")
		assert.NotContains(t, result, "utf8mb4_bin")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("removeTableOptions hung on column COLLATE")
	}
}

func TestPGGenerator_RemoveTableOptions_StripsMySQLDDLDecorations(t *testing.T) {
	g := NewPGGenerator()
	input := `CREATE TABLE "t_vm_cbt" ("vmid" DECIMAL(20,0) NOT NULL COMMENT '虚拟机ID',"disks" TEXT COLLATE utf8mb4_bin NOT NULL COMMENT '磁盘CBT信息',"status" VARCHAR(31) NOT NULL COMMENT 'CBT状态',PRIMARY KEY("vmid")) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='虚拟机CBT状态记录表'`

	result := g.removeTableOptions(input)

	require.NotEmpty(t, result)
	assert.NotContains(t, result, "ENGINE=")
	assert.NotContains(t, result, "CHARSET")
	assert.NotContains(t, result, "ROW_FORMAT")
	assert.NotContains(t, result, " COMMENT ")
	assert.NotContains(t, result, " COMMENT=")
	assert.NotContains(t, result, "utf8mb4_bin")
	assert.Contains(t, result, `CREATE TABLE "t_vm_cbt"`)
}
```

**Step 2: Run the targeted tests to verify they fail**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestPGGenerator_RemoveTableOptions -v -timeout 5s
```

Expected before the fix:

- `TestPGGenerator_RemoveTableOptions_DoesNotHangOnColumnCollate` 可能超时或卡在 goroutine select
- `TestPGGenerator_RemoveTableOptions_StripsMySQLDDLDecorations` 会看到 `ROW_FORMAT` / `COMMENT` / `utf8mb4_bin` 残留

**Step 3: Commit the red test checkpoint**

```powershell
git add pkg\sqlrewrite\pg_generator_test.go
git commit -m "test: capture create table collate rewrite regression"
```

### Task 2: 用端到端回归测试锁定用户 SQL 场景

**Files:**
- Modify: `pkg\sqlrewrite\ast_rewriter_v3_test.go:377-433`
- Reference: `pkg\sqlrewrite\rewriter.go:44-85`

**Step 1: Write the failing end-to-end test**

在 `pkg\sqlrewrite\ast_rewriter_v3_test.go` 追加用户 SQL 的端到端用例。这个测试同时校验：

- `Rewriter.Rewrite()` 不挂住
- 输出中不残留 MySQL 特有 DDL 装饰
- 输出仍保留 `CREATE TABLE` 主体

```go
func TestRewriter_CreateTableWithColumnCollateCommentsAndRowFormat(t *testing.T) {
	rewriter := NewRewriter(true)
	input := `CREATE TABLE \`t_vm_cbt\` (
	  \`vmid\` bigint(16) unsigned NOT NULL COMMENT '虚拟机ID',
	  \`disks\` text COLLATE utf8mb4_bin NOT NULL COMMENT '磁盘CBT信息',
	  \`status\` varchar(31) NOT NULL COMMENT 'CBT状态',
	  \`checkpoint_id\` bigint unsigned NOT NULL DEFAULT 0 COMMENT 'CBT版本号',
	  \`created_at\` datetime NOT NULL COMMENT '创建时间',
	  \`updated_at\` datetime COMMENT '更新时间',
	  PRIMARY KEY (\`vmid\`)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC COMMENT='虚拟机CBT状态记录表'`

	type rewriteResult struct {
		output string
		err    error
	}

	done := make(chan rewriteResult, 1)
	go func() {
		output, err := rewriter.Rewrite(input)
		done <- rewriteResult{output: output, err: err}
	}()

	select {
	case result := <-done:
		require.NoError(t, result.err)
		assert.Contains(t, result.output, `CREATE TABLE "t_vm_cbt"`)
		assert.NotContains(t, result.output, "ROW_FORMAT")
		assert.NotContains(t, result.output, "COMMENT")
		assert.NotContains(t, result.output, "utf8mb4_bin")
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Rewrite hung on CREATE TABLE with column COLLATE")
	}
}
```

**Step 2: Run the focused test to verify it fails**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestRewriter_CreateTableWithColumnCollateCommentsAndRowFormat -v -timeout 5s
```

Expected before the fix:

- 测试超时或命中 `Rewrite hung...`
- 如果没有挂住，也会因为 `COMMENT` / `ROW_FORMAT` / `utf8mb4_bin` 残留而失败

**Step 3: Commit the second red checkpoint**

```powershell
git add pkg\sqlrewrite\ast_rewriter_v3_test.go
git commit -m "test: cover create table collate comment rewrite path"
```

### Task 3: 在 `pg_generator.go` 中做最小根因修复

**Files:**
- Modify: `pkg\sqlrewrite\pg_generator.go:177-220`
- Modify: `pkg\sqlrewrite\pg_generator.go:1027-1199`
- Test: `pkg\sqlrewrite\pg_generator_test.go`
- Test: `pkg\sqlrewrite\ast_rewriter_v3_test.go`

**Step 1: Refactor table-option cleanup so non-table tokens are skipped safely**

先把当前会无限命中自身标记的 `COLLATE` 占位逻辑移除，改成“显式推进游标”的扫描逻辑。不要再使用：

```go
result = result[:idx] + "xCOLLATE" + result[idx+7:]
```

改成类似下面的模式：

```go
collateSearchPos := 0
for {
	upperPart := strings.ToUpper(result[collateSearchPos:])
	idx := strings.Index(upperPart, "COLLATE")
	if idx == -1 {
		break
	}

	idx += collateSearchPos

	if g.isInString(result, idx) {
		collateSearchPos = idx + len("COLLATE")
		continue
	}

	i := idx + len("COLLATE")
	for i < len(result) && result[i] == ' ' {
		i++
	}

	if i >= len(result) {
		break
	}

	if result[i] == '=' {
		removeStart := idx
		i++
		for i < len(result) && result[i] == ' ' {
			i++
		}
		for i < len(result) && result[i] != ' ' && result[i] != ',' && result[i] != ';' && result[i] != ')' {
			i++
		}
		result = result[:removeStart] + result[i:]
		collateSearchPos = removeStart
		continue
	}

	// Column-level COLLATE should be removed by a dedicated pass.
	collateSearchPos = idx + len("COLLATE")
}
```

这个改动只解决“不再死循环”，还不够。

**Step 2: Add dedicated removal for column-level `COLLATE`, column `COMMENT`, table `COMMENT`, and `ROW_FORMAT`**

新增一个专门处理 `CREATE TABLE` MySQL DDL 装饰的 helper，并从 `PostProcess()` 中调用。建议把行为拆清楚：

- 表级：`ENGINE=...`、`DEFAULT CHARSET=...`、`CHARSET=...`、`COLLATE=...`、`ROW_FORMAT=...`、`COMMENT='...'`
- 列级：`COLLATE xxx`、`COMMENT '...'`

建议增加如下 helper 签名：

```go
func (g *PGGenerator) stripCreateTableColumnOptions(sql string) string
func (g *PGGenerator) stripCreateTableTailOptions(sql string) string
```

列级处理可以用单次扫描实现，只在 `CREATE TABLE (...)` 主体内、且不在字符串字面量中时移除 `COLLATE <identifier>` 和 `COMMENT '...'/"..."`：

```go
func (g *PGGenerator) stripCreateTableColumnOptions(sql string) string {
	result := sql

	result = stripKeywordWithValue(result, "COLLATE", false)
	result = stripKeywordWithQuotedValue(result, "COMMENT")

	return result
}
```

表尾处理在现有 `removeTableOptions()` 基础上补齐 `ROW_FORMAT` 和 `COMMENT=`：

```go
result = g.stripAssignmentOption(result, "ROW_FORMAT")
result = g.stripAssignmentOption(result, "COMMENT")
```

实现约束：

- 只对 `CREATE TABLE` 使用这些剥离逻辑
- 不要误删字符串字面量里的 `COMMENT` / `COLLATE`
- 继续保留现有 `ENGINE` / `CHARSET` / `COLLATE=` 清理能力
- 修完后，`removeTableOptions()` 不应再承担列级 token 占位逻辑

**Step 3: Wire the helper into `PostProcess()`**

把 `PostProcess()` 中的 DDL 清理顺序调整为：

```go
sql = g.convertAutoIncrement(sql)
sql = g.convertInsertNullToDefault(sql)
sql = g.removeTableOptions(sql)
sql = g.stripCreateTableColumnOptions(sql)
```

如果实现后发现“先列级后表级”更稳定，就按实际稳定顺序保留，但必须让两个新测试都通过。

**Step 4: Run the targeted tests to verify they pass**

Run:

```powershell
go test .\pkg\sqlrewrite -run TestPGGenerator_RemoveTableOptions -v -timeout 5s
go test .\pkg\sqlrewrite -run TestRewriter_CreateTableWithColumnCollateCommentsAndRowFormat -v -timeout 5s
```

Expected after the fix:

- 不再超时
- 输出不再包含 `ROW_FORMAT`
- 输出不再包含 `COMMENT`
- 输出不再包含 `utf8mb4_bin`

**Step 5: Commit the root-cause fix**

```powershell
git add pkg\sqlrewrite\pg_generator.go pkg\sqlrewrite\pg_generator_test.go pkg\sqlrewrite\ast_rewriter_v3_test.go
git commit -m "fix: prevent create table collate rewrite hang"
```

### Task 4: 跑 `pkg/sqlrewrite` 包级回归并做最终验证

**Files:**
- Verify: `pkg\sqlrewrite\pg_generator.go`
- Verify: `pkg\sqlrewrite\ast_rewriter_v3_test.go`

**Step 1: Run the package regression suite**

Run:

```powershell
go test .\pkg\sqlrewrite\... -v -timeout 30s
```

Expected:

- 现有 `pkg\sqlrewrite` 测试通过
- 新增 `CREATE TABLE` 回归测试通过

**Step 2: Optional manual smoke verification with the original SQL**

如果本地 proxy 与后端 PostgreSQL 环境可用，再手工执行用户 SQL。优先使用现有代理，而不是重新写探针程序。

Expected after the fix:

- 客户端不再阻塞在 `CREATE TABLE`
- 若还有 PostgreSQL 执行错误，应直接返回错误而不是挂住

**Step 3: Commit the verification checkpoint**

```powershell
git add pkg\sqlrewrite\pg_generator.go pkg\sqlrewrite\pg_generator_test.go pkg\sqlrewrite\ast_rewriter_v3_test.go
git commit -m "test: verify create table rewrite regression coverage"
```

## Notes for the Implementer

- 先保证“不挂住”，再保证“输出合法”，不要把两类失败混在一起凭感觉改。
- 如果你发现 `stripCreateTableColumnOptions()` 会误删字符串内容，先把扫描边界收紧到 `CREATE TABLE` 主体再继续。
- 不要在协议层 `handler.go` 加 timeout 当作修复；那只是把 root cause 从无限循环变成超时错误。
- 如果想把 MySQL `COMMENT` 映射成 PostgreSQL `COMMENT ON ...`，那是后续 feature，不属于这次最小修复范围；本次只需要确保重写不挂住且输出可执行。