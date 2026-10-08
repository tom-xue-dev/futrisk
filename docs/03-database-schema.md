# futrisk 数据库设计与字段说明

| 项 | 内容 |
|---|---|
| 文档编号 | FR-DBDD-03 |
| 版本 | v1.0 |
| 状态 | 实现对齐 |
| 日期 | 2026-10-08 |
| 适用范围 | 各服务的 SQLite 库（PostgreSQL 为迁移目标，§3.1）、tick 与历史数据 Parquet 文件、回测输出、报表文件 |
| 对应标准 | GB/T 8567-2006 数据库设计说明（DBDD）：数据库级设计决策、详细设计、访问方式、需求可追踪性 |
| 数据库版本 | md.db 1；td.db 3；risk-{shard}.db 3；clearing.db 2；api.db 2；supervisor.db 2（各库的迁移历史见 §9.1）；sim.db 2（`PRAGMA user_version`） |
| 代码对照 | 表结构：各服务的 `schema.py`（SQLAlchemy `MetaData` 和 `MIGRATIONS`）、`futrisk.db`（`common_tables`、`database`、`migrations`、`outbox`）、`futrisk.sim.exchange.store`；tick 文件：md-gateway 的 `shards.py`、`recording.py`；与代码的一致性由 `scripts/check_docs.py` 机械核对（README §3.6） |
| 关联文档 | [README](README.md) · [01 架构](01-architecture.md) · [02 通信协议](02-communication-protocol.md) · [11 配置参考](11-config-reference.md) |

> 本文是**表结构、列、键、约束以及仅用于存储的枚举**的唯一权威定义。消息负载和通用枚举见 [02](02-communication-protocol.md)；本文引用这些负载时，列名与 02 中的字段名保持一致。本文的每张表、每一列（含可空性）、类型、键、约束、每个库的迁移历史和冻结的枚举取值都与代码一一对应；表、列和可空性的偏差由校验脚本发现，其余靠审查（§9）。

## 目录
1. [范围](#1-范围)
2. [数据库级设计决策](#2-数据库级设计决策)
3. [逻辑类型与通用约定](#3-逻辑类型与通用约定)
4. [各数据库详细设计](#4-各数据库详细设计)
5. [文件型存储](#5-文件型存储)
6. [跨库引用与一致性](#6-跨库引用与一致性)
7. [数据访问与事务边界](#7-数据访问与事务边界)
8. [数据生命周期、备份与恢复](#8-数据生命周期备份与恢复)
9. [迁移与版本](#9-迁移与版本)
10. [需求可追踪性](#10-需求可追踪性)
11. [修订记录](#11-修订记录)

---

## 1. 范围

| 存储 | 文件（位于 `<home>` 下） | 所属服务（唯一写者） | 内容 |
|---|---|---|---|
| md.db | `db/md.db` | md-gateway | Feed A 续传游标、每个合约的仲裁水位、行情事件（含人工排除数据源的持久化记录）、缺口、结算价输入、outbox、命令幂等记录 |
| td.db | `db/td.db` | trade-gateway | 已路由委托、路由墓碑、柜台回报、成交、柜台游标、kill switch 状态、outbox |
| risk-{shard}.db | `db/risk-0.db`、`db/risk-1.db` | risk-engine-{shard} | 日初快照副本、委托、已应用成交、出入金、规则状态、告警、风险事件、账户状态、审计、指标样本、EOD 状态、outbox |
| clearing.db | `db/clearing.db` | clearing-worker | 成交与出入金副本、EOD 运行记录、结算价、结算结果、日初快照、对账、报表索引、回测作业、配置历史、outbox |
| api.db | `db/api.db` | api-gateway（**单实例**，它是 api.db 的唯一写者） | API key、接口审计、通知、配置版本、人工 kill switch 的期望状态 |
| supervisor.db | `db/supervisor.db` | supervisor | 进程事件、市场时钟、outbox |
| tick 文件 | `ticks/raw/…`、`ticks/canonical/…`、`ticks/spill/…`、`ticks/wire/…` | md-gateway；新浪录制命令 `futrisk data record` | 原始 tick 分片与去重后的规范 tick；写入失败时的备用目录；新浪原始 HTTP 应答（只有录制命令写）（Parquet，§5.1） |
| 历史数据 | 应用目录下的 `data/hist/…`（源码运行时为仓库根目录，exe 为 exe 所在目录） | 开发期脚本 `scripts/fetch_hist.py`（运行时只读） | 回测用的历史日线（§5.2） |
| 回测与报表文件 | `backtests/…`、`reports/…` | clearing-worker；`futrisk backtest run`（只写 `backtests/`） | §5.2、§5.3 |
| sim.db（外部模拟组件） | `db/sim.db` | sim-exchange | 模拟柜台的委托、成交、持仓和序号，以及价格引擎的当日累计状态（§4.7），用来模拟真实柜台和行情"重启不丢状态"的特性 |

**不在本文范围内的数据**：
- **配置数据**：品种、账户、规则、端点等，存放在 `config/*.yaml`，由 pydantic 模型定义，属于主数据，不入库。版本快照 `config/versions/{N}/` 和 `active.json` 见 11 §1.2。
- **纯内存状态**：各服务的最新值缓存、槽位、可信状态等，重启后通过快照和重放重建。仲裁水位和 Feed A 的续传游标是例外，它们会持久化（§4.1）。
- **strategy-sim**：没有持久化，重启后向风控分片查询持仓、权益和状态。sim-exchange 的柜台和价格引擎的当日累计状态见 §4.7；Feed A 的重放环形缓冲只在内存中，sim-exchange 重启后换新的 epoch。

---

## 2. 数据库级设计决策

| 编号 | 决策 | 理由 |
|---|---|---|
| DB-01 | **每个服务一个库，库的写者唯一**。其他服务**禁止**直接读写别人的库，只能通过总线交换数据。数据库从不共享；共享的只有以下几个目录，每个都只有一个运行时写者，其余进程只读：<br>• `config/`：管理员编辑工作目录；版本快照 `config/versions/{N}/` 和 `active.json` 由 api-gateway 在热加载时写入（02 §8.5），停机时也可以由 `futrisk config commit` 离线写入（11 §1.3），版本 1 由首次启动的命令或服务创建；其他服务只读；<br>• `reports/`、`backtests/`：clearing-worker 写入，api-gateway 只读，用来下载报表；离线命令 `futrisk backtest run` 在同一个 home 上运行时也写 `backtests/`。<br>`ticks/` 不共享：运行时只有 md-gateway 写；`futrisk data record` 在同一个 home 上运行时也写其中的 `raw/{trading_day}/sina/` 和 `wire/`（§5.1）。<br>docker compose 部署（`deploy/compose.yml`）按同样的划分挂载卷：`config`、`reports`、`backtests` 各是一个共享卷，只有写者以读写方式挂载（`config` 卷中的版本 1 由一次性容器 `config-init` 创建）；`ticks` 是 md-gateway 独占的卷；另有一个一次性的建账容器 `seed`，以只读方式挂载 api-gateway 的 home，只为读取 bootstrap 管理员 key。没有单独的密钥卷 | 所属关系清晰；各库独立演进；故障相互隔离（01 ADR-010） |
| DB-02 | 使用 SQLite，开启 WAL；每个库只有一个写库线程，写事务一律 `BEGIN IMMEDIATE`。连续的非关键写任务合并成一次提交（最多 64 个），每个任务在自己的 SAVEPOINT 中执行，失败只回滚它自己；关键任务（DB-03）单独提交 | 零运维，可以打进 exe；单写者不会发生写锁升级失败；合并提交摊薄每次提交的开销 |
| DB-03 | PRAGMA 设置：`journal_mode=WAL`、`foreign_keys=ON`、`busy_timeout=5000`；读连接设 `query_only=ON`。写连接设 `wal_autocheckpoint=0` 和 `journal_size_limit`（128 MB）：WAL checkpoint 不在提交路径上，由单独的连接在后台执行（每 50 ms 一次 `PRAGMA wal_checkpoint(PASSIVE)`，WAL 仍超过 128 MB 时做一次 `PRAGMA wal_checkpoint(RESTART)`）；服务关闭时做一次 `PRAGMA wal_checkpoint(TRUNCATE)`。`synchronous` 按库和事务区分：<br>• md、risk 库默认为 `NORMAL`：提交时不 fsync，进程崩溃不丢数据，只有断电或操作系统崩溃时，可能丢失最后几条提交；<br>• risk 库中的**关键事务**在提交前临时切换为 `FULL`，逐次落盘：锁存熔断与解除、进入或离开 `HALTED`、kill switch 与公司级控制、出入金、日初快照的加载（含临时快照）、EOD 冻结与换日、对账异常和委托补建、减仓计划、人工命令、配置提交；<br>• td、clearing、api、supervisor 库整库使用 `FULL`：它们要么写入量小，要么不在下单往返的路径上 | 下单往返的延迟目标（01 §1.2）容不下每条委托意图都 fsync，笔记本上一次 fsync 就要毫秒级。断电时丢失的高频写入都能补回：成交从 trade-gateway 重新拉取，委托按 `td.query.orders` 补建（02 §8.3），行情本来就可以重新获取；而熔断锁存这类必须"任何重启后都保持"的状态，仍然逐次落盘 |
| DB-04 | 通过 SQLAlchemy Core 访问，用一份 `MetaData` 定义表结构，SQLite 的 DDL 由它生成（迁移见 §9）；生产环境迁移到 PostgreSQL 时，每个服务一个 schema，类型按 §3.1 的目标映射改写 | 只需维护一份表定义 |
| DB-05 | 事实类消息写入**事务性 outbox**（§4.0），与业务数据在同一个事务中提交；事务提交之后再发布消息；重放时读 outbox。主题序号由**永不清理**的 `outbox_topics` 计数表分配 | 保证事实不丢，并且可以重放；outbox 清理之后序号也不会回退（02 §4.5） |
| DB-06 | 清算相关的金额和价格存为**十进制规范字符串**（逻辑类型为 `DEC_*`），所有算术都在 Python 的 Decimal 中完成，SQL 只负责存取 | SQLite 没有精确的十进制类型；用字符串既精确又便于阅读 |
| DB-07 | 盘中估值和风控比率用 REAL（float64） | 与 02 §4.3 一致 |
| DB-08 | 时间统一存为 UTC 纳秒整数；交易日存为 `YYYYMMDD` 文本 | 与消息的编码一致，排序简单 |
| DB-09 | 不同库之间没有外键，只做**逻辑引用**，靠对账保证一致（§6）；同一个库内部可以声明外键 | 这是每服务一库的必然结果 |
| DB-10 | 幂等由唯一键保证：`client_order_id`、成交键、`alert_key`、`movement_id`、`command_id`、`(trading_day, version)` | 重试、重放和重启都不会产生重复数据（01 §8.4） |
| DB-11 | 每个库有一个**流 ID**（`db_meta.stream_id`），新建库时生成，从备份恢复时要重新生成（§8）；使用 `NORMAL` 的库在断电之后也要重新生成（判断方法见 §4.0 `db_meta`）。FACT 消息的信封中带有它 | 从备份恢复之后，或者断电丢失了最后几条提交之后，outbox 的序号可能回退；换一个流 ID，订阅方就能识别出来，并用领域查询重新核对（02 §8.2），不会把新消息当成重复丢掉 |

---

## 3. 逻辑类型与通用约定

### 3.1 逻辑类型

| 逻辑类型 | SQLite DDL（由 `MetaData` 生成） | PostgreSQL（目标映射） | 说明 |
|---|---|---|---|
| `ID` | `VARCHAR(64)`（TEXT 亲和性） | VARCHAR(64) | 业务标识，如账户、合约、实例 ID |
| `UUID` | `VARCHAR(36)` | UUID | UUIDv7 字符串（按时间有序） |
| `TD` | `VARCHAR(8)`，`CHECK(length(x)=8)` | CHAR(8) | 交易日 `YYYYMMDD` |
| `TS` | `INTEGER` | BIGINT | UTC 纪元纳秒 |
| `INT` | `INTEGER` | BIGINT | 手数、序号、计数 |
| `DEC_PRICE` | `TEXT` | NUMERIC(18,6) | 价格，存为规范十进制字符串（02 §4.3 的 `DecStr`） |
| `DEC_MONEY` | `TEXT` | NUMERIC(20,2) | 金额，精确到分 |
| `DEC_AMOUNT` | `TEXT` | NUMERIC(24,6) | 未取整的金额，例如累计成交额 |
| `DEC_RATE` | `TEXT` | NUMERIC(12,8) | 精确比例，例如保证金率、费率、结算后的风险度 |
| `REAL` | `FLOAT`（REAL 亲和性） | DOUBLE PRECISION | 浮点值，用于盘中指标 |
| `BOOL` | `INTEGER`，`CHECK(x IN (0,1))` | BOOLEAN | 布尔值 |
| `ENUM(名称)` | `TEXT`，`CHECK(x IN (…))` | TEXT + CHECK | 取值见 02 §7.3 或本文 §3.3；允许为空的列，空值也能通过 CHECK |
| `JSON` | `TEXT`，`CHECK(json_valid(x))` | JSONB | JSON 文本 |
| `BLOB` | `BLOB` | BYTEA | msgpack 编码的负载 |
| `HASH` | `VARCHAR(64)`，64 位十六进制小写 | CHAR(64) | SHA-256 |
| `TEXT` | `TEXT` | TEXT | 其他文本 |

SQLite 一列由 `MetaData` 实际生成，是库中的真实类型。PostgreSQL 一列是迁移目标：代码中只有 `INT` / `TS` 带 PostgreSQL 变体（BIGINT），`BLOB` 按方言自动生成 BYTEA，其余类型按同一份 `MetaData` 生成时仍是 VARCHAR / TEXT / FLOAT / INTEGER，并且 CHECK 中用到的 `json_valid()` 函数和 GLOB 运算符是 SQLite 专有的，迁移前要按方言补上类型变体和约束（DB-04）。

### 3.2 通用约定
- **命名**：
  - 表名用 snake_case 复数（例外见 README §3.3），列名用 snake_case；
  - 普通索引命名为 `ix_{表}__{列或用途}`，唯一约束和唯一索引命名为 `uq_{表}__{列}`，具名的表级 CHECK 为 `ck_{表}__{用途}`，具名外键为 `fk_{表}__{被引用表}`。
- **时间列**：只追加的日志型表带 `ts_ns` 或 `created_ns`；会被修改的表带 `updated_ns`，或者记录各阶段时刻的 `*_ns` 列（如 `gaps.opened_ns` / `closed_ns`）；例外是 `db_meta` 和 `api_keys`，只有 `created_ns`（`api_keys` 另有 `expires_ns`）。`*_ns` 列一般是市场时间（02 §8.8：演示模式下等于本机时间加偏移）；记录本机时间的列在说明中注明。
- **可空约定**：字段表中"可空"一列为"否"的列，都声明 `NOT NULL`。
- **默认值**：约束一列中的"默认"是数据库的 DEFAULT 子句。`BOOL` 列都默认 0，只有允许为空的 `risk_events.latched` 没有默认值。
- **列名对齐**：表直接保存某种负载时，列名与 02 中对应的字段名**完全一致**，例如 `client_order_id`、`trading_day`、`trade_seq`、`updated_ns`。像 `eod_runs`、`sod_*` 这类聚合或派生的表，与负载之间的对应关系在该表的说明中单独写明。
- **账户和合约**：`account_id`、`instrument_id` 的合法取值由配置（`accounts.yaml`、`products.yaml`）决定。写入时由应用层校验，数据库不另设主表。

### 3.3 仅用于存储的枚举
02 §7.3 中的枚举在库和文件中直接沿用（例如 `ConfigStatus`、`ReconResolution`、`ReconBreakKind`、`ReconBreakStatus`、`TradeOrigin`、`ControlOrigin`、`ClockMode`；tick 文件的 `mode` 列用 `TickMode`）。库中 CHECK 约束的取值按迁移冻结（§9）。下面这些枚举只在存储中出现：

| 枚举 | 取值 | 使用位置 |
|---|---|---|
| `AlertStatus` | `ACTIVE` `CLEARED` `RESET` `ROLLED` | risk 库 `alerts.status`：生效中 / 已自动解除 / 已人工重置 / 已随换日解除 |
| `RiskEodStatus` | `OPEN` `FROZEN` `PROVISIONAL` `ROLLED` `FORCE_UNFROZEN` | risk 库 `eod_states.status`。`PROVISIONAL` 表示正在使用临时日初快照（02 §8.4） |
| `AuditAction` | `RISK_RESET` `RISK_HALT` `ALERT_ACK` `KILL_SWITCH_ON` `KILL_SWITCH_OFF` `FIRM_RESTRICTIONS` `CASH_MOVE` `LIQUIDATION_CONFIRM` `LIQUIDATION_CANCEL` `EOD_FORCE_UNFREEZE` `RECON_RESOLVE` `DAYROLL` `DAYROLL_REBASE` `PROVISIONAL_SOD` `CONFIG_COMMIT` | risk 库 `audit_events.action` |
| `MarginMethod` | `GROSS` `BIG_SIDE` | clearing 库 `margin_groups.method` |
| `CloseType` | `TODAY` `YD` | clearing 库 `close_details.close_type` |
| `NotificationStatus` | `PENDING` `SENT` `FAILED` `SUPPRESSED` | api 库 `notifications.status` |
| `SodSource` | `EOD` `SEED` | clearing 库 `sod_versions.source`：EOD 结算生成，或者 `settle.seed_sod` 建账/更正 |


---

## 4. 各数据库详细设计

### 4.0 通用表
下表列出各库包含哪些通用表：

| 表 | md | td | risk | clearing | api | supervisor |
|---|---|---|---|---|---|---|
| `schema_version` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `db_meta` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `outbox` 和 `outbox_topics` | ✓ | ✓ | ✓ | ✓ | — | ✓ |
| `processed_commands` | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `fact_cursors` | — | ✓ | ✓ | ✓ | ✓ | — |

#### `db_meta`：库元数据（单行表）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `id` | INT | 否 | PK，`CHECK(id=1)` | 固定为 1 |
| `stream_id` | UUID | 否 | | 流 ID，即信封中的 `stream`（02 §4.2）。新建库时生成；**从备份恢复时必须重新生成**：没有恢复命令，恢复步骤中调用 `futrisk.db.regenerate_stream_id`（§8） |
| `created_ns` | TS | 否 | | 库创建时刻 |
| `restored_from` | TEXT | 是 | | 如果是从备份恢复的，记录备份文件名 |
| `clean_shutdown` | BOOL | 否 | 默认 0 | 服务正常停机时置 1，每次启动时置 0 |
| `host_boot_ns` | TS | 是 | | 上次启动时主机的开机时刻（本机时间）。本次启动时，如果 `clean_shutdown=0` 且主机的开机时刻变了（相差超过 10 s），说明期间发生过断电或系统崩溃，使用 `NORMAL` 的库要重新生成 `stream_id`（DB-11）。单纯的进程崩溃不会丢数据，不需要重新生成 |

#### `outbox_topics`：FACT 主题序号计数（**永不清理**）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `topic` | TEXT | 否 | PK | 完整主题 |
| `last_seq` | INT | 否 | | 已分配的最大序号。写 outbox 时，在同一事务里加 1 并取用 |
| `updated_ns` | TS | 否 | | 最近一次分配序号的时刻（即那条 outbox 行的 `created_ns`） |

#### `schema_version`：迁移记录

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `version` | INT | 否 | PK | 迁移版本号 |
| `description` | TEXT | 否 | | 迁移说明 |
| `checksum` | HASH | 否 | | 迁移的 checksum（三种迁移的算法见 §9）；与代码不一致时服务拒绝启动，除非记录的是代码声明接受的旧 checksum（此时就地改写为新值） |
| `applied_ns` | TS | 否 | | 执行时刻 |

#### `outbox`：事务性发件箱（所有 FACT 主题都从这里发布和重放）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `outbox_id` | INT | 否 | PK，自增 | 本库内的全局顺序 |
| `topic` | TEXT | 否 | `uq_outbox__topic_seq(topic, topic_seq)` | 完整主题，例如 `td.exec.ACC3` |
| `topic_seq` | INT | 否 | 同上 | 就是信封中的 `seq`，由 `outbox_topics.last_seq + 1` 分配，在同一个事务中完成。**不能**用 outbox 中的 MAX()+1，否则 outbox 按保留期清理之后序号会回退 |
| `type` / `v` | TEXT / INT | 否 | | 负载类型和版本 |
| `epoch` | TEXT | 否 | | 写入时发布方进程的 epoch；重放时原样放进信封，用于审计 |
| `td` | TD | 是 | | 交易日 |
| `corr_id` | TEXT | 是 | | 业务关联键 |
| `payload` | BLOB | 否 | | msgpack 编码的负载 |
| `created_ns` | TS | 否 | | 写入时刻（市场时间），发布和重放时都作为信封的 `ts_ns` |
| `published_ns` | TS | 是 | `ix_outbox__unpublished`（部分索引：`published_ns IS NULL`） | 发布成功的时刻（本机时间：保留期按本机时间计算，演示时钟的跳转不会缩短重放窗口，§8）。服务启动时，把尚未发布的消息按 `outbox_id` 顺序补发 |

#### `processed_commands`：命令幂等记录

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `command_id` | TEXT | 否 | PK | 幂等键：`command_id`、`movement_id`，或者 REST 请求头中的 `Idempotency-Key` |
| `command` | TEXT | 否 | | 命令名，例如 `risk.reset` |
| `requester` | TEXT | 否 | | 调用方的 DEALER identity |
| `request_hash` | HASH | 否 | | 请求中**业务字段**的哈希：只忽略负载顶层的 `sent_ns`、`requested_ns`、`ts_ns`、`received_ns`，嵌套数据中的时间照样参与。幂等键相同但哈希不同时，返回 `CONFLICT` |
| `result_code` | TEXT | 否 | | 首次处理的结果码 |
| `reply` | BLOB | 否 | | 首次处理的完整 `Reply`，重复请求时原样返回，并置 `duplicate=true` |
| `trading_day` | TD | 否 | `ix_processed_commands__td` | 记录时所属服务的当前交易日（clearing-worker 的命令和 `td.import_trades` 取请求中有效的交易日），用于按交易日清理 |
| `received_ns` | TS | 否 | | 收到的时刻 |

说明：
- `route.order` 以 `routed_orders` 的主键去重；`order.submit` 以 `orders` 的主键加 `request_hash` 去重，内容不同时返回 `REJECT_DUPLICATE_ORDER_ID`。这两个命令**不再**另外写 `processed_commands`。td.db 中写本表的是 `route.cancel`、`td.import_trades` 和 `firm.kill_switch`。
- `route.cancel` 以 `command_id` 为幂等键，撤单失败之后可以换一个新的 `command_id` 再撤。
- `route.resolve` 天然幂等：第一次调用之后，结果已经落在 `routed_orders` 或 `route_tombstones` 中，再次调用返回同样的结果。
- `eod.freeze`、`eod.rollover` 和 `ctl.config.*` 的幂等体现在各自的状态表中（`eod_states`、`config_versions`），不写本表。

#### `fact_cursors`：FACT 订阅游标

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `src` | ID | 否 | PK(src, topic) | 发布方实例 |
| `topic` | TEXT | 否 | 同上 | 完整主题 |
| `stream` | UUID | 否 | | 最近一次应用的消息所属的流 ID。与新消息的 `stream` 不一致时，按 02 §8.2 重置游标并核对 |
| `last_seq` | INT | 否 | | 已应用的最大连续 seq，下一条期望的是 `last_seq + 1` |
| `start_policy` | ENUM(FactStartPolicy) | 否 | | 首次订阅时采用的起始策略（02 §4.5） |
| `updated_ns` | TS | 否 | | 更新时刻 |

**规则**：
- 更新游标，必须与应用这条消息产生的业务写入放在**同一个事务**里（02 §8.2）。
- 游标在应用第一条消息时才写入，此前的起始位置取自发布方心跳中的 `stream` 和 `fact_heads`（02 §4.5），所以 `stream` 列不会为空。
- 只为触发库维护而订阅的 `sys.eod` 不写游标（02 §4.1）。md-gateway 和 supervisor 没有其他 FACT 订阅，所以它们的库没有本表。

### 4.1 md.db（md-gateway）

```mermaid
erDiagram
    GAPS |o--o{ MD_EVENTS : "gap_id"
    FEED_CURSORS {
        string source PK
        string epoch
        int last_contiguous_seq
        int last_exch_ts_ns
    }
    MD_EVENTS {
        string event_id PK
        string kind
        string gap_id FK
        string actor
    }
    GAPS {
        string gap_id PK
        string status
        int filled
    }
    WATERMARKS {
        string instrument_id PK
        string trading_day
        int wm_volume
        int wm_exch_ts_ns
        int wm_fingerprint
    }
    SETTLE_INPUTS {
        string trading_day PK
        string instrument_id PK
        int version PK
        string quality
    }
```
*这张图回答：md.db 中各表之间的关系。只有缺口事件通过可空的 `gap_id` 关联到缺口表，游标、水位和结算价输入各自独立。*

#### `feed_cursors`：各数据源的续传游标

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `source` | ENUM(SourceId) | 否 | PK | 数据源 |
| `epoch` | TEXT | 是 | | Feed A 的 epoch；轮询源为 null |
| `last_contiguous_seq` | INT | 是 | | Feed A **已落盘**的最大连续 seq。重启后从这里续传：`resume.from_seq = last_contiguous_seq + 1`（02 §9.1）；轮询源为 null |
| `last_exch_ts_ns` | TS | 是 | | 轮询源（Feed B、新浪）已落盘的最大交易所时间，只供排查，重启后不使用；Feed A 为 null |
| `trading_day` | TD | 否 | | 写入时的当前交易日 |
| `updated_ns` | TS | 否 | | 写入时刻。游标每秒检查一次，只有对应数据源缓冲的原始 tick 已经全部写进已关闭的分片（§5.1）才推进，所以游标永远不会超前于录制，崩溃之后缺口可以补齐。录制丢过行的数据源，本进程内不再推进游标 |

#### `watermarks`：每个合约的仲裁水位

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `instrument_id` | ID | 否 | PK | 合约 |
| `trading_day` | TD | 否 | | 最后一个 LIVE 状态的交易日 |
| `wm_volume` / `wm_exch_ts_ns` | INT / TS | 否 | | 与 `trading_day` 一起构成最后一个 LIVE 状态的 `order_key = (trading_day, volume, exch_ts_ns)`（01 §8.13） |
| `wm_fingerprint` | INT | 是 | | 最后一个 LIVE 状态的指纹（无符号 64 位，按二进制补码存成有符号整数）。重启后，轮询源再次送来同一个状态、只是时间戳更晚时，按指纹识别为重复，不再发布 |
| `updated_ns` | TS | 否 | | 写入时刻；有变化的水位每 `arbitration.watermark_flush_s`（默认 1 s，11 §7）写一次，停机时再写一次 |

md-gateway 重启后先加载水位，只有 `order_key` 大于它的状态才能发布为 LIVE，保证 02 §5 中"`order_key` 严格递增"的承诺在重启之后依然成立。risk-engine 这一侧也会丢弃 `order_key` 不大于已应用值的 Tick，作为第二道防线。

#### `md_events`：行情事件（`md.event` 的业务表）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `event_id` | UUID | 否 | PK | 对应 `MdEvent.event_id` |
| `kind` | ENUM(MdEventKind) | 否 | `ix_md_events__td_kind(trading_day, kind)` | 事件类别 |
| `trading_day` | TD | 否 | 同上 | 交易日 |
| `ts_ns` | TS | 否 | | 发生时刻 |
| `source` | ENUM(SourceId) | 是 | | 数据源 |
| `instrument_id` | ID | 是 | `ix_md_events__inst_ts(instrument_id, ts_ns)` | 合约 |
| `from_state` / `to_state` | TEXT | 是 | | 迁移前后的状态 |
| `gap_id` | TEXT | 是 | FK → `gaps.gap_id` | 与缺口相关的事件才有 |
| `actor` | TEXT | 是 | | 由人工操作（`md.source.set`）引起的事件，记录操作者，作为审计。带 `actor` 的 `SOURCE_STATE` 事件同时是人工排除的持久化记录：启动时按每个数据源最后一条人工事件恢复 `EXCLUDE`，这类事件永不清理（§8） |
| `detail` | JSON | 否 | 默认 `{}` | 例如 `reason`（取值见 02 §11.3）、坏点价格、投票详情；与缺口相关的事件在 `detail.gap` 中保存事件发生时的缺口快照 |

#### `gaps`：缺口台账

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `gap_id` | TEXT | 否 | PK | 格式见 02 §7.1 `GapInfo` |
| `source` | ENUM(SourceId) | 是 | | 数据源；交易时段内没有任何可信源时打开的 `all:` 缺口为 null |
| `epoch` | TEXT | 是 | | Feed A 的 epoch |
| `from_seq` / `to_seq` | INT | 是 | | Feed A 序号缺口的区间；时间窗缺口为 null |
| `from_ns` / `to_ns` | TS | 否 / 是 | | 缺口覆盖的时间范围（接收时间）；缺口关闭前 `to_ns` 可以为 null，关闭时补上 |
| `trading_day` | TD | 否 | | 交易日 |
| `status` | ENUM(GapStatus) | 否 | | 缺口状态 |
| `filled` / `duplicates` / `unrecoverable` | INT | 否 | 默认 0 | 补齐条数 / 重复条数 / 无法恢复的条数 |
| `opened_ns` / `closed_ns` | TS | 否 / 是 | | 打开 / 关闭时刻 |

#### `settle_inputs`：结算价输入（`md.settle_inputs` 的数据源）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` | TD | 否 | PK(trading_day, instrument_id, version) | 交易日 |
| `instrument_id` | ID | 否 | 同上 | 合约 |
| `version` | INT | 否 | 同上 | 从 1 开始；重新计算的结果与最新版本不同时加 1，相同则沿用原版本 |
| `exchange` | ENUM(Exchange) | 否 | | 交易所 |
| `method` | ENUM(SettleMethod) | 否 | | 计算方法 |
| `multiplier` | INT | 否 | | 合约乘数 |
| `tick_size` | DEC_PRICE | 否 | | 最小变动价位 |
| `day_volume` | INT | 否 | | 全日累计成交量（当日最后一个状态） |
| `day_turnover` | DEC_AMOUNT | 否 | | 全日累计成交额 |
| `window_start_ns` | TS | 是 | | 仅 `VWAP_LAST_HOUR`：结算时间窗（`products.yaml` 的 `settle_window`）的起点 |
| `window_volume` / `window_turnover` | INT / DEC_AMOUNT | 是 | | 仅 `VWAP_LAST_HOUR`：窗口内的成交量和成交额，即窗口内最后一个状态减去窗口起点之前的最后一个状态 |
| `hourly` | JSON | 是 | | 仅 `VWAP_LAST_HOUR` 品种（CFFEX）：按交易小时分段（并在第一个交易小时的结束处切开）的 `{分段起点 ns: [成交量, 成交额]}`，只列有成交的分段。最后一小时没有成交时，回退链按它逐小时向前回溯（`FALLBACK_HOURLY`，01 §8.15） |
| `vwap_raw` | DEC_PRICE | 是 | | 未取整的 VWAP；相关成交量为 0 时为 null |
| `last` / `bid1` / `ask1` | DEC_PRICE | 是 | | 当日最后一个状态的价格，供回退链使用 |
| `limit_lock` | ENUM(LimitLock) | 否 | | 收盘前 5 分钟内的状态全部封涨停 / 跌停时为 `UP` / `DOWN` |
| `pre_settle` | DEC_PRICE | 否 | | 昨结算（来自行情状态） |
| `quality` | ENUM(SettleInputQuality) | 否 | | 输入质量：相关成交量为 0 时 `NO_TRADE`；最后一小时的边界状态缺失、离窗口起点太远且跨界成交量显著、来自补数，或窗口终点附近还没有状态时 `APPROX` |
| `computed_ns` | TS | 否 | | 计算时刻 |

**计算方式**：收到 `md.settle_inputs` 请求时依次执行：
1. 先把缓冲中的原始 tick 全部写进分片；
2. 如果该交易日的规范 tick 文件（§5.1）还没有生成，或者原始分片的列表变了，就重新合并生成（幂等）；
3. 从规范 tick **重新计算**结算价输入：只用累计量（盘中的缺口不影响结果），后面出现累计成交量更小的状态视为离群值、不用；成交额优先取有精确成交额的状态（新浪的成交额是估算的）。不依赖内存中的累加值，所以 md-gateway 在盘中重启也不会影响结算价；
4. 每个合约与最新版本比较，不同才写入新版本。

### 4.2 td.db（trade-gateway）

```mermaid
erDiagram
    ROUTED_ORDERS |o--o{ BROKER_ORDER_EVENTS : "client_order_id（外部委托为空）"
    ROUTED_ORDERS |o--o{ TRADES : "client_order_id（外部成交为空）"
    ROUTED_ORDERS {
        string client_order_id PK
        string status
        string broker_order_ref
    }
    BROKER_ORDER_EVENTS {
        int event_id PK
        string client_order_id FK
        int broker_seq
    }
    TRADES {
        int trade_seq PK
        string trade_id
        string client_order_id FK
    }
    BROKER_CURSORS {
        string broker PK
        string trading_day PK
        int last_broker_seq
    }
    FIRM_CONTROLS {
        int control_id PK
        string action
        string origin
        int active
    }
    ROUTE_TOMBSTONES {
        string client_order_id PK
        string trading_day
    }
```
*这张图回答：一笔委托在柜台侧留下哪些记录。`route_tombstones` 与其他表没有关联，它记录的正是 trade-gateway 从没收到过的委托；`broker_cursors` 和 `firm_controls` 也各自独立。*

#### `routed_orders`：已路由的委托

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `client_order_id` | UUID | 否 | PK | 幂等键 |
| `account_id` / `instrument_id` | ID | 否 | `ix_routed_orders__td_acct(trading_day, account_id)` | 账户 / 合约 |
| `exchange` | ENUM(Exchange) | 否 | | 交易所 |
| `direction` | ENUM(Direction) | 否 | | 买卖方向 |
| `offset` | ENUM(Offset) | 否 | | 已经归一化的开平标志 |
| `order_type` / `tif` | ENUM | 否 | | 价格类型 / 有效期 |
| `price` | DEC_PRICE | 是 | | 限价 |
| `qty` | INT | 否 | `CHECK(qty>0)` | 委托手数 |
| `trading_day` | TD | 否 | | 交易日 |
| `src_instance` | ID | 否 | | 来源分片，如 `risk-engine-1`（即 `RouteOrder.src_instance`） |
| `expire_ns` | TS | 否 | | 路由有效期截止时刻（`RouteOrder.expire_ns`） |
| `broker` | TEXT | 否 | | `sim` 或 `ctp` |
| `broker_order_ref` | TEXT | 是 | `uq_routed_orders__broker_ref`（非空时唯一） | `{front_id}:{session_id}:{order_ref}` |
| `order_sys_id` | TEXT | 是 | | 交易所委托编号 |
| `status` | ENUM(BrokerOrderStatus) | 否 | 部分索引 `ix_routed_orders__open`（状态不是 `FILLED`、`CANCELED`、`REJECTED`、`EXPIRED`） | 柜台视角的状态。`EXPIRED` 表示委托到达时已经过期或者被 kill switch 拒绝，没有报给柜台，原因码记在 `broker_code`；它同样起墓碑的作用。从没收到过的委托，墓碑记在 `route_tombstones` |
| `filled_qty` | INT | 否 | 默认 0 | 已成交手数 |
| `broker_code` / `broker_msg` | TEXT | 是 | | 柜台原因码和说明 |
| `cancel_requested_ns` | TS | 是 | | 收到 `route.cancel` 的时刻（版本 2 新增）。与该命令的 `processed_commands` 记录在同一个写任务中写入，撤单因此是持久的：柜台暂时不可用时，在下一次补回报之后或该委托的下一条回报到达时重发，直到委托终结 |
| `received_ns` / `submitted_ns` / `updated_ns` | TS | 否 / 是 / 否 | | 收到 / 报出 / 更新时刻 |

#### `route_tombstones`：路由墓碑（从没收到过的委托）

risk-engine 用 `route.resolve` 确认 `UNKNOWN` 状态的委托，或者熔断时撤销 `ROUTING` 状态的委托，而 trade-gateway 从没收到过这笔委托时，就写一行墓碑（02 §8.3）。之后同一 `client_order_id` 的 `route.order` 再到达，一律以 `ROUTE_LOST` 拒绝。`td.query.orders` 是只读查询，不会写本表。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `client_order_id` | UUID | 否 | PK | 被判定为丢失的委托；与 `routed_orders` 的主键不会重复 |
| `trading_day` | TD | 否 | `ix_route_tombstones__td` | 交易日 |
| `command` | TEXT | 否 | | 写入它的命令：`route.resolve` 或 `route.cancel` |
| `command_id` | TEXT | 是 | | `route.cancel` 的 `command_id` |
| `requested_by` | ID | 否 | | 请求方实例，如 `risk-engine-1` |
| `created_ns` | TS | 否 | | 写入时刻 |

#### `broker_order_events`：柜台委托回报流水

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `event_id` | INT | 否 | PK，自增 | 流水号 |
| `trading_day` | TD | 否 | `uq_broker_order_events__td_seq(trading_day, broker_seq)` | 交易日 |
| `broker_seq` | INT | 否 | 同上 | 柜台回报序号（当日全局递增），用于去重 |
| `client_order_id` | UUID | 是 | FK → `routed_orders`（可空），`ix_broker_order_events__client_order_id` | 委托；外部委托为空 |
| `broker_order_ref` | TEXT | 否 | | 柜台委托引用 |
| `origin` | ENUM(TradeOrigin) | 否 | | 委托来源 |
| `status` | ENUM(BrokerOrderStatus) | 否 | | 回报中的状态 |
| `filled_qty` / `remaining_qty` | INT | 否 | | 累计成交 / 剩余手数 |
| `broker_code` / `broker_msg` | TEXT | 是 | | 柜台原因码和说明 |
| `detail` | JSON | 否 | 默认 `{}` | 原始回报中的关键字段 |
| `ts_ns` | TS | 否 | | 时刻 |

#### `trades`：成交（**成交事实的源头**）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trade_seq` | INT | 否 | PK，自增 | 对应 `Trade.trade_seq`，供 `td.query.trades` 续查 |
| `trading_day` | TD | 否 | `uq_trades__key(trading_day, exchange, trade_id, direction)` | 成交键（02 §7.5） |
| `exchange` | ENUM(Exchange) | 否 | 同上 | 交易所 |
| `trade_id` | TEXT | 否 | 同上 | 交易所成交编号 |
| `direction` | ENUM(Direction) | 否 | 同上 | 买卖方向 |
| `account_id` / `instrument_id` | ID | 否 | `ix_trades__td_acct_seq(trading_day, account_id, trade_seq)` | 账户 / 合约 |
| `client_order_id` | UUID | 是 | FK → `routed_orders`（可空） | 委托。外部成交（柜台强平、柜台终端下单、导入）为空 |
| `origin` | ENUM(TradeOrigin) | 否 | | 成交来源 |
| `import_batch` | TEXT | 是 | | `td.import_trades` 导入时的 `command_id` |
| `broker_order_ref` | TEXT | 否 | | 柜台委托引用；导入的成交写成 `IMPORT:{command_id}` |
| `order_sys_id` | TEXT | 是 | | 交易所委托编号 |
| `offset` | ENUM(Offset) | 否 | | 回报中的开平标志，可能是 `FORCE_CLOSE`。未知的取值在适配器这一层就拒收（02 §9.4），不会写到这里 |
| `price` | DEC_PRICE | 否 | | 成交价，由柜台给出的 double 按 `Decimal(str(x))` 转换 |
| `qty` | INT | 否 | `CHECK(qty>0)` | 成交手数 |
| `trade_ts_ns` | TS | 否 | | 成交时间 |
| `broker_seq` | INT | 是 | | 柜台回报序号；导入的成交（`td.import_trades`）可以为空 |
| `received_ns` | TS | 否 | | 收到时刻 |

#### `broker_cursors`：柜台补回报游标

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `broker` | TEXT | 否 | PK(broker, trading_day) | 柜台 |
| `trading_day` | TD | 否 | 同上 | 交易日 |
| `last_broker_seq` | INT | 否 | | 已处理的最大回报序号；断线重连后从 `last_broker_seq + 1` 开始查询 |
| `session_id` / `front_id` | TEXT / INT | 是 | | 最近一次登录的会话信息 |
| `updated_ns` | TS | 否 | | 更新时刻 |

#### `firm_controls`：kill switch 状态（trade-gateway 保存的副本）

列、索引与 risk 库的 `firm_controls` 相同（§4.3），`action` 也是从版本 3 起允许 `RULE_STATE`。trade-gateway 只执行 kill switch：每种来源（`origin`）各取最新一条 `KILL_SWITCH_ON`、`KILL_SWITCH_OFF` 或 `RULE_STATE` 记录的 `active`，**生效状态 = 人工标志 OR 规则标志**；`SET_RESTRICTIONS` 行和 `RULE_STATE` 行中的公司级限制只作审计副本。人工记录来自 `firm.kill_switch`（幂等记录在 `processed_commands`）；规则记录来自 `risk.control.firm`，与 FACT 游标在同一事务中写入，按 `(origin, action, ts_ns, src_instance)` 去重，比已存的最新规则状态更旧的记录不写入，所以从 seq 1 重放也不会把状态改回去。trade-gateway 启动时先加载它，再接收这两类消息；kill switch 生效时，开仓路由一律以 `ROUTE_KILL_SWITCH` 拒绝。

### 4.3 risk-{shard}.db（risk-engine，每个分片一个库）

```mermaid
erDiagram
    SOD_ACCOUNTS ||--o{ SOD_LOTS : "account_id"
    ORDERS ||--o{ ORDER_EVENTS : "client_order_id"
    ORDERS |o--o{ FILLS : "client_order_id（外部成交为空）"
    RULE_STATES ||--o{ ALERTS : "rule_id+level+scope_key"
    ALERTS |o--o{ RISK_EVENTS : "alert_key"
    ALERTS ||--o| LIQUIDATION_PLANS : "trigger_alert_key"
    ACCOUNT_STATES }o--o| ALERTS : "cause_alert_key"
    SOD_ACCOUNTS {
        string trading_day PK
        string account_id PK
        string pre_balance
    }
    SOD_LOTS {
        string trading_day PK
        string account_id PK
        string instrument_id PK
        string direction PK
        int lot_no PK
    }
    ORDERS {
        string client_order_id PK
        string status
    }
    FILLS {
        int fill_seq PK
        string trade_id
    }
    RULE_STATES {
        string trading_day PK
        string rule_id PK
        string level PK
        string scope_key PK
    }
    ALERTS {
        string alert_key PK
        string status
    }
    RISK_EVENTS {
        string event_id PK
        string kind
    }
    ACCOUNT_STATES {
        string account_id PK
        string state
    }
    LIQUIDATION_PLANS {
        string plan_id PK
        string status
    }
```
*这张图回答：风控核心的持久化状态如何组织，告警、事件、账户状态之间如何关联。*

**重启恢复用到的表**（01 §6.5）：
- `sod_*`：当日起点；
- `fills`：按 `fill_seq` 重放，重建 lot 和资金账本；
- `cash_movements`：当日出入金，用来恢复静态权益和累计净入金；
- `rule_states`、`alerts`、`account_states`、`firm_controls`：恢复状态机、episode、锁存、kill switch 和公司级限制；
- `orders`（当日的委托和所有未终结的委托，含撤单重发记录）：恢复未终结的委托和冻结的保证金，其中 `ROUTING` 状态的委托转为 `UNKNOWN`（02 §8.3）；
- `risk_events`、`processed_commands`：当日事件的查询，以及命令幂等；
- `eod_states`：恢复交易模式（`EOD_FROZEN` / `PROVISIONAL`）；
- `liquidation_plans`、`recon_breaks`：恢复待确认的减仓计划，以及未处理的对账异常（对应的禁开限制也随之恢复）；
- `metric_samples`：当日 `perf_equity` 的最大值就是日内峰值，用来恢复 `intraday_dd` 的基准（行情质量不是 OK 的样本不计）。样本每秒写一次，所以重启后峰值最多低估 1 秒之内的波动，这一点在 01 §11 中列为已知的精度限制；
- 合规计数（当日开仓手数、撤单次数）：从当日的 `orders`、`order_events` 和 `fills` 推导。

#### `sod_accounts`：日初快照副本（账户部分）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` | TD | 否 | PK(trading_day, account_id) | 快照生效的交易日 |
| `account_id` | ID | 否 | 同上 | 账户 |
| `sod_version` | INT | 否 | | 对应 clearing.db 中 `sod_versions.version`；临时快照为 −1 |
| `pre_balance` | DEC_MONEY | 否 | | 上日结存 |
| `initial_capital` | DEC_MONEY | 否 | | 初始资金，是日亏损比例的分母 |
| `cum_net_deposit` | DEC_MONEY | 否 | | 截至上一交易日的累计净入金（不含初始资金），用来计算业绩权益 |
| `hwm_equity` | DEC_MONEY | 否 | | 业绩权益的历史高水位 |
| `provisional` | BOOL | 否 | 默认 0 | 是否为临时日初快照（02 §8.4）；为 1 时 `sod_version` 为 −1 |
| `loaded_ns` | TS | 否 | | 载入时刻 |

#### `sod_lots`：日初快照副本（持仓 lot）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `account_id` / `instrument_id` / `direction` / `lot_no` | TD / ID / ID / ENUM(PosDirection) / INT | 否 | PK（五列） | lot 标识 |
| `sod_version` | INT | 否 | | 快照版本 |
| `open_td` | TD | 否 | | 开仓交易日 |
| `open_ts_ns` | TS | 否 | | 开仓时刻，决定 FIFO 顺序 |
| `open_price` | DEC_PRICE | 否 | | 开仓价，用于逐笔口径 |
| `qty` | INT | 否 | `CHECK(qty>0)` | 手数 |
| `basis_price` | DEC_PRICE | 否 | | 盯市基准，即昨结算 |

#### `sod_prices`：昨结算价

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `instrument_id` | TD / ID | 否 | PK | 交易日 / 合约 |
| `pre_settle` | DEC_PRICE | 否 | | 昨结算价 |
| `sod_version` | INT | 否 | | 快照版本 |

#### `orders`：委托（事前检查的结果与委托生命周期）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `client_order_id` | UUID | 否 | PK | 幂等键 |
| `account_id` / `instrument_id` | ID | 否 | `ix_orders__td_acct(trading_day, account_id)` | 账户 / 合约 |
| `exchange` | ENUM(Exchange) | 否 | | 交易所 |
| `direction` / `offset` | ENUM | 否 | | `offset` 为归一化之后的值 |
| `order_type` / `tif` | ENUM | 否 | | 价格类型 / 有效期 |
| `price` | DEC_PRICE | 是 | | 限价 |
| `qty` | INT | 否 | `CHECK(qty>0)` | 委托手数 |
| `trading_day` | TD | 否 | | 交易日 |
| `source` | ENUM(OrderSource) | 否 | | 委托来源 |
| `strategy_tag` / `actor` | TEXT | 是 | | 策略标签 / 操作者 |
| `status` | ENUM(OrderStatus) | 否 | 部分索引 `ix_orders__open`（状态不是 `REJECTED`、`FILLED`、`CANCELED`、`BROKER_REJECTED`） | 状态机见 02 §8.3 |
| `filled_qty` | INT | 否 | 默认 0 | 已成交手数 |
| `avg_fill_price` | DEC_PRICE | 是 | | 成交均价 |
| `reject_code` | TEXT | 是 | | 拒单码（02 §11.2） |
| `reject_detail` | JSON | 是 | | 拒单依据 |
| `broker_order_ref` | TEXT | 是 | | 柜台委托引用 |
| `frozen_margin` / `frozen_fee` | DEC_MONEY | 否 | 默认 "0" | 未成交部分冻结的保证金和手续费 |
| `cancel_command_id` / `cancel_sent_ns` / `cancel_attempts` | TEXT / TS / INT | 是 | `cancel_attempts` 默认 0 | 撤单重发的记录（版本 2 新增）：最近一次 `route.cancel` 的 `command_id`、发出时刻、已发次数。撤单超时仍未终结时换新的 `command_id` 重发，超时依次为 5s、10s、20s、40s，之后每次 60s（02 §8.3） |
| `risk_check_us` | INT | 否 | | 事前检查耗时 |
| `request_hash` | HASH | 否 | | 请求中**业务字段**的哈希，不含 `sent_ns`，用来识别"同一个 ID、不同内容"的重复提交 |
| `config_hash` | HASH | 否 | | 检查时生效的规则配置 |
| `created_ns` / `updated_ns` | TS | 否 | | 创建 / 更新时刻 |

#### `order_events`：委托状态流水

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `event_id` | INT | 否 | PK，自增 | 流水号 |
| `client_order_id` | UUID | 否 | FK → `orders`，`ix_order_events__coid` | 委托 |
| `ts_ns` | TS | 否 | | 时刻 |
| `from_status` | ENUM(OrderStatus) | 是 | | 新建时为 null |
| `to_status` | ENUM(OrderStatus) | 否 | | 新状态 |
| `reason` | TEXT | 是 | | 例如 `CIRCUIT_BREAK_CANCEL`、`ROUTE_TIMEOUT` |
| `detail` | JSON | 否 | 默认 `{}` | 附加信息 |

#### `fills`：已应用的成交

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `fill_seq` | INT | 否 | PK，自增 | 应用顺序，重放时按它排序 |
| `trading_day` / `exchange` / `trade_id` / `direction` | TD / ENUM / TEXT / ENUM | 否 | `uq_fills__key`（四列） | 成交键（幂等） |
| `account_id` / `instrument_id` | ID | 否 | `ix_fills__td_acct(trading_day, account_id)` | 账户 / 合约 |
| `client_order_id` | UUID | 是 | FK → `orders`（可空） | 委托；外部成交为空 |
| `origin` | ENUM(TradeOrigin) | 否 | | 成交来源 |
| `broker_order_ref` | TEXT | 否 | | 柜台委托引用 |
| `offset` | ENUM(Offset) | 否 | | 开平标志 |
| `price` | DEC_PRICE | 否 | | 成交价 |
| `qty` | INT | 否 | | 成交手数 |
| `trade_ts_ns` | TS | 否 | | 成交时间 |
| `trade_seq` | INT | 否 | | 该成交在 trade-gateway 中的 `trade_seq`（跨账户全局递增，与 `Trade.trade_seq` 同名同义）。恢复时**按账户**分别取本库中该账户的最大值加 1，调用 `td.query.trades`（带 `account_id`）续查 |
| `broker_seq` | INT | 是 | | 与 `Trade.broker_seq` 相同；导入的成交为空 |
| `received_ns` | TS | 否 | | 与 `Trade.received_ns` 相同。`risk.query.fills` 直接用本表生成 `Trade` |
| `close_today_qty` / `close_yd_qty` | INT | 否 | 默认 0 | 应用时实际平掉的今仓和昨仓手数 |
| `fee_est` | DEC_MONEY | 否 | | 盘中估算的手续费，正式值以清算结果为准 |
| `applied_ns` | TS | 否 | | 应用时刻 |

#### `cash_movements`：出入金（`risk.cash` 的业务表）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `movement_id` | UUID | 否 | PK | 幂等键 |
| `account_id` | ID | 否 | `ix_cash_movements__td_acct(trading_day, account_id)` | 账户 |
| `trading_day` | TD | 否 | 同上 | 生效交易日，由 risk-engine 按自身的 EOD 状态判定（02 §6.3 `cash.move`） |
| `type` | ENUM(CashType) | 否 | | 入金 / 出金 |
| `amount` | DEC_MONEY | 否 | `CHECK(amount GLOB '[0-9]*' AND amount NOT GLOB '*[^0-9.]*' AND amount NOT GLOB '*.*.*')` | 金额，正数，DecStr 格式。应用层先用 Decimal 严格校验（大于 0、最多两位小数），数据库的 CHECK 只做兜底 |
| `actor` / `reason` | TEXT | 否 | | 操作者 / 原因 |
| `ts_ns` | TS | 否 | | 时刻 |

#### `rule_states`：告警状态机持久化

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `rule_id` / `level` / `scope_key` | TD / ID / ID / TEXT | 否 | PK（四列） | 一个状态机实例 |
| `state` | ENUM(RuleSmState) | 否 | | 当前状态，迁移规则见 01 §8.14 |
| `episode` | INT | 否 | 默认 0 | 当日进入 ACTIVE 的次数，换日后归零 |
| `latched` | BOOL | 否 | | 是否锁存 |
| `confirm_count` | INT | 否 | 默认 0 | PENDING 期间看到的不同 LIVE 状态数 |
| `pending_since_ns` / `active_since_ns` / `clear_since_ns` | TS | 是 | | 各阶段的起始时刻 |
| `cooldown_until_ns` | TS | 是 | | 冷却截止时刻 |
| `rearm_threshold` | REAL | 是 | | 进入 `REARMED` 后的重新布防线 |
| `last_value` | REAL | 是 | | 最近一次评估的值 |
| `config_hash` | HASH | 否 | | 评估时生效的规则配置 |
| `updated_ns` | TS | 否 | | 更新时刻 |

#### `alerts`：告警（每个 episode 一行）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `alert_key` | TEXT | 否 | PK（`ON CONFLICT DO NOTHING`） | 格式见 02 §7.3 |
| `trading_day` / `rule_id` / `level` / `scope_key` / `episode` | TD / ID / ID / TEXT / INT | 否 | `ix_alerts__td(trading_day)` | 由 `alert_key` 拆出的各部分 |
| `account_id` | ID | 是 | | 公司级告警为 null |
| `action` | ENUM(Action) | 否 | | 动作 |
| `severity` | ENUM(Severity) | 否 | | 严重程度 |
| `metric` | TEXT | 否 | | 指标 |
| `value` / `threshold` | REAL | 否 | | 触发时的值 / 阈值 |
| `latched` | BOOL | 否 | | 是否锁存 |
| `status` | ENUM(AlertStatus) | 否 | | 告警状态 |
| `raised_ns` | TS | 否 | | 触发时刻 |
| `cleared_ns` / `reset_ns` / `acked_ns` | TS | 是 | | 解除 / 重置 / 确认时刻 |
| `reset_by` / `acked_by` | TEXT | 是 | | 重置人 / 确认人 |
| `message` | TEXT | 否 | | 说明 |
| `detail` | JSON | 否 | 默认 `{}` | 附加信息 |

#### `risk_events`：风险事件（`risk.event` 的业务表）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `event_id` | UUID | 否 | PK | 对应 `RiskEvent.event_id` |
| `kind` | ENUM(RiskEventKind) | 否 | `ix_risk_events__td_kind(trading_day, kind)` | 事件类别 |
| `trading_day` | TD | 否 | 同上 | 交易日 |
| `ts_ns` | TS | 否 | | 时刻 |
| `account_id` / `scope_key` / `alert_key` | ID / TEXT / TEXT | 是 | `ix_risk_events__alert_key` | 账户 / 范围 / 关联告警 |
| `rule_id` / `level` / `episode` | ID / ID / INT | 是 | | 规则 / 级别 / episode |
| `action` | ENUM(Action) | 是 | | 动作 |
| `severity` | ENUM(Severity) | 否 | | 严重程度 |
| `latched` | BOOL | 是 | | 熔断级别的告警为 1 |
| `metric` / `value` / `threshold` | TEXT / REAL / REAL | 是 | | 指标 / 当时的值 / 阈值 |
| `actor` | TEXT | 是 | | 操作者 |
| `message` | TEXT | 否 | | 说明 |
| `detail` | JSON | 否 | 默认 `{}` | 与 `RiskEvent.detail` 相同，例如 RECON 类的差异明细 |

#### `retro_breaches`：只在补数区间内出现的越线（预留，没有写入方）

设计中的"只在补数区间内出现的越线写审计记录"（`RETRO_BREACH` 影子评估）没有实现（01 TD-13）：这张表在版本 1 中已经建立，代码既不写入也不读取，risk-engine 也不发出 `RETRO_BREACH` 类的 `risk.event`。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `retro_key` | TEXT | 否 | PK | `RETRO\|{td}\|{rule_id}\|{level}\|{scope_key}\|{gap_id}` |
| `trading_day` / `rule_id` / `level` / `scope_key` | TD / ID / ID / TEXT | 否 | | 规则与范围 |
| `gap_id` | TEXT | 否 | | 逻辑引用 md.db 的 `gaps.gap_id` |
| `first_breach_ns` | TS | 否 | | 首次越线的时刻 |
| `worst_value` / `threshold` | REAL | 否 | | 最差值 / 阈值 |
| `detected_ns` | TS | 否 | | 发现时刻 |
| `detail` | JSON | 否 | | 附加信息 |

#### `account_states`：账户风控状态（当前值）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `account_id` | ID | 否 | PK | 账户 |
| `trading_day` | TD | 否 | | 交易日 |
| `state` | ENUM(AccountState) | 否 | | 风控状态 |
| `halted_latched` | BOOL | 否 | | 是否锁存 |
| `restrictions` | JSON | 否 | 默认 `[]` | list[Restriction]（02 §7.6） |
| `cause` | TEXT | 否 | | `RULE` / `MANUAL` / `SYSTEM` / `RESET` / `DAYROLL` / `KILL_SWITCH` |
| `cause_alert_key` | TEXT | 是 | | 由规则引起时的告警 |
| `state_version` | INT | 否 | | 每次变化加 1，随 `risk.state` 和 `risk.snapshot` 一起发布（02 §7.6） |
| `since_ns` / `updated_ns` | TS | 否 | | 进入当前状态 / 更新的时刻 |

状态变化的历史通过 `risk.state` 的 outbox 行和 `audit_events` 查询。

#### `firm_controls`：公司级控制记录

- 人工 kill switch 由 api-gateway 直接下发到**每个**分片，各分片各写一行（`origin=MANUAL`，`action` 为 `KILL_SWITCH_ON` 或 `KILL_SWITCH_OFF`，不带限制）；
- 公司级规则层由 coordinator 作为**一条**事实发布：`origin=RULE`、`action=RULE_STATE`，`active` 是规则层的 kill switch，`restrictions` 是生效中的全部公司级限制（空列表表示全部解除）。coordinator 自己写一行并发布 `risk.control.firm`，其他分片应用后各写一份副本；最新一条就能恢复整个规则层。旧版本写入的规则记录（`origin=RULE` 的 `KILL_SWITCH_ON` / `KILL_SWITCH_OFF`，以及 `SET_RESTRICTIONS`）仍按原含义应用；
- **当前状态**：启动时按 `control_id` 顺序重放全部记录。结果等价于：kill switch 按来源分开，每种来源各取最新一条 `KILL_SWITCH_ON`、`KILL_SWITCH_OFF` 或 `RULE_STATE` 的 `active`，**生效状态 = 人工标志 OR 规则标志**；公司级限制取最新一条 `RULE_STATE` 或 `SET_RESTRICTIONS` 的 `restrictions`。所以 coordinator 发出的规则记录永远关不掉人工开启的 kill switch，反之亦然；
- coordinator 下线时，其他分片保持最后一次已知的状态，并告警。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `control_id` | INT | 否 | PK，自增 | 记录号 |
| `action` | ENUM(FirmAction) | 否 | | `KILL_SWITCH_ON` / `KILL_SWITCH_OFF`（人工，或旧版本的规则记录）、`RULE_STATE`（规则层的完整状态，版本 3 起）、`SET_RESTRICTIONS`（旧版本的公司级限制） |
| `origin` | ENUM(ControlOrigin) | 否 | `ix_firm_controls__origin_action(origin, action, control_id)` | 来源：人工（`MANUAL`）或公司级规则（`RULE`） |
| `active` | BOOL | 否 | 默认 0 | 本来源的 kill switch 标志在执行后的值。对 `KILL_SWITCH_ON`、`KILL_SWITCH_OFF`、`RULE_STATE` 有意义；`SET_RESTRICTIONS` 行固定写 0，不参与计算 |
| `restrictions` | JSON | 否 | 默认 `[]` | 这条记录携带的公司级限制，每次都是完整集合（`FirmControl.restrictions`）；人工记录为空 |
| `trading_day` | TD | 否 | | 交易日 |
| `actor` / `reason` | TEXT | 是 / 否 | | 操作者 / 原因 |
| `cause_alert_key` | TEXT | 是 | | 由公司级规则触发时的告警 |
| `src_instance` | ID | 否 | | 记录的来源实例：人工操作为 `api-gateway`，规则触发为 coordinator（`services.yaml` 中 `coordinator: true` 的分片，默认 `risk-engine-0`） |
| `ts_ns` | TS | 否 | | 时刻 |

#### `liquidation_plans`：减仓计划

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `plan_id` | UUID | 否 | PK | 计划 |
| `account_id` / `trading_day` | ID / TD | 否 | | 账户 / 交易日 |
| `trigger_alert_key` | TEXT | 否 | FK → `alerts` | 触发该计划的告警 |
| `status` | ENUM(LiquidationStatus) | 否 | `ix_liquidation_plans__status` | 计划状态；启动时据此恢复待确认的计划 |
| `steps` | JSON | 否 | | 与 02 §7.6 `LiquidationPlan.steps` 相同 |
| `created_ns` | TS | 否 | | 创建时刻 |
| `confirmed_by` / `confirmed_ns` / `finished_ns` | TEXT / TS / TS | 是 | | 确认人 / 确认时刻 / 完成时刻 |

#### `audit_events`：审计记录（只追加）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `audit_id` | INT | 否 | PK，自增 | 记录号 |
| `ts_ns` / `trading_day` | TS / TD | 否 | `ix_audit_events__td` | 时刻 / 交易日 |
| `actor` | TEXT | 否 | | 操作者，系统动作写 `system` |
| `role` | ENUM(Role) | 是 | | 角色 |
| `action` | ENUM(AuditAction) | 否 | | 动作 |
| `target` | TEXT | 否 | | 对象，例如 `acct:ACC3` 或 `firm` |
| `before` / `after` | JSON | 是 | | 变更前后的状态和指标值 |
| `reason` | TEXT | 是 | | 原因 |
| `command_id` / `corr_id` | TEXT | 是 | | 命令 ID / 关联 ID |

#### `metric_samples`：账户指标样本（每秒一条，批量异步写入）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `account_id` / `ts_ns` | ID / TS | 否 | PK(account_id, ts_ns) | 账户 / 采样时刻 |
| `trading_day` | TD | 否 | `ix_metric_samples__td_acct` | 交易日 |
| `equity` / `perf_equity` / `daily_pnl` / `gross_notional` / `net_notional` | REAL | 否 | | 资金与敞口（`perf_equity` 的定义见 02 §7.6） |
| `rule_util` | JSON | 否 | 默认 `{}` | 各规则在各范围上的使用率：`{rule_id/scope_key: utilization}`。日报用它计算"盘中峰值"这一列 |
| `nav` / `daily_pnl_pct_init` / `intraday_dd` / `hwm_dd` / `risk_degree` / `leverage` | REAL | 否 | | 单位净值与风控比率（定义见 02 §7.6） |
| `state` | ENUM(AccountState) | 否 | | 当时的风控状态 |
| `quality` | ENUM(MdQuality) | 否 | | 当时的行情质量 |

#### `eod_states`：本分片的 EOD 状态

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` | TD | 否 | PK | 交易日 |
| `status` | ENUM(RiskEodStatus) | 否 | | EOD 状态 |
| `freeze_version` | INT | 是 | | 最近一次 `eod.freeze` 带来的结算版本 |
| `frozen_ns` | TS | 是 | | 冻结时刻 |
| `digest` | JSON | 是 | | 最近一次返回的 `EodFreezeAck` 中 `accounts` 和 `cash` 两部分的内容，只作审计；应答每次都重新计算（02 §6.3） |
| `provisional_ns` | TS | 是 | | 进入 `PROVISIONAL` 的时刻 |
| `next_trading_day` / `sod_version` / `rolled_ns` | TD / INT / TS | 是 | | 换日结果 |
| `forced_by` / `forced_reason` | TEXT | 是 | | 强制放行时填写 |

#### `recon_breaks`：对账异常

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `break_id` | UUID | 否 | PK | 异常编号 |
| `trading_day` / `account_id` | TD / ID | 否 | | 交易日 / 账户 |
| `kind` | ENUM(ReconBreakKind) | 否 | | 异常类型 |
| `status` | ENUM(ReconBreakStatus) | 否 | `ix_recon_breaks__open(account_id, status)` | 处理状态 |
| `blocking` | BOOL | 否 | 默认 0 | 是否阻塞开仓（02 §8.7）：`POSITION_MISMATCH` 一般为 1（盘中对账连续两次不一致、迟到的成交、平仓超过持仓、重新定基时重放成交失败），换日时日初快照与本地持仓的核对（`detail.reason=SOD_VS_EOD`）为 0；`REPLAY_EXPIRED`、`ORDER_STATE_CONFLICT` 为 0。账户只要还有 `blocking=1` 且为 `OPEN` 的异常，就带有 `BLOCK_OPEN`（`SYSTEM:RECON_BREAK`） |
| `resolution` | ENUM(ReconResolution) | 是 | | 处理方式（`risk.recon.resolve`），系统自动补齐的为 `RESYNCED` |
| `note` | TEXT | 是 | | 处理说明 |
| `detail` | JSON | 否 | | 差异明细 |
| `created_ns` | TS | 否 | | 发现时刻 |
| `resolved_ns` / `resolved_by` | TS / TEXT | 是 | | 处理时刻 / 处理人（自动处理的写 `system`） |

### 4.4 clearing.db（clearing-worker）

```mermaid
erDiagram
    EOD_RUNS ||--o{ SETTLEMENT_PRICES : "trading_day+version"
    EOD_RUNS ||--o{ ACCOUNT_SETTLEMENTS : "trading_day+version"
    ACCOUNT_SETTLEMENTS ||--o{ POSITION_SETTLEMENTS : "account_id"
    ACCOUNT_SETTLEMENTS ||--o{ MARGIN_GROUPS : "account_id"
    ACCOUNT_SETTLEMENTS ||--o{ TRADE_SETTLEMENTS : "account_id"
    TRADE_SETTLEMENTS ||--o{ CLOSE_DETAILS : "trade key"
    EOD_RUNS ||--o{ RECON_CHECKS : "trading_day+version"
    EOD_RUNS ||--o{ REPORTS : "trading_day+version"
    EOD_RUNS |o--o| SOD_VERSIONS : "from_trading_day+from_version"
    SOD_VERSIONS ||--o{ SOD_ACCOUNTS : "trading_day+version"
    SOD_VERSIONS ||--o{ SOD_PRICES : "trading_day+version"
    SOD_ACCOUNTS ||--o{ SOD_LOTS : "trading_day+account_id+version"
    EOD_RUNS {
        string trading_day PK
        int version PK
        string phase
    }
    ACCOUNT_SETTLEMENTS {
        string trading_day PK
        string account_id PK
        int version PK
        string equity
    }
    POSITION_SETTLEMENTS {
        string instrument_id PK
        string direction PK
        string settle_price
    }
    SOD_VERSIONS {
        string trading_day PK
        int version PK
        string source
        int pushed_ns
    }
    SOD_ACCOUNTS {
        string trading_day PK
        string account_id PK
        int version PK
    }
```
*这张图回答：一次结算运行（`trading_day + version`）会产出哪些结果。日初快照的三张表通过外键挂在 `sod_versions` 下（`sod_lots` 经由 `sod_accounts`）；其余关系是逻辑引用。*

**版本规则**：
- 同一交易日的每一次结算运行都有一个版本号 `version`，从 1 开始；
- 重跑同一版本是幂等的，结果完全相同；
- 官方结算价更正或人工覆盖之后，以 `force_new_version` 生成新版本；
- **当前有效的结果**是该交易日中 `eod_runs.phase` 为 `SETTLED`、`ROLLED`、`REPORTED` 或 `DONE` 的最大版本（`FAILED` 的版本不算）；
- 换日时推给各分片的是哪一个日初快照版本，记录在 `sod_versions.pushed_ns` 中；`settle.sod_snapshot` 不指定版本时，只返回已经推送过的版本（02 §6.5）。

#### `trades`：成交副本（订阅 `td.exec.*`，只保存其中的 `Trade`）

这张表只保存结算需要的列，列名与 `Trade` 的字段同名：
- **主键**：`(trading_day, exchange, trade_id, direction)`；
- **成交相关列**：`account_id`、`instrument_id`、`client_order_id`（可空）、`origin`、`offset`、`price`（DEC_PRICE）、`qty`（`CHECK(qty>0)`）、`trade_ts_ns`；
- **来源列**：`trade_seq`（td.db 中的 `trade_seq`）、`received_ns`；
- **不保存的列**：柜台侧的 `broker_order_ref`、`order_sys_id`、`broker_seq` 在结算中用不到，不保存；
- **索引**：`ix_trades__td_acct(trading_day, account_id)`。

#### `cash_movements`：出入金副本（订阅 `risk.cash.*` 写入）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `movement_id` | UUID | 否 | PK | 幂等键 |
| `account_id` / `trading_day` | ID / TD | 否 | `ix_cash_movements__td_acct` | 账户 / 生效交易日 |
| `type` | ENUM(CashType) | 否 | | 入金 / 出金 |
| `amount` | DEC_MONEY | 否 | | 金额 |
| `actor` / `reason` | TEXT | 否 | | 操作者 / 原因 |
| `ts_ns` | TS | 否 | | 时刻 |
| `src_instance` | ID | 否 | | 来源分片（发布这条 `risk.cash` 的实例） |

#### `eod_runs`：结算运行（saga 状态）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `version` | TD / INT | 否 | PK | 结算运行 |
| `phase` | ENUM(EodPhase) | 否 | | 当前阶段；重启后从这里继续 |
| `next_trading_day` | TD | 否 | | 下一交易日 |
| `triggered_by` | TEXT | 否 | | `scheduler` 或操作者 |
| `config_hash` | HASH | 否 | | 本次运行使用的配置 |
| `step_log` | JSON | 否 | 默认 `{}` | 各阶段的完成时刻和统计（同 `EodPhaseEvent.detail`），键见下文 |
| `error` | TEXT | 是 | | 失败原因 |
| `started_ns` / `updated_ns` / `finished_ns` | TS | 否 / 否 / 是 | | 开始 / 更新 / 结束时刻 |

`step_log` 的键是 `EodPhase` 的取值，每个阶段一项 `{ts_ns, …统计}`；另有一个 `WARNINGS` 列表（最多 50 条），记录不改变阶段的 WARN 事件（如 md-gateway 给出的昨结算与日初快照不一致）。其中两项在恢复时会被原样复用，保证中途崩溃后重跑的结果不变：
- `BEGIN`：`triggered_by`、`sod_version`、`config_version`、`config_hash`；重新结算时还有 `resettle_of`（被取代的版本）或 `force_new_version`。第 4、5 步只使用这里记下的配置版本，运行期间提交的新配置不影响正在进行的结算；
- `PRICES_FIXED`：除统计外，固定本次结算的全部输入：`prices`（`SettlementPriceView` 列表）、`sod_version`、`input_pre_settle`、`pre_settle_fallback_prices`、`skipped`。

`REPORTED` 在报表生成失败时带 `errors`；重新生成成功后改记为 `errors_cleared` 和 `regenerated_ns`。

#### `config_history`：各配置版本的结算生效日（版本 2 新增）

产品参数（保证金、手续费、最小变动价位、涨跌停、结算规则）和交易日历按 `NEXT_TRADING_DAY` 生效（02 §7.10），而运行时会立即换上新配置。clearing-worker 因此记下见过的每个配置版本，以及其中结算相关部分从哪个交易日起适用；结算某一天时，使用当天已经生效的最新版本。系统运行时 md-gateway 否决这两类改动，所以它们实际上只能停机后离线提交（11 §1.3），由 clearing-worker 启动时记录。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `config_version` | INT | 否 | PK | 配置版本 |
| `config_hash` | HASH | 否 | | 配置哈希 |
| `settle_hash` | HASH | 否 | | 按 `config_hash` 的算法（02 §7.10）只对 `products.yaml` 和 `calendar/` 下的文件计算的哈希 |
| `settle_from` | TD | 是 | | 结算相关部分开始适用的交易日；null 表示在所有已记录的交易日之前就已生效。结算文件没变时沿用上一版本的值；变了时取 api-gateway 在 `active.json` 的 `effective` 中为 `products` / `calendar` 记下的交易日，没有时取下一交易日（运行中提交），或当前交易日（clearing-worker 停机期间发生的变更） |
| `recorded_ns` | TS | 否 | | 记录时刻 |

clearing-worker 启动时记录当前运行的版本，收到 `ctl.config.commit` 时记录新提交的版本。结算要用的旧版本从 `<home>/config/versions/{N}/` 加载。

#### `official_prices`：导入的官方结算价

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `instrument_id` | TD / ID | 否 | PK(trading_day, instrument_id, import_id) | 交易日 / 合约 |
| `import_id` | UUID | 否 | 同上 | 等于 `settle.import_official` 的 `command_id` |
| `price` | DEC_PRICE | 否 | | 官方结算价 |
| `source` | TEXT | 否 | | 来源说明（自由文本），例如 `shfe_file`、`akshare` |
| `imported_by` / `imported_ns` | TEXT / TS | 否 | | 导入人 / 导入时刻 |

同一合约有多次导入时，以最新的一次为准。结算价的优先级为：人工覆盖 > 官方 > VWAP > 回退链。

#### `price_overrides`：人工覆盖的结算价（审计）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `override_id` | UUID | 否 | PK，等于命令的 `command_id` | 覆盖记录 |
| `trading_day` / `instrument_id` | TD / ID | 否 | `ix_price_overrides__td_inst` | 交易日 / 合约 |
| `price` | DEC_PRICE | 否 | | 必须在 tick 网格上 |
| `reason` / `actor` | TEXT | 否 | | 原因 / 操作者 |
| `created_ns` | TS | 否 | | 创建时刻 |
| `superseded_ns` | TS | 是 | | 被同一合约更新的覆盖取代的时刻 |

#### `settlement_prices`：结算价

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `instrument_id` / `version` | TD / ID / INT | 否 | PK | 交易日 / 合约 / 结算版本 |
| `price` | DEC_PRICE | 否 | | 最终采用的结算价（已按规则取整） |
| `source` | ENUM(SettlePriceSource) | 否 | | 结算价来源 |
| `method` | ENUM(SettleMethod) | 是 | | 计算方法 |
| `vwap_raw` | DEC_PRICE | 是 | | 未取整的 VWAP |
| `rounding` | ENUM(Rounding) | 否 | | 取整方式 |
| `input_quality` | ENUM(SettleInputQuality) | 是 | | 输入数据的质量 |
| `operator` / `reason` | TEXT | 是 | | `source=MANUAL` 时必须填写 |
| `created_ns` | TS | 否 | | 创建时刻 |

#### `account_settlements`：账户结算结果（结算单"资金状况"部分）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `account_id` / `version` | TD / ID / INT | 否 | PK | 交易日 / 账户 / 结算版本 |
| `pre_balance` | DEC_MONEY | 否 | | 期初结存（上日结存） |
| `deposit` / `withdraw` | DEC_MONEY | 否 | | 当日入金 / 出金 |
| `close_pnl` | DEC_MONEY | 否 | | 平仓盈亏（盯市口径） |
| `position_pnl` | DEC_MONEY | 否 | | 持仓盯市盈亏 |
| `fee` | DEC_MONEY | 否 | | 手续费合计 |
| `balance` | DEC_MONEY | 否 | 无 CHECK，由对账检查 `EQUITY_IDENTITY` 核对 | 期末结存 = 期初结存 + 入金 − 出金 + 平仓盈亏 + 持仓盈亏 − 手续费 |
| `equity` | DEC_MONEY | 否 | | 客户权益，盯市口径下等于 `balance` |
| `margin` | DEC_MONEY | 否 | | 保证金占用，等于该账户各 `margin_groups.charged_margin` 之和 |
| `available` | DEC_MONEY | 否 | | 可用资金 = 权益 − 保证金 |
| `risk_degree` | DEC_RATE | 否 | | 风险度 = 保证金 ÷ 权益 |
| `margin_call` | DEC_MONEY | 否 | 默认 "0" | 追加保证金 = max(0, 保证金 − 权益) |
| `exch_formula_pnl` | DEC_MONEY | 否 | | 交易所公式算出的当日盈亏，是第二道校验 |
| `fifo_realized_pnl` / `fifo_float_pnl` | DEC_MONEY | 否 | | 逐笔对冲口径的平仓盈亏 / 浮动盈亏 |
| `initial_capital` | DEC_MONEY | 否 | | 初始资金 |
| `cum_net_deposit` | DEC_MONEY | 否 | | 截至当日的累计净入金（不含初始资金） |
| `hwm_equity` | DEC_MONEY | 否 | | 更新后的**业绩权益**高水位。业绩权益 = 权益 − 累计净入金（02 §7.6） |
| `nav` | DEC_RATE | 否 | | 净值 = 业绩权益 ÷ 初始资金，出入金不影响净值 |
| `max_drawdown` | DEC_RATE | 否 | | 业绩权益距高水位的回撤 |
| `checks_passed` | BOOL | 否 | 默认 0 | 本账户的所有对账检查中没有 `FAIL` |
| `created_ns` | TS | 否 | | 创建时刻 |

#### `position_settlements`：持仓结算结果（"持仓明细 / 持仓汇总"部分）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `account_id` / `instrument_id` / `direction` / `version` | TD / ID / ID / ENUM(PosDirection) / INT | 否 | PK（五列） | 持仓结算记录 |
| `product` / `exchange` | ID / ENUM(Exchange) | 否 | | 品种 / 交易所 |
| `sod_qty` | INT | 否 | | 期初持仓（全部是昨仓） |
| `open_qty` / `close_qty` | INT | 否 | | 当日开仓 / 平仓手数 |
| `close_today_qty` / `close_yd_qty` | INT | 否 | | 平今 / 平昨手数，按交易所的计费规则拆分 |
| `eod_qty` | INT | 否 | `ck_position_settlements__eod_qty`：`CHECK(eod_qty = sod_qty + open_qty - close_qty)` | 期末持仓 |
| `settle_price` / `pre_settle_price` | DEC_PRICE | 否 | | 今结算 / 昨结算 |
| `multiplier` | INT | 否 | | 合约乘数 |
| `margin_rate` | DEC_RATE | 否 | | 公司保证金率（交易所费率 + 经纪商加收） |
| `margin_gross` | DEC_MONEY | 否 | | 按单边全额计算的保证金，单向大边之前的值 |
| `close_pnl` / `position_pnl` / `fee` | DEC_MONEY | 否 | | 平仓盈亏 / 持仓盈亏 / 手续费 |
| `avg_open_price` | DEC_PRICE | 是 | | 期末持仓的开仓均价 |

#### `margin_groups`：保证金分组（单向大边）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `account_id` / `margin_group` / `version` | TD / ID / TEXT / INT | 否 | PK | 分组。`margin_group` 的取值：SHFE 为 `SHFE:{product}`，CFFEX 为 `CFFEX:INDEX` 或 `CFFEX:TREASURY`，不适用单向大边的为 `GROSS:{instrument_id}` |
| `method` | ENUM(MarginMethod) | 否 | | 计算方法 |
| `long_margin` / `short_margin` | DEC_MONEY | 否 | | 组内多头 / 空头保证金合计，**按金额**比较 |
| `excluded_margin` | DEC_MONEY | 否 | 默认 "0" | 已退出单向大边的合约按双边收取的部分。退出的时点：SHFE 在最后交易日前第 5 个交易日收盘之后；CFFEX 的实物交割合约（国债期货）在交割月前一个交易日收盘之后，股指期货不退出（01 §8.15） |
| `charged_margin` | DEC_MONEY | 否 | | 实收保证金 = max(多, 空) + 已退出部分；采用 `GROSS` 方法时为多空之和 |
| `detail` | JSON | 否 | | 组内各合约的明细 |

#### `trade_settlements`：逐笔成交结算（"成交记录"部分）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `exchange` / `trade_id` / `direction` / `version` | TD / ENUM / TEXT / ENUM / INT | 否 | PK（五列） | 成交键加版本 |
| `account_id` / `instrument_id` | ID | 否 | | 账户 / 合约 |
| `fee` | DEC_MONEY | 否 | | 本笔手续费，逐笔四舍五入到分 |
| `fee_type` | ENUM(FeeType) | 否 | | 手续费类别（平今按 CFFEX、DCE 的规则判定） |
| `fee_rate` / `fee_per_lot` | DEC_RATE / DEC_MONEY | 是 | | 适用的费率或每手费用，二者只填一个 |
| `close_pnl` | DEC_MONEY | 否 | 默认 "0" | 本笔的平仓盈亏（盯市口径） |
| `matched_today_qty` / `matched_yd_qty` | INT | 否 | 默认 0 | 本笔平掉的今仓 / 昨仓手数 |

#### `close_details`：平仓明细（与 lot 的匹配）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `version` / `account_id` / `detail_no` | TD / INT / ID / INT | 否 | PK | 明细编号 |
| `instrument_id` | ID | 否 | | 合约 |
| `exchange` / `trade_id` / `direction` | ENUM / TEXT / ENUM | 否 | 逻辑引用 `trade_settlements`（无外键） | 平仓成交 |
| `close_type` | ENUM(CloseType) | 否 | | 平今 / 平昨 |
| `lot_open_td` / `lot_open_ts_ns` | TD / TS | 否 | | 被平 lot 的开仓交易日和时刻 |
| `lot_open_price` | DEC_PRICE | 否 | | 被平 lot 的开仓价 |
| `basis_price` | DEC_PRICE | 否 | | 盯市基准：平今为开仓价，平昨为昨结算 |
| `close_price` | DEC_PRICE | 否 | | 平仓价 |
| `qty` | INT | 否 | | 手数 |
| `close_pnl_mtm` / `close_pnl_fifo` | DEC_MONEY | 否 | | 盯市口径 / 逐笔口径的平仓盈亏 |

#### `sod_versions`：日初快照版本

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `trading_day` / `version` | TD / INT | 否 | PK | **新交易日** / 快照版本。首日建账为 0，此后每个结算版本对应一个快照版本 |
| `source` | TEXT | 否 | `CHECK(source IN ('EOD','SEED'))`（`SodSource`，§3.3） | 快照来源：EOD 结算生成，或者 `settle.seed_sod` 建账/更正 |
| `from_trading_day` / `from_version` | TD / INT | 是 | | `source=EOD` 时，记录生成它的结算运行 |
| `reason` / `actor` | TEXT | 是 | | `source=SEED` 时必须填写 |
| `created_ns` | TS | 否 | | 创建时刻 |
| `pushed_ns` | TS | 是 | | 所有分片都确认 `eod.rollover` 的时刻，为空表示还没有推送过。换日时 risk-engine 按 `eod.rollover` 中指定的版本取快照，不看这一列；这一列只用于没有本地快照的重启场景（02 §6.5） |

#### `sod_accounts` / `sod_lots` / `sod_prices`：日初快照内容（下一交易日的起点，也是 `settle.sod_snapshot` 的数据源）
- **`sod_accounts`**：
  - 主键：`(trading_day, account_id, version)`，其中 `trading_day` 是**新交易日**；外键 `fk_sod_accounts__sod_versions`：`(trading_day, version)` 引用 `sod_versions`；
  - 资金：`pre_balance`、`initial_capital`、`cum_net_deposit`、`hwm_equity`（均为 DEC_MONEY）；
  - 其他：`created_ns`。
- **`sod_lots`**：
  - 主键：`(trading_day, account_id, instrument_id, direction, lot_no, version)`；外键 `fk_sod_lots__sod_accounts`：`(trading_day, account_id, version)` 引用 `sod_accounts`；
  - 其他列：`open_td`、`open_ts_ns`、`open_price`、`qty`（`CHECK(qty>0)`）、`basis_price`（等于结算日的结算价）。
- **`sod_prices`**：
  - 主键：`(trading_day, instrument_id, version)`；外键 `fk_sod_prices__sod_versions`：`(trading_day, version)` 引用 `sod_versions`；
  - 其他列：`pre_settle`（DEC_PRICE）。

risk 库中的同名表是这三张表的**副本**，两边的对应关系如下：
- 本库主键中的 `version`，在副本中叫 `sod_version`，并且不在主键里（每个交易日副本中只保存当前生效的一个版本，重新定基时整体替换）；
- 副本多出 `loaded_ns` 和 `provisional` 两列；
- 本库的 `created_ns` 不复制到副本。

#### `recon_checks`：对账检查结果

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `check_id` | INT | 否 | PK，自增 | 记录号 |
| `trading_day` / `version` | TD / INT | 否 | `ix_recon_checks__run` | 结算运行 |
| `account_id` | ID | 是 | | 公司层检查为 null |
| `check` | ENUM(ReconCheck) | 否 | | 检查项 |
| `status` | ENUM(CheckStatus) | 否 | | 检查结果 |
| `expected` / `actual` / `diff` | TEXT | 是 | | 用 DecStr 或计数表示 |
| `detail` | JSON | 否 | | 明细 |
| `created_ns` | TS | 否 | | 创建时刻 |

#### `reports`：报表索引

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `report_id` | UUID | 否 | PK | 报表 |
| `trading_day` | TD | 是 | `ix_reports__td_type` | 回测报表为 null |
| `type` | ENUM(ReportType) | 否 | 同上 | 报表类型 |
| `account_id` | ID | 是 | | 结算单等账户级报表才有 |
| `version` | INT | 是 | | 对应的结算版本；回测报表（`BACKTEST_HTML`）为空 |
| `path` | TEXT | 否 | | 相对于 `<home>` 的路径（§5.3） |
| `sha256` | HASH | 否 | | 文件哈希 |
| `size_bytes` | INT | 否 | | 文件大小 |
| `created_ns` | TS | 否 | | 生成时刻 |

#### `backtest_runs`：回测作业

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `run_id` | UUID | 否 | PK | 作业 |
| `name` | TEXT | 否 | | 回测配置名 |
| `config_hash` / `data_hash` | HASH | 否 | | 配置 / 数据的哈希，用于重现 |
| `seed` | INT | 否 | | 随机种子 |
| `git_sha` | TEXT | 否 | | 代码版本；没有 git 仓库时写 `nogit`。作业的输出目录为 `<home>/backtests/{run_id}/`（§5.2） |
| `state` | ENUM(BacktestState) | 否 | | 作业状态 |
| `progress` | REAL | 否 | 默认 0 | 进度 |
| `submitted_by` | TEXT | 否 | | 提交人 |
| `submitted_ns` / `started_ns` / `finished_ns` | TS | 否 / 是 / 是 | | 提交 / 开始 / 结束时刻 |
| `report_id` | UUID | 是 | 逻辑引用 `reports`（无外键） | 回测报表 |
| `summary` | JSON | 是 | | 两组对照的关键结果（与 `BacktestStatus.summary` 相同） |
| `error` | TEXT | 是 | | 失败原因 |

### 4.5 api.db（api-gateway）

#### `api_keys`：API key 与角色

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `key_id` | ID | 否 | PK | key 的标识：具名 key 为 `app.yaml` 中的 `name`，bootstrap key 为 `bootstrap`；也是审计中的操作者 |
| `key_hash` | HASH | 否 | `uq_api_keys__key_hash` | API key 的 SHA-256，**不保存明文** |
| `name` | TEXT | 否 | | 持有人或用途 |
| `role` | ENUM(Role) | 否 | | 角色 |
| `created_ns` / `expires_ns` | TS | 否 / 是 | | 创建 / 过期时刻（本机时间） |
| `disabled` | BOOL | 否 | 默认 0 | 是否停用：从 `app.yaml` 删除或不可用（环境变量缺失、过短、与其他 key 重复）的具名 key，以及配置了可用的具名 ADMIN key 之后的 bootstrap key。api-gateway 启动时和每次提交新配置时按 `app.yaml` 同步本表（11 §2） |

#### `api_audit`：接口访问审计（只追加）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `audit_id` | INT | 否 | PK，自增 | 记录号 |
| `ts_ns` | TS | 否 | `ix_api_audit__ts` | 时刻。每个 REST 请求一行，每个 WebSocket 会话一行（`method=WS`、`path=/ws`） |
| `key_id` / `role` | ID / ENUM(Role) | 是 | | 调用者；匿名访问（如 `/health`）或认证失败时为 null |
| `method` / `path` | TEXT | 否 | | HTTP 方法 / 路径 |
| `command` / `command_id` / `target` | TEXT | 是 | | 转发到总线的命令、命令 ID 和对象 |
| `http_status` / `result_code` | INT / TEXT | 否 | | HTTP 状态码 / 结果码 |
| `latency_us` | INT | 否 | | 耗时 |
| `client_ip` | TEXT | 否 | | 客户端地址 |

#### `notifications`：通知去重与发送状态

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `notify_key` | TEXT | 否 | PK(notify_key, channel) | 取值规则见 02 §9.8。与告警相关的事件用 `{alert_key}\|{kind}`，同一告警的触发、解除、确认、重置各通知一次 |
| `channel` | TEXT | 否 | 同上 | 通知渠道，如 `wecom`、`dingtalk`、`feishu` |
| `kind` / `severity` | TEXT / ENUM(Severity) | 否 | | 事件类别 / 严重程度 |
| `status` | ENUM(NotificationStatus) | 否 | | 发送状态 |
| `attempts` | INT | 否 | 默认 0 | 已尝试次数 |
| `last_error` | TEXT | 是 | | 最近一次失败原因 |
| `payload` | JSON | 否 | | 发出的消息体 |
| `first_seen_ns` / `sent_ns` | TS | 否 / 是 | | 首次收到 / 发送成功的时刻 |

#### `config_versions`：配置版本与两阶段提交记录

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `config_version` | INT | 否 | PK | 单调递增 |
| `config_hash` | HASH | 否 | | 配置哈希（算法见 02 §7.10） |
| `path` | TEXT | 否 | | 不可变的版本快照目录 `config/versions/{N}/` |
| `files` | JSON | 否 | | 文件名 → sha256 |
| `effective` | JSON | 否 | | 各配置段的生效时点；`NEXT_TRADING_DAY` 类还要记录生效的交易日，与 `config/active.json` 中的内容相同 |
| `status` | ENUM(ConfigStatus) | 否 | | 提交状态（02 §7.3）。先持久化为 `COMMITTING`，再下发 commit（02 §8.5） |
| `acks` | JSON | 否 | 默认 `{}` | 各实例的 prepare 回复：instance → `{ok, code, errors, effects}` |
| `requested_by` | TEXT | 否 | | 发起人 |
| `requested_ns` / `finished_ns` | TS | 否 / 是 | | 发起 / 结束时刻 |
| `error` | TEXT | 是 | | 失败原因 |

#### `kill_switch`：人工 kill switch 的期望状态（单行表，版本 2 新增）

`POST /risk/firm/kill-switch` 先把期望状态和各目标的确认情况写入这里，再向每个风控分片和 trade-gateway 发送 `firm.kill_switch`（02 §6.3）。新命令会取代尚在重试的旧命令；可以重试的应答（`TIMEOUT`、`UNAVAILABLE`、`BUSY`、`RISK_BUSY`）按固定间隔用**当前**期望状态重试，其他应答或重试用尽时该目标记为 `FAILED` 并发出 CRITICAL 通知；api-gateway 启动时向尚未确认（`PENDING` 或 `FAILED`）的目标重新推送。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `id` | INT | 否 | PK，`CHECK(id=1)` | 固定为 1 |
| `desired_on` | BOOL | 否 | 默认 0 | 期望开启还是关闭 |
| `command_id` | TEXT | 否 | | 这次期望状态的命令 ID；重试沿用它，目标据此去重 |
| `reason` / `actor` | TEXT | 否 | | 原因 / 操作者 |
| `role` | ENUM(Role) | 否 | | 操作者的角色 |
| `requested_ns` / `updated_ns` | TS | 否 | | 发起 / 更新时刻 |
| `targets` | JSON | 否 | 默认 `{}` | 目标实例 → `{status, code, message, attempts, updated_ns}`，`status` 为 `PENDING`、`CONFIRMED` 或 `FAILED` |

通用表：
- `processed_commands`：用于 REST 请求头 `Idempotency-Key`；
- `fact_cursors`：通知服务消费 `risk.event`、`md.event`、`sys.eod`、`sys.service` 的游标，保证重启后不漏发、不重发。

### 4.6 supervisor.db（supervisor）

#### `service_events`：进程事件（`sys.service` 的业务表，也是日报"系统运行"一节的数据源）

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `event_id` | INT | 否 | PK，自增 | 记录号 |
| `ts_ns` | TS | 否 | `ix_service_events__ts` | 时刻 |
| `trading_day` | TD | 是 | | 交易日 |
| `instance` / `service` | ID | 否 | | 实例 / 服务 |
| `pid` | INT | 是 | | 进程号 |
| `from_state` / `to_state` | ENUM(ProcessState) | 否 | | 迁移前 / 迁移后的状态 |
| `exit_code` | INT | 是 | | 退出码 |
| `restarts` | INT | 否 | | 当日累计重启次数 |
| `reason` | ENUM(ServiceChangeReason) | 否 | | 原因（02 §7.3）；版本 2 起允许 `GAVE_UP`、`SUPERVISOR_RESTART` |
| `actor` | TEXT | 是 | | 人工操作（`svc.*`）的操作者，作为审计 |
| `env_keys` | JSON | 是 | | 该实例生效的环境变量覆盖的**名字**列表（不记录取值），来自 `svc.start` / `svc.restart` 的 `env` 或 `futrisk up --svc-env`（版本 2 新增） |
| `detail` | TEXT | 是 | | 给人看的原因说明，例如端口冲突、为什么暂停拉起某个服务（版本 2 新增） |

#### `clock_state`：市场时钟（单行表）

市场时钟的偏移只保存在这里，由 supervisor 通过 `sys.clock` 下发（02 §8.8）。

| 列 | 类型 | 可空 | 约束 | 说明 |
|---|---|---|---|---|
| `id` | INT | 否 | PK，`CHECK(id=1)` | 固定为 1 |
| `mode` | ENUM(ClockMode) | 否 | | `REAL` 或 `DEMO` |
| `offset_ns` | INT | 否 | `ck_clock_state__real_offset`：`CHECK(mode='DEMO' OR offset_ns=0)` | 市场时间减本机时间；`REAL` 模式下为 0 |
| `version` | INT | 否 | | 每次跳转加 1 |
| `updated_ns` | TS | 否 | | 更新时刻（本机时间） |
| `actor` / `reason` | TEXT | 是 | | 跳转的操作者 / 原因 |

### 4.7 sim.db（sim-exchange，外部模拟组件）

sim.db 不属于 futrisk 的服务，只是为了让模拟柜台和价格引擎像真实环境一样"重启不丢状态"。有了它，`broker_seq` 和 `trade_id` 在重启后会接着之前的继续，不会与 td.db 中已有的记录冲突（02 §9.4）。sim-exchange 通过一个写线程访问它（`BEGIN IMMEDIATE`），`journal_mode=WAL`、`synchronous=NORMAL`；表定义在 `futrisk.sim.exchange.store`，下表只列主要列。

| 表 | 主要列 | 说明 |
|---|---|---|
| `sim_meta` | `id`（固定为 1）、`trading_day`、`last_broker_seq`、`last_trade_no`、`last_order_no`、`last_session_id`、`md_epoch`、`md_rng_td`、`md_step`、`updated_ns` | 单行表：柜台回报序号（按交易日）、成交编号、委托编号和会话号的计数器（重启后不重复），以及行情侧的 epoch 和随机数流的位置 |
| `sim_orders` | `order_sys_id` PK、`trading_day`、`front_id`、`session_id`、`order_ref`、`client_order_id`、`investor_id`、`instrument_id`、`exchange_id`、`direction`、`comb_offset_flag`、`order_price_type`、`limit_price`、`volume_total_original`、`volume_traded`、`order_status`、`status_msg`、`time_condition`、`volume_condition`、`origin`、`pending_match`、`last_broker_seq`、`insert_ns`、`updated_ns` | 柜台侧的委托。`pending_match=1`（版本 2）表示还没做过第一次撮合，重启后这样的委托按对手价撮合，不直接挂单 |
| `sim_trades` | `trade_id` PK、`trading_day`、`order_sys_id`、`broker_seq`、`front_id`、`session_id`、`order_ref`、`client_order_id`、`investor_id`、`instrument_id`、`exchange_id`、`direction`、`offset_flag`、`price`、`volume`、`trade_ts_ns` | 柜台侧的成交 |
| `sim_positions` | `investor_id`、`instrument_id`、`posi_direction`（联合主键，`LONG` / `SHORT`）、`today_position`、`yd_position`、`updated_ns` | 柜台侧的持仓，`ReqQryInvestorPosition` 的数据源 |
| `sim_md_state` | `instrument_id` PK、`trading_day`、`state`（JSON）、`updated_ns` | 价格引擎每个合约的当日累计状态，在每一轮行情发布**之前**写入。所以 sim-exchange 在交易日内重启后，累计成交量和成交额仍然单调递增，md-gateway 的水位（§4.1）不会挡住新行情；Feed A 仍然换新的 epoch、环形缓冲清空 |

---

## 5. 文件型存储

### 5.1 tick 文件（Parquet）

| 文件 | 路径（`<home>` 下） | 写者 | 内容与写入方式 |
|---|---|---|---|
| 原始 tick 分片 | `ticks/raw/{trading_day}/{source 小写}/part-{epoch_ms}-{n}.parquet` | md-gateway；`futrisk data record`（只写 `sina`） | 每个收到并归一化的状态一行，带最终的 `mode`（`LIVE`、`BACKFILL`、`DUPLICATE`、`REJECTED`）和拒收原因；无法解析的记录只有标识列，行情列为空。被跳过的轮询记录和订阅快照不记录：收到时合约不在交易时段内、记录仍停留在上一交易日，或者时段已开而交易所时间落在该交易日所有交易时段之外且不晚于接收时刻（还没有更新的陈旧快照，例如 CFFEX 09:25 集合竞价期间）。目录的 `trading_day` 按**接收时刻**所属的交易日划分。每个数据源一个写者，每 5 s 或满 10k 行关闭一个分片：先写同目录下的隐藏临时文件，flush、fsync 后原子改名，读者只读已关闭的分片。`epoch_ms` 是写者启动的时刻，`n` 是六位计数，文件名按时间排序，重启后也不会冲突 |
| 备用目录 | `ticks/spill/{trading_day}/{source 小写}/…` | 同上 | 主目录连续 3 次写入失败，或停机时最后一次写入失败，分片就写到这里，日志提示修好主目录后把文件移回。`futrisk data record` 还会再试系统临时目录下的 `futrisk-spill`。合并规范 tick 时也读取这里的分片 |
| 原始 HTTP 应答 | `ticks/wire/{trading_day}/sina/wire-{epoch_ms}-{n}.parquet` | `futrisk data record`（`--wire`，默认开启） | 每次轮询一行：`recv_ns`、`sent_ns`、`poll_seq`、`elapsed_ms`、`http_status`、`error`、`codes`、`body`（未改动的应答字节），供日后重新解析。md-gateway 不写 |
| 规范 tick | `ticks/canonical/{trading_day}.parquet` | md-gateway | 去重后的当日行情状态。交易日最后一次收盘之后 120 s 自动生成；`md.settle_inputs` 请求时先确认它是最新的（§4.1）。参与合并的原始分片（含备用目录，以及下一交易日目录中迟到的补数）列表有变化时重新生成 |

原始分片和规范 tick 的列（与 02 §7.1 的 `Tick` 字段同名）：

| 列 | Arrow 类型 | 说明 |
|---|---|---|
| `instrument_id`、`exchange`、`trading_day`、`action_day` | string（字典编码；`exchange` 可空） | 同 `Tick` |
| `exch_ts_ns`、`recv_ns` | int64（`exch_ts_ns` 可空） | 同 `Tick` |
| `last`、`bid1`、`ask1`、`open`、`high`、`low`、`upper_limit`、`lower_limit`、`pre_settle`、`pre_close`、`avg_price`、`turnover` | float64（可空） | 同 `Tick` |
| `volume`、`open_interest`、`bid_vol1`、`ask_vol1`、`pre_open_interest` | int64（可空） | 同 `Tick` |
| `source` | string（字典编码） | `SourceId` |
| `source_seq` | int64（可空） | 数据源序号 |
| `epoch` | string（可空） | Feed A 的 epoch |
| `fingerprint` | uint64（可空） | 指纹 |
| `flags` | int32（可空） | 位标志 |
| `mode` | string（字典编码） | `TickMode`（02 §7.3） |
| `reject_reason` | string（可空） | 拒收原因码（02 §11.3） |

"可空"指无法解析的记录（只有标识列）。文件压缩用 zstd，Parquet 元数据带 `futrisk.kind`（`ticks.raw` 或 `ticks.wire`）和 `futrisk.schema_version`（当前为 `1`）；规范 tick 文件另带 `futrisk.sources`：生成时所用原始分片的名称和大小，用来判断是否需要重新生成。

**规范 tick 的合并规则**：
- 只取 `LIVE` 和 `BACKFILL` 行，并且行的 `trading_day` 等于该交易日；
- 按 `(instrument_id, fingerprint, exch_ts_ns)` 去重，保留的优先次序为：LIVE 优先，其次时间戳精确（新浪是秒级时间戳，排在后面），再次数据源优先级高（`feeds.yaml` 的 `priority` 数字小），最后接收早；
- 同一合约、同一指纹已经有精确时间戳的状态时，丢弃秒级时间戳的行；
- 结果按 `(instrument_id, volume, exch_ts_ns)` 排序，也就是 `order_key` 的顺序。

md-gateway 重启后，最后一次持久化的游标（§4.1 `feed_cursors`）之后写入的行会被重放，所以原始分片中可能出现同一个状态两次；规范 tick 的去重会消除它们。

**用途**：
- EOD 时从规范 tick 重新计算 `settle_inputs`（§4.1）；
- 事后审计和补数统计；`futrisk data summary` 按 `mode` 统计录制的新浪 tick。

### 5.2 历史数据与回测输出

历史数据由开发期脚本 `scripts/fetch_hist.py` 生成，运行时只读；回测从 `data.dir`（默认为应用目录下的 `data/hist`，11 §10）读取，并把读到的每个文件的哈希记进 `manifest.json`。

| 文件 | 路径 | 内容 |
|---|---|---|
| 主力连续日线 | `data/hist/daily/{product}.parquet` | 每个交易日一行：当天的主力合约 `instrument_id` 及其 `open`、`high`、`low`、`close`、`settle`，`pre_settle`（上一行的结算价）、`contract_pre_settle`（同一合约的上一结算价）、`volume`、`open_interest`、`roll`（是否换月日）、`settle_source`、`instrument_source`、`sina_instrument_id` |
| 逐合约日线 | `data/hist/daily/{EXCHANGE}/{product}.parquet` | 列：`trading_day`、`instrument_id`、`open`、`high`、`low`、`close`、`settle`、`pre_settle`、`volume`、`open_interest`、`turnover`、`is_main`、`settle_source`。SHFE、CFFEX 的数据来自交易所，带每个合约的结算价和昨结算，主力为前一交易日持仓量最大的合约（并且不回到更早的交割月）；DCE 的数据来自新浪，主力是新浪连续合约背后的合约，只有主力行带结算价，2024-07-17 之后只有主力合约 |
| 分钟线 | `data/hist/minute/{product}_{period}m.parquet` | 新浪 5/15/60 分钟线，只作 `SOURCES.md` 中数据校验的依据，回测不读取 |
| 官方数据 | `data/hist/official/{EXCHANGE}.parquet` | 最近若干个交易日的交易所官方逐合约数据，回测不读取 |
| 数据来源说明 | `data/hist/SOURCES.md`、`data/hist/manifest.json` | 抓取时间、所用接口、换月识别规则、已知缺陷；各文件的行数、日期范围、SHA-256 和抓取失败记录 |

回测的输出目录：clearing-worker 的回测作业为 `backtests/{run_id}/`（另有记录进度的 `progress.json`，报表登记在 `reports` 表中，类型 `BACKTEST_HTML`）；离线命令 `futrisk backtest run` 默认为 `backtests/{name}-{YYYYMMDDTHHMMSS}/`（北京时间，`--out` 可以改）。目录中的文件：

| 文件 | 内容 |
|---|---|
| `report.html` | 自包含的 HTML 回测报告 |
| `daily.csv` | 每个方案、账户、交易日一行：结算数字（权益、日盈亏、手续费、保证金、风险度、净值）、各级别是否触发及触发值、是否熔断及是否锁存、被拒的开仓和加仓、是否换月日、是否用了估计的结算价；UTF-8 带 BOM |
| `events.csv` | 列：`run`、`arm`、`tier`、`threshold`、`trading_day`、`ts_ns`、`ts`、`account_id`、`kind`、`rule_id`、`level`、`action`、`severity`、`latched`、`value`、`threshold_value`、`alert_key`、`episode`、`step`、`message`；UTF-8 带 BOM |
| `orders.csv` | 每笔委托：方案、交易日、时刻、账户、`client_order_id`、合约、方向、开平、手数、价格、用途、是否受理、应答码、剧本步骤；UTF-8 带 BOM |
| `summary.json` | 各方案各账户的统计、相对基线的差值、规则生效日明细、一致性检查（10 §2.5） |
| `manifest.json` | 回测配置及其哈希 `config_hash`、基础配置文件的哈希、`data_hash`（及读到的每个数据文件的哈希）、`seed`、`git_sha`、代码哈希、软件版本、各输出文件的 SHA-256；作业的 `config_hash`、`data_hash`、`seed`、`git_sha` 与 `backtest_runs` 表一致 |

### 5.3 报表文件（由 clearing-worker 生成，登记在 `reports` 表中）

| 类型（`ReportType`） | 路径 | 说明 |
|---|---|---|
| `RISK_DAILY_HTML` | `reports/{trading_day}/risk_daily.html` | 风控日报，自包含 HTML（图片 base64 内嵌），可以打印成 PDF |
| `STATEMENT_TXT` | `reports/{trading_day}/statement_{account_id}.txt` | CTP 风格的文本结算单：资金状况、成交记录、平仓明细、持仓明细、持仓汇总 |
| `STATEMENT_CSV` | `reports/{trading_day}/statement_{account_id}.csv` | 结算单的结构化数据，UTF-8 带 BOM |
| `SETTLEMENT_JSON` | `reports/{trading_day}/settlement.json` | 当日完整的结算结果（同 `SettlementResult`，含取默认值的字段），供外部系统对接 |
| `BACKTEST_HTML` | `backtests/{run_id}/report.html` | 见 §5.2 |

- 文件先写到唯一命名的临时文件再改名；文件名中的账户 ID 只保留 `[A-Za-z0-9_.-]`，替换过字符时加一个短哈希后缀，两个账户不会共用一个文件。
- 结算产生新版本时，报表覆盖到同名文件，同时在 `reports` 表中新增一行（`version` 更大），旧文件的哈希保留在旧行中作为审计依据。同一版本重新生成（例如报表失败后重试）时，按 `(trading_day, version, type, account_id, path)` 找到原来的行，内容变了才更新它的哈希、大小和时间，`report_id` 不变。

---

## 6. 跨库引用与一致性
不同库之间没有外键（DB-09），一致性靠逻辑键加对账来保证。

| 逻辑键 | 源头（库.表） | 引用方 | 一致性保证 |
|---|---|---|---|
| `client_order_id` | risk.`orders` | td.`routed_orders`、td.`trades`、risk.`fills`、clearing.`trades` | 委托先在 risk 库落库，再路由。risk-engine 重启时，把 `ROUTING` 和 `UNKNOWN` 状态的委托用 `route.resolve` 确认，trade-gateway 从没收到过的写进 `route_tombstones`（02 §8.3）。外部成交的 `client_order_id` 为空 |
| 成交键 `(trading_day, exchange, trade_id, direction)` | td.`trades` | risk.`fills`、clearing.`trades`、clearing.`trade_settlements`、clearing.`close_details` | 三处都有唯一约束；EOD 时做三方摘要核对（`TRADE_DIGEST`，02 §8.4 第 3 步）；盘中定时做柜台持仓对账（02 §8.7） |
| 流 ID `stream_id` | 各库的 `db_meta` | 各订阅方 `fact_cursors.stream` | 流 ID 变化，说明发布方的库是新建的或者从备份恢复过。订阅方重置游标，并用领域查询核对（02 §8.2） |
| 日初快照版本 `(trading_day, version)` | clearing.`sod_versions` | risk.`sod_*`（`sod_version`）、risk.`eod_states.sod_version` | 换日时，各分片加载 `eod.rollover` 中指定的版本；只有在没有本地快照的重启场景，才取已推送（`pushed_ns` 非空）的最新版本。推送更新的版本时重新定基 |
| `movement_id` | risk.`cash_movements` | clearing.`cash_movements` | 由 FACT 流保证不丢；EOD 第 3 步按账户核对笔数和金额合计（`CASH_DIGEST`） |
| `alert_key` | risk.`alerts` | risk.`risk_events`、risk.`account_states`、risk.`liquidation_plans`、api.`notifications`（作为 `notify_key`） | 主键加 `ON CONFLICT DO NOTHING`；通知按 `(notify_key, channel)` 去重 |
| `gap_id` | md.`gaps` | md.`md_events`（risk.`retro_breaches` 预留，没有写入方） | 格式由 02 §7.1 统一规定 |
| 结算版本 `(trading_day, version)` | clearing.`eod_runs` | clearing 的各结算表、risk.`sod_*`（`sod_version`）、risk.`eod_states` | risk 库只接受 `eod.rollover` 中指定的版本 |
| `config_version` / `config_hash` | api.`config_versions` | 各服务心跳、risk.`rule_states`、risk.`orders`、clearing.`eod_runs`、clearing.`config_history` | 两阶段提交；提交后 30 s 内各实例心跳必须报告新版本，否则记为 `FAILED` 并告警（02 §8.5）；仪表盘显示各实例的哈希 |
| `report_id` | clearing.`reports` | clearing.`backtest_runs`、api-gateway（通过 `report.get`） | — |
| `instance` | `config/services.yaml` | supervisor.`service_events`、各库 outbox 的来源、`fact_cursors.src`、td.`routed_orders.src_instance` | 以配置为准 |
| kill switch 状态 | 人工操作由 api-gateway 扇出下发；公司级规则由 coordinator 发出 | risk.`firm_controls`（每个分片一份）、td.`firm_controls` | 每个目标独立持久化；api-gateway 逐一确认下发结果 |
| `account_id`、`instrument_id` | `config/accounts.yaml`、`config/products.yaml` | 所有库 | 写入时由应用层校验 |

**对账检查**：与柜台的持仓对账在 risk-engine 启动的预热阶段和交易时段内定期进行，日终对账在 EOD 时进行。

| 检查 | 位置 | 失败时的处理 |
|---|---|---|
| 成交三方摘要（`TRADE_DIGEST`）、出入金核对（`CASH_DIGEST`） | EOD 第 3 步 | 补拉数据并重新 `eod.freeze`，最多 3 次仍不一致：EOD `FAILED` |
| 资金恒等式（`EQUITY_IDENTITY`）、持仓恒等式（`POSITION_IDENTITY`） | EOD 第 5 步 | `FAIL`：不换日 |
| 交易所公式（`EXCH_FORMULA`）、逐日盯市与逐笔对冲对比（`MTM_VS_FIFO`） | EOD 第 5 步 | `FAIL`：不换日 |
| 日内状态与结算结果对比（`INTRADAY_VS_SETTLE`） | EOD 第 5 步 | 差异超过"价格差异 × 持仓"的解释范围时记为 `WARN` |
| 各账户合计等于公司汇总（`FIRM_SUM`） | EOD 第 5 步 | `FAIL`：不换日 |
| 与柜台的持仓核对（`recon_breaks`） | risk-engine 预热阶段一次，交易时段内每 `system.recon_interval_s`（默认 300 s，11 §6）一次（02 §8.7） | 连续两次不一致：账户禁开。自动补拉成交后复核一致的，自动处理为 `RESYNCED` 并解除；否则由人工处理 |

---

## 7. 数据访问与事务边界
每个库只有所属服务的写库线程可以写入，读取也只在所属服务内部进行。下表列出主要事务，同一行中的表**在同一个事务里**提交。

| 库 | 事务 | 包含的表 | 触发时机 |
|---|---|---|---|
| md | 行情事件 | `md_events`（+ `gaps`）+ `outbox`（`md.event`） | 数据源状态迁移、行情质量变化、缺口开合、坏点、交叉校验 |
| md | 人工控制 | `processed_commands` + `md_events`（含 `actor`）+ `outbox`（`md.event`） | `md.source.set` |
| md | 游标 | `feed_cursors` | 每秒检查一次，只推进原始 tick 已全部写进分片的数据源（§4.1） |
| md | 水位 | `watermarks` | 每 `arbitration.watermark_flush_s`（默认 1 s）写一次有变化的水位 |
| md | 结算输入 | `settle_inputs` | 收到 `md.settle_inputs` 请求时（先写出原始 tick、按需重建规范 tick；内容与最新版本相同的合约不写） |
| td | 路由受理 | `routed_orders`（新建；到达时已过期或被 kill switch 拒绝的，状态直接为 `EXPIRED`） | `route.order` |
| td | 撤单 | `processed_commands` + `routed_orders`（更新），或者 `route_tombstones`（从没收到过的委托） | `route.cancel` |
| td | 路由确认 | `route_tombstones`（只在从没收到过时写入） | `route.resolve` |
| td | 柜台回报 | `broker_order_events` + `routed_orders`（更新）+ `trades` + `broker_cursors` + `outbox`（`td.exec.*`） | 每条回报，或者补回报的每一批 |
| td | 导入成交 | `processed_commands` + `trades`（`origin=EXTERNAL`）+ `outbox`（`td.exec.*`） | `td.import_trades` |
| td | kill switch | `processed_commands` 或 `fact_cursors` + `firm_controls` | `firm.kill_switch`、`risk.control.firm` |
| risk | 委托意图 | `orders` + `order_events` + `outbox`（`oms.order`） | `order.submit`，包括被拒的委托 |
| risk | 路由结果 | `orders`（更新）+ `order_events` + `outbox`（`oms.order`） | `RouteAck`、路由超时转 `UNKNOWN`、`route.resolve` 的结果 |
| risk | 撤单 | `processed_commands` + `orders` + `order_events` + `outbox`（`oms.order`） | `order.cancel`、`order.cancel_all`、熔断撤单 |
| risk | 应用执行回报 | `orders`（更新）+ `order_events` + （成交时再加 `fills`）+ `fact_cursors`（`td.exec.*`）+ `outbox`（`oms.order`，外部成交再加 `risk.event`） | 每条 `td.exec` 消息 |
| risk | 规则迁移 | `rule_states` + `alerts` + `risk_events` + `account_states` + `audit_events` + `outbox`（`risk.event`、`risk.state`），必要时加 `liquidation_plans` | 状态机迁移（02 §8.1 的第 1 步） |
| risk | 公司级控制 | `firm_controls` + `account_states` + `outbox`（coordinator 另加 `risk.control.firm`）+ `fact_cursors` 或 `processed_commands` | 公司级规则触发、`firm.kill_switch`、应用 `risk.control.firm` |
| risk | 人工命令 | `processed_commands` + 相应的业务表 + `audit_events` + `outbox` | `risk.reset`、`risk.halt`、`risk.ack_alert`、`risk.liquidation.confirm`、`risk.recon.resolve`、`eod.force_unfreeze` |
| risk | 对账异常 | `recon_breaks` + `risk_events` + `outbox`（`risk.event`），`blocking=1` 时再加 `account_states` 和 `outbox`（`risk.state`） | 持仓对账不一致、重放区间缺失、终态委托收到成交 |
| risk | 出入金 | `cash_movements` + `processed_commands` + `audit_events` + `outbox`（`risk.cash`） | `cash.move` |
| risk | EOD 冻结 | `eod_states` | `eod.freeze` |
| risk | 临时快照 | `sod_*`（`sod_version=-1`，`provisional=1`）+ `eod_states`（`PROVISIONAL`，必要时先写入冻结）+ `rule_states`（重置）+ `alerts`（改为 `ROLLED`）+ `liquidation_plans`（尚未确认的改为 `EXPIRED`）+ `account_states` + `audit_events`（`PROVISIONAL_SOD`）+ `outbox`（`sys.dayroll`、`risk.state`） | 下一交易日的第一个交易时段开始时，生效的日初快照仍属于上一交易日（02 §8.4） |
| risk | 换日或重新定基 | `sod_*`（整体替换）+ `eod_states` + `rule_states`（换日时重置；从临时快照重新定基时已经换过日，不再重置）+ `alerts`（换日时把状态改为 `ROLLED`）+ `liquidation_plans`（换日时，尚未确认的改为 `EXPIRED`；重新定基时不动）+ `account_states` + `audit_events`（`DAYROLL` / `DAYROLL_REBASE`）+ `outbox`（`sys.dayroll`、`risk.state`） | `eod.rollover` |
| risk | 指标样本 | `metric_samples` | 异步批量写入 |
| clearing | 副本 | `trades` 或 `cash_movements` + `fact_cursors` | 每条 FACT 消息 |
| clearing | 结算价导入与覆盖 | `processed_commands` + `official_prices` 或 `price_overrides` | `settle.import_official`、`settle.override_price` |
| clearing | 结算提交 | `settlement_prices` + `account_settlements` + `position_settlements` + `margin_groups` + `trade_settlements` + `close_details` + `recon_checks` + `sod_versions` + `sod_accounts` + `sod_lots` + `sod_prices` + `eod_runs` + `outbox`（`clearing.settled`、`sys.eod`） | EOD 第 5 步，**单个事务** |
| clearing | 建账 | `processed_commands` + `sod_versions`（`source=SEED`）+ `sod_accounts` + `sod_lots` + `sod_prices` | `settle.seed_sod` |
| clearing | 推送完成 | `sod_versions.pushed_ns` + `eod_runs` + `outbox`（`sys.eod`） | 所有分片都确认 `eod.rollover` |
| clearing | 阶段推进 | `eod_runs` + `outbox`（`sys.eod`） | EOD 的其他阶段 |
| clearing | 报表 | `reports` + `outbox`（`report.ready`） | 每份报表 |
| clearing | 回测作业 | `processed_commands` + `backtest_runs`（状态更新另起事务） | `backtest.run` 以及作业进度 |
| api | 通知 | `notifications` + `fact_cursors` | 每条要通知的事件 |
| api | 配置版本 | `config_versions` | 两阶段提交的每一步（02 §8.5） |
| supervisor | 进程事件 | `service_events` + `outbox`（`sys.service`） | 进程状态迁移、`svc.*` |
| supervisor | 时钟跳转 | `processed_commands` + `clock_state`；提交后发布 `sys.clock`（LV，不经过 outbox） | `svc.clock` |

---

## 8. 数据生命周期、备份与恢复

| 数据 | 保留期 | 清理方式 |
|---|---|---|
| 原始 tick（`ticks/raw/`） | 30 个自然日 | md-gateway 维护时删除早于"结算日 − 30 天"的交易日目录 |
| 规范 tick（`ticks/canonical/`） | 365 个自然日 | 同上，删除文件 |
| 备用目录（`ticks/spill/`）、原始 HTTP 应答（`ticks/wire/`） | 不自动清理 | 手工处理 |
| md.`settle_inputs` | 永久 | — |
| md.`md_events`、md.`gaps` | 90 个自然日 | md-gateway 维护时分批删除；人工 `md.source.set` 产生的事件（人工排除的持久化记录）永久保留；`gaps` 只删除已关闭、并且没有被任何事件引用的行 |
| md.`feed_cursors`、md.`watermarks` | 当前值 | 原地更新 |
| td.`routed_orders`、td.`trades`、td.`route_tombstones` | 永久（审计） | — |
| td.`broker_order_events` | 90 个自然日 | 分批删除 |
| risk 的委托、成交、出入金、告警、风险事件、审计、对账异常、状态机、账户状态、公司级控制、减仓计划和 EOD 状态 | 永久 | — |
| risk.`metric_samples` | 90 个自然日 | 分批删除 |
| 所有库的 `processed_commands` | 当前交易日和前一个交易日：维护时删除早于结算日 T 的记录；md、td、clearing 库多保留 T 的前一交易日 | 维护时删除 |
| 有 outbox 的库的 `outbox` | 已发布且超过 7 天（按本机时间，**重放窗口为 7 天**） | 分批删除：td、supervisor 按 `services.yaml` 的 `bus.replay_window_days`，md、risk、clearing 固定为 7 天。超出窗口的请求按 02 §4.6 的规定回退到领域查询。序号计数在 `outbox_topics` 中，**永不清理**，所以清理 outbox 不会让序号回退 |
| clearing 的成交和出入金副本、结算、日初快照、对账、报表索引、回测作业、配置历史 | 永久 | — |
| api.`api_audit` | 180 天（按 `ts_ns`） | api-gateway 维护时删除 |
| api.`notifications` | 90 天（按 `first_seen_ns`） | 同上 |
| api.`api_keys`、`config_versions`、`kill_switch` | 永久 | — |
| supervisor.`service_events` | 90 天 | 分批删除 |
| supervisor.`clock_state` | 当前值 | — |
| 每日备份（`<home>/backup/{trading_day}/`） | 最近 20 个交易日目录（每个库各自计算） | md、td、risk、clearing、supervisor 维护时删除自己库的旧备份，目录空了再删目录；**api.db 的备份不清理** |
| 日志文件（`<home>/logs/`） | 30 天（`app.yaml` 的 `logging.retention_days`） | loguru 按天滚动（单个文件超过 `logging.rotation_mb`，默认 100MB，再切分），旧文件压缩为 zip，超期自动删除 |

保留期是代码中的常量；可以配置的只有日志（`app.yaml` 的 `logging`），以及 td、supervisor 的 outbox 重放窗口。

- **维护**：每个交易日 `sys.eod` 到达 `DONE` 之后（每个交易日只做一次），各服务对自己的库执行下面几步。写操作都是写库线程上的短事务，分批进行，不阻塞业务主循环：
  1. checkpoint：`PRAGMA wal_checkpoint(TRUNCATE)`。risk 库改用 `PRAGMA wal_checkpoint(PASSIVE)`（不等待读者、不阻塞写入，库忙时跳过）；
  2. 备份：`VACUUM INTO '<home>/backup/{trading_day}/{db}.db.tmp'`，完成后原子改名为 `.db`。`VACUUM INTO` 在目标文件已经存在时会失败，所以先删除残留的临时文件；同一交易日再次备份时，覆盖之前的结果；
  3. 按保留期分批清理（md-gateway 还删除过期的 tick 目录）；
  4. 删除超过 20 个交易日的旧备份（只删本服务的库文件）。

  api-gateway 的顺序是先清理再备份（备份前做 checkpoint），并且不做第 4 步。risk-engine 的整个维护只在没有交易时段开着、10 分钟内也没有时段开始时运行，否则每 60 s 再检查一次；其他服务收到 `DONE` 就执行（08 §5.2 KD-22）。
- **恢复**：没有专门的恢复命令，按以下步骤手工操作：
  1. 停止对应的服务；
  2. 用备份文件替换库文件，并删除同名的 `-wal`、`-shm` 文件；
  3. **重新生成 `db_meta.stream_id`**：调用 `futrisk.db.regenerate_stream_id`（可以同时记下 `restored_from`），这样订阅方能识别出序号已经回退（02 §8.2）；
  4. 启动服务。服务会按 02 §8.6 的握手流程，从各发布方重放缺失的事实（重放窗口是 7 天），并与柜台对账。
- **灾难恢复**：如果 risk 库丢失，可以用 clearing 最近一次的日初快照，加上 trade-gateway 当日的全部成交重建。告警历史和审计记录会有缺口，缺口本身要记入审计。

---

## 9. 迁移与版本
- **迁移的写法**：每个库的迁移是它的 `schema.py` 中的 `MIGRATIONS` 列表（`futrisk.db.migrations.Migration`），只能向前执行，版本号从 1 开始连续递增。迁移体有三种：
  - 按冻结的表定义建表（`Migration.create_all`）：md、td、risk、supervisor 库的版本 1。checksum 为表结构稳定描述（`schema_fingerprint`：列、类型、可空、主键、默认值、CHECK、唯一约束、外键、索引）的 SHA-256，与 SQLAlchemy 生成 DDL 的格式无关；早期按 DDL 文本计算的 checksum 仍被接受，启动时改写为新值；
  - SQL 脚本（`Migration.sql_script`）：td、risk 库的版本 2、3，clearing、api 库的全部版本。checksum 为脚本的 SHA-256（换行统一为 `\n`）；脚本中的 `-- futrisk:accept-checksum <sha256>` 注释行声明另外接受的旧 checksum；
  - Python 函数（`Migration.from_callable`，用于 SQLite 无法 `ALTER` 的重建）：supervisor 库的版本 2。checksum 为声明的 `checksum_source` 文本的 SHA-256。

  SQL 直接写在模块里，不放单独的 `.sql` 文件，这样打包成 exe 时不需要额外的数据文件（`futrisk.db.migrations` 也能加载 `NNNN_*.sql` 文件，目前没有库使用）。
- **启动时的执行**：服务打开数据库时，在同一个写事务（`BEGIN IMMEDIATE`，按关键事务 `synchronous=FULL` 提交）中核对 `schema_version`、执行尚未执行的迁移，并处理 `db_meta`，失败则整体回滚。以下情况拒绝启动：
  - 已执行记录的 checksum 与代码不一致，并且不是代码声明接受的旧 checksum（已发布的迁移不能修改，只能追加新迁移）；
  - 数据库中有本版本代码不认识的版本（用旧程序打开了新库）；
  - 待执行的迁移版本小于已执行的最大版本。
- **枚举取值冻结**：md、td、risk 库的迁移 1 用冻结的取值表（代码中的 `_V1_ENUM_VALUES` / `_ENUMS_V1`）生成 CHECK，clearing、api 库的迁移是冻结的 SQL 文本，所以代码给枚举加一个取值，不会改变这些迁移的 checksum；要把新取值存进库，必须追加一个重建该表 `CHECK` 的迁移（如 td、risk 库的版本 3）。单元测试保证当前的取值都能存进库：md、td 库把每个枚举的全部当前取值写入新建的库，risk 库比较取值表与代码中的枚举，clearing、api 库比较 SQL 文本与当前的 `MetaData`；新取值存不进去时测试失败。例外：各库共用的 `fact_cursors.start_policy` 取自代码中的 `FactStartPolicy`；supervisor 库的迁移 1 取自代码中的 `ProcessState`、`ClockMode`，迁移 2 重建 `service_events` 时取自当时的 `ServiceChangeReason`、`ProcessState`。`FactStartPolicy` 增加取值时，td、risk 库迁移 1 的 checksum 会变（测试固定了这两个库的 checksum，会发现）。supervisor 库没有测试固定 checksum：`ProcessState`、`ClockMode` 增加取值后，已有的库会因迁移 1 的 checksum 不符而拒绝启动；`ServiceChangeReason` 增加取值后，已经执行过迁移 2 的库存不进新取值（08 §5.2 KD-18）。
- **表结构的来源**：当前表结构以各服务 `schema.py` 中的 SQLAlchemy Core `MetaData` 为准，SQLite 的 DDL 由它生成，类型映射见 §3.1。本文与代码的一致性由 `scripts/check_docs.py` 机械核对每张表、每一列及其可空性；类型、键、约束、索引和默认值靠审查。
- **兼容规则**：
  - 只加列（允许为空，或者有默认值）属于兼容变更；
  - 删除列、改变列的含义、改变主键，都必须同时升级对应消息的版本 `v`（02 §4.10），并在本文的修订记录中写明。

### 9.1 各库的迁移历史

| 库 | 版本 | 迁移体 | checksum（前 12 位） | 内容 |
|---|---|---|---|---|
| md.db | 1 | `create_all` | `f58aff75b9de` | 初始表结构（§4.1）。2026-10-07 重新基线：版本 1 已包含 `watermarks.wm_fingerprint`，`md_events.kind` 不再允许已删除的事件类别，删除了 1 分钟线的表；之前建立的 md.db（版本 2）在启动时被拒绝，要删除后重建（行情事件、缺口和结算价输入的历史随之丢失，tick 文件不受影响，结算价输入可以从规范 tick 重新计算） |
| td.db | 1 | `create_all` | `d9b563923bc0` | 初始表结构（§4.0、§4.2） |
| td.db | 2 | SQL | `866026017a3d` | `routed_orders` 增加 `cancel_requested_ns`（持久化的撤单请求） |
| td.db | 3 | SQL | `d847f70f741e` | 重建 `firm_controls`，`action` 允许 `RULE_STATE`（保留原有行、`control_id` 和自增计数） |
| risk-{shard}.db | 1 | `create_all` | `157270b37fc6` | 初始表结构（§4.0、§4.3） |
| risk-{shard}.db | 2 | SQL | `d3f4e1080bfb` | `orders` 增加 `cancel_command_id`、`cancel_sent_ns`、`cancel_attempts`（撤单重发记录） |
| risk-{shard}.db | 3 | SQL | `6fba00270535` | 重建 `firm_controls`，`action` 允许 `RULE_STATE`（coordinator 的规则层完整状态） |
| clearing.db | 1 | SQL | `99b5cf0f5ad5` | 初始表结构（§4.4） |
| clearing.db | 2 | SQL | `1059b71d8b96` | 新增 `config_history` |
| api.db | 1 | SQL | `a3c76786c147` | 初始表结构（§4.5） |
| api.db | 2 | SQL | `df7865d685fc` | 新增 `kill_switch`（人工 kill switch 的期望状态） |
| supervisor.db | 1 | `create_all` | `d677a6c60c80` | 初始表结构（§4.6） |
| supervisor.db | 2 | Python 函数 | `97760ed3ac8e` | 重建 `service_events`：`reason` 允许 `GAVE_UP`、`SUPERVISOR_RESTART`，增加 `env_keys`、`detail` |

checksum 的前 12 位与服务拒绝启动时日志中显示的一致（`recorded …, code …`）。

sim.db 不走这套机制，用 `PRAGMA user_version` 记版本，当前为 2：打开版本 1 的文件时自动补上 `sim_orders.pending_match` 列（§4.7）。

---

## 10. 需求可追踪性

| task.md 条目 | 本文章节 |
|---|---|
| 行情中断恢复后补全数据，不产生重复或错误的信号 | §4.1（`gaps`、`feed_cursors`、`settle_inputs`）；§5.1（原始 tick 与规范 tick）；§4.0（`outbox`、`fact_cursors`） |
| 开平仓记录，持仓数据的来源与更新机制 | §4.2（`trades`）；§4.3（`orders`、`fills`、`sod_lots`） |
| 按账户、按品种查询；多账户独立核算与汇总 | §4.3（每个表都带 `account_id`，每个分片一个库）；§4.4（`account_settlements`、`position_settlements`） |
| 风控动作与熔断状态的持久化（重启后不丢失） | §4.3（`rule_states`、`alerts`、`account_states`、`firm_controls`） |
| 逐日盯市、保证金核算 | §4.4（`settlement_prices`、`account_settlements`、`position_settlements`、`margin_groups`、`trade_settlements`、`close_details`） |
| 风控日报 | §4.4（`reports`）；§5.3 |
| 附加项：回测 | §4.4（`backtest_runs`）；§5.2 |
| 附加项：配置化 | §4.5（`config_versions`） |
| 生产级能力：审计、幂等、备份与恢复（总表见 01 §8.17） | §4.0、§4.3（`audit_events`）、§4.5（`api_audit`）、§6、§8 |
| 盘中风控指标的留存（含单位净值） | §4.3（`metric_samples`） |
| 工程化：日志系统 | §8（日志文件的保留期） |

---

## 11. 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-25 | 初版：6 个库和文件型存储的完整定义，包括跨库一致性、事务边界、生命周期和迁移 |
| v0.2 | 2026-09-25 | 交叉验证后的修订：<br>1）新增 `db_meta`（流 ID）、`outbox_topics`（永不清理的序号计数）、`watermarks`、td 库的 `firm_controls`、`official_prices`、`sod_versions`，以及外部组件的 sim.db；<br>2）`fills`、`broker_order_events`、`risk_events` 补全与负载对齐的列；外部成交允许 `client_order_id` 为空；<br>3）回撤和净值改按业绩权益（`cum_net_deposit`）计算；<br>4）增加临时快照、重新定基、对账处理、路由墓碑等事务；<br>5）备份先写临时文件再重命名，从备份恢复时重新生成流 ID；<br>6）金额 CHECK 改为 GLOB 格式校验 |
| v0.3 | 2026-09-25 | 第二轮交叉验证（回归审查）后的修订：<br>1）td 库新增 `route_tombstones`；<br>2）`firm_controls` 增加 `origin`，kill switch 按来源计算生效状态；<br>3）`recon_breaks` 增加 `blocking`，`ReconBreakKind` 删去没有产生方的取值；<br>4）`trades.broker_seq` 和 `fills.broker_seq` 允许为空；<br>5）明确游标的写入时机和 `sys.eod` 的例外，api 库的通知游标加上 `md.event`；<br>6）日初快照版本、出入金核对（`CASH_DIGEST`）、持仓对账时机与 02 对齐；临时快照事务包含风控状态换日；<br>7）复核后的补充：`ReconBreakKind`、`ReconBreakStatus` 改由 02 §7.3 定义；重新定基时不再作废减仓计划 |
| v0.4 | 2026-09-25 | 需求对齐审查后的修订：`metric_samples` 增加 `nav`；数据生命周期表增加日志文件；需求可追踪性补充指标留存和日志 |
| v0.5 | 2026-09-25 | 设计问题复核后的修订：<br>1）DB-03 改为 md、risk 库默认 `NORMAL`，risk 库的关键事务和其余各库用 `FULL`；<br>2）`db_meta` 增加 `clean_shutdown`、`host_boot_ns`，断电后重新生成流 ID；<br>3）supervisor.db 新增 `clock_state`；<br>4）减仓计划换日时只作废尚未确认的 |
| v0.6 | 2026-09-26 | 实现对齐：以代码为准逐表逐列核对，并交给校验脚本机械核对（`scripts/check_docs.py` 导入各服务的 `schema.py`，核对每张表、每一列及其可空性）。主要修订：<br>1）补齐代码中已有而本文缺少的内容：clearing 库的 `config_history`（配置版本的结算生效日）、api 库的 `kill_switch`（人工 kill switch 的期望状态）、`watermarks.wm_fingerprint`、`routed_orders.cancel_requested_ns`、`orders` 的撤单重发记录、`service_events.env_keys` / `detail`；<br>2）写明 `eod_runs.step_log` 的键，以及恢复时原样复用的输入；<br>3）§9 按实现重写：迁移写在各服务 `schema.py` 的 `MIGRATIONS` 中，三种迁移体及其 checksum、拒绝启动的条件、枚举取值按迁移冻结；新增 §9.1 各库的迁移历史（md 2、td 3、risk 3、clearing 2、api 2、supervisor 2）；<br>4）DB-01 列出全部共享目录及其唯一写者，与 docker compose 的卷划分一致 |
| v0.7 | 2026-09-26 | 范围精简（完整版保留为 git 标签 `v0.1-full`）：§5.2 的回测输出改为 `daily.csv`、`events.csv`、`orders.csv`、`summary.json`；分钟线只作数据校验的依据；§5.1 与 DB-01 删除 C 层回测这一用途（`ticks/` 不再共享） |
| v0.7 | 2026-10-07 | md.db 重新基线为版本 1（含 `wm_fingerprint`，不再有 v1→v2 迁移；旧 home 的 md.db 不兼容，需删除重建）；删除 bars_1m 表及其事务、保留期条目 |
| v1.0 | 2026-10-08 | 按代码重建（md-gateway 重写之后），逐库核对表、列、类型、可空、主键、唯一约束、索引、默认值、外键、迁移和 checksum：<br>1）md.db 按重写后的 md-gateway 改写：版本 1（重新基线，§9.1 记下 checksum），关系图删除 1 分钟线；`feed_cursors` 的续传与落盘规则（轮询源的 `last_exch_ts_ns` 只供排查）、`watermarks` 的指纹存法和写入周期、`md_events` 的人工排除记录和 `detail.gap`、`settle_inputs` 的版本规则以及 `hourly`、`quality` 的口径；<br>2）§5.1 重写：原始分片记录 `LIVE`、`BACKFILL`、`DUPLICATE`、`REJECTED`（不再有隔离模式，被跳过的状态不记录）、原子写入和备用目录、录制命令的原始 HTTP 应答、规范 tick 的生成时机和去重规则；§5.2 按实际的历史数据目录（主力连续和逐合约日线、官方数据、manifest）和回测输出目录（`{run_id}` / `{name}-{时间戳}`）改写；§5.3 写明同一版本重新生成时的登记规则；<br>3）补上代码中有而文档缺少的约束：索引 `ix_broker_order_events__client_order_id`、risk 库的 `ix_cash_movements__td_acct`、`ix_liquidation_plans__status`，部分索引名、具名 CHECK、clearing 日初快照的外键；关系图改正可空关系；<br>4）`firm_controls` 写明 `RULE_STATE`（coordinator 把规则层作为一条事实发布，启动时按 `control_id` 重放）；`recon_breaks.blocking` 按代码列出例外；sim.db 补上 `sim_md_state` 和新增列；<br>5）§3.1 区分 SQLite 实际 DDL 和 PostgreSQL 目标映射；DB-01 写明 `ticks/` 不共享以及离线命令写入的目录；DB-02、DB-03 补上合并提交、后台 checkpoint 和关键事务的完整范围；<br>6）§8 按各服务的维护代码改写：保留期按自然日、人工事件永久保留、risk 库用 `PASSIVE` checkpoint 并避开交易时段、outbox 窗口的来源、api.db 的备份不清理、没有恢复命令；§9 写明三种迁移体及适用的库、旧 checksum 的接受规则、枚举冻结的例外，§9.1 增加迁移体和 checksum 两列。<br>`scripts/check_docs.py` 对本文为 0 个问题；同日的独立交叉审查又改正 9 处：`retro_breaches` 标为预留（没有写入方）、§5.1 补上被跳过的陈旧快照（当日的校验器修改）、`processed_commands.trading_day` 的取值、risk-engine 维护的时段限制、两行 v0.7 修订记录的顺序等 |

