# futrisk 测试方案与测试报告

| 项 | 内容 |
|---|---|
| 文档编号 | FR-STP-06 |
| 版本 | v1.0 |
| 状态 | 实现对齐 |
| 日期 | 2026-10-08 |
| 适用范围 | futrisk 0.1.0 的全部自动化测试：单元测试、黄金用例、性质测试、真实进程与回环网络上的集成测试、故障注入、端到端验收、前端测试、静态检查与文档一致性检查；性能基准的结果见 13 |
| 对应标准 | GB/T 8567-2006 软件测试计划（STP）与软件测试报告（STR） |
| 关联文档 | [README](README.md)（规范）· [01 架构](01-architecture.md)（质量需求见 01 §10）· [02 通信协议](02-communication-protocol.md)（错误码见 02 §11）· [04 风控规则](04-risk-rules.md) · [05 清算](05-clearing.md) · [07 使用说明](07-user-guide.md) · [08 部署](08-deployment.md)（exe 的冒烟测试见 08 §2.2）· [10 回测分析](10-backtest-analysis.md) · [13 性能基准](13-benchmarks.md) |

> 本文前半部分是测试方案：测什么、怎么分层、每条需求由哪些测试覆盖、故障注入矩阵、端到端验收；后半部分（§7、§8）是一次完整执行的报告：结果、耗时、覆盖率和已知问题。每个结论都对应能直接重跑的测试文件或命令（§9）。

## 目录
1. [范围与目标](#1-范围与目标)
2. [测试策略](#2-测试策略)
3. [测试环境](#3-测试环境)
4. [需求与测试的追踪](#4-需求与测试的追踪)
5. [故障注入矩阵](#5-故障注入矩阵)
6. [端到端验收测试](#6-端到端验收测试)
7. [执行结果](#7-执行结果)
8. [已知问题与未覆盖项](#8-已知问题与未覆盖项)
9. [如何运行](#9-如何运行)
- [附录 A 修订记录](#附录-a-修订记录)

---

## 1. 范围与目标

测试要回答三个问题：
1. **结果对不对**：清算金额逐分相等，风控指标、规则和状态机按 04、05 的定义工作；
2. **故障下会不会出错**：行情中断、跳号、重复、乱序、坏点、时钟偏差，服务崩溃和重启，都不产生重复或错误的风险信号，熔断状态不丢；
3. **题目的验收场景能否复现**：ACC3 当日亏损达到初始资金的 3%（`daily_pnl_pct_init` ≤ −0.03）时自动熔断，此后开仓被拒、平仓照常，风控服务被杀后熔断仍然保持。

质量目标以 01 §10.2 的质量场景（QS-01 ~ QS-11）为准。性能场景（QS-07、QS-08、QS-11）的测量见 13；本文只包含几个宽松的性能回归用例（§2.1）。

本文不测试的内容：
- 真实期货公司柜台（CTP）的连接：只测试按 CTP 语义实现的模拟柜台（sim-exchange 的 SimBroker）；
- 新浪行情的联网抓取：只用抓包样本和模拟的 HTTP 传输测试解析、校验和录制（§2.2）；
- docker compose 部署：不在自动化测试中，人工验证的情况见 §7、§8。

## 2. 测试策略

### 2.1 测试分层

| 层 | 做法 | 位置 | 用例数 |
|---|---|---|---|
| 单元测试 | 纯函数和单个类，不启动进程；时间由注入的时钟给出，结果能复现 | `tests/unit/<包>/`（§2.3） | 见 §7 |
| 黄金用例 | 3 组多日手算清算案例（SHFE rb2701 三个交易日、CFFEX IF2612、SHFE 双向持仓的单向大边），以及 CFFEX 平今 FAQ、DCE 日内开平两腿、SHFE 结算价向下取整、封板回退等交易所规则用例；金额逐分比对（算术用 Decimal 复核，用例说明见 `tests/golden/CASES.md`，逐步推导见 05 §9） | `tests/golden/` | 42 |
| 性质测试 | hypothesis 随机生成多日成交序列和价格路径，检查恒等式；全部用 `derandomize=True`（固定的样例序列，不用样例库）。清算（每条 200 例）：逐日盯市累计 = 逐笔对冲累计 = 独立的现金流核算、交易所公式 = "平仓 + 持仓"、资金恒等式、持仓守恒、账户独立、手续费与独立算法一致、盘中账本收敛到结算结果、单向大边与独立的保证金核算一致；风控内核：告警状态机的不变量（1000 例）、随机价格路径中途重启恢复后不变量仍成立（HALTED 与熔断状态机一致、每个告警键只告警一次、HALTED 期间开仓都被拒）、增量估值 = 全量重算、快速 what-if = 完整 what-if、持仓累计量 = 重新计数 | `tests/property/`（9）、`tests/unit/engine/test_engine_properties.py`（5）；`test_engine_perf.py`（各缓存与快速路径 = 直接计算）和 `test_core_ledger.py::test_incremental_equals_straightforward` 也用 hypothesis | 14 |
| 服务级集成 | 服务外壳跑在真实的 ZeroMQ 回环套接字上（端点取临时空闲端口），对面是另一个真实服务或脚本化的替身（fake）；行情和交易网关的用例在同一进程内启动真实的 sim-exchange；少数用例启动真实子进程（如 `test_re_subprocess.py`、`test_supervisor_realstack.py`、`test_supervisor_run.py`） | 各服务目录，如 `test_*_integration.py`、`test_re_runtime.py`、`test_clearing_flows.py`、`test_mdg_resilience.py` | 含在单元测试中 |
| 故障注入 | 在上面两层中注入行情故障（sim-exchange 的故障注入：断线、跳号、重复、乱序、冻结、坏点、畸形数据、重启、柜台断线与强平）、进程崩溃（包括在事务的各个阶段崩溃）、写库失败、链路丢消息、时钟回拨与跳转，断言系统行为（§5） | 分布在各服务目录 | 含在单元测试中 |
| 端到端验收 | `futrisk demo --scenario breaker --headless` 在临时 home 中启动 supervisor 及其管理的 8 个服务实例，只通过 REST、CLI 和 md-gateway 的 PUB 套接字观察，覆盖熔断、kill 风控、行情中断、EOD（§6） | `tests/e2e/` | 5 |
| 回测 | D 层合成剧本（`breaker_synthetic`，正确结果已知）在同一个 `EngineCore` 上精确断言验收场景和单个坏点，剧本的 16 项检查全部通过；数据加载、换月与 DCE 昨结算估计、日内路径合成、模拟成交、逐日结算与换日、双均线策略（含熔断锁存后翻空只平不开）、统计定义、配对不变量（静态持仓两组相同，加仓被挡住时两组不同）、账户独立、结果的确定性、运行入口与 exe 环境；1 个慢速用例在真实历史数据上短跑 `default`（2016 年 11 月，`data/hist` 缺失时跳过） | `tests/unit/backtest/` | 59 |
| 前端 | vitest（`environment: node`）：工具函数（格式、统计、对比视图、规则与使用率、归一化、推送批处理）、REST 与 WebSocket 客户端、store、模拟后端，以及对照真实 api-gateway 应答样本（`web/tests/fixtures/gateway/`）的契约测试；不挂载页面组件；`vue-tsc --noEmit` 类型检查 | `web/tests/`（12 个文件） | 173 |
| 静态检查 | ruff（规则集 E、F、W、I、B、UP、SIM、TID；TID251 banned-api 不允许调用系统时钟、全局随机数和随机 UUID，豁免 `common`、`services`、`sim`、`bus`、`cli`、`main.py`、`scripts`、`tests`，所以对 `core` 在内的其余代码生效）；mypy（`src/futrisk` 全部模块，`core`、`messages`、`bus` 按 strict） | `pyproject.toml` | — |
| 文档一致性 | `scripts/check_docs.py`：文档之间的交叉引用、主题、命令、枚举，以及与代码的表结构、消息字段、枚举取值、错误码、环境变量的对照；`scripts/gen_config_reference.py --check`：11 与配置模型一致 | — | — |
| 性能 | 完整版（git 标签 `v0.1-full`）的 `futrisk bench`，结果与分析见 13；精简版没有基准工具，单元测试中只保留几个宽松的回归用例，只拦截数量级上的退化：`test_core_ledger_bench.py`（日内账本热路径）、`test_mdg_throughput.py`（md-gateway 接入速率 > 1500 条/s）、`test_re_latency.py`（下单往返与 tick→判定的 p50 < 50ms） | — | — |

### 2.2 原则
- **确定性**：风控与清算内核不读系统时钟、不用全局随机数和随机 UUID（ruff TID251 强制），时间、随机数、ID 都由调用方注入；hypothesis 固定样例序列。同一输入产生相同的效果序列和库表（`test_engine_misc.py::TestDeterminism::test_same_inputs_same_effects`）；回测同一配置运行两次，事件、汇总和配置的哈希相同（`test_bt_arms.py::test_determinism_same_config_same_hashes`）。
- **不依赖网络**：新浪行情用抓包样本（`tests/fixtures/sina/`）和 httpx 的 `MockTransport` 测试；外部世界由 sim-exchange 模拟；集成测试的总线端点和模拟源端口取临时空闲端口（`futrisk.runtime.testing` 的 `config_with_free_ports`、`free_tcp_port`）。
- **先替身、后真实**：每个服务先对着替身测协议和边界情况，再与真实的相邻服务和真实进程一起测。
- **故障要断言结果**：每个故障场景都必须断言可观察的结果（告警条数、`alert_key` 不重复、HALTED 保持、补数条数、重复条数为 0、`order_key` 严格递增），不能只断言"不崩溃"。
- **超时**：`pyproject.toml` 没有全局超时。启动进程、回环网络或真实 sim-exchange 的模块用 `pytest.mark.timeout` 各自设上限（30~600s，例如 `test_mdg_integration.py` 90s、`test_re_subprocess.py` 240s、`test_supervisor_realstack.py` 600s），其余用例的上限由命令行的 `--timeout` 给出（§9）。Windows 没有 SIGALRM 信号，pytest-timeout 改用 thread 方式：一个用例超时，它打印各线程的栈后直接结束整个 pytest 进程（`os._exit(1)`），没有汇总行。
- **标记**：`slow` 标在 9 个耗时的模块上：`test_e2e_acceptance.py`、`test_bt_realdata.py`、`test_bots_calibration.py`、`test_mdg_integration.py`、`test_mdg_resilience.py`、`test_mdg_throughput.py`、`test_re_chain.py`、`test_re_subprocess.py`、`test_supervisor_realstack.py`；默认也运行，用 `-m "not slow"` 排除。其他启动子进程的模块（如 `test_supervisor_run.py`、`test_bt_runner.py`）没有这个标记。`e2e` 标在端到端模块上：`tests/e2e/conftest.py` 默认跳过它，`-m` 的表达式中出现 `e2e`（如 `-m e2e`）或环境变量 `FUTRISK_E2E=1` 时才运行。两个标记都在 `pyproject.toml` 中声明。
- **异步**：`asyncio_mode = "auto"`，`async def` 的用例和夹具不需要额外标记；夹具的事件循环按用例创建（`asyncio_default_fixture_loop_scope = "function"`）。

### 2.3 目录与文件

| 目录 | 文件名前缀 | 被测对象 |
|---|---|---|
| `tests/unit/core/` | `test_core_` | 清算与风控的纯函数：合约参数、lot、手续费、保证金、盈亏、结算价、结算、日内账本 |
| `tests/unit/engine/` | `test_engine_` | `EngineCore`：指标、规则与状态机、事前检查、验收场景、委托、EOD、公司层、性质测试 |
| `tests/golden/` | `test_clearing_golden` | 黄金用例（`builders.py` 是构造工具，`CASES.md` 是用例说明） |
| `tests/property/` | `test_clearing_properties` | 清算性质测试 |
| `tests/unit/bus/` | `test_bus_` | 总线：PUB/SUB、请求应答、事实流补齐、化身、LV 缓存与快照 |
| `tests/unit/db/` | `test_db_` | 数据库层：写库线程、组提交、outbox、迁移 |
| `tests/unit/runtime/` | `test_runtime_` | 服务运行时：两阶段提交参与方、写库链、幂等、父进程监视、GC 策略、卡顿检测 |
| `tests/unit/md_gateway/` | `test_mdg_` | 行情网关：数据源适配、序号跟踪、归一化、新浪解析、交易时段、校验、仲裁、可信状态、行情质量、事件与缺口、录制分片、结算价输入、存储与迁移、故障场景 |
| `tests/unit/risk_engine/` | `test_re_` | 风控服务外壳：真实套接字、存储、恢复、崩溃矩阵、硬杀子进程、与真实 md-gateway 的链路 |
| `tests/unit/trade_gateway/` | `test_tg_` | 交易网关：路由、墓碑、重同步与补回报、kill switch、柜台适配、与模拟柜台集成 |
| `tests/unit/clearing/` | `test_clearing_` | 清算服务：EOD saga、对账、补结算、崩溃恢复、报表、停机 |
| `tests/unit/api_gateway/` | `test_apigw_` | REST、认证与 RBAC、WS、读模型、通知、两阶段提交协调、kill switch、时钟跳转编排、交易日、页面托管 |
| `tests/unit/supervisor/` | `test_supervisor_` | 进程管理、重启策略、市场时钟、只管时钟模式、端口与身份守卫、真实整套进程 |
| `tests/unit/sim/` | `test_sim_` | 虚拟交易所：价格引擎、Feed A/B、模拟柜台、控制接口与故障注入、存储 |
| `tests/unit/bots/` | `test_bots_` | 策略机器人与熔断剧本的校准 |
| `tests/unit/client/` | `test_client_` | 策略 SDK |
| `tests/unit/cli/` | `test_cli_`、`test_tools_`、`test_config_commit`、`test_seed_body` | 命令行工具，含 `futrisk data`（新浪录制）和录制进程的守护 |
| `tests/unit/backtest/` | `test_bt_` | 回测 |
| `tests/unit/test_foundation.py` | — | 日历、十进制、时钟、消息、配置 |
| `tests/e2e/` | `test_e2e_acceptance` | 端到端验收（`e2e_support.py` 是辅助函数） |
| `web/tests/` | `*.spec.ts` | 仪表盘前端 |

共享的夹具在 `tests/conftest.py`（默认配置、新浪抓包样本）以及各目录的 `conftest.py` 和辅助模块（`*_support.py`、`helpers.py` 等）中。测试文件名在 `tests/` 下唯一，§4、§5 只写文件名，`::` 后是用例名，类中的用例写成 `类名::用例名`；同一格中接着引用同一文件的用例时省略文件名，以 `::` 开头。

## 3. 测试环境

| 项 | 内容 |
|---|---|
| 机器 | AMD Ryzen 7 7735H（8 核 16 线程），64 GB 内存 |
| 操作系统 | Windows 11 家庭中文版（10.0.26200）。系统时区是 UTC+9（不是北京时间），代码一律显式使用 `Asia/Shanghai`，所以全部测试同时验证了"与本机时区无关" |
| Python | 3.12.10，Poetry 在项目内建的虚拟环境 `.venv`；pyzmq 27.2.0（libzmq 4.3.5）、msgspec 0.19.0、FastAPI 0.141.1、SQLAlchemy 2.1.0、pyarrow 21.0.0 |
| 测试工具 | pytest 8.4.2 及插件 pytest-asyncio 1.4.0、pytest-timeout 2.4.0、pytest-cov 6.3.0；hypothesis 6.168.1；ruff 0.16.9；mypy 1.20.2。版本范围见 `pyproject.toml` 的 dev 依赖组，pytest 的配置在 `[tool.pytest.ini_options]`（`testpaths = ["tests"]`、`addopts = "-ra"`、标记 `slow` 和 `e2e`） |
| Node.js | 20.20.1；vitest 3.2.7、vue-tsc 2.2.12、Vite 6.4.3 |
| 浏览器 | Microsoft Edge（无头模式，由 `web/` 的开发依赖 playwright-core 驱动），用于在真实系统上检查仪表盘和截图（07）；驱动脚本不在仓库中 |
| 测试数据 | `tests/fixtures/sina/`：2026-09-25 从 `hq.sinajs.cn` 抓取的原始响应（GB18030 字节，清单在 `manifest.json`）；`web/tests/fixtures/gateway/`：从真实 api-gateway 抓取的 REST 应答；`data/hist/`：回测用的历史日线 |

## 4. 需求与测试的追踪

task.md 的每条要求 → 覆盖它的测试 → 测试断言了什么。文件名的写法见 §2.3。

| task.md 要求 | 测试 | 断言（证据） |
|---|---|---|
| 行情接入：至少 2 个行情源，统一格式（更新频率与格式自行设计） | `test_mdg_normalizer.py`（Feed A 的 CTP 字段、Feed B 的厂商字段 → 规范 `Tick`）；`test_mdg_sources.py`（Feed A 的帧，Feed B 和新浪的轮询，不连网）；`test_mdg_sina.py`（新浪抓包样本的解析、交易日归属、拒收）；`test_sim_feed_a.py`、`test_sim_feed_b.py`（两路模拟源的协议）；`test_cli_data.py`（`futrisk data record` 用 md-gateway 的同一条流水线录制新浪行情） | 同一市场状态经 Feed A 和 Feed B 得到相同的 `order_key` 和指纹（`test_mdg_normalizer.py::test_feed_a_and_feed_b_of_the_same_state_share_order_key_and_fingerprint`）；两路模拟源给出同一快照（`test_sim_feed_b.py::TestFeedB::test_same_state_as_feed_a`）；新浪 tick 用同一指纹算法（`test_mdg_sina.py::TestFingerprint`）；录制时过期的和时段外的新浪快照被跳过、不记录 |
| 行情接入：中断恢复后补全缺失数据 | `test_mdg_seqtracker.py`（跳号、重排、区间补发、重连续传、`SEQ_EVICTED`、`EPOCH_CHANGED`）；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover`、`::test_restart_resumes_from_cursor_and_watermarks`；`test_mdg_resilience.py::test_all_sources_lost_opens_an_all_gap_closed_when_a_source_is_trusted_again`；`test_mdg_recording.py`（原始记录与当日规范文件）；`test_e2e_acceptance.py::test_feed_a_outage_fails_over_and_backfills_without_duplicates` | 缺口从 `GAP_OPEN` 到 `GAP_FILLED`，补数条数 > 0、重复 0；补回的状态以 BACKFILL 写入原始记录；当日规范文件对同一状态只留一条，优先 LIVE（`test_mdg_recording.py::test_canonical_dedupes_by_fingerprint_preferring_live_and_is_rebuilt_when_raw_changes`）；全部源中断时开 `all:` 缺口，有源恢复可信后关闭 |
| 行情接入：不产生重复或错误的风险信号 | `test_mdg_resilience.py::test_wire_faults_on_both_feeds_never_duplicate_or_reorder_live`；`test_mdg_arbiter.py`（水位、指纹去重、LIVE 条件、交叉校验）；`test_mdg_validator.py`（时段、涨跌停、最小变动价位、未来时间戳、同源单调、无成交变价）；`test_mdg_pipeline.py`；`test_engine_acceptance.py::TestAcceptanceBreaker::test_single_bad_state_reverting_before_confirmation_revokes_the_halt`、`::TestAcceptanceBreaker::test_quality_gating_freezes_the_machines`；`test_bus_fact.py::TestRandomisedDelivery::test_exactly_once_in_order`；`test_apigw_notifier.py::TestDelivery::test_dedupe_cursor_and_restart` | `md.tick` 按合约 `order_key` 严格递增、没有重复；补数和重放不进入 `md.tick`；轮询源（Feed B、新浪）中本时段还没更新的合约快照按过期跳过，不算拒收（`test_mdg_validator.py::test_stale_polled_snapshot_in_an_open_session_is_skipped_not_rejected`，F35）；单个坏状态不锁存熔断；行情质量不是 OK 时状态机不迁移；事实流恰好一次、按序；通知按 `notify_key` 去重，跨重启保持；端到端验收中告警键不重复（本文 §6） |
| 持仓：开仓与平仓记录；数据来源与更新机制 | `test_core_lots.py`（lot、平今平昨、冻结、换日）；`test_engine_orders.py`（委托生命周期与成交入账）；`test_tg_core.py`（成交入库并按柜台顺序发布、重复回报、重同步）；`test_db_outbox.py::TestOutboxSink` | 持仓只由成交推导；重复成交按成交键去重（`test_engine_orders.py::TestHappyPath::test_duplicate_trade_is_ignored`、`test_tg_core.py::TestReports::test_duplicate_reports_are_ignored`）；outbox 中的事实在事务提交之后才发布，事务失败则什么都不发布 |
| 持仓：按账户和品种维度查询 | `test_apigw_rest.py::TestViews::test_accounts_and_names`、`::TestViews::test_positions_group_by`；`test_apigw_views.py::TestPositions::test_rows_and_groups`；`test_tools_trading.py::test_positions_table_filters_and_missing_accounts`、`::test_positions_group_by_and_raw_json`（`futrisk positions`） | `/positions` 按账户、品种、板块、交易所过滤和汇总，并列出没有数据的账户；不支持的分组字段返回 400 |
| 持仓：多账户汇总与独立核算 | `test_clearing_properties.py::test_account_independence`；`test_bt_review.py::test_accounts_are_independent`；`test_engine_firm.py::TestCoordinator::test_firm_snapshot_aggregation` | 多个账户一起结算与逐个单独结算，每个账户的结果完全相同；回测中一个账户的策略不改变另一个账户的盈亏；公司层汇总权益、各状态的账户数和品种净持仓 |
| 盘中风控：资金维度与持仓维度的指标 | `test_engine_metrics.py`；`test_core_ledger.py`；`test_engine_compliance.py`；`test_engine_quality_materiality.py`；`test_engine_firm.py` | 指标定义与 04 §3 一致；`risk_rules.yaml` 用到的每个指标都能求值（`test_engine_metrics.py::TestRegistry::test_every_configured_metric_resolves`）；出入金不计入盈亏 |
| 盘中风控：告警 / 限制下单 / 熔断 | `test_engine_rules_sm.py`（状态机：确认、滞回、保持、冷却、锁存、重新布防、质量门控）；`test_engine_pretrade.py`（事前检查的每一步和平仓豁免）；`test_engine_acceptance.py`；`test_engine_misc.py::TestRestrictionKinds`、`::TestLiquidation`（第四种动作 `LIQUIDATE` 的减仓计划） | 02 §11.2 的每个拒单码都在 `test_engine_pretrade.py` 或 `test_engine_acceptance.py` 中有断言；熔断时撤销挂着的开仓单（包括 `ROUTING` 和 `UNKNOWN` 的），平仓单保留 |
| **验收：单日浮亏达到初始资金的 3% 自动熔断，阻止开仓** | `test_engine_acceptance.py`（9 例，含 `floating_only` 口径、按时间确认、重启恢复）；`test_re_runtime.py::test_acceptance_acc3_breaker_latches_and_survives_a_crash`；`test_re_subprocess.py::test_hard_killed_process_restarts_still_halted_without_duplicate_alerts`；`test_bt_scenarios.py`（D 层剧本，精确断言）；`test_bots_calibration.py`（演示剧本在真实风控内核上的校准）；`test_e2e_acceptance.py` 的前 3 例 | −0.02 告警、−0.025 限制（单笔上限乘 0.5、亏损方向不得加仓）、−0.03 熔断：第一个越线的状态立即禁开，第二个不同的 LIVE 状态或 1s 后锁存，熔断告警只有一条；平仓照常；演示剧本在 20 个带噪声的随机种子中至少 19 个做到每段恰好触发一级 |
| 盘后：逐日盯市（盈亏结算、保证金核算） | `test_clearing_golden.py`（42 例）；`test_clearing_properties.py`；`test_core_settlement.py`、`test_core_margin.py`（含单向大边）、`test_core_settle_price.py`、`test_core_fees.py`、`test_core_pnl.py`；`tests/unit/clearing/`（EOD saga、对账、补结算、崩溃恢复）；`test_e2e_acceptance.py::test_eod_settles_reconciles_and_reports` | 黄金用例逐分精确比对；每天每个账户和合约的交易所公式盈亏 = 平仓盈亏 + 持仓盈亏（`test_clearing_properties.py::test_daily_exchange_formula_equals_close_plus_position`，端到端验收对 ACC3 也校验这一项）；端到端验收中全部对账检查为 PASS 或 WARN |
| 盘后：风控日报（盈亏汇总、风险敞口、关键指标） | `test_clearing_units.py::TestReports`（日报 HTML、结算单 TXT 与 CSV、结算 JSON、使用率口径）；`test_clearing_review.py::TestReports::test_reports_render_with_peers_down`、`::TestReports::test_regeneration_runs_once_clears_errors_and_registers_idempotently`、`::TestReports::test_full_day_under_a_chinese_home`；`test_e2e_acceptance.py::test_eod_settles_reconciles_and_reports` | 日报的 10 节（表头、摘要、资金表、盈亏与敞口、限额使用、事件与动作、数据质量、系统运行、图表、压力测试）都渲染出来；对端服务不可用时照常出报表，并注明哪部分不可用；端到端验收中当日日报含"熔断"和 ACC3 |
| 附加：回测验证规则有效性 | `tests/unit/backtest/`（59 例，§2.1） | D 层剧本的 16 项检查全部通过，熔断事件逐条精确比对；同一配置两次运行的哈希相同；方法与结论见 10 |
| 附加：可视化仪表盘 | `web/tests/`（12 个文件、173 例）；`test_apigw_ws.py`（推送协议与会话限额）；`test_apigw_readmodel.py`（读模型补齐）；`test_apigw_dashboard.py`；`test_apigw_static.py`（页面的安全响应头、缓存与压缩） | 前端对真实 api-gateway 应答样本的契约测试通过；页面在真实系统上的检查与截图见 07 §3 |
| 附加：配置化 | `test_apigw_config2pc.py`、`test_runtime_config2pc.py`（两阶段提交与回滚）；`test_config_commit.py`（离线提交）；`test_foundation.py::TestConfig`；`test_bots_config.py`；`test_engine_misc.py::TestRestrictionKinds::test_hot_reload_of_thresholds`、`::TestHotReload`；`test_re_runtime.py::test_config_reload_commits_new_rules_immediately`；`test_mdg_resilience.py::test_config_changes_outside_arbitration_are_vetoed` | 风控阈值、品种参数都在 YAML 中；热加载按两阶段提交全部提交或全部放弃，心跳中的配置哈希不收敛时标为 FAILED；md-gateway 只热加载仲裁参数，其余改动在准备阶段被否决（需要重启）；11 由配置模型生成，`--check` 校验一致 |
| 附加：多账户批量风控对比视图 | `compare.spec.ts`（`web/tests/`）；`test_apigw_rest.py::TestViews::test_compare_view`、`::TestViews::test_compare_history_with_breakers`；`test_apigw_views.py::TestUtilization`；`test_apigw_dashboard.py::TestWorstUtilization` | 规则 × 账户的使用率矩阵、按离下一级别的远近排名、公司净敞口、各日熔断次数；使用率取内核给出的 `hard_utilization`，与日报同一定义（02 §7.6） |
| 工程化：配置管理、日志系统、报表生成；异常处理与容错 | `test_apigw_security.py::TestErrors::test_unexpected_errors_are_scrubbed`；`test_runtime_review_fixes.py::TestWriteChain`；`test_db_database.py::TestWriter::test_group_commit_isolates_failing_job`；`test_supervisor_cli.py::test_logs_pretty_raw_and_errors`、`::test_logs_follow_does_not_block_loguru_rotation`；`test_supervisor_process.py::test_console_log_rotation`；`test_apigw_ws.py::TestAuth::test_key_never_in_logs_or_audit`；配置和报表见上面的行，容错见 §5 | 未预期的异常只返回 `INTERNAL` 和 `error_id`，细节只写日志；写库失败后不再写入，进程以退出码 1 退出，不标记干净关闭；组提交中一个失败的任务不连累同批的其他任务；`futrisk logs` 按服务显示结构化日志（美化或原始的 JSON 行），控制台日志按大小轮转；密钥不进日志和审计 |
| 提交：可编译运行的项目和可执行文件 | `scripts/build_exe.ps1` 的冒烟测试（08 §2.2）；`test_bt_frozen.py`（exe 中的数据路径与代码标识）；`test_supervisor_process.py::test_service_argv_dev_and_frozen`；`test_clearing_contracts.py::test_main_calls_freeze_support_before_anything_else` | 构建脚本打包后用 exe 做冒烟测试；打包后的 exe 中各服务的启动参数和多进程入口正确 |

01 §10.2 的质量场景与测试的对应关系见该表的"实测 / 验证"一列；01 附录 A 从架构章节的角度追踪同一组需求，本文不重复。

## 5. 故障注入矩阵

期望行为按当前代码，参数取默认配置（`feeds.yaml`、`risk_rules.yaml`）。md-gateway 的服务级测试为了缩短用时，把计时参数调小（`mdg_support.fast_params()`：STALE 0.6s、UNKNOWN 2s、试用 1s、最短停留 0.5s）。每行的测试都包含在 §7 的执行中，结果以 §7 为准。

| # | 故障 | 期望行为 | 测试 |
|---|---|---|---|
| F1 | Feed A 断开（端到端验收中 20s，服务级测试中约 2s），Feed B 正常 | Feed A 立即被排除（`EXCLUDED`），Feed B 的下一次轮询就接替 LIVE（瞬时切换）；A 重连后从最后的连续序号续传，缺口以 BACKFILL 补回（写入原始记录，不发布），`GAP_OPEN` → `GAP_FILLED`，重复 0；A 先试用（`PROBATION`）再回到 `TRUSTED`；`md.tick` 不重复、不回退 | `test_e2e_acceptance.py::test_feed_a_outage_fails_over_and_backfills_without_duplicates`；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover`；`test_mdg_pipeline.py::test_outage_failover_backfill_never_published_and_no_duplicate_ticks`；`test_mdg_arbiter.py::test_failover_is_instantaneous_and_newest_trusted_source_advances_wm` |
| F2 | Feed A 跳号、重复、乱序 | 已收到的序号丢弃（计为重复）；超前的序号在重排缓冲区（32 条、100ms）内归位，溢出或超时则把缺的序号记为缺口，在连接内按区间请求补发，补回的状态为 BACKFILL；心跳的 `head_seq` 连续 2 次超过已收到的序号时判为尾部缺口 | `test_mdg_seqtracker.py`（16 例）；`test_mdg_resilience.py::test_wire_faults_on_both_feeds_never_duplicate_or_reorder_live`（重排窗口 64 超过缓冲区、重复比例 0.5、跳号 40 条，之后 Feed B 返回较旧的和重复的应答）；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover`（连接上的跳号、重复、乱序） |
| F3 | 没有成交的价格变化（坏点） | 以 `MD_REJECT_PRICE_WITHOUT_TRADE` 拒收：写入原始记录（REJECTED），计入该源的拒收率，发 `BAD_TICK` 事件（同一源、合约、原因 10s 内只报一次）；不进入 `md.tick`；单个坏点远低于排除数据源的拒收率 | `test_mdg_pipeline.py::test_bad_tick_price_without_trade_is_rejected_with_event`；`test_mdg_validator.py::test_ordering_and_price_without_trade`；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover`（注入 `BAD_TICK`：rb2701 偏离 0.04、无成交） |
| F4 | 随成交出现的价格跳变；两个源在同一成交量上价格不一致 | 涨跌停以内、随成交量增加出现的价格就是市场状态，照常发布（没有尖刺过滤）。同一 `(trading_day, volume)` 两源的最新价不同时：只有一方 `TRUSTED`，就对另一方投票（`MD_INTEGRITY` 事件，`VOTE`，发 HIGH 级通知），不可信的后来者以 `MD_REJECT_INTEGRITY` 拒收，5 分钟内 2 票排除该源；双方都可信（或都不可信）时只记 `NO_VOTE` 事件（不通知），后到的状态为 BACKFILL、不发布，先到的保持水位，谁也不排除，下一笔成交自然纠正。风控侧还要第二个不同的 LIVE 状态或 1s 才确认熔断（F5） | `test_mdg_pipeline.py::test_every_received_state_is_recorded_exactly_once`、`::test_untrusted_newcomer_disagreeing_with_the_trusted_source_is_rejected_and_voted`、`::test_both_trusted_conflict_is_recorded_without_votes_and_nobody_is_excluded`；`test_mdg_arbiter.py::test_cross_check_rejects_an_untrusted_newcomer_with_the_integrity_reason`、`::test_trusted_newcomer_disagreeing_with_an_earlier_untrusted_state_votes_the_untrusted_one`、`::test_both_trusted_disagreement_is_recorded_without_a_vote_and_the_earlier_state_keeps_the_watermark`；`test_mdg_trust.py::test_integrity_votes_exclude`；`test_apigw_notifier.py::test_md_integrity_severity_follows_the_arbitration_action` |
| F5 | 单个坏状态越过熔断线后回落 | 第一个越线的状态即进入 HALTED（熔断级别待确认），立即禁开并撤销挂着的开仓单；在确认（第二个不同的 LIVE 状态或 1s）之前回落，则撤销 HALTED，不锁存，不产生熔断告警；已撤的单保持撤销，之后又能开仓 | `test_engine_acceptance.py::TestAcceptanceBreaker::test_single_bad_state_reverting_before_confirmation_revokes_the_halt`；`test_bt_scenarios.py::test_single_bad_tick_never_latches`（D 层剧本） |
| F6 | 指标在阈值附近来回震荡 | 回到解除线（滞回，按规则配置，`daily_loss` 为 0.2）以内并保持 `hold_ms`（默认 3000）才清除；`ALERT` 级别清除后有冷却（`cooldown_s`，默认 60），冷却期内同一级别不再触发，更严重的级别照常升级；`RESTRICT`、`CIRCUIT_BREAK`、`LIQUIDATE` 级别没有冷却 | `test_engine_rules_sm.py::TestStateMachine::test_hysteresis_hold_and_cooldown`、`::TestStateMachine::test_cooldown_only_for_alert_levels`、`::TestRules::test_configured_hysteresis_is_per_rule`；`test_engine_properties.py::test_level_machine_invariants` |
| F7 | 全部数据源中断 | 按合约计时，只在连续交易时段内判定（每次开盘后宽限 30s）：未被覆盖 5s → STALE，30s → UNKNOWN（`QUALITY_CHANGE`）；每个合约每次中断只发一条 `MD_DOWN`，恢复后一条 `MD_RECOVERED`；配置了但从未收到行情的合约不发事件；没有可信源时 md-gateway 为 DEGRADED，并开一个 `all:` 缺口。risk-engine 对质量不是 OK 的合约禁开（`REJECT_MD_UNAVAILABLE`）、平仓照常，质量不是 OK 时状态机不迁移 | `test_mdg_quality.py::test_outage_stale_unknown_single_md_down_and_recovery`（默认的 5s、30s）、`::test_no_judging_outside_continuous_session`、`::test_open_grace_after_each_open`、`::test_startup_is_unknown_without_events_and_unobserved_instruments_stay_silent`；`test_mdg_integration.py::test_all_feeds_down_stale_unknown_single_md_down_and_recovery`（真实 sim-exchange，计时缩短）；`test_mdg_resilience.py::test_all_sources_lost_opens_an_all_gap_closed_when_a_source_is_trusted_again`；`test_engine_pretrade.py::TestStep2System::test_md_quality_blocks_opens_only`；`test_re_runtime.py::test_stale_instrument_blocks_opens_only_on_that_instrument`；`test_engine_acceptance.py::TestAcceptanceBreaker::test_quality_gating_freezes_the_machines` |
| F8 | 数据源延迟；时间戳只到秒（新浪） | 收到时比交易所时间晚 3s 以上（`live_max_latency_ms`）的状态只作 BACKFILL，不作为 LIVE；时间戳只到秒的源（`COARSE_TS` 标志）只有成交量增加时才推进 LIVE，成交量相同的新状态只记录 | `test_mdg_arbiter.py::test_latency_rule_is_two_sided`、`::test_coarse_timestamp_source_needs_a_volume_increase`；`test_mdg_resilience.py::test_sina_coarse_timestamps_only_advance_live_on_a_volume_increase`（整个服务） |
| F9 | sim-exchange 重启（`SIM_RESTART`，epoch 变化） | md-gateway：续传被拒（`EPOCH_CHANGED`），旧 epoch 中已知没收到的尾部记为不可恢复（`GAP_UNRECOVERABLE`），新 epoch 环形缓冲区中的数据按区间补回（BACKFILL，不发布），该源的排序参照清空，`md.tick` 的 `order_key` 不回退；trade-gateway：会话失效，重新登录（新会话）后把因未登录被拒的撤单重发，订单最终 CANCELED | `test_mdg_resilience.py::test_sim_restart_epoch_change_end_to_end`；`test_mdg_seqtracker.py::test_epoch_changed_loses_known_old_tail_and_backfills_new_ring`；`test_mdg_pipeline.py::test_epoch_change_forgets_the_ordering_references`；`test_tg_integration.py::test_a_cancel_refused_by_a_restarted_broker_is_sent_again` |
| F10 | Feed B 返回畸形数据、新浪返回格式不符的文本 | 单条记录畸形：以 `MD_REJECT_MALFORMED` 拒收（计入该源的拒收率），同一应答中的其他记录照常处理；整个应答不可解析或 HTTP 出错：这次轮询算失败，连续 3 次失败排除该源，交易时段内开一个按时间计的缺口（`feed_b:`），恢复轮询后以 `GAP_UNRECOVERABLE` 关闭；其他源不受影响 | `test_mdg_sources.py::test_feed_b_polls_success_failure_and_bad_record`；`test_mdg_normalizer.py::test_malformed_values`；`test_mdg_sina.py::TestRejects`；`test_mdg_trust.py::test_failed_polls_exclude_after_three`；`test_mdg_resilience.py::test_all_sources_lost_opens_an_all_gap_closed_when_a_source_is_trusted_again`（Feed B 的应答全部畸形时只开 `feed_b:` 缺口，Feed A 一直覆盖）；`test_sim_feed_b.py::TestFeedBFaults::test_malformed_record_keeps_valid_json`（模拟源的 `MALFORMED` 故障本身） |
| F11 | 熔断锁存后硬杀 risk-engine | 风控进程不在时，下单请求得到 `UNAVAILABLE` 或 `TIMEOUT`（策略 SDK 把订单记为存疑，用同一 `client_order_id` 重发）；重启后 ACC3 仍为 HALTED 且锁存，告警不重复（仍是同一个 episode），开仓仍被拒 | `test_e2e_acceptance.py::test_halt_survives_risk_engine_kill`（supervisor 拉起）；`test_re_subprocess.py::test_hard_killed_process_restarts_still_halted_without_duplicate_alerts`（真实进程，`TerminateProcess`）；`test_re_runtime.py::test_acceptance_acc3_breaker_latches_and_survives_a_crash`；`test_engine_acceptance.py::TestRestartRestore::test_restore_rebuilds_identical_state_and_no_duplicate_alerts`；`test_client_bus.py::TestCommands::test_unavailable_shard`、`::TestCommands::test_timeout_leaves_order_in_doubt_and_resubmit_resolves` |
| F12 | risk-engine 在事务的各个阶段崩溃：撤单待定已提交、撤单还没发出；补齐成交（`CATCHUP` 阶段）中；预热（`WARMUP` 阶段）中；EOD 冻结之后；临时日初快照（`PROVISIONAL`）中；两次崩溃之间夹着次日成交 | 重启后 HALTED 且锁存、没有新告警；撤单重发直到 CANCELED；EOD 冻结的状态和摘要与崩溃前相同；`PROVISIONAL` 保持；成交不丢 | `test_re_review.py::test_p5a_crash_between_the_cancel_pending_commit_and_route_cancel`、`::test_p5cd_crash_during_catch_up_and_during_warm_up`、`::test_p5e_crash_after_eod_freeze`、`::test_p5f_crash_while_provisional`、`::test_l3_a_next_day_fill_survives_a_double_crash` |
| F13 | 提交失败（写库出错）或内核抛出异常 | 该分片停止处理（毒化），进程以退出码 1 退出；同一批命令都没有成功的应答（`UNAVAILABLE`，出错的命令为 `INTERNAL`，排在后面没执行的为 `REJECT_SYSTEM_NOT_READY`），其中的委托不路由；重启后状态完整 | `test_re_review.py::test_p1_a_failed_commit_stops_every_command_of_its_batch`、`::test_c1_a_failed_commit_in_a_reply_batch_answers_held_commands_and_stops`、`::test_c1_an_engine_error_poisons_the_shard_and_its_batch`；`test_runtime_review_fixes.py::TestWriteChain` |
| F14 | md-gateway 重启 | 从持久化的游标续传（`GAP_FILLED`，`kind` 为 `resume`，重复 0）；水位和指纹每 `watermark_flush_s`（1s）落盘一次，停止时也写，重启后旧状态不再以 LIVE 发布；人工排除跨重启保持 | `test_mdg_integration.py::test_restart_resumes_from_cursor_and_watermarks`；`test_mdg_resilience.py::test_restart_inside_the_watermark_flush_interval_republishes_nothing`、`::test_manual_exclude_survives_a_restart`；`test_mdg_arbiter.py::test_watermark_reload_with_fingerprint_prevents_republishing_after_restart` |
| F15 | trade-gateway 重启、柜台断线 | 重启：从持久化的柜台序号向柜台补查，成交不重复，`trade_seq` 连续，事实流序号跨重启连续，停机期间的柜台强平作为外部成交补回；断线：检测到后服务为 DEGRADED，期间下单以 `REJECT_GATEWAY_UNAVAILABLE` 拒绝，重新登录（新会话）后补回断线期间的回报 | `test_tg_integration.py::test_restart_resyncs_without_duplicates_and_keeps_trade_seq_continuous`、`::test_broker_disconnect_is_detected_and_the_session_resynced`；`test_tg_core.py::TestResync` |
| F16 | clearing-worker 在 EOD 的每个阶段崩溃 | 从最近完成的步骤继续，结果和推送不重复；进程池中的结算步骤被打断或工作进程死掉时重试 | `test_clearing_review.py::TestCrashPoints::test_crash_at_every_phase_resumes_without_duplicates`、`::TestCrashPoints::test_restart_between_settled_and_rollover`、`::TestPool::test_settlement_is_retried_once_when_a_worker_died`；`test_clearing_flows.py::TestRestart::test_restart_mid_saga_resumes_from_last_step`；`test_clearing_shutdown.py::TestInterruptedPoolStep::test_settle_step_interrupted_in_pool_resumes_after_restart` |
| F17 | EOD 跑两次；漏跑两天后补结算 | 同一 `command_id` 重放得到同一应答，内容不同时 `CONFLICT`，运行中再发起也 `CONFLICT`；漏跑的交易日按先后顺序补结算，分片先 `CATCH_UP` 再 `REBASE`，结果与从未漏跑的分片相同 | `test_clearing_flows.py::TestIdempotency::test_settle_run_idempotency_and_preconditions`；`test_clearing_review.py::TestCatchUp::test_two_missed_eods_settle_in_order_catch_up_then_rebase`；`test_engine_eod.py::TestCatchUp::test_two_day_gap_catch_up_equals_a_shard_that_never_missed`；`test_re_runtime.py::test_two_missed_eods_are_caught_up_and_rebased_like_a_shard_that_never_missed` |
| F18 | EOD 期间 md-gateway 不可用 | 结算失败（`FAILED`），错误信息给出补救办法（`settle.override_price`）；人工给出结算价后重跑成功，新版本使用 `MANUAL` 价格 | `test_clearing_flows.py::TestRecoveryByOperator::test_md_down_fails_then_manual_prices_rerun`；`test_clearing_review.py::TestPriceInputs::test_new_contract_with_md_down` |
| F19 | 硬杀 supervisor | 它管理的服务发现父进程消失后自行退出；在同一 home 重启后复用市场时钟，把崩溃留下的未关闭记录记为 `EXITED`（原因 `SUPERVISOR_RESTART`），全部服务回到 RUNNING；`futrisk down` 后没有残留进程。上一层同理：`futrisk demo` 交互运行时，demo 进程消失后 supervisor 停止全部服务 | `test_supervisor_realstack.py::test_hard_killed_supervisor_takes_its_services_along_and_restarts_cleanly`；`test_supervisor_run.py::test_supervise_stops_everything_when_the_parent_dies`；`test_runtime_review_fixes.py::TestParentWatch` |
| F20 | 订阅端丢消息（链路有损） | 事实流按序号发现缺口后用 `ctl.replay` 补发，恰好一次、按序；发布方已清理的区间回 `NOT_FOUND`，按过期处理后跳过，只有已清理的部分算过期 | `test_bus_fact.py::TestWithInprocBus::test_lossy_link_repaired_by_replay_and_expired_by_purge`、`::TestRandomisedDelivery::test_exactly_once_in_order`、`::TestExpired`；`test_bus_review_fixes.py::TestPartialExpiry::test_only_purged_part_is_expired` |
| F21 | 发布方重启后本机时钟回拨 | 以心跳宣告的 epoch 和 stream 识别新的化身，不按 UUIDv7 的大小判断；事实流随 stream 变化重新开始，不误判序号 | `test_bus_incarnation.py`（14 例）；`test_runtime_followups.py::TestPublisherRestartWithClockSteppedBack`；`test_bus_review_fixes.py::TestRandomisedRestartsAndStreams::test_exactly_once_per_stream` |
| F22 | 演示时钟向前跳转 | 跳转按"暂停 sim-exchange → supervisor 设时钟 → sim-exchange 设时钟 → 恢复"编排，部分实例滞后不阻塞；跳进休市或开盘时不产生行情误报（没有 `MD_DOWN`，没有非 OK 的 `QUALITY_CHANGE`），`md.tick` 不回退；risk-engine 已到期的确认计时立即生效，跨过夜盘开盘时转入 `PROVISIONAL` | `test_mdg_resilience.py::test_demo_clock_jumps_raise_no_false_alarms`；`test_apigw_clock.py::TestOrchestrator`；`test_re_runtime.py::test_clock_jumps_forward_confirm_timers_and_enter_provisional`；`test_supervisor_clock.py::test_clock_state_is_persisted_reused_and_only_moves_forward` |
| F23 | 越权操作（VIEWER 重置熔断等） | api-gateway 返回 403（`FORBIDDEN`）；每个 `/api/` 请求（包括被拒的）都写一行 `api_audit`；risk-engine、trade-gateway 对 `risk.reset`、kill switch 等命令再按角色复核 | `test_apigw_rest.py::TestAuth::test_permission_matrix`、`::TestSystemAndAudit::test_audit_rows`；`test_apigw_security.py::TestEveryRoute::test_every_route_requires_a_key_and_the_right_role`；`test_engine_misc.py::TestManualCommands::test_halt_reset_and_role_checks`；`test_tg_core.py::TestKillSwitch::test_manual_command_is_idempotent_and_role_checked` |
| F24 | 周五夜盘、周六凌晨的行情 | 归属下一交易日（周一） | `test_mdg_sessions.py::test_copper_saturday_early_morning_belongs_to_monday`；`test_mdg_sina.py::TestToTick::test_saturday_early_morning_belongs_to_monday`；`test_mdg_normalizer.py::test_night_session_belongs_to_next_trading_day`；`test_foundation.py::TestCalendar::test_night_timestamps_belong_to_next_trading_day` |
| F25 | api-gateway 读模型漏收（跳号、流变化、周期主题沉默、分片下线） | 按序号补齐或重新取快照，流变化时重新同步，补齐失败时重试后作废；下线分片的账户标记为陈旧 | `test_apigw_readmodel.py`（18 例） |
| F26 | kill switch 下发时部分目标不可用 | 持久化期望状态；新的命令取代未完成的重试；只重试可重试的错误码，每次都发送当前的期望状态；重试耗尽或不可重试时发 CRITICAL 通知；api-gateway 重启后把未确认的目标再推一次 | `test_apigw_killswitch.py`（5 例） |
| F27 | 交易所时间戳超前本机收到的时刻（时钟偏差或坏数据） | 超前 1s 以上：校验时以 `MD_REJECT_FUTURE_TS` 拒收并发 `BAD_TICK`，不作为同源排序的参照，之后正常的状态照常 LIVE，该源保持可信；超前 120s 以上在归一化时就拒收 | `test_mdg_validator.py::test_future_stamped_state_is_rejected_and_never_a_reference`；`test_mdg_pipeline.py::test_future_stamped_state_is_rejected_and_later_states_flow`；`test_mdg_normalizer.py::test_future_timestamp`；`test_mdg_sina.py::TestFutureTimestamps` |
| F28 | 某个数据源持续出错 | 立即排除（`EXCLUDED`）：断线，或超过 3 个心跳周期收不到任何帧（Feed A）；连续 3 次轮询失败（Feed B、新浪）；最近 1 分钟的拒收率超过 0.05（至少 20 个样本，按源统计，一个合约坏也算；跳过的记录不计入，见 F35）；交叉校验 2 票；人工排除（跨重启保持）。恢复：连上并健康、过了最短停留（5s）进入试用（`PROBATION`），健康 10s、跟上水位、没有未补的缺口才回到 `TRUSTED` | `test_mdg_trust.py`（10 例）；`test_mdg_pipeline.py::test_one_bad_instrument_raises_the_reject_rate_of_its_source` |
| F29 | 可信源的某个合约不再更新，被排除的源却显示市场在动 | 该合约的 LIVE 流跟不上市场：落后超过 5s 立即 STALE（未覆盖时长从落后的时长起算）并发 `MD_DOWN`，继续落后到 30s 为 UNKNOWN；其他合约不受影响 | `test_mdg_resilience.py::test_frozen_trusted_feed_goes_stale_while_an_excluded_source_shows_the_market`；`test_mdg_quality.py::test_a_lagging_live_stream_is_stale_at_once_and_holds_count`；`test_mdg_arbiter.py::test_currency_and_lag` |
| F30 | 原始记录落盘失败（磁盘写不进、目录消失） | 行留在内存中并退避重试；持续失败时写到备用目录，磁盘恢复后改回主目录；内存缓冲有上限，超出的行丢弃并计数，丢过行的源不推进持久游标 | `test_mdg_shards.py::test_failed_write_keeps_rows_and_backs_off`、`::test_spill_after_persistent_failures_then_recover`、`::test_memory_is_bounded_and_drops_are_counted`、`::test_vanished_day_folder_is_recreated`；`test_mdg_recording.py::test_dropped_rows_never_make_a_cursor_durable`、`::test_spilled_shards_are_visible_to_the_day_readers` |
| F31 | 运行中修改 md-gateway 不能热加载的配置（`feeds.yaml` 中仲裁参数以外的部分、`products.yaml`、日历） | 两阶段提交的准备阶段投否决票（提示需要重启），整次热加载放弃；仲裁参数照常热加载 | `test_mdg_resilience.py::test_config_changes_outside_arbitration_are_vetoed`；`test_apigw_config2pc.py::TestReload::test_rejected_vote_aborts` |
| F32 | md-gateway 整体不可用（进程退出、心跳停止） | risk-engine 把行情质量视为 STALE：禁开，平仓照常，服务为 DEGRADED；md-gateway 回来后等拿到新的快照才恢复 | `test_re_runtime.py::test_md_gateway_down_blocks_opens_until_heartbeat_and_snapshot_return`；`test_re_chain.py::test_real_md_gateway_ticks_drive_the_shell_and_its_loss_fails_safe`（真实 sim-exchange 和 md-gateway） |
| F33 | trade-gateway 不可用，或对路由没有应答 | 下单以 `REJECT_GATEWAY_UNAVAILABLE` 拒绝（开仓和平仓都拒），恢复后照常路由；路由没有应答的委托转为 `UNKNOWN`，再按查询结果处理 | `test_re_runtime.py::test_trade_gateway_down_rejects_orders_and_recovers`、`::test_unanswered_route_becomes_unknown_and_is_resolved_as_lost`；`test_engine_orders.py::TestRouting::test_gateway_down_marks_in_flight_orders_unknown` |
| F34 | 柜台强平（`BROKER_FORCE_CLOSE`） | trade-gateway 把它作为外部成交（`EXTERNAL`）入库并发布；risk-engine 据此更新持仓，并发一条 `EXTERNAL_FILL` 风险事件（HIGH） | `test_tg_integration.py::test_broker_force_close_publishes_an_external_trade`；`test_tg_core.py::TestReports::test_broker_force_close_is_an_external_trade`；`test_engine_orders.py::TestFillsAndReservations::test_external_force_close` |
| F35 | 轮询源（Feed B、新浪）的应答或 Feed A 的订阅快照中，有合约在本时段还没更新（时间戳早于当前交易窗口，例如 CFFEX 09:25 集合竞价时的股指期货）；合约整体不在交易时段（午休）或仍带上一交易日 | 当作过期数据跳过：不校验、不记录、不计入该源的拒收率，所以不会因此排除数据源（否则 CFFEX 集合竞价期间 Feed B 的拒收率会超过排除线）；Feed A 推送的状态落在交易时段之外，或轮询记录的时间晚于收到时刻，仍以 `MD_REJECT_OUT_OF_SESSION` 拒收 | `test_mdg_validator.py::test_stale_polled_snapshot_in_an_open_session_is_skipped_not_rejected`、`::test_polled_record_out_of_session_now_is_skipped`；`test_mdg_integration.py::test_promotion_while_the_market_is_closed`（休市时轮询记录全部跳过，拒收为 0） |

## 6. 端到端验收测试

`tests/e2e/test_e2e_acceptance.py` 用一个模块级夹具启动整套系统，5 个用例按顺序共用它（后面的用例依赖前面留下的状态）：
1. `e2e_support.run_home()` 在 `var/e2e/run-<时间戳>/home` 新建 home（只保留最近 3 次运行，便于事后查看日志、数据库和报表）；`make_home()` 复制自带的配置，并给每个总线端点、HTTP 端口以及 sim-exchange 的 Feed A、Feed B、柜台和控制端口分配临时空闲端口（避开默认的 7000~8100）；
2. 以子进程运行 `futrisk demo --scenario breaker --headless --timeout 240 --start-at 2026-09-28T08:59:40 --home <home>`：supervisor 启动它管理的 8 个服务实例（sim-exchange、md-gateway、trade-gateway、clearing-worker、risk-engine-0、risk-engine-1、api-gateway、strategy-sim），首次启动时建账，市场时间从日盘开盘前 20s 开始；等 ACC3 的多头黑色系持仓达到权益的 0.7 倍后启动 `breaker` 剧本；
3. 用例只通过公开接口观察：REST（`/api/v1`，用 home 中的 bootstrap ADMIN 密钥）、CLI（`futrisk kill`、`futrisk down`）和 md-gateway 的 PUB 套接字（订阅 `md.tick.`）；
4. 结束时 `futrisk down` 停止全部进程。

| 用例 | 过程 | 断言 |
|---|---|---|
| `test_breaker_levels_raise_once_each` | 等 ACC3 的 `daily_loss/halt` 变为 ACTIVE（最长 600s） | 账户 HALTED，`daily_pnl_pct_init` ≤ −0.03；`daily_loss` 的 warn、restrict、halt 三级都已告警，时间先后为 warn ≤ restrict ≤ halt；ACC3 的 `ALERT_RAISED` 中没有重复的 `alert_key` |
| `test_opens_rejected_closes_fill_while_halted` | 熔断后对 ACC3 下 rb2701 的市价开仓单和平今单（各 1 手） | 开仓被拒（`REJECT_ACCOUNT_HALTED`）；平仓单 30s 内 FILLED |
| `test_halt_survives_risk_engine_kill` | `futrisk kill risk-engine-1`（ACC3 所在的分片） | supervisor 拉起后（重启次数 ≥ 1，进程状态和心跳状态都是 RUNNING，最长 90s）`daily_loss/halt` 仍为 ACTIVE，账户仍为 HALTED；hc2701 开仓仍被拒；告警键没有重复，杀之前的告警键都还在，`daily_loss` 的告警都属于第 1 个 episode |
| `test_feed_a_outage_fails_over_and_backfills_without_duplicates` | 订阅 `md.tick.`；等 `breaker` 剧本结束后用 `POST /demo/scenario` 启动 `feed_outage`（Feed A 断开 20s；第 45s 丢 15 条造成跳号；第 60s 起 15s 内重复比例 0.2；第 90s 起 15s 乱序），从启动起观察 75s | 每个合约的 `order_key` 严格递增（LIVE 不重复、不回退）；中断的前 20s 内 rb2701 的 tick 间隔都小于 4s（Feed B 接替）；`/market/events` 中 `FEED_A` 有 `TRUSTED → EXCLUDED` 和 `PROBATION → TRUSTED` 两次迁移；有 `GAP_FILLED` 且补数条数 > 0，所有 `GAP_FILLED` 的重复条数为 0 |
| `test_eod_settles_reconciles_and_reports` | `POST /demo/clock` 把市场时钟拨到 15:29，EOD 按 `eod_start_at`（15:30）自动开始 | 180s 内 20260928 的 EOD 到 DONE（中途 FAILED 即失败）；5 个账户都已结算、`checks_passed` 都为真，全部检查为 PASS 或 WARN；ACC3 的交易所公式盈亏与"平仓 + 持仓"之差小于 0.01；报表列表中有 `RISK_DAILY_HTML`，`reports/20260928/risk_daily.html` 含"熔断"和 ACC3；60s 内 ACC3 换到 20260929 |

`feed_outage` 剧本第 90s 起的乱序在 75s 的观察窗口之外，这个用例没有覆盖它；乱序由 F2 的服务级测试覆盖。

本次执行的结果见 §7。此前的记录：2026-09-26 在精简版上运行（`pytest -m e2e tests/e2e -v`），5 例全部通过，用时 400s；精简前（`v0.1-full`）的一次运行 5 例全部通过（354s），从库中核对到：
- ACC3 的 `daily_loss` 告警、限制、熔断三个级别各恰好 1 条告警（`alert_key` 各不相同）；
- 硬杀 `risk-engine-1` 后，supervisor 1.0s 后开始重启（退避 1s），再过 1.9s 回到 RUNNING，从被杀到恢复共 2.9s（QS-04 的目标 < 3s）；熔断仍然锁存；
- Feed A 中断产生 2 个缺口，全部补齐：补数 438 条，重复 0 条，不可恢复 0 条；
- EOD 在 15:30 自动开始并完成，5 个账户的对账检查全部通过，日报与 5 个账户的结算单（TXT、CSV）都已生成。

这两次运行都早于 2026-10-07 md-gateway 的重写，Feed A 中断一项的数字是旧 md-gateway 的结果。

## 7. 执行结果

### 7.1 Python 测试

执行日期 2026-10-08，当前代码（分支 `md-slim`：md-gateway 精简、`marketdata` 包并入 md-gateway 之后）上按目录分别运行（`pytest tests\<目录> -q --timeout 300`，含慢速用例），共 1673 例，全部通过，没有跳过。与 2026-09-26 的 1747 例相比少 74 例，全部在行情部分：当时 `unit/marketdata` 154 例加 `unit/md_gateway` 159 例，共 313 例；现在 `unit/md_gateway` 203 例加 `unit/cli` 中新浪录制命令与进程守护的 36 例，共 239 例（精简删除了已删除功能的测试，`marketdata` 的测试并入 md-gateway）。其余目录的用例数不变。

同日的截图会话中发现并修正了 md-gateway 校验器的一个缺陷（§8 第 11 项），`unit/md_gateway` 新增 1 例，变为 204 例。修正后的代码在 §7.4 的覆盖率测量中把全部 1674 例又运行了一遍，全部通过；§6 的端到端验收也在修正后的代码上重跑（5 例全部通过，用时 357.1s）。

| 目录 | 内容 | 用例数 | 结果 | 用时 |
|---|---|---:|---|---:|
| `unit/core` | 清算与风控内核的纯函数：lot、手续费、保证金、盈亏、结算价、结算、日内账本 | 145 | 通过 | 4s |
| `unit/engine` | `EngineCore`：事前检查、状态机、验收场景、委托、EOD、公司层、性质测试 | 194 | 通过 | 10s |
| `golden` | 黄金用例 | 42 | 通过 | 2s |
| `property` | 清算性质测试 | 9 | 通过 | 18s |
| `unit/bus` | 总线：PUB/SUB、请求应答、事实流补齐、化身、LV 与快照 | 87 | 通过 | 7s |
| `unit/db` | 数据库层：写库线程、组提交、outbox、迁移 | 71 | 通过 | 4s |
| `unit/runtime` | 服务运行时：两阶段提交参与方、GC 策略、存活检测 | 43 | 通过 | 23s |
| `unit/md_gateway` | 行情网关：Feed A 序号与续传、轮询传输、Feed B 与新浪解码、归一化与指纹、校验、仲裁、可信状态、质量、录制分片、结算价输入、故障场景 | 203（修正后 204） | 通过 | 94s |
| `unit/risk_engine` | 风控服务外壳：真实套接字、恢复、崩溃矩阵、硬杀子进程 | 55 | 通过 | 100s |
| `unit/trade_gateway` | 交易网关：路由、墓碑、补回报、kill switch、与模拟柜台集成 | 115 | 通过 | 21s |
| `unit/clearing` | 清算服务：EOD saga、对账、补结算、崩溃恢复、报表 | 74 | 通过 | 63s |
| `unit/api_gateway` | REST、RBAC、WS、读模型、通知、两阶段提交协调、kill switch、页面托管 | 192 | 通过 | 62s |
| `unit/supervisor` | 进程管理、重启策略、市场时钟、只管时钟模式、真实整套进程 | 79 | 通过 | 168s |
| `unit/sim` | 虚拟交易所：价格引擎、Feed A/B、模拟柜台、控制接口 | 83 | 通过 | 62s |
| `unit/bots` | 策略机器人与剧本校准 | 56 | 通过 | 22s |
| `unit/client` | 策略 SDK | 37 | 通过 | 6s |
| `unit/cli` | 命令行工具，含新浪录制命令（会话门控、信号、落盘）与进程守护 | 104 | 通过 | 33s |
| `unit/backtest` | 回测：数据加载与换月、路径合成、双均线策略、统计、合成剧本、运行入口（含 1 个真实数据的慢速用例） | 59 | 通过 | 24s |
| `unit/test_foundation.py` | 日历、十进制、时钟、消息、配置 | 25 | 通过 | 1s |
| 合计 | | 1673（修正后 1674） | 全部通过 | 约 12 分钟 |

用时是每个目录的墙钟时间（含解释器启动），运行时本机同时有文档核对任务在执行。§5 故障注入矩阵引用的测试都在上表的目录中，随之全部通过。

端到端验收（§6）：`pytest -m e2e tests\e2e -v --timeout 1500`，5 例全部通过，用时 377.6s（修正前的代码）；校验器修正后重跑一次，5 例全部通过，用时 357.1s。

### 7.2 前端测试

`web/`：vitest 12 个测试文件、173 例全部通过（3.4s）；`vue-tsc --noEmit` 类型检查通过（44s）；`npm run build` 在 §7.5 的构建中成功。测试前清理了仪表盘中已删除功能的残留定义（行情事件中的切换抖动和尖刺隔离、tick 的隔离模式、1 分钟 K 线的 `Bar` 类型），离线模拟器的 `BAD_TICK` 改为与当前校验器一致的拒收码，并按 `futrisk.messages` 重新生成了 `web/src/mock/wire.gen.ts`。

### 7.3 静态检查与文档一致性

| 检查 | 命令 | 结果 |
|---|---|---|
| ruff | `.venv\Scripts\ruff check src tests scripts` | 通过 |
| mypy | `.venv\Scripts\mypy src\futrisk` | 通过（266 个源文件） |
| 文档一致性 | `.venv\Scripts\python scripts\check_docs.py` | 0 个问题（与代码核对 55 张表 659 列、152 个消息结构 922 个字段、52 个枚举和全部错误码；各篇引用的主题和命令都在 02 定义的 20 个主题、63 个命令之中） |
| 配置参考 | `.venv\Scripts\python scripts\gen_config_reference.py --check` | 11 的生成块与配置模型一致 |

### 7.4 覆盖率

方案（01 §8.11）要求风控内核、risk-engine、clearing-worker、md-gateway 和总线的行覆盖率不低于 85%。2026-10-08 在修正后的代码上用 pytest-cov 测量：全部单元、黄金和性质测试按目录分别运行，覆盖数据累加到同一个文件，再按包汇总（`--cov=futrisk --cov-append`；§9 中的覆盖率命令是以前只测 7 个包的口径）。

| 包 | 语句数 | 未覆盖 | 行覆盖率 |
|---|---:|---:|---:|
| `futrisk.core`（风控与清算内核） | 7193 | 374 | 94.8% |
| `futrisk.services.api_gateway` | 5602 | 505 | 91.0% |
| `futrisk.services.clearing` | 4190 | 309 | 92.6% |
| `futrisk.services.md_gateway` | 3935 | 215 | 94.5% |
| `futrisk.cli` | 3648 | 507 | 86.1% |
| `futrisk.sim.exchange`（虚拟交易所） | 3082 | 280 | 90.9% |
| `futrisk.bus` | 2626 | 216 | 91.8% |
| `futrisk.backtest` | 2406 | 194 | 91.9% |
| `futrisk.services.trade_gateway` | 2392 | 196 | 91.8% |
| `futrisk.sim.bots`（策略机器人） | 2036 | 190 | 90.7% |
| `futrisk.services.supervisor` | 1965 | 204 | 89.6% |
| `futrisk.runtime` | 1867 | 179 | 90.4% |
| `futrisk.messages` | 1785 | 8 | 99.6% |
| `futrisk.client` | 1681 | 134 | 92.0% |
| `futrisk.services.risk_engine`（服务外壳） | 1678 | 150 | 91.1% |
| `futrisk.db` | 1180 | 73 | 93.8% |
| `futrisk.common` | 1054 | 71 | 93.3% |
| `futrisk.main`（命令行入口） | 49 | 11 | 77.6% |
| 合计 | 48370 | 3816 | 92.1% |

只统计测试进程内执行的代码：由测试启动的子进程（例如硬杀测试中的 risk-engine 进程、真实的 sim-exchange、端到端测试的整套进程）不计入，所以服务外壳和虚拟交易所的实际覆盖高于表中数字。01 §8.11 要求的这几个包都达到 85%，其余服务也都在 89% 以上；最低的是命令行 `futrisk.cli`（86.1%）和只有 49 条语句的入口 `futrisk.main`。与 2026-09-26 之前在完整版（`v0.1-full`）上的测量（10 个目录、7 个包合计 92.7%）相比，这次覆盖全部包；md-gateway 重写后语句数从 4925 降到 3935，覆盖率从 91.2% 升到 94.5%。

### 7.5 可执行文件

`scripts\build_exe.ps1` 在 2026-10-08 构建了两次，两次的 17 项冒烟测试全部通过：

| 构建 | 代码 | 用时 | 冒烟测试 |
|---|---|---|---|
| 第 1 次 | 校验器修正前 | 296s（仪表盘 64s、PyInstaller 108s、冒烟测试 107s、打包 16s） | 17 项全部通过 |
| 第 2 次（交付） | 校验器修正后 | 259s（仪表盘 70s、PyInstaller 59s、冒烟测试 110s、打包 19s） | 17 项全部通过 |

冒烟测试覆盖：`version`、`config validate`、`doctor`（frozen exe、时区、仪表盘文件、pyzmq、SQLite、非 ASCII 路径）、api-gateway 健康检查与页面、父进程退出后服务自行停止、回测 `breaker_synthetic`；在临时 home 中 `futrisk up --headless` 拉起全部 8 个实例、`futrisk demo --headless` 建账，60s 后仍全部 RUNNING、重启 0 次，`futrisk status` 正常，`futrisk down` 后没有残留进程；以及在中文路径下重复前六项。产物：`dist\futrisk\`（约 192 MiB）和 `dist\futrisk-win64-0.1.0.zip`（94406024 字节，约 90 MiB），SHA-256 写在同名的 `.sha256` 文件中（`1c5b301bc9c659f3028fd976f60b4adfc265ffc2fd98ab1320e5e60ee4d9d765`）。

同一天用第 1 次构建的 exe 完成了 07 的全部截图（`docker-ps.png` 除外，它取自 §7.7 的 docker 复跑；§8 第 11 项的缺陷正是在这次运行中发现的），又用第 2 次构建的 exe 跨过 09:25 的中金所集合竞价重拍了行情与系统页：市场时间从 09:20 开始，09:25 集合竞价前后两个源一直是 TRUSTED、拒收率为 0，没有再出现 §8 第 11 项的排除；随后的 `feed_outage` 剧本中，Feed A 在 09:27:44 断开被排除，Feed B 接续，09:28:08 重连后按序号续传补齐，10s 观察期后于 09:28:18 恢复 TRUSTED，剧本在 09:28:30 制造的 15 条跳号也按序号补齐、重复 0 条（07 §3.5 的两张截图取自这次运行）。

### 7.6 性能

性能基准的方法、结果和未达标项见 13。

### 7.7 docker compose

| 日期 | 代码 | 结果 |
|---|---|---|
| 2026-10-07 | 校验器修正前 | 首次完整运行（Docker Desktop，Docker 29.2.1、Compose v5.0.2，Linux 容器）：构建约 8 分钟（拉取基础镜像与 `npm ci` 各约 4.5 分钟、并行；pip 安装依赖 50s；镜像 896 MB）。`up -d` 后 config-init 写入配置版本 1（docker 档位）并退出，supervisor 以只管时钟模式运行并健康，seed 在市场时间 08:58 为 5 个账户建账后退出，9 个常驻容器全部 RUNNING；宿主机 `127.0.0.1:8000` 的仪表盘与 REST 可用，api-gateway 容器内的 `futrisk status`、`ps`、`clock show`、`accounts` 正常；09:00 开盘后机器人下单成交（6 分钟内 22 笔订单、5 个持仓），5 个账户风控状态 NORMAL；`docker compose restart md-gateway` 只重启这一个容器，20s 内回到 RUNNING、两个源重新 TRUSTED；`down` 后再 `up -d`，config-init 报告保留版本 1，seed 发现账本已存在、不再建账，市场时钟接着走；全部容器日志没有 ERROR 或 Traceback |
| 2026-10-08 | 校验器修正后 | 以独立的项目名和全新的数据卷运行（不影响 10-07 的卷）：增量构建 289s（基础镜像和依赖层命中缓存），镜像 896 MB；`up -d` 后 5s 内 9 个常驻容器全部就绪，config-init 写入配置版本 1，seed 在市场时间 08:58:13 首日建账；开盘后（09:00:55）两个源都是 TRUSTED，08:55~09:00 集合竞价期间 Feed B 没有被排除（修正前的 10-07 在 08:58:06 被排除、6 分钟后才恢复），机器人在开盘后 1 分钟内下了 5 笔委托；全部容器日志 0 条 ERROR 或 Traceback；最后用 `down -v` 删除这次的卷 |

10-07 那次运行中还看到两类可以自行恢复的现象，事后根据录制的 tick 分清了原因：

- **Feed B 在 08:58:06 被排除**：首日是节后第一个交易日、没有夜盘，08:55~09:00 是集合竞价，Feed B 返回的快照时间戳仍停在 08:53:53，被校验器判为 `MD_REJECT_OUT_OF_SESSION`，拒收率达到阈值。这是 §8 第 11 项的校验器缺陷，已在 10-08 修正。
- **09:03 起的 `MD_REJECT_FUTURE_TS`**：宿主机时钟慢 4 分 11 秒（Windows Time 服务停止，`futrisk doctor` 的 clock sync 项有警告），Docker Desktop 每约 30s 把虚拟机时钟拨回宿主机时间、1s 内又纠正回来，各进程锚定的时间相差 1~2 秒；md-gateway 把交易所时间比收到时刻晚 1 秒以上的 tick 判为未来时间戳（Feed A 12182 条、Feed B 1342 条），期间源状态有过切换，都自行恢复，没有服务崩溃或重启。时钟正常的机器上不会出现；部署前应当先校准宿主机时钟（08 §3.3）。

## 8. 已知问题与未覆盖项

| # | 问题或缺口 | 影响 | 处理 |
|---|---|---|---|
| 1 | 在**一个** pytest 进程中跑完整个 `tests/` 时，曾有一次停在一个启动 `spawn` 子进程的用例上超过 20 分钟；另有一次在后台的 Git Bash 中按目录连续运行，`sim`、`supervisor`、`trade_gateway` 三个目录的 pytest 进程中途退出、没有汇总行。项目没有全局超时（`pytest-timeout` 只在命令行加 `--timeout` 时生效），而 Windows 上 `pytest-timeout` 的 thread 方式在超时时打印线程栈后直接结束整个进程，这很可能就是"没有汇总行"的原因 | 一次性全量运行可能挂住或中途退出，但不是某个用例失败 | 按目录运行（§9），并始终带 `--timeout`；2026-10-08 的运行（§7.1）按此方式全部通过 |
| 2 | 端到端的 EOD 用例在 2026-09-26 之前没有完整跑通过：当时发现并修正了用例自身的两处错误（`POST /demo/clock` 的请求体缺少必填的 `target`；报表列表的类型字段名是 `type`），产品代码未改 | 2026-09-26 之前的端到端证据不含 EOD | 已修正，此后每次运行都包含 EOD |
| 3 | docker compose 只在本机的 Docker Desktop 上完整跑过（§7.7 的两次），没有在 Linux 服务器上部署过 | 容器部署的证据来自同一台开发机 | 上线前在目标机器上重复 §7.7 的步骤 |
| 4 | 只测了模拟柜台（按 CTP 语义实现），没有连接真实的期货公司柜台 | CTP 适配器只有接口 | 生产差距见 08 §5 |
| 5 | 新浪真实 tick 从 2026-09-28 开始录制；精简版删除了用真实 tick 回放的 C 层回测，录下的 tick 只作真实行情样本。`futrisk data record` 现在直接运行 md-gateway 的轮询传输、新浪解码和校验、仲裁流水线（新浪是唯一可信源），不在交易时段或仍是上一交易日的快照跳过、不落盘；`futrisk data rebuild` 已删除 | 真实 tick 的回放不在精简版中 | 需要时检出 `v0.1-full`，运行 `futrisk backtest run ticks` |
| 6 | 01 §10.2 的 QS-10（服务状态变化 3s 内反映到仪表盘）没有自动化的端到端断言，只有读模型与推送的单元测试，以及在真实系统上的人工检查 | 3s 指标没有自动回归 | — |
| 7 | 全部自动化测试只在 Windows 11 上运行过 | Linux 上的证据只有 §7.7 的 docker compose 运行 | 同第 3 项 |
| 8 | 性能目标有未达标项 | 见 13 | 见 13 |
| 9 | 仪表盘每次加载时浏览器控制台有一条 CSP 报告：vue-echarts 初始化时用 `new Function` 探测能否注册自定义元素，被页面的 CSP（不允许 `eval`）拦下后自动改用普通容器；报表查看器以沙箱方式显示报告，控制台另有一条沙箱文档不能访问 `localStorage` 的报告 | 只是控制台多了报告，功能不受影响（真实系统上各页检查过） | 保持 CSP 和沙箱不放宽 |
| 10 | 账户页的日内权益曲线在权益变化很小时（例如 999.9 万 ~ 1000.4 万），纵轴刻度都显示为"1000万"（2026-10-08 的截图会话中在 ACC1 上再次看到） | 纯显示问题，曲线本身正确 | 刻度格式按范围增加小数位 |
| 11 | **2026-10-08 发现并修正**：轮询源（Feed B、新浪）和快照的记录如果在交易时段已开、但本时段尚未更新（时间戳仍停在时段开始之前），校验器把它判为 `MD_REJECT_OUT_OF_SESSION` 并计入拒收率。截图会话中 09:25 中金所集合竞价一开始，Feed B 返回的 IF、IH、IC、T 快照时间戳仍是 09:20:01，13 个合约中 4 个每次轮询都被拒，拒收率约 0.3，Feed B 在 09:25:07 被排除，数分钟后才恢复；无夜盘交易日的 08:55 集合竞价同样触发（§7.7 中 10-07 的 docker 运行）。重写前的校验器也是这样判定的，这个缺陷不是精简引入的，只是以往的演示和测试都没有跨过集合竞价 | 系统在集合竞价开始之前启动时（如从 09:20 开始的演示、首次启动的 docker），竞价一开始就失去第二个源的冗余，持续数分钟（一直运行的系统中，竞价前的快照仍属于上一交易日，按跳过处理，不受影响）；这段时间如果 Feed A 也中断，行情会中断 | 已修正：这类记录按过期快照跳过，不计拒收；推送的实时 tick 和时间戳晚于收到时刻的记录仍按时段外拒收。新增 `unit/md_gateway/test_mdg_validator.py::test_stale_polled_snapshot_in_an_open_session_is_skipped_not_rejected`；修正后的 exe 跨过 09:25 的运行见 §7.5 |
| 12 | §6 的 `feed_outage` 剧本第 90s 制造的乱序不在端到端用例 75s 的观察窗口内；md-gateway 的测试中没有注入 `FEED_LATENCY` 的用例 | 这两种情况没有端到端或服务级的自动化证据（乱序有 Feed A 序号跟踪的单元测试） | 需要时延长观察窗口或补用例 |
| 13 | 交付前的独立评审报告过一批疑似缺陷，2026-10-08 在当前代码上复核后仍有 12 项存在，另有 12 项是复核和本次重建文档、截图时发现的 | 见 08 §5.2 的"代码复核确认的缺陷"（KD-01~KD-24） | 功能封版期间不修复，列入后续工作 |

## 9. 如何运行

在仓库根目录的 Windows PowerShell 中执行，Python 用 `.venv` 中的解释器：

```powershell
# Python 测试：每个目录一个 pytest 进程（含 slow 用例，每个用例限时 300s）。
# 一个用例超时会结束整个进程（§2.2），分开运行时只影响它所在的目录
$dirs = 'unit\core', 'unit\engine', 'golden', 'property', 'unit\bus', 'unit\db', 'unit\runtime',
        'unit\md_gateway', 'unit\risk_engine', 'unit\trade_gateway', 'unit\clearing', 'unit\api_gateway',
        'unit\supervisor', 'unit\sim', 'unit\bots', 'unit\client', 'unit\cli', 'unit\backtest', 'unit\test_foundation.py'
foreach ($d in $dirs) { .venv\Scripts\python -m pytest "tests\$d" -q --timeout 300 }

# 只跑没有 slow 标记的用例（一个进程）
.venv\Scripts\python -m pytest tests -m "not slow" -q --timeout 300

# 一个文件或一个用例
.venv\Scripts\python -m pytest tests\unit\md_gateway\test_mdg_pipeline.py -q
.venv\Scripts\python -m pytest "tests\unit\engine\test_engine_acceptance.py::TestAcceptanceBreaker::test_floating_only_variant_also_triggers" -q

# 覆盖率（01 §8.11 的目标：core、risk-engine、clearing-worker、md-gateway、bus 不低于 85%）
.venv\Scripts\python -m pytest tests\unit\core tests\unit\engine tests\golden tests\property tests\unit\bus tests\unit\db tests\unit\md_gateway tests\unit\risk_engine tests\unit\clearing tests\unit\trade_gateway -q --timeout 300 --cov=futrisk.core --cov=futrisk.db --cov=futrisk.bus --cov=futrisk.services.md_gateway --cov=futrisk.services.risk_engine --cov=futrisk.services.clearing --cov=futrisk.services.trade_gateway --cov-report=term
# 2026-10-08 的全量测量（§7.4）：全部目录分别运行，覆盖数据累加到同一个文件后汇总
Remove-Item .coverage -ErrorAction SilentlyContinue
foreach ($d in 'core','engine','bus','db','runtime','md_gateway','risk_engine','clearing','trade_gateway','api_gateway','supervisor','sim','client','bots','cli','backtest') { .venv\Scripts\python -m pytest "tests\unit\$d" -q --timeout 300 --cov=futrisk --cov-append --cov-report= }
.venv\Scripts\python -m pytest tests\unit\test_foundation.py tests\golden tests\property -q --timeout 300 --cov=futrisk --cov-append --cov-report=
.venv\Scripts\python -m coverage report

# 端到端验收（supervisor 加 8 个服务实例，需要几分钟；运行目录留在 var\e2e\ 下）
.venv\Scripts\python -m pytest -m e2e tests\e2e -v --timeout 1500
# 或者用环境变量
$env:FUTRISK_E2E = '1'; .venv\Scripts\python -m pytest tests\e2e -v; Remove-Item Env:FUTRISK_E2E

# 前端（在 web 目录中）
Push-Location web; npm test; npm run typecheck; Pop-Location

# 静态检查与文档
.venv\Scripts\ruff check src tests scripts
.venv\Scripts\mypy src\futrisk
.venv\Scripts\python scripts\check_docs.py
.venv\Scripts\python scripts\gen_config_reference.py --check
```

---

## 附录 A 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-26 | 初版：测试分层与原则、需求追踪、26 项故障注入矩阵、端到端验收，以及代码冻结后的一次完整执行报告 |
| v0.2 | 2026-09-26 | 范围精简（完整版保留为 git 标签 `v0.1-full`）：删除基准工具的 14 项测试；回测测试按精简后的代码重写（§2 测试分层、§8 第 5 项）；在精简版上重新执行 §7.1（1747 例全部通过）、§6 端到端验收（5 例通过）和 §7.5 可执行文件构建（17 项冒烟全部通过）；§8 第 1 项补充一次后台运行的观察和修正的负载敏感用例 |
| v0.3 | 2026-10-07 | docker compose 在本机完整跑通一次（§7.7：构建、启动、建账、交易、单服务重启、停止后再启动），§1 与 §8 第 3、7 项随之更新；记录了宿主机时钟不准引起的虚拟机时钟跳变以及系统的自行恢复 |
| v1.0 | 2026-10-08 | 按代码重建（实现对齐）：§1~§6、§9 逐条对照 `tests/`、`pyproject.toml` 与代码核对，全部 295 处测试引用经 AST 核对存在（类中的用例写出类名）。§4 按 task.md 改为 17 行，删除已不存在的 `test_mdg_bars.py` 与"尖刺隔离"，订正用例出处（`test_halt_reset_and_role_checks` 在 `test_engine_misc.py`，写库失败停写在 `test_runtime_review_fixes.py`，回测为 59 例、1 个真实数据用例）；§5 按 2026-10-07 精简后的 md-gateway 改写 F1~F26（交叉校验 `VOTE` / `NO_VOTE`、`EPOCH_CHANGED`、Feed B 畸形应答与 `feed_b:` 缺口、风控不可用时的应答），新增 F27~F35（未来时间戳、数据源排除与恢复、LIVE 流落后、落盘失败、不可热加载的配置、md-gateway 或 trade-gateway 不可用、柜台强平、轮询源中本时段尚未更新的快照按过期跳过），去掉"结果"列；§2 订正超时（没有全局超时，Windows 上超时会结束整个 pytest 进程）、`slow` 标记的 9 个模块、性质测试与前端测试的范围，新增 §2.3 目录与文件；§3 按 `.venv` 与 `web/node_modules` 核对版本；§6 按 `test_e2e_acceptance.py`、`e2e_support.py` 重写（启动参数、观察面含 md-gateway 的 PUB 套接字、各用例实际的断言，`feed_outage` 第 90s 的乱序不在观察窗口内）；§9 改为 PowerShell 下可直接运行的命令；§7 改为 2026-10-08 的全量验证结果：按目录运行 1673 例全部通过（与 2026-09-26 相比少的 74 例全部来自行情部分的精简与合并），截图会话中发现并修正 md-gateway 校验器的缺陷后新增 1 例，修正后的 1674 例在覆盖率测量中再次全部通过，端到端 5 例修正前后各跑一次均通过；前端 173 例、ruff、mypy、文档一致性检查通过；覆盖率改为对全部包测量（合计 92.1%）；exe 构建两次，17 项冒烟测试均通过；docker compose 增加 2026-10-08 的复跑，并更正 10-07 两类现象的原因（Feed B 被排除是校验器缺陷；未来时间戳的阈值是 1 秒）；§8 更新第 1~5、7、9、10 项，新增第 11~13 项（校验器缺陷及修正、端到端观察窗口与 `FEED_LATENCY` 的缺口、指向 08 §5.2 的代码复核缺陷）；独立交叉审查（同日）对照各次运行的日志、构建记录、截图和代码逐条复核 §7、§8，改正 11 处（端到端命令的 `--timeout`、文档一致性检查对主题和命令的实际核对范围、01 §8.11 覆盖率目标的范围和 §9 命令的差别、exe 冒烟测试由 `up` 拉起、exe 目录约 192 MiB、`docker-ps.png` 的来源、"冻结"一词、§8 第 11 项的影响范围、复核缺陷的来源计数、本行的 §8 项号） |
