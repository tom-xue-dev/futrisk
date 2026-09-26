# futrisk 测试方案与测试报告

| 项 | 内容 |
|---|---|
| 文档编号 | FR-STP-06 |
| 版本 | v0.1（按已实现的系统编写） |
| 状态 | 实现对齐 |
| 日期 | 2026-09-26 |
| 适用范围 | futrisk 0.1.0 的全部自动化测试：单元测试、黄金用例、性质测试、真实进程与回环网络上的集成测试、故障注入、端到端验收、前端测试、静态检查与文档一致性检查；性能基准的结果见 13 |
| 对应标准 | GB/T 8567-2006 软件测试计划（STP）与软件测试报告（STR） |
| 关联文档 | [README](README.md)（规范）· [01 架构](01-architecture.md)（质量需求见 01 §10）· [04 风控规则](04-risk-rules.md) · [05 清算](05-clearing.md) · [07 使用说明](07-user-guide.md) · [13 性能基准](13-benchmarks.md) |

> 本文前半部分是测试方案：测什么、怎么分层、每条需求由哪些测试覆盖、故障注入矩阵；后半部分是 2026-09-26 在代码冻结后的一次完整执行的报告：结果、耗时、覆盖率和已知问题。每个结论都给出可以直接重跑的测试文件或命令。

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
2. **故障下会不会出错**：行情中断、重复、乱序、坏点，服务崩溃和重启，都不产生重复或错误的风险信号，熔断状态不丢；
3. **题目的验收场景能否复现**：ACC3 当日亏损达到初始资金的 3% 时自动熔断，此后开仓被拒、平仓照常，风控服务被杀后熔断仍然保持。

质量目标以 01 §10.2 的质量场景（QS-01 ~ QS-11）为准。本文不测试的内容：真实期货公司柜台（CTP）的连接，只测试按 CTP 语义实现的模拟柜台；docker compose 部署只做了配置语法检查（§8）。

## 2. 测试策略

### 2.1 测试分层

| 层 | 做法 | 位置 | 用例数 |
|---|---|---|---|
| 单元测试 | 纯函数和单个类，不启动进程；时间全部由注入的时钟给出，结果可以复现 | `tests/unit/` 各目录 | 见 §7.1 |
| 黄金用例 | 3 组多日手算清算案例和交易所 FAQ 案例，金额逐分比对（算术用 Decimal 复核，05 文档有逐步推导） | `tests/golden/` | 42 |
| 性质测试 | hypothesis 随机生成多日成交序列和价格路径，检查恒等式。清算：资金恒等式、持仓守恒、账户独立、逐日盯市累计 = 逐笔对冲累计、交易所公式 = "平仓 + 持仓"、手续费与独立算法一致、盘中账本收敛到结算结果、单向大边；风控内核：告警状态机的不变量、随机价格路径中途恢复后状态与不中断时一致、增量估值 = 全量重算、快速 what-if = 完整 what-if | `tests/property/`、`tests/unit/engine/test_engine_properties.py` | 14 |
| 服务级集成 | 服务外壳跑在真实的 ZeroMQ 回环套接字上，对面是另一个真实服务或脚本化的替身（fake）；部分用例启动真实的 sim-exchange 和子进程 | `tests/unit/*/test_*_integration.py`、`*_runtime.py`、`*_flows.py` 等 | 含在 §7.1 中 |
| 故障注入 | 在上面两层中注入行情故障、进程崩溃（包括在事务的各个阶段崩溃）、网络丢包、时钟回拨，断言系统行为（§5） | 分布在各服务目录 | 含在 §7.1 中 |
| 端到端验收 | 8 个真实进程（`futrisk demo`），只通过 REST 和 CLI 观察，覆盖熔断、kill 风控、行情中断、EOD（§6） | `tests/e2e/` | 5 |
| 回测 | D 层合成剧本（正确结果已知）在同一个 `EngineCore` 上精确断言验收场景和单个坏点；路径合成、统计定义、配对不变量（`alert_only` 的交易与 `no_rules` 完全相同）、结果的确定性；4 个慢速用例在真实历史数据上短跑各配置 | `tests/unit/backtest/` | 91 |
| 前端 | vitest：工具函数、store、页面组件；vue-tsc 类型检查 | `web/tests/` | 173 |
| 静态检查 | ruff（含禁止内核调用系统时钟的 banned-api 规则）、mypy（`src/futrisk` 全部模块） | — | — |
| 文档一致性 | `scripts/check_docs.py`：文档之间的交叉引用、主题、命令、枚举，以及与代码的表结构、消息字段、枚举取值、错误码、环境变量的对照；`scripts/gen_config_reference.py --check`：11 与配置模型一致 | — | — |
| 性能基准 | 完整版（git 标签 `v0.1-full`）的 `futrisk bench`，结果与分析见 13；精简版不含基准工具 | — | — |

### 2.2 原则
- **确定性**：风控内核不读系统时钟（ruff 规则强制），时间、随机数、ID 都由调用方注入；回测和属性测试固定种子。同一输入的风险事件哈希必须相同。
- **不依赖网络**：新浪行情用抓包样本和模拟的 HTTP 传输测试；外部世界由 sim-exchange 模拟。
- **先替身、后真实**：每个服务先对着替身测协议和边界情况，再与真实的相邻服务和真实进程一起测。
- **故障要断言结果**：每个故障场景都断言可观察的结果（告警条数、`alert_key` 不重复、HALTED 保持、补数条数、重复条数为 0），不只是"不崩溃"。
- **超时与标记**：所有测试带 `pytest-timeout`；多进程和长时间的测试标记为 `slow`（默认也运行，可以用 `-m "not slow"` 排除）；端到端测试标记为 `e2e`，默认跳过，用 `-m e2e` 或环境变量 `FUTRISK_E2E=1` 运行。

## 3. 测试环境

| 项 | 内容 |
|---|---|
| 机器 | AMD Ryzen 7 7735H（8 核 16 线程），64 GB 内存 |
| 操作系统 | Windows 11 家庭中文版。系统时区是 UTC+9（不是北京时间），代码一律显式使用 `Asia/Shanghai`，因此全部测试也验证了"与本机时区无关" |
| Python | 3.12.10（Poetry 虚拟环境 `.venv`）；pyzmq 27.2.0（libzmq 4.3.5）、msgspec 0.19.0、FastAPI 0.141.1、SQLAlchemy 2.1.0、pytest 8.4、hypothesis 6 |
| Node.js | 20.20.1（vitest、vue-tsc、Vite 构建） |
| 浏览器 | Microsoft Edge（无头模式，playwright-core 驱动），用于仪表盘在真实系统上的检查和截图 |

## 4. 需求与测试的追踪

task.md 的每条要求 → 覆盖它的测试 → 证据。文件名省略 `tests/` 前缀；`::` 后是具体用例。

| task.md 要求 | 测试 | 证据 |
|---|---|---|
| 接入至少 2 个数据源，统一格式 | `unit/md_gateway/test_mdg_normalizer.py`（Feed A、Feed B → 规范 Tick）；`unit/marketdata/test_md_sina.py`（新浪抓包样本，41 例）；`unit/sim/test_sim_feed_a.py`、`test_sim_feed_b.py`（两路模拟源的协议） | 同一快照经两路源得到相同的指纹（`unit/marketdata/test_md_fingerprint.py`） |
| 行情中断恢复后补全数据 | `unit/md_gateway/test_mdg_seqtracker.py`（跳号补发、缓冲区淘汰、epoch 变化）；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover`、`::test_restart_resumes_from_cursor_and_watermarks`；`e2e::test_feed_a_outage_fails_over_and_backfills_without_duplicates` | 缺口记录从 `GAP_OPEN` 到 `GAP_FILLED`，补数条数 > 0；K 线按累计成交量差分补齐（`test_mdg_bars.py`） |
| 不产生重复或错误的风险信号 | `test_mdg_resilience.py::test_wire_faults_on_both_feeds_never_duplicate_or_reorder_live`；`test_mdg_arbiter.py`（水位、指纹去重）；`test_mdg_validator.py`（坏点、尖刺隔离）；`unit/engine/test_engine_acceptance.py::test_single_bad_state_reverting_before_confirmation_revokes_the_halt`；`unit/bus/test_bus_fact.py::test_exactly_once_in_order`；`unit/api_gateway/test_apigw_notifier.py`（通知按 `notify_key` 去重，跨重启） | LIVE 流中重复和倒序为 0；单个坏点不会锁存熔断；每个级别只产生一条告警（e2e） |
| 持仓：开平仓记录、来源与更新机制 | `unit/core/test_core_lots.py`（lot、平今平昨、冻结）；`unit/engine/test_engine_orders.py`（委托生命周期与成交入账）；`unit/trade_gateway/test_tg_core.py`（成交先落库再发布、补回报、幂等） | 持仓只由成交推导；重复成交按成交键去重 |
| 按账户、按品种查询；多账户独立核算 | `unit/api_gateway/test_apigw_rest.py`、`test_apigw_views.py`（持仓与汇总接口）；`unit/cli/test_tools_trading.py`（`futrisk positions`）；`property/test_clearing_properties.py`（账户独立性） | 其他账户的成交不影响本账户的任何结果 |
| 盘中风控：资金维度与持仓维度的指标 | `unit/engine/test_engine_metrics.py`；`unit/core/test_core_ledger.py`；`unit/engine/test_engine_compliance.py`；`unit/engine/test_engine_quality_materiality.py`；`unit/engine/test_engine_firm.py`（公司层汇总） | 指标定义与 04 §3 一致 |
| 告警、限制下单、熔断三种动作 | `unit/engine/test_engine_rules_sm.py`（状态机：确认、滞回、冷却、锁存、重新布防）；`unit/engine/test_engine_pretrade.py`（事前检查每一步的拒单码，含平仓豁免） | 02 §11.2 的每个拒单码都有用例 |
| **验收：日亏损达初始资金 3% 自动熔断、禁止开仓** | `unit/engine/test_engine_acceptance.py`（9 例，含 `floating_only` 口径）；`unit/backtest/test_bt_scenarios.py`（D 层精确断言）；`unit/bots/test_bots_calibration.py`（真实风控内核上的剧本校准）；`e2e` 前 3 例；`unit/risk_engine/test_re_subprocess.py` | §6 |
| 逐日盯市、保证金计算 | `golden/test_clearing_golden.py`（42 例）；`property/test_clearing_properties.py`；`unit/core/test_core_settlement.py`、`test_core_margin.py`（含单向大边）、`test_core_settle_price.py`、`test_core_fees.py`；`unit/clearing/`（74 例） | 黄金用例误差为 0 分；交易所公式校验逐日通过 |
| 风控日报 | `unit/clearing/test_clearing_units.py`（报表模型与渲染）；`test_clearing_review.py::test_reports_render_with_peers_down`、`::test_regeneration_runs_once_clears_errors_and_registers_idempotently`；`e2e::test_eod_settles_reconciles_and_reports` | e2e 断言日报存在且包含熔断事件 |
| 附加：回测验证规则有效性 | `unit/backtest/`（91 例，含 4 个真实数据的慢速用例） | 方法与结论见 10 |
| 附加：仪表盘 | `web/tests/`（173 例）；`unit/api_gateway/test_apigw_ws.py`（推送协议）、`test_apigw_readmodel.py`（读模型补齐）、`test_apigw_static.py`（页面安全响应头） | 真实系统上的页面检查与截图（07 §3） |
| 附加：配置化 | `unit/api_gateway/test_apigw_config2pc.py`、`unit/runtime/test_runtime_config2pc.py`（两阶段提交与回滚）；`unit/cli/test_config_commit.py`（离线提交）；`unit/test_foundation.py`（配置模型） | 11 由模型生成且 `--check` 通过 |
| 附加：多账户对比 | `web/tests/compare.spec.ts`；`unit/api_gateway/test_apigw_views.py`（对比矩阵与余量排名） | 使用率与日报、核心同一定义（02 §7.6 `hard_utilization`） |
| 工程化：错误处理、日志、注释 | `unit/api_gateway/test_apigw_security.py`（未预期异常只返回 `error_id`）；`unit/runtime/test_runtime_review_fixes.py`；`unit/db/test_db_review_fixes.py`（提交失败后停止写入） | 静态检查全部通过 |

01 §10.2 的质量场景与测试的对应关系见该表的"实测 / 验证"一列，本文不重复。

## 5. 故障注入矩阵

"结果"一列是 §7 这次执行的结果。

| # | 故障 | 期望行为 | 测试 | 结果 |
|---|---|---|---|---|
| F1 | Feed A 断开 20s，Feed B 正常 | 瞬时切换到 B；A 恢复后先观察再回到可信；补数不进风控；LIVE 不重复 | `e2e::test_feed_a_outage_fails_over_and_backfills_without_duplicates`；`test_mdg_integration.py::test_normal_flow_and_feed_a_outage_failover` | 通过 |
| F2 | Feed A 跳号、重复、乱序 | 重复丢弃，乱序缓冲归位，缺口按区间补发 | `test_mdg_seqtracker.py`（16 例）；`test_mdg_resilience.py::test_wire_faults_on_both_feeds_never_duplicate_or_reorder_live` | 通过 |
| F3 | 无成交的价格跳变（坏点） | 拒收并记 `BAD_TICK` 事件 | `test_mdg_validator.py::test_bad_tick_price_without_trade_is_rejected_with_event` | 通过 |
| F4 | 单一数据源带成交量的尖刺 | 暂扣，另一个源确认或同源下一状态确认后才放行 | `test_mdg_validator.py::test_self_confirmed_spike_is_released_when_the_other_source_stays_silent`、`::test_polled_source_serving_the_held_spike_again_keeps_it_held` | 通过 |
| F5 | 单个坏状态越过熔断线后回落 | 进入 HALTED（待确认）立即禁开；回落后撤销，不锁存，不产生熔断事件 | `test_engine_acceptance.py::test_single_bad_state_reverting_before_confirmation_revokes_the_halt`；`test_bt_scenarios.py`（D 层单个坏点） | 通过 |
| F6 | 指标在阈值附近来回震荡 | 滞回和保持时间抑制反复告警 | `test_engine_rules_sm.py::test_hysteresis_hold_and_cooldown`、`::test_configured_hysteresis_is_per_rule` | 通过 |
| F7 | 全部数据源中断 30s | 5s STALE、30s UNKNOWN；该合约禁开；每个合约只告警一次 `MD_DOWN` | `test_mdg_integration.py::test_all_feeds_down_stale_unknown_single_md_down_and_recovery`；`test_mdg_quality.py` | 通过 |
| F8 | 数据源延迟、时间戳到秒（新浪） | 延迟超过 3s 的状态不作为 LIVE；秒级时间戳只有成交量增加时才推进 | `test_mdg_arbiter.py`（LIVE 条件）；`test_mdg_resilience.py::test_sina_coarse_timestamps_only_advance_live_on_a_volume_increase` | 通过 |
| F9 | sim-exchange 重启（epoch 变化） | md-gateway 重拉快照，trade-gateway 重连并补回报 | `test_mdg_resilience.py::test_sim_restart_epoch_change_end_to_end`；`test_tg_integration.py::test_a_cancel_refused_by_a_restarted_broker_is_sent_again` | 通过 |
| F10 | Feed B 返回畸形数据、新浪返回格式不符的文本 | 拒收并计数，不影响其他源 | `unit/sim/test_sim_feed_b.py::test_malformed_record_keeps_valid_json`（`MALFORMED` 故障）；`test_mdg_normalizer.py::test_malformed_values`；`unit/marketdata/test_md_sina.py::test_malformed_payloads`、`::test_layout_mismatch_is_malformed` | 通过 |
| F11 | 熔断锁存后硬杀 risk-engine | 下单超时即拒；supervisor 拉起后 HALTED 保持，告警不重复 | `e2e::test_halt_survives_risk_engine_kill`；`test_re_subprocess.py::test_hard_killed_process_restarts_still_halted_without_duplicate_alerts` | 通过 |
| F12 | risk-engine 在事务各阶段崩溃（撤单待定、补数、预热、EOD 冻结后、临时快照） | 重启后状态一致，不重复路由、不重复告警 | `test_re_review.py` 的 P5 崩溃矩阵、`::test_l3_a_next_day_fill_survives_a_double_crash` | 通过 |
| F13 | 提交失败（写库出错） | 该分片停止处理（poison），同批命令一律不应答成功 | `test_re_review.py::test_p1_a_failed_commit_stops_every_command_of_its_batch`、`::test_c1_an_engine_error_poisons_the_shard_and_its_batch` | 通过 |
| F14 | md-gateway 重启 | 从游标续传；水位和指纹保证重启后不重复发布 | `test_mdg_integration.py::test_restart_resumes_from_cursor_and_watermarks`；`test_mdg_resilience.py::test_restart_inside_the_watermark_flush_interval_republishes_nothing` | 通过 |
| F15 | trade-gateway 重启、柜台断线 | 按序号补回报，成交不重复，`trade_seq` 连续 | `test_tg_integration.py::test_restart_resyncs_without_duplicates_and_keeps_trade_seq_continuous`、`::test_broker_disconnect_is_detected_and_the_session_resynced` | 通过 |
| F16 | clearing-worker 在 EOD 每个阶段崩溃 | 从最近完成的步骤继续，结果不重复 | `test_clearing_review.py::test_crash_at_every_phase_resumes_without_duplicates`；`test_clearing_flows.py::test_restart_mid_saga_resumes_from_last_step` | 通过 |
| F17 | EOD 跑两次；漏跑两天后补结算 | 第二次幂等；漏跑的日子按顺序补结算 | `test_clearing_flows.py::test_settle_run_idempotency_and_preconditions`；`test_clearing_review.py::test_two_missed_eods_settle_in_order_catch_up_then_rebase` | 通过 |
| F18 | EOD 期间 md-gateway 不可用 | 结算失败并给出原因；人工给出结算价后重跑成功 | `test_clearing_flows.py::test_md_down_fails_then_manual_prices_rerun` | 通过 |
| F19 | 硬杀 supervisor | 它管理的服务一起退出；重启后干净地重新拉起 | `test_supervisor_realstack.py::test_hard_killed_supervisor_takes_its_services_along_and_restarts_cleanly` | 通过 |
| F20 | 订阅端丢消息（链路有损） | 事实流按序号补发，恰好一次；超出保留期的缺口按 `NOT_FOUND` 处理 | `test_bus_fact.py::test_lossy_link_repaired_by_replay_and_expired_by_purge`（25 例中的一例） | 通过 |
| F21 | 发布方重启后本机时钟回拨 | 按心跳识别新的化身，不误判序号 | `unit/bus/test_bus_incarnation.py`（14 例） | 通过 |
| F22 | 演示时钟向前跳转 | 不产生行情误报 | `test_mdg_resilience.py::test_demo_clock_jumps_raise_no_false_alarms`；`unit/api_gateway/test_apigw_clock.py` | 通过 |
| F23 | 越权操作（VIEWER 重置熔断等） | 403，写审计 | `test_apigw_rest.py::test_halt_reset_and_role_checks`；`test_apigw_security.py::test_every_route_requires_a_key_and_the_right_role` | 通过 |
| F24 | 周五夜盘、周六凌晨的行情 | 归属下一交易日（周一） | `unit/marketdata/test_md_sessions.py::test_cu_saturday_early_morning_is_live_for_monday`；`unit/test_foundation.py` 的日历用例 | 通过 |
| F25 | api-gateway 读模型漏收（跳号、流变化、分片下线） | 按序号补齐或重新同步；下线分片的账户标记为陈旧 | `test_apigw_readmodel.py`（18 例） | 通过 |
| F26 | kill switch 下发时部分目标不可用 | 持久化期望状态，按当前状态重试；失败告 CRITICAL | `unit/api_gateway/test_apigw_killswitch.py` | 通过 |

## 6. 端到端验收测试

`tests/e2e/test_e2e_acceptance.py` 在一个新建的 home（`var/e2e/run-<时间戳>/home`，全部端口随机）中执行 `futrisk demo --scenario breaker --headless`，启动 8 个真实进程，然后只通过 REST 和 CLI 观察系统：

| 用例 | 过程 | 断言 |
|---|---|---|
| `test_breaker_levels_raise_once_each` | 熔断剧本对 ACC3 施加黑色系冲击 | `daily_loss` 的告警、限制、熔断三个级别依次触发，**每个级别恰好一条**风险事件；账户 HALTED 且锁存 |
| `test_opens_rejected_closes_fill_while_halted` | 熔断后对 ACC3 下开仓单和平仓单 | 开仓被拒（`REJECT_ACCOUNT_HALTED`），平仓成交 |
| `test_halt_survives_risk_engine_kill` | 硬杀 `risk-engine-1`（ACC3 所在分片） | 期间下单被拒或超时；supervisor 拉起后 HALTED 仍然锁存，告警键不重复 |
| `test_feed_a_outage_fails_over_and_backfills_without_duplicates` | 运行 `feed_outage` 剧本：Feed A 断开 20s，并注入跳号、重复、乱序 | 故障切换到 Feed B；恢复后补数条数 > 0，LIVE 重复条数为 0 |
| `test_eod_settles_reconciles_and_reports` | 把市场时钟调到 15:29，EOD 在 15:30 自动开始 | 5 个账户全部结算，对账检查全部 PASS 或 WARN；ACC3 的交易所公式盈亏等于"平仓 + 持仓"；生成的风控日报包含 ACC3 的熔断；各分片换到下一交易日 |

本次执行（2026-09-26，`pytest -m e2e tests/e2e -v`）：5 例全部通过，用时 354s。从这次运行的库中核对到的数字：
- ACC3 的 `daily_loss` 告警、限制、熔断三个级别各恰好 1 条告警（`alert_key` 各不相同）；
- 硬杀 `risk-engine-1` 后，supervisor 1.0s 后开始重启（退避 1s），再过 1.9s 回到 RUNNING，从被杀到恢复共 2.9s（QS-04 的目标 < 3s）；熔断仍然锁存；
- Feed A 中断产生 2 个缺口，全部补齐：补数 438 条，重复 0 条，不可恢复 0 条；
- EOD 在 15:30 自动开始并完成，5 个账户的对账检查全部通过，日报与 5 个账户的结算单（TXT、CSV）都已生成。

这组用例前两次运行时 EOD 用例失败，原因是用例自身的错误（§8 第 2 项），修正后的这次运行全部通过。

## 7. 执行结果

### 7.1 Python 测试

执行日期 2026-09-26，代码冻结之后，在同一份代码上按目录分别运行（`pytest tests\<目录> -q --timeout 300`，含慢速用例），共 1793 例，全部通过，没有跳过；加上 §6 的端到端 5 例，合计 1798 例。精简版删除了其中 `unit/bench` 的 14 例（基准工具已删除），剩余 1779 例，下表已去掉该行。

| 目录 | 内容 | 用例数 | 结果 | 用时 |
|---|---|---:|---|---:|
| `unit/core` | 清算与风控内核的纯函数：lot、手续费、保证金、盈亏、结算价、结算、日内账本 | 145 | 通过 | 2s |
| `unit/engine` | `EngineCore`：事前检查、状态机、验收场景、委托、EOD、公司层、性质测试 | 194 | 通过 | 7s |
| `golden` | 黄金用例 | 42 | 通过 | 0.3s |
| `property` | 清算性质测试 | 9 | 通过 | 11s |
| `unit/bus` | 总线：PUB/SUB、请求应答、事实流补齐、化身、LV 与快照 | 87 | 通过 | 5s |
| `unit/db` | 数据库层：写库线程、组提交、outbox、迁移 | 71 | 通过 | 3s |
| `unit/runtime` | 服务运行时：两阶段提交参与方、GC 策略、存活检测 | 43 | 通过 | 22s |
| `unit/marketdata` | 新浪解析、录制、交易时段、指纹 | 154 | 通过 | 10s |
| `unit/md_gateway` | 行情网关：归一化、校验、仲裁、可信状态、补数、K 线、结算价输入、故障场景 | 159 | 通过 | 86s |
| `unit/risk_engine` | 风控服务外壳：真实套接字、恢复、崩溃矩阵、硬杀子进程 | 55 | 通过 | 93s |
| `unit/trade_gateway` | 交易网关：路由、墓碑、补回报、kill switch、与模拟柜台集成 | 115 | 通过 | 19s |
| `unit/clearing` | 清算服务：EOD saga、对账、补结算、崩溃恢复、报表 | 74 | 通过 | 50s |
| `unit/api_gateway` | REST、RBAC、WS、读模型、通知、两阶段提交协调、kill switch、页面托管 | 192 | 通过 | 49s |
| `unit/supervisor` | 进程管理、重启策略、市场时钟、只管时钟模式、真实整套进程 | 79 | 通过 | 150s |
| `unit/sim` | 虚拟交易所：价格引擎、Feed A/B、模拟柜台、控制接口 | 83 | 通过 | 47s |
| `unit/bots` | 策略机器人与剧本校准 | 56 | 通过 | 14s |
| `unit/client` | 策略 SDK | 37 | 通过 | 4s |
| `unit/cli` | 命令行工具 | 68 | 通过 | 15s |
| `unit/backtest` | 回测（含 4 个真实数据的慢速用例） | 91 | 通过 | 101s |
| `unit/test_foundation.py` | 日历、十进制、时钟、消息、配置 | 25 | 通过 | 0.4s |
| 合计 | | 1779 | 全部通过 | 约 14 分钟 |

### 7.2 前端测试
`web/`：vitest 12 个测试文件、173 例全部通过（6.4s）；`vue-tsc --noEmit` 类型检查通过；`npm run build` 成功。

### 7.3 静态检查与文档一致性

| 检查 | 命令 | 结果 |
|---|---|---|
| ruff | `.venv\Scripts\ruff check src tests scripts` | 通过 |
| mypy | `.venv\Scripts\mypy src\futrisk` | 通过 |
| 文档一致性 | `.venv\Scripts\python scripts\check_docs.py` | 0 个问题（与代码核对 56 张表 672 列、157 个消息结构 946 个字段、53 个枚举、全部错误码） |
| 配置参考 | `.venv\Scripts\python scripts\gen_config_reference.py --check` | 11 与配置模型一致 |

### 7.4 覆盖率
方案（01 §8.11）要求风控内核、各服务和总线的行覆盖率不低于 85%。用 pytest-cov 测量（`tests/unit/core`、`engine`、`golden`、`property`、`bus`、`db`、`md_gateway`、`risk_engine`、`clearing`、`trade_gateway`，共 951 例，全部通过，带覆盖率用时 948s）：

| 包 | 语句数 | 未覆盖 | 行覆盖率 |
|---|---:|---:|---:|
| `futrisk.core`（风控与清算内核） | 7193 | 384 | 94.7% |
| `futrisk.db` | 1180 | 76 | 93.6% |
| `futrisk.services.clearing` | 4190 | 289 | 93.1% |
| `futrisk.bus` | 2626 | 219 | 91.7% |
| `futrisk.services.trade_gateway` | 2392 | 208 | 91.3% |
| `futrisk.services.md_gateway` | 4925 | 432 | 91.2% |
| `futrisk.services.risk_engine`（服务外壳） | 1678 | 155 | 90.8% |
| 合计 | 24184 | 1763 | 92.7% |

只统计测试进程内执行的代码：由测试启动的子进程（例如硬杀测试中的 risk-engine 进程、真实的 sim-exchange）不计入，所以服务外壳的实际覆盖略高于表中数字。

### 7.5 可执行文件
`scripts\build_exe.ps1` 在代码冻结后重新构建（2026-09-26，用时 406s）：仪表盘构建 → PyInstaller one-folder（`dist\futrisk\` 约 196 MB）→ 17 项冒烟测试全部通过 → `dist\futrisk-win64-0.1.0.zip`（91 MB）及 SHA-256 文件。冒烟测试覆盖：`version`、`config validate`、`doctor`（冻结环境、时区、仪表盘文件、pyzmq、SQLite、非 ASCII 路径）、api-gateway 健康检查与页面、父进程退出后服务自行停止、回测 `breaker_synthetic`；在临时 home 中 `futrisk demo --headless` 拉起全部 8 个进程，65s 内保持 RUNNING、重启 0 次，`futrisk down` 后没有残留进程；以及在中文路径下重复前六项。

### 7.6 性能
性能基准的方法、结果和未达标项见 13。

## 8. 已知问题与未覆盖项

| # | 问题或缺口 | 影响 | 处理 |
|---|---|---|---|
| 1 | 在**一个** pytest 进程中跑完整个 `tests/` 时，曾有一次停在一个启动 `spawn` 子进程的用例上超过 20 分钟；按目录分别运行（每个用例限时 300s）全部通过，原因尚未查明 | 一次性全量运行可能挂住，但不是某个用例失败 | 按目录运行（§9），并始终带 `--timeout` |
| 2 | 端到端的 EOD 用例在本次验证之前没有完整跑通过：本次发现并修正了用例自身的两处错误（`POST /demo/clock` 的请求体缺少必填的 `target`；报表列表的类型字段名是 `type`），产品代码未改 | 此前的端到端证据不含 EOD | 已修正，§6 的结果为修正后的运行 |
| 3 | docker compose 只做了配置语法检查（`docker compose config`），没有实际构建镜像并启动 | 容器部署的端到端行为没有验证 | 08 §3 已注明；上线前需要跑一次完整部署 |
| 4 | 只测了模拟柜台（按 CTP 语义实现），没有连接真实的期货公司柜台 | CTP 适配器只有接口 | 生产差距见 08 |
| 5 | 新浪真实 tick 从 2026-09-28 开始录制，回测 C 层目前只用合成的录制文件测试（`unit/backtest/test_bt_ticks.py`） | 真实 tick 回放的结果尚无 | 录制完成后运行 `futrisk backtest run ticks` |
| 6 | 01 §10.2 的 QS-10（服务状态变化 3s 内反映到仪表盘）没有自动化的端到端断言，只有读模型与推送的单元测试，以及在真实系统上的人工检查 | 3s 指标没有自动回归 | — |
| 7 | 全部测试只在 Windows 11 上运行过 | Linux（docker 镜像）上的行为未经测试验证 | 同第 3 项 |
| 8 | 性能目标有未达标项 | 见 13 | 见 13 |
| 9 | 仪表盘每次加载时浏览器控制台有一条 CSP 报告：vue-echarts 初始化时用 `new Function` 探测能否注册自定义元素，被页面的 CSP（不允许 `eval`）拦下后自动改用普通容器 | 只是控制台多一条报告，图表功能不受影响（真实系统上各页检查过） | 保持 CSP 不放宽 |
| 10 | 账户页的日内权益曲线在权益变化很小时（例如 999.9 万 ~ 1000.4 万），纵轴刻度都显示为"1000万" | 纯显示问题，曲线本身正确 | 刻度格式按范围增加小数位 |

## 9. 如何运行

```powershell
# 全部单元、黄金、性质测试（按目录分别运行更稳妥，见 §8）
.venv\Scripts\python -m pytest tests\unit\core tests\unit\engine tests\golden tests\property -q
.venv\Scripts\python -m pytest tests\unit\md_gateway -q --timeout 300
# 只跑快速用例
.venv\Scripts\python -m pytest tests -m "not slow" -q
# 端到端验收（约 6 分钟，8 个真实进程）
.venv\Scripts\python -m pytest -m e2e tests\e2e -v
# 前端
cd web; npm test; npx vue-tsc --noEmit
# 静态检查与文档
.venv\Scripts\ruff check src tests scripts; .venv\Scripts\mypy src\futrisk
.venv\Scripts\python scripts\check_docs.py
```

---

## 附录 A 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-26 | 初版：测试分层与原则、需求追踪、26 项故障注入矩阵、端到端验收，以及代码冻结后的一次完整执行报告 |
