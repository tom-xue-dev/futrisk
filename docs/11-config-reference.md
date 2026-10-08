# futrisk 配置参考

| 项 | 内容 |
|---|---|
| 文档编号 | FR-DOC-11 |
| 版本 | v1.0 |
| 状态 | 实现对齐 |
| 日期 | 2026-10-08 |
| 适用范围 | `config/` 下的全部配置文件（运行时位于 `<home>/config/`），以及 `bots.yaml` 和相关环境变量 |
| 对应标准 | —（参考手册）；字段表由 pydantic 模型生成 |
| 关联文档 | [README](README.md)（规范）· [01 架构](01-architecture.md) · [02 通信协议](02-communication-protocol.md) · [03 数据库](03-database-schema.md) · [04 风控规则](04-risk-rules.md) · [07 使用说明](07-user-guide.md) · [08 部署运维](08-deployment.md) |

> 本文逐个配置文件列出每个字段的类型、是否必填、含义、默认值和热加载效果，并给出示例。字段表（`<!-- gen:... -->` 标记之间的内容）由 `scripts/gen_config_reference.py` 从 pydantic 模型生成，**不要手工修改**；改了配置模型之后重新生成（§1.6）。配置管理的设计见 01 §8.9，热加载协议见 02 §8.5。

## 目录
1. [配置的组织方式](#1-配置的组织方式)
2. [app.yaml](#2-appyaml)
3. [services.yaml](#3-servicesyaml)
4. [products.yaml](#4-productsyaml)
5. [accounts.yaml](#5-accountsyaml)
6. [risk_rules.yaml](#6-risk_rulesyaml)
7. [feeds.yaml](#7-feedsyaml)
8. [sim.yaml](#8-simyaml)
9. [scenarios/*.yaml](#9-scenariosyaml)
10. [backtest/*.yaml](#10-backtestyaml)
11. [calendar/trading_days.csv](#11-calendartrading_dayscsv)
12. [bots.yaml（可选）](#12-botsyaml可选)
13. [环境变量](#13-环境变量)
- [附录 A 修订记录](#附录-a-修订记录)

---

## 1. 配置的组织方式

### 1.1 文件一览

<!-- gen:files begin -->
| 文件 | pydantic 模型 | 读取方 | 热加载（概要） |
|---|---|---|---|
| `app.yaml` | `futrisk.common.config.app.AppConfig` | 全部服务、CLI | 大部分重启生效；`eod_start_at`、`api_keys`、`notify` 即时 |
| `services.yaml` | `futrisk.common.config.services.ServicesConfig` | 全部内部服务、CLI | 端点、总线参数拒绝热改；`supervisor` 段重启 supervisor 生效 |
| `products.yaml` | `futrisk.common.config.products.ProductsConfig` | 全部服务、sim-exchange、strategy-sim、回测 | 拒绝热改（md-gateway 否决）：停机后离线提交，重启后生效（§1.3） |
| `accounts.yaml` | `futrisk.common.config.accounts.AccountsConfig` | risk-engine、api-gateway、clearing-worker、strategy-sim、首日建账（`futrisk demo`、docker compose 的 `seed`）、回测 | 分片归属拒绝热改；其余见各字段 |
| `risk_rules.yaml` | `futrisk.common.config.risk_rules.RiskRulesConfig` | risk-engine（各分片）、api-gateway、仪表盘、回测 | 即时（删除级别、只改阈值或 `latch` 时，已锁存的熔断保持到重置或换日，见 §6.4） |
| `feeds.yaml` | `futrisk.common.config.feeds.FeedsConfig` | md-gateway；`futrisk data`（读包内默认配置） | `arbitration` 即时；数据源段拒绝热改 |
| `sim.yaml` | `futrisk.common.config.sim.SimConfig` | sim-exchange；trade-gateway（柜台端点）、api-gateway（控制接口地址）、supervisor（健康检查地址）、首日建账（昨结算） | 重启 sim-exchange（外部组件，不参与两阶段提交）；端点改动还要重启连接它的服务 |
| `scenarios/*.yaml` | `futrisk.sim.exchange.scenario.ScenarioFile` | sim-exchange（每次 `scenario run`） | 每次运行时从 sim-exchange 启动时的配置版本目录读取：改动要提交新版本并重启 sim-exchange |
| `backtest/*.yaml` | `futrisk.backtest.config.BacktestConfig` | `futrisk backtest`、clearing-worker 的回测作业 | 每次运行时读取（包内 `config/backtest/` 或给定的 YAML 路径） |
| `bots.yaml` | `futrisk.sim.bots.config.BotsConfig` | strategy-sim（可选文件） | 重启 strategy-sim |
| `calendar/trading_days.csv` | —（CSV，`futrisk.common.calendar.TradingCalendar`） | 全部服务、sim-exchange、回测 | 拒绝热改（md-gateway 否决）：停机后离线提交，重启后生效（§1.3） |
<!-- gen:files end -->

### 1.2 工作目录、版本快照与生效版本

| 位置 | 作用 | 谁来写 |
|---|---|---|
| 仓库的 `config/`（exe 为 `futrisk\_internal\config\`） | 包内默认配置 | 开发者 |
| `<home>/config/` | **工作目录**：管理员编辑的地方。首次启动时从包内默认配置复制过来 | 管理员 |
| `<home>/config/versions/<N>/` | 不可变的版本快照：热加载或离线提交时把工作目录复制成一个新版本 | api-gateway（热加载）；`futrisk config commit`（离线提交）；首次启动时创建版本 1 的进程（docker compose 中是一次性容器 `config-init`） |
| `<home>/config/active.json` | 当前生效的版本：`{config_version, path, effective}` | api-gateway（提交时原子替换）；`futrisk config commit`；首次启动时创建版本 1 的进程 |

- 各服务和运维命令（`futrisk demo`、`futrisk up` 等）启动时**只加载 `active.json` 指向的版本**，不读工作目录；读取工作目录本身的只有 `futrisk config validate|show|commit --home`、`futrisk doctor` 和热加载（api-gateway 把它复制成新版本）。所以只改工作目录、不提交，重启后也不会生效；被拒绝的改动也不会在重启时被误加载（01 §6.7）。
- 首次启动（`<home>/config/app.yaml` 不存在）时，把包内默认配置复制到工作目录，并提交为版本 1。
- `effective` 按配置段记录生效方式：`IMMEDIATE`，或 `NEXT_TRADING_DAY`（`products`、`calendar`）加上从哪个交易日起生效（02 §7.10）；clearing-worker 据此按交易日选用配置版本结算（03 §4.4 `config_history`）。离线提交写入的 `effective` 为空。

### 1.3 热加载与生效时机

改配置的标准流程：编辑 `<home>/config/` 下的文件 → `futrisk config validate --home <home>` → 调用 `POST /api/v1/config/reload`（ADMIN，操作步骤见 07 §5.4）。api-gateway 把工作目录复制成新的版本快照，用两阶段提交让 `services.yaml` 中的全部实例（包括 api-gateway 和 supervisor 自己）一起切换（02 §8.5）：每个实例在 prepare 阶段加载新版本、核对配置哈希、检查自己能否接受这些改动；任一实例否决或 5 s 内没有回复，这一轮整体放弃（`ABORTED`），生效版本不变。全部同意后 api-gateway 先改写 `active.json`，再通知各实例提交，各实例在内存中换上新配置。服务是否马上使用新值，取决于它什么时候读取该字段，这就是字段表中"热加载"一列：

| 取值 | 含义 |
|---|---|
| 即时 | 提交后立即生效（风控阈值在下一次评估时就用新值） |
| 即时（需环境变量） | 同上；字段引用的环境变量必须已经在 api-gateway 进程的环境中（§2） |
| 重启 X | 可以提交，但 X 只在启动时读取该字段，要重启 X 才生效：仪表盘"行情与系统"页的"重启"，或 `POST /api/v1/services/{instance}/restart`；supervisor 本身要 `futrisk down` 再 `futrisk up` |
| 拒绝热改 | 有实例在 prepare 阶段否决（见下表），这一轮热加载整体放弃。只能停机后离线提交（见下文），或换一个新的 home |
| 仅首次启动 | 只在 supervisor 第一次建立演示时钟时读取 |
| 首日建账时读取 | 只在首日建账（`futrisk demo`、docker compose 的 `seed`）时读取，之后的改动不影响已有账本 |
| 下次 `futrisk demo` | `futrisk demo` 每次运行时读取已提交的版本 |
| 每次运行时读取 | 不参与热加载：每次执行命令或作业时现读文件 |
| 见子字段 | 对象的各个子字段生效方式不同 |

prepare 阶段的否决规则：

| 否决方 | 被否决的改动 |
|---|---|
| 每个实例（运行时的公共检查） | 新版本早于当前版本，或文件哈希与请求不符；`services.yaml` 的 `instances`、`bus`、`profile` 有变化（`supervisor` 段除外）；`feeds.yaml` 中任一数据源的 `url` 有变化 |
| md-gateway | `feeds.yaml` 的 `feed_a`、`feed_b`、`sina` 段有任何变化；`products.yaml` 的内容有变化（只改注释不算）；`calendar/` 下的文件有任何变化。只有 `arbitration` 段能在运行时生效 |
| risk-engine（每个分片） | 本分片的账户集合有变化（增删账户、改 `shard`）；新的规则编译失败（未知的 `scope`、`ALERT` 或 `RESTRICT` 级别设了 `latch`、`confirm` 为 0 个状态且 0 ms、`overrides` 引用不存在的级别）；启用的规则引用了未注册的指标 |

被否决的改动仍然留在工作目录里，下一次热加载还会带上它们，所以要先改回，或者走离线提交。

**离线提交**（`futrisk config commit`）：`futrisk down` 停机 → 编辑 `<home>/config/` 下的文件 → `futrisk config validate --home <home>` → `futrisk config commit --home <home>`（把工作目录提交为新版本并改写 `active.json`；工作目录的 `services.yaml` 中 supervisor 的端点上有 supervisor 应答（系统还在运行）时拒绝执行，`--force` 可以强制）→ `futrisk up`，各服务从新版本启动。`futrisk config validate` 和离线提交只做配置模型（pydantic）校验并加载交易日历，不编译风控规则，所以上表中由 risk-engine 否决的规则错误要到服务启动或运行时才暴露（§6.1、08 §5.2）。

**产品参数和交易日历**：协议规定 `products.yaml` 和交易日历按 `NEXT_TRADING_DAY` 生效（02 §7.10）：热加载提交时 api-gateway 在 `effective` 中记下生效的交易日，risk-engine 把它们暂存到进程下一次启动（01 TD-09），clearing-worker 按交易日选用版本结算。但 md-gateway 在 prepare 阶段否决这两类改动，系统运行时它们无法热加载，只能离线提交。离线提交之后，md-gateway、risk-engine、supervisor 等在启动时直接使用新值；clearing-worker 把结算相关文件有变化的版本记为从启动时的当前交易日起适用（03 §4.4）。所以应当在 EOD 完成（`sys.eod` 到达 `DONE`）之后、下一交易日的第一个交易时段（有夜盘的品种为 20:55 的集合竞价）开始之前停机提交，这样新参数从下一交易日起同时用于风控和结算，与交易所公告的生效方式一致。

sim-exchange 和 strategy-sim 是外部组件，不参与两阶段提交，只在启动时读取已提交的版本。

### 1.4 书写约定
- 金额、价格、费率、保证金率写成**带引号的十进制字符串**（如 `"0.0001"`、`"10000000"`），按 `Decimal` 精确解析，避免二进制浮点误差；比率一律为小数，−0.03 表示 −3%（README §3.4）。
- 时刻写成北京时间 `HH:MM`；交易日写成加引号的 `YYYYMMDD` 字符串（不加引号时 YAML 解析成整数，字符串字段的校验会失败）。
- 所有配置模型都不接受多余的键（pydantic `extra="forbid"`）：拼错的键会让校验失败，而不是被悄悄忽略。例外是类型为 `dict[str, any]` 的字段：`risk_rules.yaml` 的 `levels[].params` 完全不校验（§6.3）；剧本的 `vars`、`steps[].params` 和回测、`bots.yaml` 的策略参数在运行时由各自的代码或模型校验，`futrisk config validate` 不检查它们。
- YAML 用 `safe_load` 读取，文件编码为 UTF-8。

### 1.5 校验、查看与导出

| 目的 | 命令 |
|---|---|
| 校验包内默认配置 | `futrisk config validate` |
| 校验某个 home 的工作目录 | `futrisk config validate --home <home>`（或 `--dir <目录>`） |
| 打印工作目录中各文件校验后的值（含默认值） | `futrisk config show risk_rules --home <home>`；不带参数打印全部 |
| 导出 JSON Schema | `futrisk config schema --out <目录>`，每个文件一个 `<section>.schema.json` |
| 停机后提交工作目录 | `futrisk config commit --home <home>`（§1.3 离线提交） |
| 运行中系统实际使用的配置 | `GET /api/v1/config`（版本号、哈希、各文件哈希和全部配置）；`GET /api/v1/config/versions`（热加载历史） |

`config validate` 和 `config show` 只覆盖 7 个 YAML 文件和交易日历，做的是配置模型校验：字段类型、必填项、枚举取值，以及 `products.yaml` 内部的引用（交易时段模板、交易所规则、合约所属品种）。它们不编译风控规则：未注册的 `metric`、未知的 `scope`、规则编译错误和 `levels[].params` 都不检查（§6.1）。场景剧本在 `scenario run` 时校验，回测配置在 `backtest run` 时校验，`bots.yaml` 在 strategy-sim 启动时校验。

### 1.6 本文的生成方式

```powershell
.venv\Scripts\python scripts\gen_config_reference.py            # 重新生成本文中的字段表
.venv\Scripts\python scripts\gen_config_reference.py --check    # 本文过期时退出码为 1（可放进 CI）
.venv\Scripts\python scripts\gen_config_reference.py --print app  # 只打印一张表
```

- 类型、是否必填、默认值直接取自 pydantic 模型；"说明"和"热加载"两列保存在脚本里。模型新增了字段而脚本里没有说明时，脚本报错并以退出码 1 结束，所以新配置键不会漏写文档。
- 只替换 `<!-- gen:名称 begin -->` 与 `<!-- gen:名称 end -->` 之间的内容，其余文字手工维护。
- 字段路径的写法：`a.b` 表示嵌套字段；`{name}` 表示映射的键（如 `products.{product}.tick_size`）；`[]` 表示列表元素（如 `rules[].levels[].threshold`）。

---

## 2. app.yaml

运行模式、演示时钟、EOD 时刻、日志、API key 和告警通知。

<!-- gen:app begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `mode` | `demo` \| `prod` | 否 | 运行模式。`demo`：市场时间 = 本机时间 + 全系统共享的偏移（可跳转），开放 `/demo/*` 演示接口和虚拟交易所控制接口，`futrisk demo` 自动建账；`prod`：市场时间就是本机时间，演示接口返回 403 | `demo` | 重启全部服务（api-gateway 的 `/demo/*` 开关在提交后立即变化） |
| `demo` | 对象 | 否 | 演示模式参数（`mode=demo` 时使用） | 见子字段 | 见子字段 |
| `demo.start_at` | `str` \| `null` | 否 | 首次启动时的市场时间，ISO 格式，不带时区即北京时间；`null` 表示此刻之后最近一个交易日的 08:58（09:00 开盘前两分钟）。只在 supervisor.db 里还没有 `DEMO` 时钟状态时使用（新的 home，或从 `prod` 改回 `demo`）；`futrisk demo --start-at` 可以把已有的时钟往后拨，不能往前 | `null` | 仅首次启动 |
| `demo.pre_close_at` | `str` | 否 | `futrisk clock jump --to pre-close`（`PRE_CLOSE`）跳到的时刻，北京时间 HH:MM，落在当前市场时间所属的交易日（夜盘期间即下一交易日） | `14:58` | 重启 supervisor |
| `demo.lead_s` | `int` | 否 | `futrisk clock jump --to next-open`（`NEXT_OPEN`）跳到下一个交易时段开始前多少秒 | `120` | 重启 supervisor |
| `demo.auto_seed` | `bool` | 否 | `futrisk demo` 发现本 home 还没有账本时，按 `accounts.yaml` 调用 `POST /settlement/seed-sod` 首日建账 | `true` | 下次 `futrisk demo` |
| `eod_start_at` | `str` | 否 | EOD 自动开始的市场时间（北京时间 HH:MM）：clearing-worker 的调度器在当日该时刻之后启动结算流程。生产环境应当晚于经纪商的出入金截止时间（一般 17:00） | `15:30` | 即时 |
| `logging` | 对象 | 否 | 日志参数（01 §8.7） | 见子字段 | 重启各进程 |
| `logging.level` | `str` | 否 | 日志级别：`TRACE`、`DEBUG`、`INFO`、`WARNING`、`ERROR` | `INFO` | 重启各进程 |
| `logging.retention_days` | `int` | 否 | 日志文件保留天数，超期的压缩文件自动删除 | `30` | 重启各进程 |
| `logging.rotation_mb` | `int` | 否 | 单个日志文件超过该大小（MB）时切分；此外每天切分一次 | `100` | 重启各进程 |
| `api_keys` | 对象列表 | 否 | 具名 API key。存在可用的具名 ADMIN key 时，bootstrap key 自动停用并删除文件 | `[]` | 即时（需环境变量） |
| `api_keys[].name` | `str` | 是 | key 的名字，即 `key_id`，作为操作者写入审计。重名的后一项和名为 `bootstrap` 的项被忽略（记 ERROR 日志）；从列表中删除的 key 在下次同步时停用 | — | 即时（需环境变量） |
| `api_keys[].role` | `VIEWER` \| `TRADER` \| `RISK_MANAGER` \| `ADMIN` | 是 | 角色，权限矩阵见 02 §9.6 | — | 即时（需环境变量） |
| `api_keys[].key_env` | `str` | 是 | 保存明文 key 的环境变量名（api-gateway 进程的环境）。明文至少 16 个字符，api.db 只存 SHA-256；变量缺失、过短，或与另一个具名 key 的明文相同时，该 key 停用 | — | 即时（需环境变量） |
| `notify` | 对象 | 否 | 告警通知（webhook，02 §9.8） | 见子字段 | 见子字段 |
| `notify.webhook_url_env` | `str` \| `null` | 否 | 保存 webhook URL 的环境变量名；`null` 或变量为空时只写日志 | `null` | 即时（需环境变量） |
| `notify.channel` | `wecom` \| `dingtalk` \| `feishu` \| `log` | 否 | 消息格式：企业微信 `wecom`、钉钉 `dingtalk`、飞书 `feishu`，或 `log`（只写日志）；webhook 渠道取不到 URL 时退回 `log` | `log` | 即时（需环境变量） |
| `notify.min_severity` | `INFO` \| `WARNING` \| `HIGH` \| `CRITICAL` | 否 | `risk.event` 低于该级别不单独通知（已经通知过触发的告警，其解除、确认、重置照常通知）；`md.event`、`sys.eod`、`sys.service` 的通知级别固定，不受它影响。CRITICAL 通知从不因积压或限流被抑制（02 §9.8） | `HIGH` | 即时 |
<!-- gen:app end -->

示例（仓库自带的 `config/app.yaml`，另加两个具名 key（ADMIN 和 RISK_MANAGER）和企业微信通知）：

```yaml
mode: demo
demo:
  start_at: null            # 首次启动时放在下一个交易日 08:58
  pre_close_at: "14:58"
  lead_s: 120
  auto_seed: true
eod_start_at: "15:30"
logging: {level: INFO, retention_days: 30, rotation_mb: 100}
api_keys:
  - {name: ops, role: ADMIN, key_env: FUTRISK_OPS_KEY}      # 明文放在环境变量 FUTRISK_OPS_KEY 中
  - {name: rm1, role: RISK_MANAGER, key_env: FUTRISK_RM1_KEY}
notify:
  webhook_url_env: FUTRISK_WECOM_URL
  channel: wecom
  min_severity: HIGH
```

- 具名 key 的明文从 api-gateway **进程的环境变量**读取。进程由 supervisor 拉起、继承它的环境，所以变量必须在执行 `futrisk up` / `futrisk demo` 之前就设置好；已经运行的系统里新增变量，要重启整个系统才能读到。
- 每次启动和每次提交新配置时，api-gateway 按 `api_keys` 重新同步 `api.db` 的 `api_keys` 表（03 §4.5）：启用可用的具名 key，停用列表中已删除或不可用的 key。
- 配置了可用的具名 ADMIN key 之后，bootstrap key（`<home>/secrets/bootstrap_admin.key`）停用、文件被删除，CLI 需要用 `FUTRISK_API_KEY` 提供 key（§13）；具名 ADMIN key 都不可用时，bootstrap key 重新生成，作为恢复手段。

## 3. services.yaml

内部服务的端点、总线参数和进程守护策略（02 §3）。

<!-- gen:services begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `profile` | `str` | 否 | 档位名称（仓库自带 `demo`，docker compose 用 `docker`），只作标识 | `demo` | 拒绝热改 |
| `bus` | 对象 | 否 | 内部总线参数（02 §4） | 见子字段 | 拒绝热改 |
| `bus.heartbeat_ms` | `int` | 否 | 每个服务发送心跳 `sys.heartbeat.{instance}` 的周期（毫秒） | `1000` | 拒绝热改 |
| `bus.peer_timeout_missed` | `int` | 否 | 连续丢失多少个心跳周期判定对端 DOWN | `3` | 拒绝热改 |
| `bus.hang_threshold_ms` | `int` | 否 | 心跳里的 `loop_lag_ms` 连续两次超过该值，supervisor 判定主循环卡死并重启该进程 | `30000` | 拒绝热改 |
| `bus.pub_hwm` | `int` | 否 | PUB 套接字的发送高水位（条），超出时按 02 §4.9 丢弃最新值类消息 | `10000` | 拒绝热改 |
| `bus.md_pub_hwm` | `int` | 否 | md-gateway 的 PUB 发送高水位（条） | `100000` | 拒绝热改 |
| `bus.router_hwm` | `int` | 否 | ROUTER 套接字的高水位（条） | `1000` | 拒绝热改 |
| `bus.replay_window_days` | `int` | 否 | FACT 主题的重放窗口：outbox 中已发布消息的保留天数（按墙钟）。目前只有 trade-gateway 和 supervisor 的日常维护读取它，md-gateway、risk-engine、clearing-worker 固定保留 7 天（03 §8、08 §5.2 KD-17） | `7` | 拒绝热改 |
| `bus.gc` | 对象 | 否 | 服务进程的垃圾回收策略（01 §11 RK-01）；环境变量 `FUTRISK_GC` 可以临时覆盖（§13） | 见子字段 | 拒绝热改 |
| `bus.gc.enabled` | `bool` | 否 | 是否启用自动垃圾回收 | `true` | 拒绝热改 |
| `bus.gc.threshold` | `tuple[int, int, int]` | 否 | 启动时设置的 `gc.set_threshold` 三代阈值；调大第 0 代阈值以减少回收停顿 | `[10000, 20, 100]` | 拒绝热改 |
| `bus.gc.freeze_on_running` | `bool` | 否 | 服务报告 RUNNING 时执行一次 `gc.collect()` 和 `gc.freeze()`，启动期创建的对象此后不再被扫描 | `true` | 拒绝热改 |
| `instances` | {instance: 对象} | 是 | 每个内部服务实例的端点；实例名见 README §3.3（分片实例为 `risk-engine-{shard}`） | — | 拒绝热改 |
| `instances.{instance}.pub` | `str` \| `null` | 否 | PUB 端点（`tcp://host:port`），发布事件和心跳 | `null` | 拒绝热改 |
| `instances.{instance}.router` | `str` \| `null` | 否 | ROUTER 端点，接收命令 | `null` | 拒绝热改 |
| `instances.{instance}.http` | `str` \| `null` | 否 | HTTP 监听地址，只有 api-gateway 使用（REST、WebSocket、仪表盘） | `null` | 拒绝热改 |
| `instances.{instance}.shard` | `int` \| `null` | 否 | risk-engine 的分片号，与 `accounts.yaml` 的 `shard` 对应 | `null` | 拒绝热改 |
| `instances.{instance}.coordinator` | `bool` | 否 | 是否兼任 coordinator（汇总公司层指标）。应当只有一个分片为 `true`；多个时取第一个，都不是时由 `risk-engine-0` 兼任 | `false` | 拒绝热改 |
| `supervisor` | 对象 | 否 | 进程守护参数（01 §5.3.6） | 见子字段 | 重启 supervisor |
| `supervisor.startup_order` | `list[str]` | 否 | 启动顺序；停机按相反顺序。每一步只等上一个进程存活，不等它进入 RUNNING | `[sim-exchange, md-gateway, trade-gateway, clearing-worker, risk-engine-0, risk-engine-1, api-gateway, strategy-sim]` | 重启 supervisor |
| `supervisor.external` | `list[str]` | 否 | 外部组件：不在总线上、没有总线心跳，按进程存活和健康检查判断 | `[sim-exchange, strategy-sim]` | 重启 supervisor |
| `supervisor.restart` | 对象 | 否 | 崩溃后的重启策略：指数退避 | 见子字段 | 重启 supervisor |
| `supervisor.restart.backoff_initial_ms` | `int` | 否 | 第一次重启前的等待（毫秒），之后每次翻倍 | `1000` | 重启 supervisor |
| `supervisor.restart.backoff_max_ms` | `int` | 否 | 重启等待的上限（毫秒） | `30000` | 重启 supervisor |
| `supervisor.restart.stable_reset_s` | `int` | 否 | 连续运行满该秒数后，退避计数清零 | `60` | 重启 supervisor |
| `supervisor.liveness_timeout_s` | `float` | 否 | 进程启动后在该秒数内没有存活信号即判定启动失败；5 分钟内失败 5 次后停止重试（`GAVE_UP`） | `20` | 重启 supervisor |
| `supervisor.shutdown_grace_s` | `float` | 否 | 停机时发出 `ctl.shutdown` 后等待的秒数，超时再发 CTRL_BREAK，最后强制结束 | `10` | 重启 supervisor |
<!-- gen:services end -->

- 默认档位把所有端点绑定在 `127.0.0.1`，端口见 07 §2.4；docker 部署使用 `deploy/config/services.yaml`（`docker` 档位，主机名换成 compose 服务名，08 §3）。
- 在同一台机器上运行第二套系统时，要给它的 home 同时改本文件、`sim.yaml` 和 `feeds.yaml` 中的全部端口，并在启动前离线提交（§1.3，操作步骤见 07 §7.2）；端点改动拒绝热改。

## 4. products.yaml

交易时段模板、交易所规则、品种和合约。这些参数决定清算的正确性（01 §8.15、05 文档）；数值是示例值，以交易所最新公告为准（01 TD-04）。

<!-- gen:products begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `session_templates` | `dict[str, list[list[str]]]` | 是 | 交易时段模板：模板名 → 时段列表，每个时段为 `[开始, 结束, 类型]`（北京时间 HH:MM；类型为 `auction`、`auction_no_night`、`continuous`）。结束早于开始表示跨午夜（夜盘） | — | 拒绝热改 |
| `exchanges` | {exchange: 对象} | 是 | 交易所级规则（01 §8.15），键为交易所代码 | — | 拒绝热改 |
| `exchanges.{exchange}.explicit_close_today` | `bool` | 否 | 平仓委托必须显式区分平今和平昨（SHFE/INE 为 `true`） | `false` | 拒绝热改 |
| `exchanges.{exchange}.close_style` | `FIFO` \| `TODAY_FIRST` | 否 | 平仓时消耗 lot 的顺序：`FIFO` 先开先平；`TODAY_FIRST` 先平今仓 | `FIFO` | 拒绝热改 |
| `exchanges.{exchange}.close_today_fee_rule` | `LOT_MATCH` \| `SAME_DAY_OPENS_FIRST` \| `INTRADAY_BOTH_LEGS` | 否 | 平今手续费的判定：`LOT_MATCH` 按实际消耗的今仓 lot；`SAME_DAY_OPENS_FIRST` 先平当日新开仓（CFFEX）；`INTRADAY_BOTH_LEGS` 当日开当日平时两腿都按日内费率（DCE） | `LOT_MATCH` | 拒绝热改 |
| `exchanges.{exchange}.settle_rounding` | `FLOOR_TICK` \| `ROUND_TICK` \| `NONE` | 否 | 结算价取整：`FLOOR_TICK` 向下取整到 tick（SHFE）、`ROUND_TICK` 四舍五入到 tick、`NONE` 不取整 | `ROUND_TICK` | 拒绝热改 |
| `exchanges.{exchange}.settle_fallback` | `list[str]` | 否 | 当日无成交时结算价的回退链，依次尝试：`MEDIAN`（买一、卖一、昨结三者中位数）、`LIMIT`（封板取涨跌停价）、`HOURLY`（逐小时回溯）、`FULLDAY`（全日 VWAP）、`BENCHMARK`（昨结算加上基准合约的涨跌点数）、`PRE_SETTLE`（昨结算）；链中没有 `PRE_SETTLE` 时自动补在末尾。名字不做配置校验：结算依次尝试到未知的名字时报错，当日结算失败 | `[PRE_SETTLE]` | 拒绝热改 |
| `exchanges.{exchange}.big_side` | `NONE` \| `SAME_PRODUCT` \| `GROUPS` | 否 | 单向大边：`NONE` 不实行；`SAME_PRODUCT` 同品种按保证金较大一边（SHFE）；`GROUPS` 按 `big_side_group` 分组（CFFEX） | `NONE` | 拒绝热改 |
| `exchanges.{exchange}.big_side_exit` | `NONE` \| `LAST_TRADE_DAY_MINUS_5` \| `PHYSICAL_DELIVERY_MONTH_MINUS_1` | 否 | 合约退出单向大边的时点：`LAST_TRADE_DAY_MINUS_5` 最后交易日前第 5 个交易日；`PHYSICAL_DELIVERY_MONTH_MINUS_1` 实物交割月前一个交易日；`NONE` 不退出 | `NONE` | 拒绝热改 |
| `exchanges.{exchange}.cancel_limit` | `int` \| `null` | 否 | 每个合约每日撤单次数上限（合规指标 `account.compliance_util` 的分母，达到即拒绝撤单）；`null` 不限制 | `null` | 拒绝热改 |
| `exchanges.{exchange}.large_cancel` | 对象 \| `null` | 否 | 大额撤单限制；`null` 不限制 | `null` | 拒绝热改 |
| `exchanges.{exchange}.large_cancel.count` | `int` | 是 | 每个合约每日大额撤单次数上限 | — | 拒绝热改 |
| `exchanges.{exchange}.large_cancel.min_qty` | `int` | 是 | 单笔手数不小于该值的撤单算大额撤单 | — | 拒绝热改 |
| `exchanges.{exchange}.self_trade_limit` | `int` | 否 | 每日自成交次数上限（合规指标的分母） | `5` | 拒绝热改 |
| `products` | {product: 对象} | 是 | 品种规则，键为品种代码（`rb`、`IF` 等） | — | 拒绝热改 |
| `products.{product}.exchange` | `SHFE` \| `DCE` \| `CFFEX` \| `INE` \| `CZCE` \| `GFEX` | 是 | 所属交易所，须在 `exchanges` 中配置 | — | 拒绝热改 |
| `products.{product}.name` | `str` | 否 | 中文名称，用于界面和报表 | `""` | 拒绝热改 |
| `products.{product}.sector` | `str` | 是 | 板块（`black`、`nonferrous`、`precious`、`agri`、`equity_index`、`rates` 等），板块类规则按它汇总 | — | 拒绝热改 |
| `products.{product}.multiplier` | `int` | 是 | 合约乘数 | — | 拒绝热改 |
| `products.{product}.tick_size` | `decimal` | 是 | 最小变动价位 | — | 拒绝热改 |
| `products.{product}.limit_pct` | `decimal` | 是 | 涨跌停幅度（小数，0.05 表示 ±5%） | — | 拒绝热改 |
| `products.{product}.limit_schedule` | 对象列表 | 否 | 带生效日期的涨跌停调整（例如节前放宽）；取生效日不晚于当前交易日的最新一条 | `[]` | 拒绝热改 |
| `products.{product}.limit_schedule[].effective_from` | `str` | 是 | 开始生效的交易日（YYYYMMDD） | — | 拒绝热改 |
| `products.{product}.limit_schedule[].pct` | `decimal` | 是 | 新的涨跌停幅度 | — | 拒绝热改 |
| `products.{product}.settle_method` | `VWAP_FULL_DAY` \| `VWAP_LAST_HOUR` | 否 | 结算价方法：`VWAP_FULL_DAY` 全日（含夜盘）成交量加权平均；`VWAP_LAST_HOUR` 最后一小时 VWAP（CFFEX） | `VWAP_FULL_DAY` | 拒绝热改 |
| `products.{product}.settle_window` | `str` \| `null` | 否 | `VWAP_LAST_HOUR` 的时间窗 `HH:MM-HH:MM`（IF 为 14:00-15:00，T 为 14:15-15:15） | `null` | 拒绝热改 |
| `products.{product}.big_side_group` | `str` \| `null` | 否 | 单向大边分组名，同组合约合并计算大边（如 `CFFEX:INDEX`） | `null` | 拒绝热改 |
| `products.{product}.physical_delivery` | `bool` | 否 | 实物交割品种（CFFEX 国债）：交割月前一个交易日退出单向大边 | `false` | 拒绝热改 |
| `products.{product}.sessions` | `list[str]` | 是 | 使用的交易时段模板名（`session_templates` 的键） | — | 拒绝热改 |
| `products.{product}.fees` | 对象列表 | 是 | 手续费规则，带生效日期；按 `contracts` 列表 → `months` → `other`（或 `months` 为 `null`）的顺序，取第一个有已生效条目（生效日不晚于当日）的档中最新的一条；各档都还没有生效的条目时取生效日最早的一条 | — | 拒绝热改 |
| `products.{product}.fees[].months` | `list[int]` \| `other` \| `null` | 否 | 适用的合约月份列表，或 `other` 表示其余月份；`null` 表示所有月份 | `null` | 拒绝热改 |
| `products.{product}.fees[].contracts` | `list[str]` \| `null` | 否 | 适用的具体合约列表（优先于 `months`） | `null` | 拒绝热改 |
| `products.{product}.fees[].effective_from` | `str` | 否 | 开始生效的交易日（YYYYMMDD） | `19700101` | 拒绝热改 |
| `products.{product}.fees[].type` | `rate` \| `per_lot` | 是 | `rate` 按成交金额的比例；`per_lot` 按手收固定金额（元） | — | 拒绝热改 |
| `products.{product}.fees[].open` | `decimal` | 是 | 开仓费率或每手金额 | — | 拒绝热改 |
| `products.{product}.fees[].close_yd` | `decimal` | 是 | 平昨费率或每手金额 | — | 拒绝热改 |
| `products.{product}.fees[].close_today_mult` | `decimal` | 否 | 平今手续费 = 平昨费率 × 该倍数（0 表示平今免收） | `1` | 拒绝热改 |
| `products.{product}.fees[].intraday` | `decimal` \| `null` | 否 | DCE：当日开当日平时，两腿都按这个费率收取；`null` 表示不适用 | `null` | 拒绝热改 |
| `products.{product}.margin` | 对象列表 | 是 | 保证金率，带生效日期，可以按合约覆盖：合约列表和 `*` 两档各取生效日不晚于当日的最新一条，两档都有时取公司保证金率较高的一条；两档都还没有生效的条目时取生效日最早的一条 | — | 拒绝热改 |
| `products.{product}.margin[].contracts` | `list[str]` \| `*` | 否 | 适用合约：`*` 表示全部，或合约列表 | `*` | 拒绝热改 |
| `products.{product}.margin[].effective_from` | `str` | 否 | 开始生效的交易日（YYYYMMDD） | `19700101` | 拒绝热改 |
| `products.{product}.margin[].exchange_rate` | `decimal` | 是 | 交易所保证金率 | — | 拒绝热改 |
| `products.{product}.margin[].broker_add` | `decimal` | 否 | 经纪商加收；公司保证金率 = `exchange_rate` + `broker_add` | `0` | 拒绝热改 |
| `products.{product}.max_order_qty` | `dict[str, int]` | 否 | 单笔最大手数，按委托类型（`LIMIT`、`MARKET`），事前检查第 1 步使用 | `{LIMIT: 1000, MARKET: 1000}` | 拒绝热改 |
| `products.{product}.position_limit` | `int` \| `null` | 否 | 单边持仓上限（手，含在途开仓），事前检查第 6 步按它拒单（`REJECT_POSITION_LIMIT`），也是合规指标的分母；`null` 不限制 | `null` | 拒绝热改 |
| `products.{product}.open_limit_per_day` | `int` \| `null` | 否 | 每个合约每日开仓手数上限（含在途开仓），事前检查第 5 步按它拒单（`REJECT_OPEN_VOLUME_LIMIT`），也是合规指标的分母；`null` 不限制 | `null` | 拒绝热改 |
| `products.{product}.notional_face` | `decimal` \| `null` | 否 | 国债期货面值：合约价值 = 价格 × 面值 / 100（策略 SDK 使用）；`null` 表示按乘数计算 | `null` | 拒绝热改 |
| `instruments` | 对象列表 | 是 | 可交易的合约列表 | — | 拒绝热改 |
| `instruments[].instrument_id` | `str` | 是 | 合约代码，按交易所原始大小写（README §3.3） | — | 拒绝热改 |
| `instruments[].product` | `str` | 是 | 所属品种（`products` 的键） | — | 拒绝热改 |
| `instruments[].last_trade_day` | `str` | 是 | 最后交易日（YYYYMMDD）；之后不再接受委托，单向大边的退出日也据此推算 | — | 拒绝热改 |
| `instruments[].listed_day` | `str` | 否 | 上市日（YYYYMMDD）；之前不接受委托 | `20200101` | 拒绝热改 |
| `instruments[].delivery_month` | `str` \| `null` | 否 | 交割月（YYYYMM）；`null` 时按合约代码推算 | `null` | 拒绝热改 |
<!-- gen:products end -->

示例（节选 `config/products.yaml`）：

```yaml
session_templates:
  night_2100_2300: [["20:55", "21:00", "auction"], ["21:00", "23:00", "continuous"]]
  day_commodity:
    - ["08:55", "09:00", "auction_no_night"]
    - ["09:00", "10:15", "continuous"]
    - ["10:30", "11:30", "continuous"]
    - ["13:30", "15:00", "continuous"]
exchanges:
  SHFE: {explicit_close_today: true, close_style: FIFO, close_today_fee_rule: LOT_MATCH,
         settle_rounding: FLOOR_TICK, settle_fallback: [MEDIAN, LIMIT, PRE_SETTLE],
         big_side: SAME_PRODUCT, big_side_exit: LAST_TRADE_DAY_MINUS_5,
         cancel_limit: 500, large_cancel: {count: 50, min_qty: 300}, self_trade_limit: 5}
products:
  rb:
    exchange: SHFE
    name: 螺纹钢
    sector: black
    multiplier: 10
    tick_size: "1"
    limit_pct: "0.05"
    settle_method: VWAP_FULL_DAY
    big_side_group: "SHFE:rb"
    sessions: [night_2100_2300, day_commodity]
    fees:
      - {months: [1, 5, 10], effective_from: "20260101", type: rate, open: "0.0001", close_yd: "0.0001", close_today_mult: "1"}
      - {months: other, effective_from: "20260101", type: rate, open: "0.00002", close_yd: "0.00002", close_today_mult: "1"}
    margin:
      - {contracts: "*", effective_from: "20260101", exchange_rate: "0.07", broker_add: "0.01"}
      - {contracts: [rb2610], effective_from: "20260901", exchange_rate: "0.10", broker_add: "0.01"}
    max_order_qty: {LIMIT: 500, MARKET: 30}
instruments:
  - {instrument_id: rb2701, product: rb, last_trade_day: "20270115", delivery_month: "202701"}
```

- 本文件和交易日历拒绝热改（md-gateway 否决），按 §1.3 在 EOD 之后停机离线提交。
- 新增合约除了写进 `instruments`，模拟环境还要在 `sim.yaml` 的 `price_engine.instruments` 中给出价格参数。

## 5. accounts.yaml

策略账户和分片归属。

<!-- gen:accounts begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `accounts` | {account_id: 对象} | 是 | 账户表，键为账户 ID（1~32 个字母、数字、`_`、`-`；演示环境为 `ACC1`~`ACC5`）。增删账户会改变分片的账户集合，被所属 risk-engine 否决（拒绝热改） | — | 见子字段 |
| `accounts.{account_id}.name` | `str` | 是 | 账户名称，用于界面、报表和结算单 | — | 即时 |
| `accounts.{account_id}.shard` | `int` | 是 | 所属 risk-engine 分片；`services.yaml` 中必须有 `risk-engine-{shard}` 实例 | — | 拒绝热改 |
| `accounts.{account_id}.initial_capital` | `decimal` | 是 | 初始资金（元）。首日建账（`futrisk demo`、docker compose 的 `seed`）时写入日初快照，同时作为上日结存和业绩权益高水位；此后单位净值和验收口径的日盈亏比率以日初快照中的初始资金为分母，建账之后修改本字段不影响风控和清算 | — | 首日建账时读取 |
| `accounts.{account_id}.strategy` | `str` \| `null` | 否 | strategy-sim 为该账户运行的策略名（`channel_breakout`、`multi_trend`、`counter_trend_add`、`index_directional`、`rb_calendar_spread`）；回测不用它（回测的策略见 `backtest/*.yaml`） | `null` | 重启 strategy-sim |
| `accounts.{account_id}.products` | `list[str]` | 否 | 策略交易的品种：strategy-sim 按它交易，回测把它作为账户的默认品种。这不是风控限制，事前检查不校验账户可交易的品种（08 §5.2） | `[]` | 重启 strategy-sim |
| `accounts.{account_id}.initial_positions` | 对象列表 | 否 | 首日建账的初始持仓：每项成为该账户的一个昨仓 lot（`lot_no` 按列表顺序从 1 编号），盯市基准取 `sim.yaml` 中该合约的 `pre_settle`，模拟器不报价的合约取 `open_price` | `[]` | 首日建账时读取 |
| `accounts.{account_id}.initial_positions[].instrument_id` | `str` | 是 | 合约代码 | — | 首日建账时读取 |
| `accounts.{account_id}.initial_positions[].direction` | `LONG` \| `SHORT` | 是 | 持仓方向 | — | 首日建账时读取 |
| `accounts.{account_id}.initial_positions[].qty` | `int` | 是 | 手数 | — | 首日建账时读取 |
| `accounts.{account_id}.initial_positions[].open_price` | `decimal` | 是 | 开仓价 | — | 首日建账时读取 |
| `accounts.{account_id}.initial_positions[].open_td` | `str` \| `null` | 否 | 开仓交易日（YYYYMMDD）；`null` 为建账日的前一交易日。开仓时刻记为该日 14:59 | `null` | 首日建账时读取 |
<!-- gen:accounts end -->

示例：

```yaml
accounts:
  ACC3:
    name: 日内逆势加仓
    shard: 1
    initial_capital: "10000000"
    strategy: counter_trend_add
    products: [rb, hc, i]
```

- 增加、删除账户或改变分片归属会被所属 risk-engine 否决（它的账户集合在进程内固定），只能停机后离线提交（§1.3）；新账户还要首日建账（`POST /api/v1/settlement/seed-sod`，07 §5.1）。
- `products` 只是策略的交易范围，不是风控限制：事前检查不校验账户可交易的品种，人工或 API 委托可以开任何已配置合约的仓位（08 §5.2）。

## 6. risk_rules.yaml

声明式风控规则、事前检查参数和风控核心的系统参数。默认阈值及其理由见 04 §4，状态机语义见 04 §6。

### 6.1 字段

<!-- gen:risk_rules begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `rules` | 对象列表 | 是 | 声明式风控规则（01 §8.14），与 02 §7.6 的 `RuleDef` 一一对应 | — | 即时 |
| `rules[].id` | `str` | 是 | 规则 ID，写入 `alert_key`（`{td}\|{rule_id}\|{level}\|{scope_key}\|{episode}`） | — | 即时 |
| `rules[].metric` | `str` | 是 | 指标名，须是已注册的指标（§6.2）。配置模型不检查：`futrisk config validate` 和离线提交都会放行未注册的名字；热加载时 risk-engine 在 prepare 阶段否决启用的规则中未注册的指标；冷启动时它在第一次评估时让 risk-engine 报错退出（08 §5.2） | — | 即时 |
| `rules[].op` | `<=` \| `>=` \| `<` \| `>` | 是 | 越线方向：亏损类用 `<=`，占比类用 `>=` | — | 即时 |
| `rules[].scope` | `str` | 否 | 评估范围：`account`、`account/product`、`account/sector`、`account/instrument`、`firm`、`firm/product`、`firm/sector`。规则编译时检查（热加载在 prepare 阶段否决，冷启动时 risk-engine 启动失败），配置模型不检查；指标类别与范围是否匹配（§6.2）不检查 | `account` | 即时 |
| `rules[].hysteresis` | `float` | 否 | 滞回比例：解除线 = 阈值向安全一侧移动 \|阈值\| × hysteresis（阈值 −0.03、hysteresis 0.2 时解除线为 −0.024） | `0.2` | 即时 |
| `rules[].cooldown_s` | `int` | 否 | 回到 OK 后，同一 `ALERT` 级别在该秒数内不再触发（同时越过更严重的级别时照常升级；`RESTRICT`、`CIRCUIT_BREAK`、`LIQUIDATE` 级别不适用） | `60` | 即时 |
| `rules[].hold_ms` | `int` | 否 | 未锁存的级别要在解除线之外连续停留该毫秒数才解除 | `3000` | 即时 |
| `rules[].pretrade` | `bool` | 否 | 是否参与事前检查的 what-if 预估（下单前预估本单会不会让指标越线） | `false` | 即时 |
| `rules[].enabled` | `bool` | 否 | `false` 时不评估该规则 | `true` | 即时 |
| `rules[].reset_on_day_roll` | `bool` | 否 | 换日时是否解除该规则的锁存；`false` 时锁存的级别带入新的一天（记为 episode 1） | `true` | 即时 |
| `rules[].min_gross_pct` | `float` \| `null` | 否 | 账户总名义敞口 / 权益低于该值时不评估（视为未越线），`null` 表示总是评估 | `null` | 即时 |
| `rules[].levels` | 对象列表 | 是 | 级别列表，每级一个阈值和动作 | — | 即时 |
| `rules[].levels[].name` | `str` | 是 | 级别名（如 `warn`、`restrict`、`halt`、`derisk`），写入 `alert_key` | — | 即时 |
| `rules[].levels[].threshold` | `float` | 是 | 阈值（比率一律为小数） | — | 即时 |
| `rules[].levels[].action` | `ALERT` \| `RESTRICT` \| `CIRCUIT_BREAK` \| `LIQUIDATE` | 是 | 动作：`ALERT` 告警；`RESTRICT` 限制下单；`CIRCUIT_BREAK` 熔断；`LIQUIDATE` 生成减仓计划 | — | 即时 |
| `rules[].levels[].params` | `dict[str, any]` | 否 | 动作参数，见本文 §6.3。任意键值：配置校验、规则编译和热加载都不检查键名、类型和取值（08 §5.2） | `{}` | 即时 |
| `rules[].levels[].latch` | `bool` | 否 | 确认后锁存，由 `risk.reset` 或换日解除（热加载把锁存中级别的动作从 `CIRCUIT_BREAK` 改掉时熔断会静默失效，08 §5.2 KD-19）；只允许用于 `CIRCUIT_BREAK`、`LIQUIDATE` 级别（用于 `ALERT`、`RESTRICT` 时规则编译失败） | `false` | 即时 |
| `rules[].levels[].confirm` | 对象 | 否 | 确认条件：满足其一即从 PENDING 转为 ACTIVE。`CIRCUIT_BREAK` 级别没写 `confirm` 时默认为 2 个状态或 1000 ms，其他级别默认 1 个状态（立即确认） | 见子字段 | 即时 |
| `rules[].levels[].confirm.states` | `int` | 否 | 需要的不同 LIVE 市场状态数；`1` 立即确认，`0` 只按时间确认（此时 `ms` 必须大于 0） | `1` | 即时 |
| `rules[].levels[].confirm.ms` | `int` | 否 | 越线持续的毫秒数 | `0` | 即时 |
| `rules[].levels[].rearm` | 对象 \| `null` | 否 | 锁存级别经人工重置后的重新布防参数；`null` 表示不重新布防 | `null` | 即时 |
| `rules[].levels[].rearm.extra` | `float` | 否 | 重新布防线 = min(阈值, 重置时的值 − extra)，按越线方向取 | `0.01` | 即时 |
| `rules[].overrides` | `dict[str, dict[str, dict[str, any]]]` | 否 | 按账户覆盖：账户 ID → 级别名 → 要覆盖的级别字段，例如 `{ACC2: {halt: {threshold: -0.04}}}`；级别名和覆盖后的级别在规则编译时检查 | `{}` | 即时 |
| `pretrade` | 对象 | 否 | 事前检查参数（01 §5.3.1、02 §11.2） | 见子字段 | 即时 |
| `pretrade.fat_finger_ticks` | `int` | 否 | 胖手指检查（`REJECT_FAT_FINGER`）：限价与标记价（当日最新价，封板时为涨跌停价，无行情时为昨结算）的偏离超过 max(该 tick 数 × 最小变动价位, `fat_finger_pct` × 标记价) 时拒单；行情质量不是 OK 时平仓单不做此检查 | `50` | 即时 |
| `pretrade.fat_finger_pct` | `float` | 否 | 胖手指检查的比例阈值，与 `fat_finger_ticks` 取较宽者 | `0.03` | 即时 |
| `pretrade.funds_buffer` | `float` | 否 | 资金检查的缓冲比例：开仓单的保证金加手续费超过 可用资金 × (1 − 该值) 时拒单（`REJECT_INSUFFICIENT_FUNDS`） | `0.05` | 即时 |
| `pretrade.throttle_rate_per_s` | `float` | 否 | 每个账户的报单速率上限（笔/秒，令牌桶；超出时 `REJECT_THROTTLED`） | `20` | 即时 |
| `pretrade.throttle_burst` | `int` | 否 | 令牌桶容量（允许的突发笔数） | `40` | 即时 |
| `pretrade.route_ttl_ms` | `int` | 否 | 通过风控后路由的有效期（毫秒）：超过 `expire_ns` 才到达的路由被 trade-gateway 以 `ROUTE_EXPIRED` 拒绝，记为 `EXPIRED`，不报给柜台 | `2000` | 即时 |
| `pretrade.default_max_order_qty` | `int` | 否 | 品种的 `max_order_qty` 中没有该委托类型（`LIMIT` / `MARKET`）时的单笔最大手数；整个 `max_order_qty` 省略时用模型默认的 `{LIMIT: 1000, MARKET: 1000}`，不用本值 | `1000` | 即时 |
| `system` | 对象 | 否 | 风控核心的系统参数 | 见子字段 | 即时 |
| `system.snapshot_hz` | `float` | 否 | 账户快照 `risk.snapshot.{account_id}` 的最高发布频率（Hz）；另外每秒至少发一次保活。0 关闭（回测使用） | `2` | 即时 |
| `system.metric_sample_s` | `float` | 否 | 账户指标采样周期（秒），写入 `metric_samples`，供日内曲线和日报使用。0 关闭 | `1` | 即时 |
| `system.md_unknown_account_block_pct` | `float` | 否 | 账户敞口中行情质量为 UNKNOWN 的比例超过该值时，整个账户禁开 | `0.5` | 即时 |
| `system.recon_interval_s` | `int` | 否 | 盘中与柜台做持仓对账的周期（秒）。0 关闭 | `300` | 即时 |
| `system.recon_quiet_s` | `int` | 否 | 对账前要求该账户最近没有成交和在途委托的静默秒数 | `5` | 即时 |
| `system.quality_stale_tolerance` | `float` | 否 | 账户范围规则的行情质量门控容忍度：质量不是 OK 的持仓合约占账户总名义敞口不超过该比例时，账户质量仍按 OK 处理（这些合约按最后已知价格估值）；超过时取最差的质量，状态机保持不动。0 表示不容忍 | `0.1` | 即时 |
<!-- gen:risk_rules end -->

### 6.2 可用的指标

`rules[].metric` 必须是下表中的名字（由 `@register_metric` 注册）。用错时热加载在 prepare 阶段被否决；`futrisk config validate` 和离线提交不检查，冷启动后 risk-engine 在第一次评估时报错退出（08 §5.2）。规则编译器还接受 `account/instrument` 范围，但没有按合约计算的指标类别；指标类别与范围是否匹配不做检查。`account.*` 对应 02 §7.6 `AccountFunds` / `AccountMetricsView` 的同名字段，`product.*` 对应 `ExposureView`，`sector.*` 为板块汇总；各指标的定义见 04 §3。

<!-- gen:metrics begin -->
| 指标类别 | 可用于的 `scope` | 指标名 = 前缀 + 字段 |
|---|---|---|
| `account` | `account` | 前缀 `account.`，字段：`available`、`cancels_today`、`close_pnl`、`compliance_util`、`cum_net_deposit`、`daily_pnl`、`daily_pnl_pct_init`（分母 `initial_capital`）、`daily_pnl_pct_sod`（分母 `static_equity`）、`effective_n`、`equity`、`fee`、`floating_pnl_pct_init`（分母 `initial_capital`）、`frozen_fee`、`frozen_margin`、`gross_notional`、`hwm_dd`、`hwm_equity`、`initial_capital`、`intraday_dd`、`intraday_peak_equity`、`leverage`、`limit_stress`、`margin`、`nav`（分母 `initial_capital`）、`net_deposit`、`net_notional`、`orders_today`、`perf_equity`、`position_pnl`、`pre_balance`、`risk_degree`、`static_equity` |
| `product` | `account/product`、`firm/product` | 前缀 `product.`，字段：`gross_notional`、`gross_pct_equity`、`long_lots`、`net_lots`、`net_notional`、`net_pct_equity`、`short_lots`、`utilization` |
| `sector` | `account/sector`、`firm/sector` | 前缀 `sector.`，字段：`gross_notional`、`gross_pct_equity`、`net_notional`、`net_pct_equity` |
| `firm` | `firm` | 前缀 `firm.`，字段：`available`、`cancels_today`、`close_pnl`、`cum_net_deposit`、`daily_pnl`、`daily_pnl_pct_init`、`daily_pnl_pct_sod`、`effective_n`、`equity`、`fee`、`floating_pnl_pct_init`、`frozen_fee`、`frozen_margin`、`gross_notional`、`hwm_dd`、`hwm_equity`、`initial_capital`、`intraday_dd`、`intraday_peak_equity`、`leverage`、`limit_stress`、`margin`、`nav`、`net_deposit`、`net_notional`、`orders_today`、`perf_equity`、`position_pnl`、`pre_balance`、`risk_degree`、`static_equity` |
<!-- gen:metrics end -->

### 6.3 动作参数（`levels[].params`）

| 动作 | 参数 | 类型 | 说明 |
|---|---|---|---|
| `RESTRICT` | `max_order_factor` | 小数 | 单笔上限系数：单笔最大手数 × 该值 |
| `RESTRICT` | `no_add_to_losers` | 布尔 | 禁止在亏损品种上加仓（`REJECT_ADD_TO_LOSER`） |
| `RESTRICT` | `exposure_cap_factor` | 小数 | 敞口上限系数：开仓使品种 \|净名义\| 增大、且开仓后 \|净名义\| ÷ 权益超过（3% ÷ 涨跌停幅度）× 该值时拒绝（`REJECT_EXPOSURE_CAP`） |
| `RESTRICT` | `block_open` | 布尔 | 在规则范围（账户、品种、板块）内禁开 |
| `RESTRICT` | `block_open_largest_product` | 布尔 | 只在 \|净名义\| 最大的品种上禁开（集中度规则使用） |
| `LIQUIDATE` | `step` | 小数 | 减仓计划对每个持仓平掉的比例（向上取整，至少 1 手，亏损的持仓排在前面），默认 0.5 |
| `LIQUIDATE` | `price_ticks` | 整数 | 建议价格为"对手价 ± N 跳"中的 N，默认 2 |

`ALERT` 和 `CIRCUIT_BREAK` 没有参数；熔断的确认和锁存由 `confirm`、`latch`、`rearm` 控制。限制的作用范围和对应的拒单码见 04 §7.2。

`params` 的键和值不做任何校验（配置模型、规则编译、热加载 prepare 都不检查，08 §5.2）：拼错的键被静默忽略，该级别触发时不产生限制；布尔参数按真值判断，`block_open: "false"` 这样的非空字符串也算开启；数值参数在级别触发时才转换，类型错误（如 `max_order_factor: "half"`、`step: "x"`）会让 risk-engine 在评估时出错退出，越线持续期间重启后又会复现。`step` 超出 0~1 时按 0~1 截断。

### 6.4 示例

```yaml
rules:
  - id: daily_loss
    metric: account.daily_pnl_pct_init   # floating_only 口径：account.floating_pnl_pct_init
    op: "<="
    scope: account
    hysteresis: 0.2
    cooldown_s: 60
    pretrade: true
    levels:
      - {name: warn, threshold: -0.020, action: ALERT}
      - {name: restrict, threshold: -0.025, action: RESTRICT, params: {max_order_factor: 0.5, no_add_to_losers: true}}
      - {name: halt, threshold: -0.030, action: CIRCUIT_BREAK, latch: true, confirm: {states: 2, ms: 1000}, rearm: {extra: 0.01}}
      - {name: derisk, threshold: -0.050, action: LIQUIDATE, params: {step: 0.5}}
    overrides:
      ACC2: {halt: {threshold: -0.040}}   # ACC2 的熔断线放宽到 -4%
pretrade: {fat_finger_ticks: 50, fat_finger_pct: 0.03, funds_buffer: 0.05, throttle_rate_per_s: 20, throttle_burst: 40, route_ttl_ms: 2000}
system: {snapshot_hz: 2.0, metric_sample_s: 1.0, md_unknown_account_block_pct: 0.5, recon_interval_s: 300, recon_quiet_s: 5}
```

- 热加载删除、禁用锁存中的级别或只改它的阈值时，**锁存保持**到 `risk.reset` 或换日（04 §6.8）；规则和级别仍在而改了它的动作（例如把熔断改成 `ALERT`）时，锁存被静默解除，这是已知缺陷（08 §5.2 KD-19）。

## 7. feeds.yaml

行情源和多源仲裁参数（01 §8.13）。md-gateway 只在启动时建立数据源，所以数据源各段的任何改动都会被否决；`arbitration` 段可以即时生效。

<!-- gen:feeds begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `feed_a` | 对象 | 否 | Feed A：CTP 风格的 WebSocket 推送源（02 §9.1） | 见子字段 | 拒绝热改 |
| `feed_a.enabled` | `bool` | 否 | 是否接入 | `true` | 拒绝热改 |
| `feed_a.url` | `str` | 否 | WebSocket 地址 | `ws://127.0.0.1:7001/feed` | 拒绝热改 |
| `feed_a.priority` | `int` | 否 | 优先级（数字小者优先）。只在生成规范 tick 文件时，同一状态有多个来源的情况下决定保留哪一条（03 §5.1）；仲裁不按优先级挑选数据源 | `1` | 拒绝热改 |
| `feed_a.reconnect_backoff_ms` | `list[int]` | 否 | 断线重连的退避序列（毫秒），用完后重复最后一个值 | `[200, 500, 1000, 2000, 5000]` | 拒绝热改 |
| `feed_b` | 对象 | 否 | Feed B：厂商风格的 REST 轮询源（02 §9.2） | 见子字段 | 拒绝热改 |
| `feed_b.enabled` | `bool` | 否 | 是否接入 | `true` | 拒绝热改 |
| `feed_b.url` | `str` | 否 | 轮询地址 | `http://127.0.0.1:7002/v1/quotes` | 拒绝热改 |
| `feed_b.poll_ms` | `int` | 否 | 轮询周期（毫秒）；上一次未完成时跳过本次 | `1000` | 拒绝热改 |
| `feed_b.timeout_ms` | `int` | 否 | 单次轮询的读超时（毫秒）；连接超时固定为 1 s | `800` | 拒绝热改 |
| `feed_b.priority` | `int` | 否 | 优先级 | `2` | 拒绝热改 |
| `sina` | 对象 | 否 | 新浪期货行情（02 §9.3），live 档使用；sim 档必须关闭（两边价格不是同一个市场） | 见子字段 | 拒绝热改 |
| `sina.enabled` | `bool` | 否 | 是否接入（md-gateway） | `false` | 拒绝热改 |
| `sina.url` | `str` | 否 | 请求地址前缀，合约代码拼在后面 | `https://hq.sinajs.cn/list=` | 拒绝热改 |
| `sina.referer` | `str` | 否 | 请求头 Referer（新浪要求） | `https://finance.sina.com.cn` | 拒绝热改 |
| `sina.poll_ms` | `int` | 否 | 轮询周期（毫秒）；小于 1000 时按 1000 处理（记 WARNING）。HTTP 403 / 429 时间隔加倍，最长 60 s；其他失败从第二次起加倍，最长 5 s | `1500` | 拒绝热改 |
| `sina.timeout_ms` | `int` | 否 | 单次轮询的读超时（毫秒）；连接超时取它和 3 s 中较大的 | `1200` | 拒绝热改 |
| `sina.priority` | `int` | 否 | 优先级 | `3` | 拒绝热改 |
| `sina.trust_env` | `bool` | 否 | 是否使用环境变量 `HTTP(S)_PROXY` 中的代理 | `false` | 拒绝热改 |
| `sina.symbols` | `dict[str, str]` | 否 | 合约代码 → 新浪代码（如 `rb2701: nf_RB2701`，`IF2612: CFF_RE_IF2612`）；`futrisk data record` 也按它录制 | `{}` | 拒绝热改 |
| `arbitration` | 对象 | 否 | 多源仲裁参数（01 §8.13） | 见子字段 | 即时 |
| `arbitration.exclude_missed_heartbeats` | `int` | 否 | Feed A 连续丢失该数量的心跳即排除（EXCLUDED） | `3` | 即时 |
| `arbitration.exclude_failed_polls` | `int` | 否 | 轮询源连续失败该次数即排除 | `3` | 即时 |
| `arbitration.exclude_reject_rate` | `float` | 否 | 最近 1 分钟的拒收率超过该值即排除（样本不少于 20 条） | `0.05` | 即时 |
| `arbitration.exclude_integrity_votes` | `int` | 否 | 5 分钟内被交叉校验投票（`MD_INTEGRITY` 的 `VOTE`）满该次数即排除 | `2` | 即时 |
| `arbitration.probation_healthy_s` | `float` | 否 | 观察期（PROBATION）内健康满该秒数，并且已经追上水位（Feed A 还要求没有序号缺口），才恢复可信 | `10` | 即时 |
| `arbitration.min_dwell_s` | `float` | 否 | 每个可信状态最少停留的秒数（防抖）：只推迟升级，排除从不推迟 | `5` | 即时 |
| `arbitration.reorder_buffer_max` | `int` | 否 | Feed A 乱序缓冲的最大条数 | `32` | 即时 |
| `arbitration.reorder_buffer_ms` | `int` | 否 | Feed A 乱序缓冲的最长等待（毫秒） | `100` | 即时 |
| `arbitration.stale_s` | `float` | 否 | 连续交易时段内合约没有可信、及时的数据源覆盖满该秒数，质量标为 STALE（该合约禁开）；有覆盖但满该秒数没有成交时发 `MD_QUIET` | `5` | 即时 |
| `arbitration.unknown_s` | `float` | 否 | 没有覆盖满该秒数，质量标为 UNKNOWN | `30` | 即时 |
| `arbitration.open_grace_s` | `float` | 否 | 每次开盘后的宽限秒数，期间不判定陈旧；md-gateway 启动后的同样时长内也不发 `MD_DOWN` | `30` | 即时 |
| `arbitration.live_max_latency_ms` | `int` | 否 | 接收时间比交易所时间晚超过该毫秒数的状态不发布为 LIVE（记为 BACKFILL）；交易所时间超前接收时间 1 s 以上的状态直接拒收 | `3000` | 即时 |
| `arbitration.watermark_flush_s` | `float` | 否 | 仲裁水位写入 md.db 的周期（秒） | `1` | 即时 |
<!-- gen:feeds end -->

示例（打开新浪作为第三个源，只适用于真实行情部署；sim 档必须保持关闭，新浪的价格和成交量来自真实市场，与模拟交易所不是同一个市场）：

```yaml
sina:
  enabled: true
  poll_ms: 1500
  symbols: {rb2701: nf_RB2701, IF2612: CFF_RE_IF2612}
```

## 8. sim.yaml

虚拟交易所 sim-exchange 的参数：价格引擎、Feed A/B、模拟柜台和控制接口（02 §9.1–9.5）。

<!-- gen:sim begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `price_engine` | 对象 | 否 | 价格引擎：相关的 GBM 加跳跃过程，价格按 tick 对齐并受涨跌停约束（01 §8.13） | 见子字段 | 重启 sim-exchange |
| `price_engine.seed` | `int` | 否 | 随机种子；同一种子重现同一条价格路径 | `20260928` | 重启 sim-exchange |
| `price_engine.tick_interval_ms` | `int` | 否 | 每个合约产生一个新行情状态的间隔（毫秒），只在连续交易时段内产生 | `500` | 重启 sim-exchange |
| `price_engine.sector_correlation` | `float` | 否 | 同板块品种之间的相关系数 | `0.6` | 重启 sim-exchange |
| `price_engine.market_correlation` | `float` | 否 | 全市场公共因子的相关系数 | `0.2` | 重启 sim-exchange |
| `price_engine.instruments` | {instrument_id: 对象} | 否 | 模拟的合约，键为合约代码（须在 `products.yaml` 中） | `{}` | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.pre_settle` | `decimal` | 是 | 昨结算价：价格路径的初值；`futrisk demo` 首日建账也把它作为昨结算 | — | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.vol_annual` | `float` | 否 | 年化波动率 | `0.2` | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.jump_intensity_per_day` | `float` | 否 | 每个交易日跳跃次数的期望 | `0.5` | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.jump_sigma` | `float` | 否 | 单次跳跃幅度的标准差（对数收益） | `0.01` | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.volume_per_s` | `float` | 否 | 平均每秒成交量（手） | `5` | 重启 sim-exchange |
| `price_engine.instruments.{instrument_id}.open_interest` | `int` | 否 | 初始持仓量（手） | `100000` | 重启 sim-exchange |
| `feed_a` | 对象 | 否 | Feed A 服务端（WebSocket） | 见子字段 | 重启 sim-exchange |
| `feed_a.host` | `str` | 否 | 监听地址 | `127.0.0.1` | 重启 sim-exchange |
| `feed_a.port` | `int` | 否 | 监听端口 | `7001` | 重启 sim-exchange |
| `feed_a.ring_size` | `int` | 否 | 重放环形缓冲的容量（条）；缺口早于环形缓冲时只能用快照补齐 | `100000` | 重启 sim-exchange |
| `feed_a.heartbeat_ms` | `int` | 否 | 心跳间隔（毫秒） | `1000` | 重启 sim-exchange |
| `feed_b` | 对象 | 否 | Feed B 服务端（HTTP），同时提供健康检查 `GET /v1/health` | 见子字段 | 见子字段 |
| `feed_b.host` | `str` | 否 | 监听地址 | `127.0.0.1` | 重启 sim-exchange、supervisor |
| `feed_b.port` | `int` | 否 | 监听端口 | `7002` | 重启 sim-exchange、supervisor |
| `broker` | 对象 | 否 | SimBroker 模拟柜台（02 §9.4） | 见子字段 | 见子字段 |
| `broker.endpoint` | `str` | 否 | ZeroMQ ROUTER 端点，trade-gateway 连接它 | `tcp://127.0.0.1:7003` | 重启 sim-exchange、trade-gateway |
| `broker.front_id` | `int` | 否 | 前置编号（仿 CTP 的 FrontID） | `1` | 重启 sim-exchange |
| `broker.ack_delay_ms` | `int` | 否 | 委托确认的模拟延迟（毫秒） | `5` | 重启 sim-exchange |
| `broker.fill_delay_ms` | `int` | 否 | 成交回报的模拟延迟（毫秒） | `20` | 重启 sim-exchange |
| `control` | 对象 | 否 | 控制接口（02 §9.5），只在 `app.mode=demo` 时开放 | 见子字段 | 见子字段 |
| `control.host` | `str` | 否 | 监听地址 | `127.0.0.1` | 重启 sim-exchange、api-gateway |
| `control.port` | `int` | 否 | 监听端口 | `7004` | 重启 sim-exchange、api-gateway |
<!-- gen:sim end -->

- `sim.yaml` 没有实例否决，提交总会成功，但 sim-exchange 只在启动时读取它。读取它的其他进程：trade-gateway 启动时按 `broker.endpoint` 连接模拟柜台，api-gateway 启动时按 `control` 建立控制接口的客户端，supervisor 启动时按 `feed_b` 拼出 sim-exchange 的健康检查地址，并在拉起 sim-exchange 之前检查这些端口是否被占用。
- Feed A、Feed B 的端口改动还要同步 `feeds.yaml` 的 `feed_a.url`、`feed_b.url`，而数据源地址拒绝热改，所以这类改动只能停机离线提交后整体重启（§1.3）。

## 9. scenarios/*.yaml

sim-exchange 的场景剧本：按时间表施加价格冲击和故障。自带 `breaker`（熔断验收）、`feed_outage`（Feed A 中断与补数）、`all_feeds_outage`（全部行情源中断）。

### 9.1 字段

<!-- gen:scenario begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `name` | `str` | 是 | 剧本名，应当与文件名相同；`futrisk scenario run <name>` 按文件名 `<name>.yaml` 查找（名字限 1~64 个字母、数字、`_`、`-`） | — | 重启 sim-exchange |
| `description` | `str` | 否 | 说明，`futrisk scenario list` 和仪表盘显示 | `""` | 重启 sim-exchange |
| `vars` | `dict[str, any]` | 否 | 变量及默认值；步骤中整值为 `${var}` 时按原类型替换，嵌在字符串中时按文本替换；运行时可用 `--var KEY=VALUE` 覆盖 | `{}` | 重启 sim-exchange |
| `steps` | 对象列表 | 是 | 步骤列表，按 `at_s` 顺序执行 | — | 重启 sim-exchange |
| `steps[].at_s` | `float` | 否 | 相对剧本开始的秒数（不小于 0）；`--speed 2` 时减半，冲击和故障的 `duration_s` 以及故障参数 `refuse_reconnect_s`、`seconds` 同样缩放 | `0` | 重启 sim-exchange |
| `steps[].action` | `shock` \| `fault` \| `clear_fault` \| `clear_faults` \| `pause` \| `resume` \| `log` | 是 | 动作：`shock` 价格冲击、`fault` 注入故障、`clear_fault` 撤销（按 `ref`）、`clear_faults` 撤销全部、`pause` / `resume` 暂停和恢复行情、`log` 只记日志 | — | 重启 sim-exchange |
| `steps[].params` | `dict[str, any]` | 否 | 动作参数，见本文 §9.2 | `{}` | 重启 sim-exchange |
| `steps[].ref` | `str` \| `null` | 否 | 给 `fault` 步骤起的名字，供后面的 `clear_fault` 引用 | `null` | 重启 sim-exchange |
| `steps[].note` | `str` \| `null` | 否 | `log` 步骤的文字 | `null` | 重启 sim-exchange |
<!-- gen:scenario end -->

- 剧本在每次 `scenario run` 时从 sim-exchange 启动时加载的配置版本目录（`<home>/config/versions/<N>/scenarios/`）读取，找不到时再找包内默认目录。版本目录不可变，所以新增或修改剧本要先提交新版本（热加载或离线提交），再重启 sim-exchange。

### 9.2 各动作的参数

| 动作 | 参数 | 说明 |
|---|---|---|
| `shock` | `sector` 或 `instruments`（二选一）、`pct`、`duration_s`、`shape` | 板块（`black`、`nonferrous`、`precious`、`agri`、`equity_index`、`rates`）或合约列表的价格冲击；`pct` 为比率（−0.03 为 −3%）；`shape` 为 `STEP`（一步到位）或 `LINEAR`（在 `duration_s` 内线性走完） |
| `fault` | `type`、`target`、`params`、`duration_s` | 注入故障，类型和参数见下表；`duration_s` 缺省时持续到撤销（一次性故障自行结束） |
| `clear_fault` | `ref` | 撤销此前 `ref` 命名的故障 |
| `clear_faults` | — | 撤销本剧本注入的全部故障 |
| `pause` / `resume` | — | 暂停 / 恢复行情推送 |
| `log` | —（用 `note`） | 只在剧本日志中记一句 |

`--speed` 同时缩放 `at_s`、冲击和故障的 `duration_s`，以及时长类的故障参数 `refuse_reconnect_s`、`seconds`；延迟、比例和条数不缩放。

故障类型（与 `futrisk fault inject` 相同，02 §9.5）：

| `type` | `target` | `params` | 效果 |
|---|---|---|---|
| `FEED_DISCONNECT` | `feed_a` / `feed_b` | `refuse_reconnect_s`、`mode`（`close` / `silent`，仅 Feed A） | 断开连接（或静默不发，靠丢失心跳发现），期间拒绝重连；Feed B 返回 503 |
| `FEED_GAP` | `feed_a` | `drop_count` | 接下来 N 条行情不发送（仍在重放环形缓冲中，可补回） |
| `FEED_DUPLICATE` | 任一源 | `ratio` | 按比例重复发送 |
| `FEED_REORDER` | 任一源 | `window` | 在窗口内打乱顺序 |
| `FEED_LATENCY` | 任一源 | `ms` | 每条消息延迟 N 毫秒 |
| `FEED_FREEZE` | 不填 / 任一源 | `instrument` | 不填：该合约停止交易；填源：该源停止更新该合约 |
| `BAD_TICK` | 不填 / 任一源 | `instrument`、`offset_pct`、`with_volume` | 发一笔偏离的坏价（在 tick 网格上、不受涨跌停约束） |
| `MALFORMED` | `feed_b` | `ratio`、`mode`（`body` / `record`） | 截断的 JSON，或一条价格为 `"N/A"` 的记录 |
| `SIM_RESTART` | — | — | 模拟交易所重启：Feed A 换 epoch、清空环形缓冲、断开柜台会话 |
| `BROKER_FORCE_CLOSE` | — | `account_id`、`instrument_id`、`qty`、`posi_direction` | 柜台强平：以对手价生成一笔外部成交 |
| `BROKER_DISCONNECT` | — | `seconds` | 柜台断线 N 秒 |

### 9.3 示例

```yaml
name: feed_outage
description: Feed A 中断 20s 后恢复，再制造一次跳号、重复和乱序
vars: {outage_s: 20, gap_ticks: 15}
steps:
  - {at_s: 0, action: log, note: "Feed A down for ${outage_s}s"}
  - {at_s: 0, action: fault, ref: feed_a_down,
     params: {type: FEED_DISCONNECT, target: feed_a, params: {refuse_reconnect_s: "${outage_s}"}}}
  - {at_s: 45, action: fault, params: {type: FEED_GAP, target: feed_a, params: {drop_count: "${gap_ticks}"}}}
  - {at_s: 60, action: fault, params: {type: FEED_DUPLICATE, target: feed_a, params: {ratio: 0.2}, duration_s: 15}}
```

运行：`futrisk scenario run feed_outage --var outage_s=30 --speed 2`（07 §4.6）。

## 10. backtest/*.yaml

回测配置（01 §8.16；方法与结果见 10 文档）。

### 10.1 字段

<!-- gen:backtest begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `name` | `str` | 是 | 回测名，文件中省略时取文件名。`futrisk backtest run` 默认输出到 `<home>/backtests/<name>-<YYYYMMDDTHHMMSS>`（北京时间），clearing-worker 的回测作业输出到 `<home>/backtests/{run_id}` | — | 每次运行时读取 |
| `description` | `str` | 否 | 说明，`futrisk backtest list` 显示第一行 | `""` | 每次运行时读取 |
| `kind` | `arms` \| `scenario` | 否 | 类型：`arms` 配对实验，必须配 `layer: A`；`scenario` 合成剧本（精确断言），必须配 `layer: D` 且 `scenarios` 不为空 | `arms` | 每次运行时读取 |
| `layer` | `A` \| `D` | 否 | 数据层：`A` 逐合约日线（2015 年至今）；`D` 合成剧本（01 §8.16） | `A` | 每次运行时读取 |
| `start` | `str` \| `null` | 否 | 起始交易日（YYYYMMDD）；`null` 为数据的第一天 | `null` | 每次运行时读取 |
| `end` | `str` \| `null` | 否 | 结束交易日；`null` 为数据的最后一天 | `null` | 每次运行时读取 |
| `products` | `list[str]` | 否 | 加载历史数据的品种（各账户交易的品种会自动加入） | `[rb, hc, cu, au, i, j, m, IF, IH, IC, IM, T]` | 每次运行时读取 |
| `accounts` | {account_id: 对象} | 否 | 回测账户，键为账户 ID | `{}` | 每次运行时读取 |
| `accounts.{account_id}.strategy` | `str` | 否 | 策略名：目前只有 `dual_ma`（双均线，10 文档） | `dual_ma` | 每次运行时读取 |
| `accounts.{account_id}.products` | `list[str]` \| `null` | 否 | 策略交易的品种；`null` 取 `accounts.yaml` 的 `products` | `null` | 每次运行时读取 |
| `accounts.{account_id}.params` | `dict[str, any]` | 否 | 策略参数（`dual_ma`：`fast`、`slow`、`band`、`notional_frac`、`cap_util`、`allow_short`） | `{}` | 每次运行时读取 |
| `accounts.{account_id}.capital` | `decimal` \| `null` | 否 | 初始资金；`null` 取 `accounts.yaml` 的 `initial_capital` | `null` | 每次运行时读取 |
| `accounts.{account_id}.enabled` | `bool` | 否 | `false` 时不参与 | `true` | 每次运行时读取 |
| `arms` | 对象列表 | 否 | 实验方案，名字唯一 | `[{name: no_rules, tier: none, threshold: null}, {name: block_open, tier: block_open, threshold: null}]` | 每次运行时读取 |
| `arms[].name` | `str` | 是 | 方案名 | — | 每次运行时读取 |
| `arms[].tier` | `none` \| `block_open` | 否 | 规则档位：`none` 不启用规则；`block_open` 生产的日亏规则（告警、限制下单、熔断禁开） | `block_open` | 每次运行时读取 |
| `arms[].threshold` | `float` \| `null` | 否 | 熔断阈值（正数，占初始资金的比例）；`null` 取 `rules.threshold` | `null` | 每次运行时读取 |
| `rules` | 对象 | 否 | 日亏损规则在各方案中的实例化方式 | 见子字段 | 每次运行时读取 |
| `rules.threshold` | `float` | 否 | 熔断阈值（正数），默认 3% | `0.03` | 每次运行时读取 |
| `rules.ladder` | `dict[str, float]` | 否 | 各级别相对熔断阈值的倍数：告警 2/3、限制 5/6、熔断 1、减仓 5/3 | `{warn: 0.666667, restrict: 0.833333, halt: 1, derisk: 1.66667}` | 每次运行时读取 |
| `path` | 对象 | 否 | A 层日内路径的合成方式（01 §8.16） | 见子字段 | 每次运行时读取 |
| `path.rule` | `open_near_extreme` \| `high_first` \| `low_first` | 否 | 极值顺序：`open_near_extreme` 开盘价靠近哪个极值先走哪个；`high_first`；`low_first` | `open_near_extreme` | 每次运行时读取 |
| `path.points_per_leg` | `int` | 否 | 每一段（开→极值 1→极值 2→收）至少的价格点数 | `4` | 每次运行时读取 |
| `path.max_step` | `float` | 否 | 自适应加密：每步最大涨跌幅（相对开盘价），0 表示固定点数 | `0.0025` | 每次运行时读取 |
| `path.max_points_per_leg` | `int` | 否 | 每段最多的价格点数 | `80` | 每次运行时读取 |
| `data` | 对象 | 否 | 数据位置 | 见子字段 | 每次运行时读取 |
| `data.dir` | `str` \| `null` | 否 | 历史数据目录；相对路径相对应用目录（exe 所在目录，或源码仓库根目录）；`null` 为 `data/hist` | `null` | 每次运行时读取 |
| `scenarios` | 对象列表 | 否 | D 层合成剧本（`kind=scenario`），结果已知、可精确断言 | `[]` | 每次运行时读取 |
| `scenarios[].name` | `str` | 是 | 剧本名 | — | 每次运行时读取 |
| `scenarios[].description` | `str` | 否 | 说明 | `""` | 每次运行时读取 |
| `scenarios[].trading_day` | `str` | 否 | 剧本所在的交易日 | `20260929` | 每次运行时读取 |
| `scenarios[].start` | `str` | 否 | 第 0 步的北京时间（须在所有合约的连续交易时段内） | `09:35:00` | 每次运行时读取 |
| `scenarios[].reference` | `dict[str, float]` | 否 | 合约 → 参考价（同时作为昨结算） | `{}` | 每次运行时读取 |
| `scenarios[].book` | `dict[str, int]` | 否 | 第 0 步按参考价买入的初始持仓（合约 → 手数） | `{}` | 每次运行时读取 |
| `scenarios[].account` | `str` | 否 | 剧本账户 | `ACC3` | 每次运行时读取 |
| `scenarios[].steps` | 对象列表 | 否 | 市场状态序列 | `[]` | 每次运行时读取 |
| `scenarios[].steps[].label` | `str` | 是 | 步骤标签（断言按它引用） | — | 每次运行时读取 |
| `scenarios[].steps[].at_s` | `float` | 是 | 相对开始的秒数（市场时间） | — | 每次运行时读取 |
| `scenarios[].steps[].move` | `dict[str, float]` | 否 | 合约、品种或板块 → 相对参考价的涨跌幅 | `{}` | 每次运行时读取 |
| `scenarios[].steps[].orders` | `list[dict[str, any]]` | 否 | 该步提交的脚本委托 | `[]` | 每次运行时读取 |
| `scenarios[].expect` | `dict[str, any]` | 否 | 期望结果（由回测和单测逐条检查） | `{}` | 每次运行时读取 |
| `seed` | `int` | 否 | 随机种子（只记入清单，回放本身是确定的） | `20260928` | 每次运行时读取 |
| `workers` | `int` \| `auto` | 否 | 进程池大小；`auto` 为 min(任务数, CPU 核数 − 2, 14)，1 为在本进程中运行，`futrisk backtest run --workers` 可覆盖 | `auto` | 每次运行时读取 |
| `warmup_days` | `int` | 否 | `start` 之前喂给策略的历史交易日数（不交易） | `0` | 每次运行时读取 |
| `base_config` | `str` \| `null` | 否 | 基础配置目录（品种、账户、风控规则、日历）；`null` 为包内的 `config/`，相对路径相对应用目录 | `null` | 每次运行时读取 |
<!-- gen:backtest end -->

### 10.2 读取位置与覆盖

- `futrisk backtest run <名字>` 只在**包内**的 `config/backtest/` 中找 `<名字>.yaml`（源码运行时即仓库的 `config/backtest/`，exe 为 `_internal\config\backtest\`），`<home>/config/backtest/` 中的副本不会被读取。自己写的配置直接传路径：`futrisk backtest run D:\my\bt.yaml`。
- clearing-worker 的回测作业（`POST /api/v1/backtests`）按同样的规则找配置。
- 回测读取的品种、账户、风控规则和交易日历默认取自包内配置（`base_config`），不是运行中系统的 home；配置哈希覆盖这几个文件和回测配置本身，写进 `manifest.json`，保证结果可复现。
- 输出文件（`report.html`、`daily.csv`、`events.csv`、`orders.csv`、`summary.json`、`manifest.json`）的内容见 03 §5.2。
- 命令行用点号覆盖任意字段，值按 YAML 解析：

```powershell
futrisk backtest run default --set start=20200101 --set rules.threshold=0.025
futrisk backtest run default --set "arms=[{name: base, tier: none}, {name: prod, tier: block_open}]"
futrisk backtest run default --set accounts.ACC3.params.fast=10 --set accounts.ACC3.params.slow=40
```

## 11. calendar/trading_days.csv

交易日历：一列 `trading_day`（`YYYYMMDD`），以 `#` 开头的行为注释。夜盘归属、交易时段、EOD 调度、结算价时间窗都依赖它（01 §8.2）。

```text
# China futures trading days (SHFE/DCE/CFFEX). 2025-2026 official; 2027 estimated.
trading_day
20260924
20260928
20260929
```

- 由 `scripts/gen_calendar.py` 按"工作日减去法定节假日"生成；2025、2026 年为官方安排，2027 年为估计值，官方安排公布后要替换并重新生成。
- 热加载效果与 `products.yaml` 相同：md-gateway 否决任何改动，按 §1.3 在 EOD 之后停机离线提交。

## 12. bots.yaml（可选）

strategy-sim 策略机器人的参数。默认值写在代码里，`src/futrisk/sim/bots/bots.yaml` 给出同样的值作为说明和模板。需要覆盖时，把 `bots.yaml` 放进 `<home>/config/`，热加载提交后重启 strategy-sim；也可以用 `FUTRISK_BOTS_CONFIG=<文件>` 指定。合并顺序（后者优先）：代码默认值 → 包内 `bots.yaml` → 已提交版本中的 `bots.yaml` → `FUTRISK_BOTS_CONFIG` → 环境开关（§13）。

<!-- gen:bots begin -->
| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |
|---|---|---|---|---|---|
| `identity` | `str` | 否 | strategy-sim 在总线上的身份（DEALER identity），必须在 `order.*` 的白名单中（02 §4.11） | `strategy-sim` | 重启 strategy-sim |
| `seed` | `int` | 否 | 随机种子；每个账户的随机数发生器为 `Random("{seed}:{account}")` | `20260928` | 重启 strategy-sim |
| `aggressive` | `bool` | 否 | 激进（剧本）模式：各账户叠加 `accounts.{account_id}.aggressive` 中的参数（包内 `bots.yaml` 让 ACC3 一次建满多头持仓，供熔断演示使用）；等同于 `FUTRISK_BOTS_MODE=scenario` | `false` | 重启 strategy-sim |
| `accounts_filter` | `list[str]` \| `null` | 否 | 只运行这些账户；`null` 为全部已配置账户 | `null` | 重启 strategy-sim |
| `order_tag` | `str` | 否 | 写进自己 `client_order_id` 的标记，重启后据此找回前一个进程的委托 | `strategy-sim/bots` | 重启 strategy-sim |
| `status_log_s` | `float` | 否 | 每个账户输出一条状态日志的周期（秒） | `60` | 重启 strategy-sim |
| `resync_interval_s` | `float` | 否 | 空闲时向风控查询持仓和权益的周期（墙钟秒）；0 关闭 | `300` | 重启 strategy-sim |
| `ready_timeout_s` | `float` | 否 | 等待风控分片 RUNNING 的最长秒数，超时后照常开始交易 | `60` | 重启 strategy-sim |
| `startup_cancel_own` | `bool` | 否 | 启动时撤销前一个进程留下的本机器人挂单（不动人工单和减仓单） | `true` | 重启 strategy-sim |
| `startup_sync` | `bool` | 否 | 启动时从风控分片同步持仓、权益和状态 | `true` | 重启 strategy-sim |
| `shutdown_cancel_own` | `bool` | 否 | 正常停止时撤销自己的挂单 | `true` | 重启 strategy-sim |
| `snapshot_resync` | `bool` | 否 | 空闲且与 `risk.snapshot` 不一致时，按快照重新同步持仓 | `true` | 重启 strategy-sim |
| `drift_confirm` | `int` | 否 | 连续多少个不一致的快照才重新同步 | `2` | 重启 strategy-sim |
| `execution` | 对象 | 否 | 下单执行参数（所有账户的默认值，账户级 `execution` 覆盖） | 见子字段 | 重启 strategy-sim |
| `execution.max_orders_per_min` | `float` | 否 | 每分钟最多报单数（令牌桶，市场时间） | `6` | 重启 strategy-sim |
| `execution.burst` | `int` | 否 | 令牌桶容量 | `3` | 重启 strategy-sim |
| `execution.order_ttl_s` | `float` | 否 | 自己的挂单超过该秒数即撤销 | `20` | 重启 strategy-sim |
| `execution.aggression_ticks` | `list[int]` | 否 | 可成交限价单越过对手价的 tick 数（随机取其一） | `[1, 2]` | 重启 strategy-sim |
| `execution.passive_prob` | `float` | 否 | 开仓单改为挂在本方价位的概率 | `0.2` | 重启 strategy-sim |
| `execution.slice_notional` | `float` | 否 | 单笔委托的最大名义金额（元） | `1500000` | 重启 strategy-sim |
| `execution.max_slice_lots` | `int` | 否 | 单笔委托的最大手数（同时受 `max_order_qty` 限制） | `50` | 重启 strategy-sim |
| `execution.deadband_frac` | `float` | 否 | 目标仓位变化小于目标的该比例时不调仓 | `0.1` | 重启 strategy-sim |
| `execution.probe_qty` | `int` | 否 | 开仓限制解除后第一笔试探开仓的手数 | `1` | 重启 strategy-sim |
| `execution.in_doubt_resubmit_s` | `float` | 否 | 结果不明（超时）的委托，隔该秒数用同一个 `client_order_id` 重发 | `2` | 重启 strategy-sim |
| `execution.in_doubt_giveup_s` | `float` | 否 | 超过该秒数放弃（不再计为在途） | `15` | 重启 strategy-sim |
| `execution.cancel_retry_s` | `float` | 否 | 结果不明的撤单隔该秒数重发 | `5` | 重启 strategy-sim |
| `execution.halt_probe_s` | `float` | 否 | 熔断期间每隔该秒数发一笔 1 手的开仓试探（被拒 `REJECT_ACCOUNT_HALTED`）；0 关闭 | `20` | 重启 strategy-sim |
| `gates` | 对象 | 否 | 被拒后的退避（市场时间秒，重复被拒时翻倍直到上限） | 见子字段 | 重启 strategy-sim |
| `gates.halt_backoff_s` | `float` | 否 | 熔断或 kill switch 拒单后的退避 | `30` | 重启 strategy-sim |
| `gates.halt_backoff_max_s` | `float` | 否 | 上述退避的上限 | `120` | 重启 strategy-sim |
| `gates.block_backoff_s` | `float` | 否 | 禁开、禁止亏损加仓、敞口上限、what-if、资金不足等拒单后的退避 | `20` | 重启 strategy-sim |
| `gates.block_backoff_max_s` | `float` | 否 | 上述退避的上限 | `300` | 重启 strategy-sim |
| `gates.transport_backoff_s` | `float` | 否 | TIMEOUT / UNAVAILABLE / BUSY / RISK_BUSY 重试失败后的退避 | `3` | 重启 strategy-sim |
| `gates.transport_backoff_max_s` | `float` | 否 | 上述退避的上限 | `60` | 重启 strategy-sim |
| `gates.size_restore_s` | `float` | 否 | 单笔上限系数限制（ORDER_SIZE_FACTOR）安静该秒数后恢复全量 | `120` | 重启 strategy-sim |
| `accounts` | {account_id: 对象} | 否 | 按账户的覆盖，键为账户 ID | `{}` | 重启 strategy-sim |
| `accounts.{account_id}.enabled` | `bool` | 否 | `false` 时不运行该账户的机器人 | `true` | 重启 strategy-sim |
| `accounts.{account_id}.strategy` | `str` \| `null` | 否 | 策略名；`null` 取 `accounts.yaml` 的 `strategy` | `null` | 重启 strategy-sim |
| `accounts.{account_id}.params` | `dict[str, any]` | 否 | 策略参数（由策略自己的模型校验，如 `instruments`、`bar_s`、`notional_frac`） | `{}` | 重启 strategy-sim |
| `accounts.{account_id}.execution` | `dict[str, any]` | 否 | 覆盖全局 `execution` 的字段 | `{}` | 重启 strategy-sim |
| `accounts.{account_id}.aggressive` | `dict[str, any]` | 否 | 只在激进（剧本）模式下叠加的 `params` / `execution` | `{}` | 重启 strategy-sim |
<!-- gen:bots end -->

## 13. 环境变量

| 变量 | 作用 | 读取方 |
|---|---|---|
| `FUTRISK_HOME` | 运行时 home 目录（优先级低于 `--home`） | 全部命令和服务 |
| `FUTRISK_API_KEY` | CLI 使用的 API key；设置后不再读取 bootstrap key 文件 | CLI（`positions`、`order`、`risk`、`settle` 等） |
| `FUTRISK_HOME_ID` | 覆盖 `<home>/home_id`：多个 home 属于同一套部署时使用（docker compose 中所有容器相同） | 全部服务、CLI |
| `app.yaml` 中 `key_env` 指定的变量 | 具名 API key 的明文（至少 16 个字符） | api-gateway |
| `app.yaml` 中 `webhook_url_env` 指定的变量 | 告警 webhook 地址 | api-gateway |
| `FUTRISK_SUPERVISOR_CLOCK_ONLY` | `1`、`true`、`yes`、`on` 时 supervisor 只维护市场时钟，不管理进程（同 `futrisk up --clock-only`） | supervisor |
| `FUTRISK_GC` | 临时覆盖 `services.yaml` 的 `bus.gc`（测量用）：`off` 关闭自动垃圾回收；`nofreeze` 不在 RUNNING 时冻结；`t0,t1,t2` 设置三代阈值，可以再加 `,nofreeze` | 全部服务进程 |
| `FUTRISK_SIM_CLOCK_WAIT_S` | sim-exchange 启动时等待 supervisor 时钟的秒数，默认 60 | sim-exchange |
| `FUTRISK_CLOCK_VERSION` | supervisor 拉起 sim-exchange 时设置：市场时钟的版本，与命令行参数 `--clock-offset-ns` 一起给出（02 §3.1），不需要手工设置 | sim-exchange |
| `FUTRISK_BOTS_MODE` | `scenario`（或 `aggressive`、`breaker`）时机器人进入激进（剧本）模式，ACC3 一次建满持仓，供熔断演示使用；其他值关闭。`futrisk demo --scenario` 自动设置 | strategy-sim |
| `FUTRISK_BOTS_AGGRESSIVE` | `1`、`true`、`yes`、`y`、`on` 打开激进模式，其他非空值关闭；设置了它时不看 `FUTRISK_BOTS_MODE` | strategy-sim |
| `FUTRISK_BOTS_SEED` | 覆盖机器人随机种子 | strategy-sim |
| `FUTRISK_BOTS_ACCOUNTS` | 只运行这些账户的机器人，如 `ACC3,ACC5` | strategy-sim |
| `FUTRISK_BOTS_CONFIG` | 额外的 `bots.yaml` 文件路径 | strategy-sim |
| `FUTRISK_HTTP_PORT`、`FUTRISK_VERSION` | docker compose：宿主机端口（默认 8000）、镜像版本 | `deploy/compose.yml` |
| `FUTRISK_API`、`FUTRISK_API_KEY_FILE`、`FUTRISK_SEED_TIMEOUT_S` | docker compose 的一次性建账容器 | `deploy/seed_books.py` |
| `FUTRISK_E2E` | `1` 时运行端到端测试 | pytest |

---

## 附录 A 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-25 | 初版：10 类配置文件和 `bots.yaml` 的字段表由 `scripts/gen_config_reference.py` 从 pydantic 模型生成；补充热加载效果（按代码核对：risk-engine 的品种参数需重启、数据源和端点拒绝热改、外部组件需重启）、动作参数、剧本动作与故障类型、回测配置的读取位置和环境变量 |
| v0.2 | 2026-09-26 | 范围精简（完整版保留为 git 标签 `v0.1-full`）：回测配置字段按精简后的模型重新生成（删除路径集合、敏感性、统计、压力窗口、扫描等字段）；`accounts.yaml` 的 `strategy` 说明改为回测不用它；§10.2 的示例改为覆盖双均线参数 |
| v1.0 | 2026-10-08 | 按代码重建（md-gateway 重写之后）：<br>1）热加载列按各服务的 prepare/commit 代码重新核对：md-gateway 在 prepare 阶段否决 `products.yaml`、交易日历和数据源各段的改动，这些字段改为"拒绝热改"，§1.3 改写为否决规则表、离线提交（`futrisk config commit`）流程和产品参数的生效时机；`notify.min_severity` 不需要环境变量；`sim.yaml` 的 `feed_b`、`control` 还要重启 supervisor、api-gateway；剧本改为"重启 sim-exchange"<br>2）`initial_positions` 由"不读取"改为首日建账时读取（成为昨仓 lot）；`initial_capital` 写明只在建账时使用；`products` 写明不是风控限制<br>3）按代码改正说明：`bus.replay_window_days` 只有 trade-gateway 和 supervisor 读取；`pretrade.funds_buffer`、胖手指检查、`route_ttl_ms`、`feed_a.priority`（只用于规范 tick 去重）、`feed_b.timeout_ms`（读超时）、仲裁参数、`demo.start_at`、`api_keys`、`coordinator`、`scope`（增加 `account/instrument`）、CIRCUIT_BREAK 级别的默认确认、`latch` 的限制、手续费和保证金的匹配规则<br>4）写明校验的实际范围：`config validate` 和离线提交只做模型校验，不检查 `metric`、`scope`、规则编译和 `levels[].params`（§1.4、§1.5、§6.3，缺陷见 08 §5.2）<br>5）本文 §13 增加 `FUTRISK_GC`，改正布尔类环境变量的取值；各示例用 pydantic 模型加载核对通过；`scripts/gen_config_reference.py --check` 通过；同日的独立交叉审查又改正 11 处：改动作会解除锁存的例外（08 §5.2 KD-19）、读取工作目录的命令、离线提交的运行检查、夜盘集合竞价时刻、示例说明、本文 §13 补上 `FUTRISK_CLOCK_VERSION` 等 |
