# futrisk 仪表盘（web/）

futrisk 的浏览器仪表盘：Vue 3 + Vite + TypeScript + Element Plus + vue-echarts（ECharts 5）+ Pinia + Vue Router。
五个页面（总览、账户、多账户对比、行情与系统、报表）对应 01 §5.3.5；接口按已实现的 api-gateway
（`src/futrisk/services/api_gateway/`：`routes/*.py`、`readmodel.py`、`views.py`、`wshub.py`）对齐。
构建产物输出到 `src/futrisk/web_static/dist`，由 api-gateway 作为静态文件托管（`/`）。

## 快速开始

需要 Node.js ≥ 20.19（npm 10）。

```bash
cd web
npm ci            # 按 package-lock.json 安装依赖
npm run dev       # 模拟模式：http://127.0.0.1:5173 ，不需要任何 Python 服务
```

`npm run dev` 读取 `.env.development`（`VITE_MOCK=1`），数据来自浏览器内置的模拟后端（`src/mock/`），
它的 REST 信封、WS 帧和每个接口的数据形状都与 api-gateway 一致（见「模拟模式」），所以页面、store、
WS 客户端走的都是真实代码路径。

| 命令 | 作用 |
|---|---|
| `npm run dev` | 开发服务器，模拟模式 |
| `npm run dev:live` | 开发服务器，连接 api-gateway：`/api`、`/ws` 代理到 `FUTRISK_BACKEND`（默认 `http://127.0.0.1:8000`） |
| `npm run build` | `vue-tsc --noEmit` 类型检查 + `vite build` → `../src/futrisk/web_static/dist` |
| `npm run build:mock` | 同上，但产物默认进入模拟模式（离线演示、截图用） |
| `npm run preview` | 预览构建产物（同样代理 `/api`、`/ws`） |
| `npm run test` | vitest 单元测试 |
| `npm run typecheck` | 只做类型检查 |

连接真实后端：先 `futrisk demo`（或 `futrisk up`）拉起服务，再 `npm run dev:live`，或者直接打开
api-gateway 托管的 `http://127.0.0.1:8000`。首次打开会弹出设置对话框，填入 API key
（首次启动时 api-gateway 生成的 bootstrap ADMIN key 在 `<home>/secrets/bootstrap_admin.key`）。

## 设置与安全

右上角齿轮打开设置：

- **API key**：REST 请求头 `X-API-Key`；WS 连接后第一帧 `{"op":"auth","key":…}`（key 不进 URL / 访问日志）。
  默认只存 **sessionStorage**（`futrisk.apiKey.v1`，关闭标签页即清除）；勾选「在本机记住此 key」才写入
  localStorage（`futrisk.settings.v1`）。旧版本留在 localStorage 里的 key 会在加载时迁移到 sessionStorage。
  key 无效（REST 401 / WS 关闭码 4401）时页头显示「key 无效」并打开设置，WS 不再重试直到换 key。
  鉴权失败过多被网关限流时（REST 429 + Retry-After；WS 先收到 `BUSY` 错误帧再以 4401 关闭），按提示的等待时间后再连。
- **角色**：启动时 `GET /auth/me` 取 `{key_id, name, role}`，页头显示；按 02 §9.6 权限矩阵隐藏 / 禁用按钮
  （重置、人工熔断、kill switch、告警确认、减仓计划需要 RISK_MANAGER 或 ADMIN；演示控制、发起结算、
  提交回测、重启服务需要 ADMIN）。角色未知时不隐藏，由网关判定。
- **报表内容**：`GET /reports/{report_id}` 的 HTML（风控日报、回测报告）只在 `<iframe sandbox srcdoc>` 中显示
  （不带任何 sandbox 许可：不执行脚本、独立不透明来源、不能导航顶层），文本 / CSV / JSON 作为转义文本显示；
  「下载」以 `application/octet-stream` 附件保存，不在本源渲染。页面从不打开报表的同源 blob / 窗口。
- **模拟数据**：默认取 `VITE_MOCK`，可在此切换（页面刷新）。其余：涨跌配色、刷新节流、告警通知。
- **WebSocket 诊断**：状态、收到帧、seq 缺口 / 重复、resync 请求、服务端 resync、重连次数；模拟模式下还有
  「模拟 WS 跳号」「模拟服务端 resync」「模拟 WS 断线」「重新开始演示」和「模拟 app.mode（demo / live）」。

## 与 api-gateway 的约定

- REST 基础路径 `/api/v1`、WS 地址 `/ws`，同源。统一信封 `{ok, code, message, data}`，每个 `/api` 答复都带
  `X-Result-Code` 和 `X-Content-Type-Options: nosniff`；幂等重放带 `"duplicate": true`。`ok=false`（包括 HTTP 200
  的业务拒单 `REJECT_*` 和扇出命令的部分失败）抛出带错误码、`data` 和 `duplicate` 的 `ApiError`；
  `getFull / postFull / delFull` 还返回 `message`（部分结果说明，如 `partial: risk-engine-0=UNAVAILABLE`、
  `0 latched level(s) reset`）和 `duplicate`。
- 错误码 → HTTP：BAD_REQUEST 400、UNAUTHORIZED 401、FORBIDDEN 403、NOT_FOUND 404、CONFLICT / NOT_OWNER /
  PRECONDITION_FAILED 409、BUSY / RISK_BUSY 429、INTERNAL 500、VERSION_UNSUPPORTED 502、UNAVAILABLE 503、TIMEOUT 504；
  429 与 503 带 `Retry-After`（提示「N s 后可重试」）。未知 `/api` 路径是 404 信封（`unknown API path …`），
  已知路径换了方法是 405、请求体超过 1 MiB 是 413（两者的码都是 BAD_REQUEST）；意外错误只返回通用信息和
  `data.error_id`，界面显示「错误编号」以便对照网关日志。
- 扇出命令（kill switch、EOD force-unfreeze）**总是 HTTP 200**：全部目标确认才 `ok=true`，否则 `ok=false`、
  `code` 为最严重的目标码、`data.targets` 为各目标结果（kill switch 另有 `control`、`retrying`、`failed`）。
- 所有 POST / DELETE 都带 `Idempotency-Key`（UUID）。网关按「方法 + 路径 + 调用者 + 请求体」指纹去重：
  同 key 同请求 → 返回首次结果并标 `duplicate`；同 key 不同请求 → 409 CONFLICT；TIMEOUT / UNAVAILABLE /
  BUSY / RISK_BUSY / INTERNAL 一般不记录（重试会再执行），但 kill switch 和全部 `/demo/*` 变更**记录每个结果**
  （同 key 重试只会拿到记录的答复；要再执行就换新 key）。人工操作对话框只在「重试且原因未改」时复用 key，
  改了原因就换新 key；业务错误后也换新 key。演示控制每次点击用新 key。界面对重放结果注明「幂等重放」。
- 路由用 hash 模式、`base: './'`：产物可放在任意路径下，api-gateway 不需要 SPA 回退。
- 网关按 msgspec `omit_defaults` 序列化：**值等于默认值的字段不出现**（如 `FirmSnapshot.kill_switch=false`、
  `RiskEvent.severity="INFO"`、`OrderUpdate.filled_qty=0`、空列表 / 字典）。TS 类型把这些字段都声明为可选，
  代码按缺省值处理。

### WebSocket 客户端（`src/api/ws.ts`）

- 帧：主题帧 `{v, type, topic, seq, ts, mts, td, data}`，会话帧（heartbeat / pong / error）`{v, type, ts, mts, data}`，
  没有 topic / seq / td。`ts` 为网关墙钟毫秒（只用于新鲜度），`mts` 为市场时钟毫秒（演示时钟可偏离墙钟）：
  页头的市场时间优先用 `mts`，否则用 `system` 主题的时钟项外推。心跳每 5s，`data: null`。
- `error` 帧 `{code, message, topics?}`：每个出错的操作一帧，`sub` 里的未知主题列在 `topics` 中；连接状态里显示。
- 订阅按主题引用计数；网关对每个新 `sub` 回一个 `snapshot`，所以订阅和重连都**不再额外发 `resync`**。
  每个 `sub / unsub / resync` 操作最多 100 个主题（超出自动拆分），消息不超过 64 KB。
- 每个主题独立跟踪 `seq`：`seq == last+1` 应用；`seq ≤ last` 视为重复丢弃；`seq > last+1` 为缺口——先应用，
  再对该主题发 `resync`（等待快照期间不重复请求，5s 未到再请求），`snapshot` 帧重置基线。
- 服务端 `resync` 帧（慢客户端溢出，事件类主题）：重置基线、请求一次快照（去重，网关在快照排队时也会忽略重复的
  resync），并用 REST 重新拉取该主题的历史。
- 断线：指数退避重连（0.5s → 1s → 2s … 上限 15s，±20% 抖动；收到第一帧后退避归零），重连后重新认证、全部重新
  `sub`；事件类主题（`events`、`eod`、`orders:*`、`equity:*`）再用 REST 补齐。被网关以 1008（超限）/ 1009（消息过大）/
  1013（客户端过慢）关闭时，退避按连续次数递增，短命连接的第一帧不会把退避清零（连接稳定 30s 后才清零）。
  4401（key 无效）不重连；4401 之前收到 `BUSY` 错误帧（鉴权限流，`retry in Ns`）时等待该时长后重连。
- 空闲看门狗：16s 内没有任何帧即主动重连；每 10s 发 `ping`。
- store 更新节流：帧先进入 `Batcher`，在一个节流窗口内按「主题 + 键」合并，事件帧和权益点从不合并，然后批量写入 Pinia。

## 页面与接口对应

| 页面 | REST | WS 主题 |
|---|---|---|
| 全局（页头） | `GET /auth/me`、`GET /accounts`（全部已配置账户）、`GET /risk/events`、`GET /risk/rules`、`GET /system/status` | `firm`、`accounts`、`events`、`system`、`eod`（EOD 警告、kill switch 目标失败、配置未收敛弹出通知） |
| 总览 | `GET /accounts`（10s）、`GET /system/status`（15s；kill switch 有目标待确认时 3s）、`POST /risk/firm/kill-switch`、`POST /risk/events/{alert_key}/ack` | `firm`（快照带 `control`）、`accounts`、`events`、`system`（`kill_switch` 进度事件） |
| 账户 | `GET /accounts/{id}`（2s）、`GET /accounts/{id}/orders`、`/trades`、`/equity-curve?step_s=5`、`GET /risk/events?account=`、`GET /risk/liquidation-plans?account=`、`POST /risk/accounts/{id}/reset`、`POST /risk/accounts/{id}/halt`、`POST /risk/liquidation-plans/{plan_id}/confirm` | `positions:{id}`、`orders:{id}`、`equity:{id}`、`events` |
| 多账户对比 | `GET /risk/compare`、`GET /risk/rules`、`GET /accounts`、`GET /accounts/{id}/equity-curve`、`GET /positions?group_by=&account=&product=&exchange=&sector=`、`GET /risk/compare/history?days=20/40/60`、`GET /risk/events?td=`（某天缺熔断次数时） | `compare`、`equity:{id}` |
| 行情与系统 | `GET /market/quotes`、`GET /market/feeds`（行情网关离线时 10s 轮询）、`GET /market/events`、`GET /system/services`（10s）、`GET /system/status`、`POST /services/{instance}/restart`、`GET /demo/state`（2~5s）、`GET /demo/faults`、`POST/DELETE /demo/faults`、`POST /demo/shock`、`GET /demo/scenarios`、`POST/DELETE /demo/scenario`、`POST /demo/kill/{instance}`、`POST /demo/clock` | `quotes`、`feeds`、`system` |
| 报表 | `GET /settlement/days?limit=60`、`GET /settlement/{td}/status`（列表外的日子；展开有警告的日子）、`GET /settlement/{td}`、`GET /settlement/{td}/statements/{account}`、`GET /reports`、`GET /reports/{report_id}`、`POST /settlement/run`、`POST /risk/eod/{td}/force-unfreeze`（分片 PROVISIONAL 时）、`GET/POST /backtests` | `eod`、`backtests` |

### 各接口的实际形状（`src/types/api.ts`）

| 接口 | 形状 | 界面处理 |
|---|---|---|
| `GET /accounts` | **全部已配置账户**：有快照的为 AccountSnapshot（有效状态已合并）+ `name`、`stale`、`age_ms`、`missing: false`；没有快照的为 `{account_id, name, shard, missing: true, stale, age_ms}`（已知 `risk.state` 时另有 `state`、`halted_latched`、`state_version`） | 已配置账户、名称从这里得知；无快照账户在总览 / 对比页列为「尚无快照」，不算陈旧数据 |
| `GET /accounts/{id}` | AccountSnapshot + `name`、`stale`、`age_ms` | 分片离线时显示「分片离线 · 数据陈旧」和说明 |
| WS `accounts` | `readmodel.account_summary` + `name` + `stale` / `age_ms`；只有 `risk.state` 没有快照时是精简项 | 精简项的数值为 null，与已知值合并 |
| `GET /risk/state[?account=]` | 每账户有效状态与限制 + `stale`、`age_ms` | — |
| `GET /risk/events` | `{trading_day, items, errors}`；`errors` 为未应答分片，`message` 为 `partial: …` | 事件流上方提示「部分结果」；按账户加载的错误不影响全局 |
| `GET /risk/liquidation-plans` | `{trading_day, items, errors}`；未应答分片的计划来自读模型并标 `stale`；LIQUIDATION_PLAN 事件：生成时 `detail {plan_id, plan}`，确认 / 取消 / 过期时 `detail {plan_id, status, actor}` | 账户页加载并轮询；陈旧计划带标记；已决定的计划不会被旧事件改回待确认 |
| `GET /risk/rules` | RuleSet；分片都不可达时回退本地配置并附说明 | 对比页提示 |
| `GET /risk/compare`、WS `compare` | 对比视图 + `liveness`（每账户 `{stale, age_ms}`）+ `accounts_stale` | 陈旧账户在对比页提示 |
| `GET /risk/compare/history` | `{days, missing, errors}`，`days` 只接受 1~60；某天有 `breakers` 仅当所有分片都应答；全部失败时 503 + `data.errors` | 熔断次数按天合并，缺失日用 `/risk/events?td=` 补数，仍不完整时标 `*` |
| `GET /positions` | `{group_by, items, groups, accounts_missing, accounts_stale}` | 对比页「持仓汇总」 |
| `GET /market/feeds` | md.status（读模型，否则 md.sources）+ md-gateway 的 `stale`、`age_ms` | 行情网关离线时说明数据源状态为最后一次 md.status；质量列显示「未知」 |
| `GET /system/services` | `{items, events, supervisor}`；items 为 ServiceInfo（supervisor 可达时）+ 心跳 + `liveness` + `since_ns`（进程状态自何时起）+ `heartbeat_ns`（网关最后收到心跳）+ `last_error` | 「已退出 / 已停止 / 失败」若早于最后心跳且在线，按「运行（按心跳）」显示；悬停看时间与最近错误 |
| `GET /system/status` | 读模型摘要：`mode`、`kill_switch`（null = 未知）、`firm_control {kill_switch, manual, rule}`、`kill_switch_retry {pending, failed, desired, updated_ns, retrying}`、`kill_switch_control`（期望状态 + 各目标 PENDING / CONFIRMED / FAILED、次数）、`auth`（未生效的命名 ADMIN key、缺环境变量、被拒、引导 key、限流次数）、`notifications`（按状态计数）、`readmodel.accounts_stale` / `repair`、`ws` | 总览显示人工 / 规则两层和各目标确认进度；服务健康区显示 key 问题、通知失败 / 待发 / 抑制 |
| WS `system` | 快照：`kind: "service"` 服务项 + `kind: "clock"` 时钟项（instance `sys.clock`）；事件：`service`（进程迁移）、`kill_switch`（`event` command / retry / reconcile + 整行状态）、`config`（如 `CONFIG_FAILED` 的 `config_version`、`lagging`） | kill switch 进度实时更新；目标失败、配置未收敛弹出通知并在服务健康区列出 |
| WS `firm` | 快照为 FirmSnapshot + `control`（两层）；事件为 FirmControl（MANUAL 开 / 关、RULE_STATE 整个规则层） | 生效 kill switch = 人工 OR 规则 |
| `POST /risk/firm/kill-switch` | HTTP 200；`{targets, all_ok, control, retrying, failed}`；部分失败 `ok=false` + 最严重目标码；每个结果都按 key 记录 | 提示已确认数、后台重试与已失败目标；进度看 `kill_switch_control` |
| `POST /risk/eod/{td}/force-unfreeze` | HTTP 200；`{targets, all_ok}`；非 PROVISIONAL 分片答 PRECONDITION_FAILED | 报表页在有分片 PROVISIONAL 时提供（RISK_MANAGER） |
| `POST /risk/accounts/{id}/reset` | ResetAck，`message` 为 `N latched level(s) reset`（N=0 时也是 OK） | N=0 时提示「无需重置」 |
| `POST /risk/liquidation-plans/{id}/confirm` | 更新后的 LiquidationPlan；已非待确认时 409 CONFLICT，`data` 为当前计划 | 更新本地状态并提示「已被处理」 |
| `GET /settlement/days` | `{items: [{trading_day, version, phase, warning_count}], errors}` | 「警告」列显示条数 |
| `GET /settlement/{td}/status` | EodPhaseEvent；`detail.warning_count`、`detail.warnings`（最近 10 条 `{kind, message}`；清算离线时为读模型记录） | 展开的日子列出警告 |
| WS `eod` | EodPhaseEvent（阶段变化；WARN 事件重复当前阶段并带 `detail.warning` / `kind` / `warn_id`，PRE_SETTLE_MISMATCH 在 PRICES_FIXED 的 `detail.warnings`）、SettlementCompleted、ReportReady、DayRoll | 警告不改变阶段；新警告弹出通知（快照重放不弹） |
| WS `backtests` | 状态主题：快照为全部回测（新的在前），之后每个作业 upsert | 回测列表与进度实时更新，不再轮询 |
| `GET /settlement/{td}/statements/{account}` | `{format: "text", text}`，文件不存在时 `{format: "settlement", settlement}` + 说明 | 显示文本或结算数据摘要 |
| `GET /reports[?td=][&kind=]` | `{items}`（ReportReady）；`GET /reports/{id}` 返回文件本身（CSP sandbox + nosniff） | 沙箱 iframe 显示 |
| `GET /demo/state`、`GET /demo/faults`、`GET /demo/scenarios` | 虚拟交易所状态（`faults`、`fault_history`、`scenario` 等）、`{items, history}`、`[{name, description}]`；虚拟交易所不可达时 503 | 演示控制列出当前故障（可撤销）、最近结束的故障、剧本列表与说明、当前剧本进度 |
| `POST /demo/scenario`、`DELETE /demo/scenario` | `{run_id, name}`（已有剧本运行时 409）；停止时返回该次运行（`status` CANCELED 等），从未运行过时 404 | 「执行」「停止剧本」 |
| `POST /demo/clock` | ClockJumpResult `{clock, steps, waited_ms, converged, lagging}`；失败也带 steps | 显示五个步骤与未确认实例；指定时刻按北京时间输入，发送 `at`（+08:00）与 `at_ns` |
| `/demo/*` | 非 demo 模式 403 FORBIDDEN | 演示控制停用并说明 |

## 模拟模式

`src/mock/` 在浏览器里模拟整个后端（REST 路由 + WS Hub），答复形状与 api-gateway 相同：

- **同一套类型**：引擎的每个接口都返回 `types/api.ts` 的类型（`RiskEventsResponse`、`SystemServicesResponse`、
  `KillSwitchResult`、`PositionsResponse` …），并经 `wire()` 按 msgspec 默认值省略字段——`wire.gen.ts`
  由 `futrisk.messages` 的 msgspec 类型信息生成（每个结构的默认值与嵌套关系）。风控规则 `rules.gen.ts`
  由 `config/risk_rules.yaml` 经 `RiskRulesConfig.model_dump(mode="json")` 生成（与 `GET /risk/rules` 相同）。
- **网关行为**：信封 + `X-Result-Code` + nosniff；错误码与 HTTP 状态、429 / 503 的 Retry-After、404 / 405 / 413、
  INTERNAL 的 `error_id`；幂等（UUID 校验、重放 `duplicate`、409、可重试结果不记录，kill switch 与 `/demo/*`
  记录全部结果）；`/demo/*` 在 app.mode 不是 demo 时 403；WS 帧 `ts` / `mts`、每 5s 心跳、服务端 `resync`、
  每个 `sub` 一帧错误并列出未知主题、单次操作超过 100 个主题回错误帧、消息超过 64 KiB 关闭 1009。
- **网关自身状态**：kill switch 管理器（唯一期望状态、各目标 PENDING / CONFIRMED / FAILED、每 2s 重试、30 次后 FAILED，
  进度推 `system` 事件）；公司层人工 / 规则两层；EOD 警告（历史中一天有 OFFICIAL_PRICE_DIFF；当日结算在
  PRICES_FIXED 带 PRE_SETTLE_MISMATCH，完成后约 2.5s 推一条 LATE_FACT）；虚拟交易所的故障列表与历史、剧本运行
  （一次一个，可停止）；`backtests` 主题。模拟后端不模拟 PROVISIONAL，force-unfreeze 在这里总是 PRECONDITION_FAILED。
- **服务依赖**：kill 某个风控分片后，该分片的账户变成陈旧（`stale`）、账户查询和命令 503
  `risk-engine-N: peer is down (no heartbeat)`、`/risk/events` 与 `/risk/liquidation-plans` 列出未应答分片、
  kill switch 部分失败并由重试补发；kill md-gateway / clearing-worker / sim-exchange 后对应接口 503（带 Retry-After）；
  分片约 12s 后由 supervisor 拉起。
- **账户与行情**：ACC1~ACC5（ACC3 按 accounts.yaml 只做 rb / hc / i），11 个合约；Feed A / Feed B 仲裁、缺口与补数、质量。
- **熔断演示（ACC3，市场时间从 2026-09-28 10:30:05 开始）**：开始时已有板块敞口告警（黑色约 1.1 倍权益）；约 80s
  当日亏损越过 −2%（告警），约 97s 越过 −2.5%（限制：单笔上限 ×0.5、禁止亏损加仓），约 121s 越过 −3%
  （先 HALTED 待确认并撤销开仓挂单，2 个 LIVE 状态 / 1s 后锁存）；此后开仓单 `REJECT_ACCOUNT_HALTED`、平仓照常。
  第二段下跌带出回撤类告警；再执行演示控制里的 `breaker` 剧本会越过 −5% 生成减仓计划。
- 刷新页面即从头重放；设置里的「重新开始演示」同理。

## 目录

```
src/
  types/        msgspec 结构的 TS 镜像（字段名一致）+ REST/WS 协议与视图类型（api.ts）
  api/          http.ts（REST 客户端）、ws.ts（WebSocket 客户端）、endpoints.ts
  realtime/     batcher.ts（节流合并）、dispatcher.ts（帧 → store、resync → REST）
  stores/       Pinia：settings、auth、connection、accounts、events、market、system、firm、compare、eod
  utils/        format、risk、compare、stats、normalize（守卫与归一化）、labels
  components/   SourceNotice（部分结果 / 不可用说明）、StaleBadge、ReportViewer（沙箱）、PositionsExplorer、图表等
  views/        五个页面
  mock/         模拟后端（engine、hub、rest、wire + wire.gen、rules.gen、history、config）
tests/          vitest：WS 协议、REST 客户端、归一化、store 规则、契约（fixtures/gateway 中真实网关的答复）、模拟后端行为、设置存储等
```

`tests/fixtures/gateway/` 是抓取的真实答复（`<path>.json` + 响应头 `<path>.h`）：无前缀的是只启动 api-gateway
（分片、行情、清算、supervisor 都未运行）时的答复；`live1_*` 是落地后的网关在 risk-engine-1 运行（ACC3 / ACC4）、
其余服务离线、做过一次 kill switch 演练后的答复（账户列表含无快照账户、kill switch 部分失败 HTTP 200、
`kill_switch_control`、减仓计划部分结果、503 + Retry-After 等）。`contract.spec.ts` 用它们检查客户端、归一化和
store，并检查模拟后端的答复包含真实答复的全部字段。消息结构或风控规则变化后，在仓库根目录运行
`.venv/Scripts/python web/scripts/gen_mock_schema.py` 重新生成 `wire.gen.ts` 与 `rules.gen.ts`。

## 已知限制与后端缺口

- 下单、撤单、出入金、结算价覆盖、对账异常处理、配置热加载不在本期仪表盘范围内。
- 纳秒时间戳超过 2^53，在 JS 中损失亚微秒精度，只用于显示。
- WS `system` 服务项不带 `since_ns` / `heartbeat_ns`：「进程状态是否早于心跳」按最近一次 `GET /system/services`
  （10s 轮询）判断，其间的 `sys.service` 事件会更新 `since_ns`。
- `live1_system_status` 抓取时还没有 `firm_control` 字段，界面把它当作可选（没有时人工 / 规则两层不显示）。
- `GET /settlement/{td}/status` 只带最近 10 条警告，更多时界面注明「另有 N 条未显示」。
- 报表类型筛选在本地进行（`/reports?kind=` 在网关端同样是取全量后过滤）。

此前列出的后端缺口都已由网关补上：虚拟交易所状态 / 故障列表 / 剧本列表 / 停止剧本（`/demo/state`、`/demo/faults`、
`/demo/scenarios`、`DELETE /demo/scenario`）、`GET /risk/liquidation-plans` 与计划决定事件、`backtests` 主题、
`GET /accounts` 列出全部已配置账户、服务的 `since_ns` / `heartbeat_ns`、行情网关的 `stale` / `age_ms`、
kill switch 进度字段（`kill_switch_retry` / `kill_switch_control`）。
