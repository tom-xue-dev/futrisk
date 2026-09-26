# futrisk：多品种期货交易风控与清算系统

futrisk 为多个 CTA 策略账户提供统一的盘中风控和盘后清算，覆盖上期所、大商所、中金所的 12 个品种。它接入两路异构的模拟行情（外加可选的新浪真实行情），行情中断后按序号补齐且不产生重复或错误的风险信号；持仓只由成交推导，每个账户独立核算，可按账户、品种等维度查询；盘中实时计算资金和持仓两个维度的风险指标，越线时告警、限制下单或熔断——某账户当日亏损达到初始资金的 3% 时立即禁止开仓，确认后锁存；每日收盘后逐日盯市，完成盈亏结算和保证金核算，生成 HTML 风控日报和 CTP 风格结算单。另外提供历史回测、Vue 3 实时仪表盘、配置热加载和多账户对比视图。系统由 6 个服务进程组成，经 ZeroMQ 总线通信，自带虚拟交易所和策略机器人，一条命令即可在一台 Windows 机器上拉起全部 8 个进程。

```mermaid
flowchart LR
    subgraph EXT["外部（模拟）"]
        SIM["sim-exchange 虚拟交易所<br/>Feed A 推送 · Feed B 轮询 · 模拟柜台"]
        BOT["strategy-sim<br/>5 个账户的策略机器人"]
        SINA["新浪行情（可选）"]
    end
    subgraph BUS["futrisk 服务（ZeroMQ：PUB/SUB 事件 + DEALER/ROUTER 命令）"]
        MD["md-gateway<br/>多源仲裁、补数、录制"]
        RE["risk-engine-0 / -1<br/>按账户分片的风控内核"]
        TG["trade-gateway<br/>路由与回报"]
        CW["clearing-worker<br/>EOD 结算、报表、回测"]
        API["api-gateway<br/>REST、WebSocket、仪表盘"]
        SUP["supervisor<br/>进程守护、市场时钟"]
    end
    UI(["浏览器仪表盘 / futrisk CLI"])
    SIM -->|"行情"| MD
    SINA -.-> MD
    MD -->|"md.tick.*（只发 LIVE）"| RE
    BOT -->|"order.submit"| RE
    RE -->|"route.order"| TG
    TG <-->|"报单 / 回报"| SIM
    TG -->|"td.exec.*"| RE
    TG -->|"td.exec.*"| CW
    CW -->|"eod.freeze / eod.rollover"| RE
    API --> RE
    UI <--> API
    SUP -.->|"拉起、监控心跳"| BUS
```
*这张图回答：系统由哪些进程组成、它们之间的主要消息方向；所有委托都必须经过 risk-engine。详见 01 §5（[架构说明](docs/01-architecture.md)）。*

## 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Windows 10 / 11 x64；docker 方式也可以在 Linux 上运行 |
| Python | 3.12（仅源码方式需要；exe 已内置运行时） |
| Poetry | 2.x（仅源码方式） |
| Node.js | 20.19 以上，**只在重新构建仪表盘时需要**；构建好的仪表盘已包含在源码和 exe 中 |
| Docker | Docker Desktop 或 Docker Engine + compose v2（仅 docker 方式） |
| 端口 | `127.0.0.1` 上的 7001–7004、7101–7602 和 8000（07 §2.4，[使用说明](docs/07-user-guide.md)） |

## 快速开始

### 方式一：exe（不需要安装 Python）

```powershell
Get-FileHash dist\futrisk-win64-0.1.0.zip -Algorithm SHA256   # 与 dist\futrisk-win64-0.1.0.zip.sha256 中的值一致
Expand-Archive dist\futrisk-win64-0.1.0.zip -DestinationPath D:\futrisk-demo
D:\futrisk-demo\futrisk\futrisk.exe demo
```

### 方式二：源码

```powershell
poetry install
.venv\Scripts\python -m futrisk.main demo
```

### 方式三：docker

```powershell
docker compose -f deploy/compose.yml up -d --build
docker compose -f deploy/compose.yml exec api-gateway cat /var/lib/futrisk/secrets/bootstrap_admin.key
```

`futrisk demo` 拉起全部 8 个进程，首次运行时为 5 个演示账户建账，并在浏览器中打开仪表盘 **http://127.0.0.1:8000**。首次打开仪表盘时，在弹出的设置对话框中填入 API key：自动生成的 ADMIN key 在 `<home>\secrets\bootstrap_admin.key`（exe 为 `futrisk\var\`，源码为仓库下的 `var\`）。按 Ctrl-C 停止全部服务；`futrisk demo --headless` 则让系统留在后台运行，用 `futrisk down` 停止。常用命令：

```powershell
futrisk status                  # 健康摘要
futrisk ps                      # 进程列表
futrisk accounts                # 账户总览
futrisk positions -g product    # 按品种汇总持仓
futrisk logs risk-engine-1 -f   # 查看日志
futrisk doctor                  # 环境检查（端口、时区、SQLite 等）
```

下文命令写成 `futrisk ...`：exe 方式用 `futrisk.exe` 的完整路径，源码方式用 `.venv\Scripts\python -m futrisk.main`。完整说明见 [07 使用说明](docs/07-user-guide.md)。

## 演示流程

| 演示内容 | 命令 | 看什么 |
|---|---|---|
| 熔断验收场景 | `futrisk demo --scenario breaker` | 约 2 分钟内 ACC3 依次触发 −2% 告警、−2.5% 限制下单、−3% 熔断，每级只告警一次；`futrisk risk events -a ACC3` |
| 熔断后禁开、可平 | `futrisk order place -a ACC3 -i rb2701 --side buy --offset open -q 1 --type market` | 开仓被拒 `REJECT_ACCOUNT_HALTED`；同样的平仓单（`--side sell --offset close-today`）照常成交 |
| 风控进程崩溃 | `futrisk kill risk-engine-1` | 期间下单失败（失效即安全）；约 3s 自动重启，熔断仍然锁存，告警不重复 |
| 行情源中断 | `futrisk scenario run feed_outage` | Feed A 断开 20s，Feed B 无缝接续；恢复后按序号补齐缺口（重复 0 条），补数不进风控 |
| 盘后清算 | `futrisk clock jump --to 15:29` | 市场时钟跳到收盘后，15:30 自动 EOD；`futrisk settle show`、`futrisk report daily --open` |
| 回测 | `futrisk backtest run breaker_synthetic` | 同一个风控内核回放合成剧本，16 项断言全部通过；报告在 `<home>\backtests\` |

约 8 分钟的完整现场演示脚本（含备用方案）见 07 §6（[使用说明](docs/07-user-guide.md)）；仪表盘各页的截图放在 `docs/screenshots/`，逐页说明见 07 §3。

## 测试

| 类型 | 命令 | 说明 |
|---|---|---|
| 单元测试（快速） | `.venv\Scripts\python -m pytest tests/unit -m "not slow"` | 1674 项，数分钟 |
| 单元测试（全部） | `.venv\Scripts\python -m pytest tests/unit\<目录> --timeout 300`，按目录分别运行 | 另含 22 项多进程、网络回环等慢测试；按目录运行的原因见 06 §8 |
| 清算黄金用例 | `.venv\Scripts\python -m pytest tests/golden` | 多日手算案例逐分比对（`tests/golden/CASES.md`） |
| 性质测试 | `.venv\Scripts\python -m pytest tests/property` | hypothesis：资金恒等式、逐日盯市 = 逐笔对冲等 |
| 端到端验收 | `.venv\Scripts\python -m pytest -m e2e tests/e2e -v` | 真实启动 8 个进程，自动走完熔断、kill、行情中断、EOD，约 6 分钟 |
| 代码规范 | `.venv\Scripts\ruff check src tests scripts`；`.venv\Scripts\mypy src` | — |
| 仪表盘 | 在 `web\` 下 `npm ci`、`npm run test` | vitest |
| 文档 | `.venv\Scripts\python scripts\check_docs.py`；`.venv\Scripts\python scripts\gen_config_reference.py --check` | 文档交叉校验；配置参考是否与代码一致 |

## 构建 exe

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1
```

依次构建仪表盘（需要 Node 20；`-SkipWeb` 跳过）、用 PyInstaller 打包为 `dist\futrisk\`、做冒烟测试（包括在临时 home 中拉起整套系统和中文路径测试），全部通过后生成 `dist\futrisk-win64-0.1.0.zip` 和 `.sha256`。参数和产物说明见 08 §2（[部署运维](docs/08-deployment.md)）。

## 文档

| 文档 | 内容 |
|---|---|
| [docs/README.md](docs/README.md) | 文档规范与索引 |
| [01-architecture.md](docs/01-architecture.md) | 整体架构：服务拆分、运行时视图、部署、横切概念、架构决策、质量场景、技术债 |
| [02-communication-protocol.md](docs/02-communication-protocol.md) | 通信协议：总线、主题、命令、消息、外部接口、REST、错误码 |
| [03-database-schema.md](docs/03-database-schema.md) | 数据库设计与字段说明 |
| [04-risk-rules.md](docs/04-risk-rules.md) | 风控指标、阈值与理由 |
| [05-clearing.md](docs/05-clearing.md) | 清算规则、公式出处与手算示例 |
| [06-test-plan.md](docs/06-test-plan.md) | 测试方案与测试报告：需求追踪、故障注入矩阵、端到端验收、执行结果与覆盖率 |
| [07-user-guide.md](docs/07-user-guide.md) | 使用说明：安装、仪表盘、CLI、日常操作、演示脚本、故障排除 |
| [08-deployment.md](docs/08-deployment.md) | 部署、运维与生产差距 |
| [09-design-decisions.md](docs/09-design-decisions.md) | 需求理解与设计决策 |
| [10-backtest-analysis.md](docs/10-backtest-analysis.md) | 回测方法、结果与规则有效性分析 |
| [11-config-reference.md](docs/11-config-reference.md) | 配置参考（由 pydantic 模型生成） |
| [12-code-walkthrough.md](docs/12-code-walkthrough.md) | 代码导读 |
| [13-benchmarks.md](docs/13-benchmarks.md) | 性能基准：方法、结果与未达标项分析（基准工具在完整版 `v0.1-full` 中） |

## 目录结构

```text
src/futrisk/          Python 包（代码导读见 docs/12）
  core/               纯领域内核：持仓、账本、指标、规则、状态机、事前检查、结算
  services/           6 个服务：md_gateway、risk_engine、trade_gateway、clearing、api_gateway、supervisor
  messages/ bus/ db/ runtime/   消息契约、ZeroMQ 总线、SQLite 访问层、服务运行时
  sim/                虚拟交易所（exchange）和策略机器人（bots）
  backtest/           回测
  cli/  main.py       命令行
  web_static/dist/    构建好的仪表盘
web/                  仪表盘源码（Vue 3 + TypeScript）
config/               默认配置（首次启动时复制到 <home>/config）
data/hist/            回测用的历史数据
tests/                unit、golden、property、e2e
scripts/              build_exe.ps1、record_forever.ps1、check_docs.py、gen_config_reference.py 等
deploy/               Dockerfile、compose.yml
packaging/            PyInstaller spec
docs/                 设计与使用文档
```
