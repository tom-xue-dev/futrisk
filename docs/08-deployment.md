# futrisk 部署、运维与生产差距

| 项 | 内容 |
|---|---|
| 文档编号 | FR-COM-08 |
| 版本 | v0.2（按已实现的系统编写） |
| 状态 | 实现对齐 |
| 日期 | 2026-09-26 |
| 适用范围 | Windows exe 的构建、安装与升级；docker compose 部署；日志、监控、重启、备份恢复、配置版本等日常运维；上线前的生产差距 |
| 对应标准 | GB/T 8567-2006 计算机操作手册（COM） |
| 关联文档 | [README](README.md)（规范）· [01 架构](01-architecture.md)（01 §7 部署视图、§8.17 生产级能力、§11 技术债）· [03 数据库](03-database-schema.md)（03 §8 生命周期）· [07 使用说明](07-user-guide.md) · [11 配置参考](11-config-reference.md) |

> 本文面向构建、部署和运维 futrisk 的人。部署视图的设计见 01 §7，本文给出操作步骤和运维要点；最后一章列出本期实现与"可以上线的生产系统"之间的差距，并逐项核对了代码。命令的用法见 07 §4，配置项见 11。

## 目录
1. [部署形态](#1-部署形态)
2. [Windows exe](#2-windows-exe)
3. [docker compose](#3-docker-compose)
4. [运维](#4-运维)
5. [生产差距与技术债](#5-生产差距与技术债)
- [附录 A 修订记录](#附录-a-修订记录)

---

## 1. 部署形态

| 形态 | 进程管理 | 适用 | 说明 |
|---|---|---|---|
| D1 Windows exe（01 §7.1） | 自研 supervisor（`futrisk up`）拉起 8 个进程 | 评审、现场演示、单机试运行 | 一个 one-folder 包，不需要安装 Python；一条命令 `futrisk demo` |
| 源码运行 | 同上 | 开发、调试 | `poetry install` 后 `.venv\Scripts\python -m futrisk.main ...` |
| D2 docker compose（01 §7.2） | 容器编排，每个服务一个容器；supervisor 只维护市场时钟 | Linux 服务器、验证服务之间没有隐藏耦合 | `docker compose -f deploy/compose.yml up -d --build` |
| D3 生产部署建议（01 §7.3） | systemd / K8s | 不在本期交付范围 | 主机布局、CURVE 加密、主备，见 §5 |

不支持把 futrisk 打成 wheel 安装到 site-packages：默认配置和历史数据按源码目录定位（TD-11）。

## 2. Windows exe

### 2.1 构建

前提：仓库根目录已执行 `poetry install`（`.venv` 中带 PyInstaller）；重新构建仪表盘时还需要 Node.js 20.19 以上（`npm.cmd` 在 `PATH` 中）。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1 -SkipWeb -UpSeconds 90
```

| 参数 | 作用 |
|---|---|
| `-SkipWeb` | 不运行 npm，直接使用已有的 `src\futrisk\web_static\dist`（前端源码比构建产物新时打印警告） |
| `-SkipTests` | 只构建和打包，不做冒烟测试 |
| `-SkipUp` | 跳过整套系统的 `futrisk up` 测试，其余冒烟测试照做 |
| `-UpSeconds <秒>` | `up` 测试保持系统运行的时长，默认 60 |
| `-Python <路径>` | 运行 PyInstaller 的 Python，默认 `.venv\Scripts\python.exe` |
| `-KeepTemp` | 保留冒烟测试的临时 home 和日志（测试失败时总是保留） |

脚本分 4 步：

1. **仪表盘**：`web\` 下 `npm ci` 和 `npm run build`（`vue-tsc` 类型检查 + `vite build`），产物写入 `src\futrisk\web_static\dist`；
2. **PyInstaller**：按 `packaging\futrisk.spec` 全新构建到 `dist\futrisk\`，完整日志在 `build\pyinstaller.log`；如果 `dist\futrisk\futrisk.exe` 正在运行，脚本拒绝继续（先 `futrisk down`）；
3. **冒烟测试**（§2.2）；
4. **打包**：生成 `dist\futrisk-win64-<版本>.zip` 和 `.zip.sha256`。

任何一步失败，脚本打印失败的步骤并以退出码 1 结束；冒烟测试失败时不生成 zip。脚本只会停止它自己启动的进程。版本号只有一处来源：`src\futrisk\__init__.py` 的 `__version__`（与 `pyproject.toml` 一致）。

### 2.2 冒烟测试

每项测试都使用独立的临时 home，配置从包内默认配置复制并改为空闲的回环端口，所以不会干扰、也不会误认一套已经在默认端口上运行的系统。

| 测试 | 通过条件 |
|---|---|
| `version` | 输出 `futrisk <版本>` |
| `config validate` | 包内默认配置有效 |
| `doctor --json` | exe 特有的检查（时区库、仪表盘文件、pyzmq、SQLite/WAL、非 ASCII 路径）全部 PASS |
| 单独运行 `svc api-gateway` | `/api/v1/health` 返回 200；`/` 返回仪表盘及其脚本资源；父进程结束后干净退出 |
| `backtest run breaker_synthetic` | 进程池能在 exe 中重新进入；合成剧本的检查全部通过；回测清单记录了 exe 的代码哈希 |
| `up --headless` 约 60s | `ps` 显示全部实例 RUNNING；`demo --headless` 建账成功；`status` 健康；`down` 之后没有残留进程 |
| 复制到中文路径 | 把整个 `dist\futrisk` 复制到 `%TEMP%\期货风控测试\`，再做 `version`、`config validate`、`doctor --json` 和 api-gateway 的检查 |

### 2.3 产物

```text
dist\
  futrisk\                         约 195MB
    futrisk.exe                    一个 exe 带全部子命令；supervisor 用 `futrisk.exe svc <name>` 拉起每个服务
    _internal\                     Python 运行时、扩展模块和 DLL（含 libzmq）、数据文件：
      futrisk\web_static\dist\     仪表盘
      futrisk\services\clearing\templates\   日报模板
      futrisk\sim\bots\bots.yaml   策略机器人默认参数
      config\                      包内默认配置（首次启动时复制到 <home>\config）
    data\hist\                     回测用的历史数据，放在 exe 旁边，可以不重新构建就更新
  futrisk-win64-0.1.0.zip          约 90MB，解压后顶层目录为 futrisk\
  futrisk-win64-0.1.0.zip.sha256   一行："<sha256>  futrisk-win64-0.1.0.zip"
```

- 不打包：pytest、ruff、mypy 等开发工具，AkShare 等开发期数据工具，`var\`（运行时 home，首次启动时创建）。
- 分发前后都应当核对哈希：`Get-FileHash futrisk-win64-0.1.0.zip -Algorithm SHA256` 的结果须与 `.sha256` 文件一致。
- 没有代码签名；个别杀毒软件可能误报（07 §7.3）。

### 2.4 安装、升级与卸载

| 操作 | 步骤 |
|---|---|
| 安装 | 把 zip 解压到任意目录（可以含中文）；运行 `futrisk\futrisk.exe doctor` 检查环境，再 `futrisk\futrisk.exe demo`。首次启动在 exe 旁边创建 `var\`（不可写时改用 `%LOCALAPPDATA%\futrisk`） |
| 升级 | ① `futrisk down`；② 把新版本解压到新目录；③ 把旧目录的 `var\` 移到新目录（或新版本运行时用 `--home <旧 var>`）；④ 启动。数据库结构的迁移在各服务打开数据库时自动执行（03 §9）。home 中已提交的配置保持不变，新版本的默认配置不会覆盖它：需要的改动手工合并到 `<home>\config\` 后热加载（07 §5.4） |
| 卸载 | `futrisk down`，删除解压目录；使用了 `%LOCALAPPDATA%\futrisk` 时一并删除 |

## 3. docker compose

`deploy/compose.yml` + `deploy/Dockerfile`。编写本文时只在本机验证了 compose 文件的语法（`docker compose -f deploy/compose.yml config --quiet`）；本章的运行方式按 `deploy/compose.yml`、`deploy/Dockerfile` 和 `deploy/config/services.yaml` 的内容与注释整理。

### 3.1 服务与卷

| 服务 | 命令 | 依赖（启动条件） | 健康检查 | 卷 |
|---|---|---|---|---|
| `config-init` | 一次性：`python /opt/futrisk-deploy/init_config.py` | — | — | `config`（写） |
| `supervisor` | `futrisk up --clock-only` | config-init 成功结束 | `futrisk ps --json`（最多试 3 次） | 自己的 home 卷；`config`（只读） |
| `sim-exchange` | `futrisk svc sim-exchange` | supervisor 健康 | Feed B 的 `/v1/health` | 自己的 home 卷；`config`（只读） |
| `md-gateway` | `futrisk svc md-gateway` | sim-exchange 已启动 | — | home；`config`（只读）；`ticks`（写） |
| `trade-gateway` | `futrisk svc trade-gateway` | md-gateway 已启动 | — | home；`config`（只读） |
| `clearing-worker` | `futrisk svc clearing-worker` | trade-gateway 已启动 | — | home；`config`（只读）；`reports`、`backtests`（写）；`ticks`（只读） |
| `risk-engine-0` / `risk-engine-1` | `futrisk svc risk-engine --shard 0` / `1` | clearing-worker 已启动 | — | home；`config`（只读） |
| `api-gateway` | `futrisk svc api-gateway` | 两个风控分片已启动 | `/api/v1/health` | home；`config`（写：热加载在这里生成新版本）；`reports`、`backtests`（只读） |
| `strategy-sim` | `futrisk svc strategy-sim` | api-gateway 已启动 | — | home；`config`（只读） |
| `seed` | 一次性：`python /opt/futrisk-deploy/seed_books.py` | supervisor、api-gateway 健康 | — | `config`（只读）；api-gateway 的 home 卷（只读，读取 bootstrap key） |

- **一个镜像**（`futrisk:0.1.0`）：多阶段构建——Node 20 构建仪表盘；按 `poetry.lock` 导出依赖；运行镜像基于 `python:3.12-slim`，装中文字体（报表图表用），以非 root 用户 `futrisk`（uid 10001）运行，`TZ=Asia/Shanghai`。保留仓库目录结构并以可编辑方式安装（TD-11）。
- **配置**：`config-init` 首次运行时把包内默认配置加上 docker 档位写入 `config` 卷并提交为版本 1：`services.yaml` 换成 `deploy/config/services.yaml`（端点主机名为 compose 服务名），`sim.yaml` 和 `feeds.yaml` 中的 `127.0.0.1` 换成 `sim-exchange`。以后再运行只校验生效版本，不覆盖。
- **身份**：所有容器设置同一个 `FUTRISK_HOME_ID`（默认 `futrisk-compose`），CLI 和 supervisor 把这些分开的 home 认作同一套部署。
- **网络**：服务在 compose 网络 `bus` 上按服务名互连；只有 api-gateway 的 8000 端口映射到宿主机的 `127.0.0.1`（`FUTRISK_HTTP_PORT` 可改）。
- 每个容器 `restart: unless-stopped`、`init: true`（tini 转发 SIGTERM 实现优雅停机，并回收进程池子进程）、`stop_grace_period: 30s`。

### 3.2 常用命令

```bash
docker compose -f deploy/compose.yml up -d --build        # 构建镜像并启动全部服务
docker compose -f deploy/compose.yml ps                    # 各容器状态和健康
docker compose -f deploy/compose.yml logs -f risk-engine-0
docker compose -f deploy/compose.yml exec api-gateway futrisk status
docker compose -f deploy/compose.yml exec api-gateway futrisk accounts
docker compose -f deploy/compose.yml exec api-gateway futrisk clock jump --to next-open
docker compose -f deploy/compose.yml exec api-gateway cat /var/lib/futrisk/secrets/bootstrap_admin.key
docker compose -f deploy/compose.yml restart md-gateway    # 重启一个服务（没有 futrisk kill）
docker compose -f deploy/compose.yml down                  # 停止；加 -v 同时删除全部数据卷
```

仪表盘地址 `http://127.0.0.1:8000`，API key 用上面 `cat` 出来的 bootstrap key。CLI 在 api-gateway 容器中运行，那里有 ADMIN key。

### 3.3 市场时钟

- supervisor 以"只管时钟"模式运行：维护并发布 `sys.clock`，保存在它的卷里，重启后继续随墙钟前进；首次启动时市场时间放在下一个交易日 08:58，`seed` 容器同时首日建账，机器人从开盘开始交易。
- 各服务启动时从 supervisor 取时钟；sim-exchange 60s 内取不到就以退出码 1 退出，由重启策略重试（`FUTRISK_SIM_CLOCK_WAIT_S` 可调），绝不在与其他服务不同的时钟上运行。
- 跳转：`docker compose -f deploy/compose.yml exec api-gateway futrisk clock jump --to 09:30`（也可以是 `next-open`、`pre-close`、`"2026-09-28 13:30"`），或 `POST /api/v1/demo/clock`。

### 3.4 与 exe 部署的差别

| 项 | exe | docker |
|---|---|---|
| 进程管理 | supervisor 拉起、崩溃重启 | 容器重启策略；`svc.start/stop/restart/kill` 回 `PRECONDITION_FAILED`（ADR-021） |
| `futrisk kill`、仪表盘"Kill 服务"和"重启" | 可用 | 不可用；用 `docker compose restart <服务>` |
| home | 所有服务共用一个 | 每个服务一个卷；`config`、`reports`、`backtests`、`ticks` 四处共享（01 §7.2） |
| 修改配置 | 编辑 `<home>\config\` 后热加载 | 把文件复制进 api-gateway 容器的 `/var/lib/futrisk/config/`（`docker compose -f deploy/compose.yml cp risk_rules.yaml api-gateway:/var/lib/futrisk/config/risk_rules.yaml`），再在宿主机上调用 `POST http://127.0.0.1:8000/api/v1/config/reload` |
| 日志 | `<home>\logs\` | `docker compose logs`（控制台）；JSON 日志在各服务卷的 `logs/` 中 |

## 4. 运维

### 4.1 日志

| 项 | 说明 |
|---|---|
| 位置 | `<home>\logs\<实例>_<YYYYMMDD>.log`（JSON Lines，每个实例一个文件）；`<实例>.console.log`（子进程捕获的标准输出和错误，超过 10MB 轮换）；`record_forever.log`（录制循环） |
| 字段 | `ts`（北京时间，带 `+08:00` 偏移，与本机时区无关）、`level`、`instance`、`trading_day`、`corr_id`（关联 `client_order_id`、`alert_key`、`command_id`）、`event`、`module`、`detail`、`exception` |
| 滚动与保留 | 每天北京时间零点切分一次（文件名中的日期取文件创建时的本机日期，本机时区不是 UTC+8 时可能与北京日期差一天，以文件内的 `ts` 为准），单个文件超过 `app.yaml` 的 `logging.rotation_mb`（100MB）再切分；旧文件压缩为 zip，保留 `logging.retention_days`（30 天） |
| 级别 | `logging.level`，默认 `INFO`；逐笔行情明细只在 `DEBUG` 输出。改动在进程重启后生效 |
| 查看 | `futrisk logs <实例> -n 200`、`-f` 跟随（日志轮换时自动接续）、`--console` 看控制台日志、`--raw` 输出原始 JSON |
| 审计 | 人工操作另外写入数据库：风控操作在 `risk-{shard}.db` 的 `audit_events`，REST 访问在 `api.db` 的 `api_audit`，进程事件在 `supervisor.db` 的 `service_events`（03 §4） |

### 4.2 监控

| 手段 | 内容 | 用法 |
|---|---|---|
| 心跳 | 每个服务每秒一次，不经过业务主循环：状态、配置版本和哈希、时钟版本、队列深度、消息速率、延迟 p50/p99、主循环滞后 `loop_lag_ms`、各 FACT 主题的最新序号（02 §4.8） | 仪表盘行情与系统页的服务健康表 |
| `futrisk status` | supervisor、各服务、市场时钟、仪表盘是否健康；不健康时退出码 1 | 放进计划任务定时检查 |
| `futrisk ps` | 进程状态、重启次数、最近的退出码和错误 | 排查反复重启 |
| `futrisk doctor` | 本机环境：时区库、时钟同步、端口、磁盘、SQLite/WAL、配置等 | 部署后、换机器后 |
| webhook 通知 | 高于 `notify.min_severity` 的风险事件、EOD 失败、服务失败、配置未收敛，按 `notify_key` 去重、限流（02 §9.8） | `app.yaml` 的 `notify`（11 §2） |
| 风控日报 | "数据质量"和"系统运行"两节：数据源可用率、中断和补数次数、服务重启次数和可用率 | 每日检查 |

本期没有把指标导出到监控平台（TD-07），无人值守时依赖 webhook。

### 4.3 重启策略

| 触发 | supervisor 的处理（01 §5.3.6） |
|---|---|
| 进程退出 | 立即发现，按退避重启 |
| 连续 3 个心跳周期没有心跳 | 判定挂起，结束进程后重启 |
| 心跳中的 `loop_lag_ms` 连续两次超过 `bus.hang_threshold_ms`（30s） | 判定主循环卡死，结束进程后重启 |
| 启动后 `supervisor.liveness_timeout_s`（20s）内没有存活信号 | 判定启动失败，重启 |
| 5 分钟内失败 5 次 | 停止重试（`GAVE_UP`），直到人工 `svc.start` / `svc.restart`（仪表盘"重启"或 `POST /api/v1/services/{instance}/restart`） |

- 退避：1s → 2s → 4s …，上限 30s；连续运行 60s 后退避计数清零（`services.yaml` 的 `supervisor.restart`）。
- 设计是"崩溃即重启"：内部异常或写库失败后服务先让自己失效再退出，由 supervisor 拉起，从持久化状态和幂等键确定性地恢复（01 §8.6）。risk-engine 带 1 万笔成交的冷启动约 1.7s。
- 重启期间下单失败或超时（失效即安全）；风控分片恢复后，锁存的熔断、告警和出入金都保持。
- 停机：`futrisk down` 按启动的相反顺序发送 `ctl.shutdown`，等 `supervisor.shutdown_grace_s`（10s），超时再发 CTRL_BREAK，最后强制结束。

### 4.4 备份、保留与恢复

每个交易日 `sys.eod` 到达 `DONE` 之后，各服务在非交易时段对自己的库做维护（03 §8）：WAL checkpoint → `VACUUM INTO <home>\backup\<交易日>\<库>.db`（先写临时文件再原子改名，同一天再次备份时覆盖）→ 按保留期分批清理 → 删除超过 20 个交易日的旧备份。

| 库 | 维护者 | 按保留期清理的数据 | 永久保留 |
|---|---|---|---|
| `md.db` | md-gateway | `md_events` 90 天（人工排除或恢复数据源的事件永久保留）及其已关闭的 `gaps`；`outbox` 7 天；原始 tick 目录 30 天、规范 tick 365 天 | `bars_1m`、`settle_inputs` |
| `td.db` | trade-gateway | `broker_order_events` 90 天；`outbox` 7 天 | `routed_orders`、`trades`、`route_tombstones` |
| `risk-{shard}.db` | 各风控分片 | `metric_samples` 90 天；`outbox` 7 天 | 委托、成交、告警、风险事件、审计、对账异常、状态机、EOD 状态 |
| `clearing.db` | clearing-worker | `outbox` 7 天 | 结算结果、日初快照、对账、报表索引、回测作业 |
| `api.db` | api-gateway | `api_audit` 180 天；`notifications` 90 天 | API key、配置版本 |
| `supervisor.db` | supervisor | `service_events` 90 天；`outbox` 7 天 | 市场时钟 |
| `sim.db` | —（外部模拟组件，不备份） | — | — |

所有库的 `processed_commands` 只保留当前和前一个交易日；outbox 清理后序号也不会回退（序号计数在永不清理的 `outbox_topics` 中）。

**恢复**（03 §8）：本期没有恢复工具，按以下步骤手工操作：

1. 停止对应服务（整机恢复时 `futrisk down`）；
2. 用 `backup\<交易日>\<库>.db` 替换 `db\<库>.db`，并删除同名的 `-wal`、`-shm` 文件；
3. **重新生成这个库的流 ID**：调用 `futrisk.db.regenerate_stream_id`（03 §4.0 `db_meta`），让订阅方识别出序号已经回退、重新核对。没有专门的命令（见 §5.2 第 3 条）；
4. 启动服务：它按启动握手从各发布方重放缺失的事实（重放窗口 7 天），并与柜台对账（02 §8.6）。

risk 库丢失时，可以用 clearing 最近一次的日初快照加上 trade-gateway 当日的全部成交重建；告警历史和审计会有缺口，缺口本身要记入审计。

### 4.5 配置版本、两阶段提交与回滚

| 位置 | 内容 |
|---|---|
| `<home>\config\` | 工作目录（编辑这里） |
| `<home>\config\versions\<N>\` | 每次热加载的不可变快照；被否决的版本目录也保留 |
| `<home>\config\active.json` | 当前生效的版本 |
| `GET /api/v1/config/versions` | 热加载历史：`PREPARING`、`COMMITTING`、`COMMITTED`、`ABORTED`、`FAILED` |

- **热加载**：见 07 §5.4 和 11 §1.3。提交决定先持久化再下发，参与方投赞成票后不会自行放弃，超时或重启后向 api-gateway 查询决定（02 §8.5）。
- **回滚**：版本号只增不减。把工作目录恢复成旧内容（从 `config\versions\<旧版本>\` 复制回 `config\`），再热加载一次，得到一个内容与旧版本相同的新版本。
- **离线提交**：端点、数据源、账户分片归属等改动会被热加载否决（11 §1.3）；而重启只会重新加载 `active.json` 指向的旧版本。本期没有提交这类改动的命令（见本文 §5.2 第 1 条），按以下步骤在停机状态下把工作目录提交为新版本（已在测试 home 上验证）：

  ```powershell
  futrisk down --home $H
  futrisk config validate --home $H
  $n = (Get-ChildItem "$H\config\versions" -Directory | ForEach-Object { [int]$_.Name } | Measure-Object -Maximum).Maximum + 1
  $dst = "$H\config\versions\$n"
  New-Item -ItemType Directory $dst | Out-Null
  Get-ChildItem "$H\config" -Exclude versions, active.json | Copy-Item -Destination $dst -Recurse
  $json = @{ config_version = $n; path = "config/versions/$n"; effective = @{} } | ConvertTo-Json
  [IO.File]::WriteAllText("$H\config\active.json", $json)   # 不带 BOM 的 UTF-8；Set-Content 会写入 BOM，无法解析
  futrisk up --home $H                                      # 或 futrisk demo --home $H --headless
  ```

  源码环境也可以调用同样的库函数：

  ```powershell
  $env:FUTRISK_HOME = $H
  .venv\Scripts\python -c "from futrisk.common.config import snapshot_working_config, write_active; from futrisk.common.paths import HomeLayout, resolve_home; l = HomeLayout(resolve_home()); n, p = snapshot_working_config(l); write_active(l, n, p); print(n, p)"
  ```

  这样提交的版本不经过两阶段提交，也不写入 `config_versions`；`products.yaml` 和交易日历的改动会从当前交易日起生效，所以这类改动仍然应当走热加载。

### 4.6 EOD 之后的检查

| 检查 | 命令或位置 |
|---|---|
| EOD 是否完成、有没有警告 | `futrisk settle days`；仪表盘报表页的"警告"列 |
| 对账检查是否全部通过 | `futrisk settle show --td <交易日>` 的 checks；日报 |
| 报表是否生成 | `futrisk report list --td <交易日>`；`<home>\reports\<交易日>\` |
| 备份是否生成 | `<home>\backup\<交易日>\` 下每个库一个文件 |
| 品种参数或交易日历有改动 | 在下一个交易时段开始前重启 `risk-engine-0`、`risk-engine-1`（TD-09） |
| 磁盘空间 | `futrisk doctor` 的 `disk` 一项 |

## 5. 生产差距与技术债

### 5.1 已知的技术债

下表按 01 §8.17 和 §11 整理，逐项核对了代码现状。编号沿用 01 §11。

| 能力或问题 | 编号 | 现状（已核对代码） | 上线前应当做什么 |
|---|---|---|---|
| 高可用 | TD-01 | 每个分片、每个服务只有一个实例；主机故障只能重启 | 风控"主备 + 租约"切换；api-gateway 多副本还要先完成 TD-05 |
| 传输加密与身份认证 | TD-02 | 代码中没有 CURVE；总线身份（DEALER identity）是自报的，演示环境只绑定 127.0.0.1 | 跨主机前启用 ZeroMQ CURVE 和白名单、HTTPS |
| 双人复核 | TD-06 | 熔断重置、kill switch 关闭、结算价覆盖只需一个人（一个 key）操作，没有审批流程 | 发起人提交、另一位 RISK_MANAGER 批准后才执行 |
| 真实交易所接入 | 01 §8.17 | `CtpBrokerAdapter` 只有接口说明，每个方法都抛出 `NotImplementedError`；行情只有模拟源和新浪 | 实现 CTP 行情和交易适配器，接入仿真环境联调 |
| 强制减仓自动执行 | TD-08 | `LIQUIDATE` 只生成减仓计划，确认后需要人工下平仓单 | 由 risk-engine 按计划分批下平仓单，走同一条事前检查 |
| 品种参数在风控中要重启才生效 | TD-09 | risk-engine 提交后只把 `products` / 交易日历暂存到下次启动（heartbeat 的 `staged_config`） | 运维上在 EOD 后提交并在开盘前重启分片；以后改为换日时切换 |
| 查询没有分页 | TD-10 | `QueryOrders`、`QueryFills`、`QueryAlerts` 没有游标或条数限制 | 游标分页 |
| 不支持 wheel 安装 | TD-11 | 默认配置和历史数据按源码目录定位 | 默认配置作为包数据读取，历史数据目录可配置 |
| 指标平台 | TD-07 | 指标只在心跳和仪表盘中 | 导出指标并接入监控告警平台 |
| api-gateway 单实例 | TD-05 | 通知去重和配置协调有状态 | 拆给选主实例，库迁到 PostgreSQL |
| 手续费和保证金是示例值 | TD-04 | 手工维护 `products.yaml`；自动刷新工具没有实现 | 按交易所和经纪商的最新公告维护，上线前逐项核对 |
| 交割、组合保证金、申报费 | TD-03 | 未实现 | 按业务需要实现 |
| live 档新浪转发 | TD-12 | sim-exchange 不转发真实价格 | 生产环境直接接 CTP 行情和厂商源 |
| 补数区间的事后审计 | TD-13 | `RETRO_BREACH` 影子评估未实现 | — |
| 公司层高水位和锁存 | TD-14 | 公司层高水位只在内存中；公司级锁存没有人工解除命令 | 持久化并增加审计的重置命令 |
| 回测回放吞吐 | TD-15 | 约 0.6~1 万 tick/s，低于 10 万的目标 | 标量快路径估值、向量化策略 |
| 部分验收度量没有自动化 | TD-16 | QS-05 事件哈希基线、QS-10 仪表盘延迟、性质测试例数 | 补自动化测试 |
| 性能目标 | 01 §10.2 | 下单往返、tick→判定等在负载较高的笔记本上未达标，待安静机器复测（13 文档） | 生产硬件上复测，必要时按 01 §8.12 的扩展顺序优化 |

### 5.2 本次核对新发现的问题

以下问题在编写 07、08、11 时通过阅读代码和实际运行发现，01 §11 尚未列出，建议补入技术债：

| # | 问题 | 影响 | 建议 |
|---|---|---|---|
| 1 | 只能重启生效的配置改动（端点、数据源、账户分片归属）没有受支持的提交方式：热加载在 prepare 阶段否决它们，重启又只加载 `active.json` 指向的旧版本（已实测） | 运维只能按 §4.5 手工离线提交，或换新 home | 增加离线提交命令（如 `futrisk config commit --offline`），或在停机状态下 `futrisk up` 时检测工作目录变化并提示提交 |
| 2 | 配置热加载、出入金、结算价覆盖、官方结算价导入、日初快照更正、成交导入、对账异常处理、数据源排除只有 REST 入口，没有 CLI 命令，仪表盘也没有（07 §4.12） | 运维要手写带 `Idempotency-Key` 的 HTTP 请求 | 在 CLI 中补齐这些命令 |
| 3 | 03 §8 的恢复流程依赖"恢复脚本"重新生成流 ID，代码只提供了库函数 `regenerate_stream_id`，没有脚本或命令 | 从备份恢复只能手工调用函数，容易漏掉这一步，订阅方识别不出序号回退 | 增加 `futrisk db restore <库> <备份>`：停服务检查、替换文件、重新生成流 ID |
| 4 | api-gateway 每天备份 `api.db`，但不清理旧备份（其他服务都只保留 20 个交易日） | `backup\` 目录随时间增长 | 复用其他服务的备份清理 |
| 5 | 具名 API key 的明文从 api-gateway 进程的环境变量读取，进程继承 supervisor 启动时的环境 | 新增或轮换 key 时，改 `app.yaml` 并热加载还不够，要设置环境变量后重启整个系统 | 支持从 `<home>\secrets\` 下的文件读取 key，或由 supervisor 在重启服务时刷新环境 |
| 6 | `futrisk backtest run <名字>` 只在包内 `config\backtest\` 中查找，`<home>\config\backtest\` 被复制进每个配置版本却从不读取；`futrisk data` 也读包内默认的 `feeds.yaml` | 修改 home 中的这些文件没有任何效果，容易误解 | 先查 home 再查包内，或不再把它们复制进 home |
| 7 | 日志的 `ts` 和每日切分已统一用北京时间，但文件名中的日期仍取本机日期（loguru 按本机时间格式化文件名） | 本机时区不是 UTC+8 时，文件名的日期可能与北京日期差一天；演示模式下日志时间是本机时间，不是市场时间 | 自定义文件命名；日志同时记录市场时间 |
| 8 | CLI 表格原样打印 API 返回的浮点数（如 `futrisk accounts` 的当日盈亏 `-161257.1799999997`、`futrisk fault list` 的剩余时间 `18.03200000000652s`） | 只影响可读性 | 金额按 0.01 元、时长按 0.1s 格式化显示 |
| 9 | `accounts.yaml` 的 `initial_positions` 已定义但运行时不读取（首日建账只写资金） | 配置了初始持仓也不会生效 | 建账时读取，或从模型中删除 |
| 10 | api.db 的 `config_versions` 只记录经热加载产生的版本；按 §4.5 离线提交的版本没有记录 | 版本历史不完整 | 离线提交命令（第 1 条）同时写入历史 |

### 5.3 上线前检查清单

| 类别 | 检查项 |
|---|---|
| 安全 | 启用 CURVE 和白名单（TD-02）；为每个人配置具名 key 并停用 bootstrap key；HTTPS；双人复核（TD-06） |
| 接入 | CTP 行情与交易适配器；与柜台对账的流程和时点；EOD 调度（`eod_start_at`）排在经纪商出入金截止之后 |
| 参数 | `products.yaml` 的费率、保证金率、涨跌停、交易时段按最新公告核对；交易日历使用官方安排（2027 年目前为估计值） |
| 风控 | `risk_rules.yaml` 的阈值和按账户覆盖经风控负责人确认（04 §4）；熔断演练：越线、锁存、重置、换日 |
| 运行 | 主机时钟同步（NTP / PTP，`futrisk doctor` 的 `clock sync` 为 PASS）；进程管理换成 systemd / K8s；日志和指标接入平台（TD-07）；webhook 通道测试 |
| 数据 | 备份复制到另一台主机；演练一次从备份恢复（§4.4）；磁盘容量按 tick 和日志的保留期估算 |
| 性能 | 在生产硬件上运行 `futrisk bench`，与 01 §10.2 的目标对比 |

---

## 附录 A 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-25 | 初版：exe 构建参数、冒烟测试、产物与安装升级；docker compose 的服务、卷、健康检查、时钟与差别；日志、监控、重启策略、备份保留与恢复、配置版本与回滚（含离线提交步骤，已在测试 home 上验证）、EOD 后检查；按 01 §8.17 与 §11 核对代码后的技术债表，新发现问题 10 条，上线前检查清单 |
| v0.2 | 2026-09-26 | 代码冻结后的核对：日志的 `ts` 与每日切分改为北京时间（与本机时区无关），只有文件名中的日期仍取本机日期，§4.1 与生产差距第 7 项随之更正；exe 重新构建，产物大小与冒烟测试结果见 06 §7.5 |
