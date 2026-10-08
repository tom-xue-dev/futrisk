# futrisk 部署、运维与生产差距

| 项 | 内容 |
|---|---|
| 文档编号 | FR-COM-08 |
| 版本 | v1.0 |
| 状态 | 实现对齐 |
| 日期 | 2026-10-08 |
| 适用范围 | futrisk 0.1.0（精简版，完整版为 git 标签 `v0.1-full`）：Windows exe 的构建、安装与升级；docker compose 部署；日志、监控、重启、备份恢复、配置版本等日常运维；上线前的生产差距 |
| 对应标准 | GB/T 8567-2006 计算机操作手册（COM） |
| 关联文档 | [README](README.md)（规范）· [01 架构](01-architecture.md)（01 §7 部署视图、§8.17 生产级能力、§11 技术债）· [03 数据库](03-database-schema.md)（03 §8 生命周期）· [06 测试](06-test-plan.md)（06 §7.5 exe 构建、§7.7 docker 实测）· [07 使用说明](07-user-guide.md) · [11 配置参考](11-config-reference.md) |

> 本文面向构建、部署和运维 futrisk 的人。部署视图的设计见 01 §7，本文给出操作步骤和运维要点；最后一章列出本期实现与"可以上线的生产系统"之间的差距，每一项都对照代码核对过。第一次运行的分步说明见 07 §2，命令的用法见 07 §4，配置项见 11。

## 目录
1. [部署形态](#1-部署形态)
2. [Windows exe](#2-windows-exe)
3. [docker compose](#3-docker-compose)
4. [运维](#4-运维)
5. [生产差距与技术债](#5-生产差距与技术债)
- [附录 A 修订记录](#附录-a-修订记录)

---

## 1. 部署形态

| 形态 | 进程管理 | 适用 | 仪表盘前端 | 启动命令 |
|---|---|---|---|---|
| D1 Windows exe（01 §7.1） | 自研 supervisor（`futrisk up`）拉起 8 个子进程，崩溃后按退避重启 | 评审、现场演示、单机试运行 | 已打在包内 | `futrisk.exe demo` |
| 源码运行 | 同上 | 开发、调试 | 不在版本库中，先在 `web\` 下构建（07 §2.2） | `.venv\Scripts\futrisk demo` |
| D2 docker compose（01 §7.2） | 每个服务一个容器，由容器的重启策略管理；supervisor 只维护市场时钟（01 ADR-021） | Linux 服务器；验证服务之间没有隐藏的耦合 | 在镜像构建时生成 | `docker compose -f deploy/compose.yml up -d --build` |
| D3 生产部署建议（01 §7.3） | systemd / K8s | 不在本期交付范围 | — | 主机布局、CURVE 加密、主备，见 §5 |

- 仪表盘的构建产物 `src\futrisk\web_static\dist\` 被 `.gitignore` 的 `dist/` 规则排除，不在版本库中。exe 包（构建脚本第 1 步）和 docker 镜像（Dockerfile 的 `web` 构建阶段）在构建时生成它；源码检出后要先在 `web\` 下执行 `npm.cmd ci` 和 `npm.cmd run build`（Node.js 20.19 以上；Windows PowerShell 中写 `npm.cmd`，07 §2.2.2）。没有构建时 api-gateway 的 `/` 只显示一行"The dashboard is not built"的说明，REST、WebSocket 和 `/docs` 照常可用，`futrisk doctor` 的 `dashboard` 一项为 WARN。
- 不支持把 futrisk 打成 wheel 安装到 site-packages：默认配置和历史数据按源码目录定位（TD-11）。

## 2. Windows exe

### 2.1 构建

前提：
- 仓库根目录执行过 `poetry install`：它在仓库下创建 `.venv`（`poetry.toml` 的 `in-project = true`），并默认安装 dev 组，其中有 PyInstaller；
- 构建仪表盘需要 Node.js 20.19 以上，`npm.cmd` 在 `PATH` 中。用 `-SkipWeb` 时必须已有 `src\futrisk\web_static\dist\index.html`，否则脚本和 `packaging\futrisk.spec` 都会报错退出。

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\build_exe.ps1 -SkipWeb -UpSeconds 90
```

| 参数 | 作用 |
|---|---|
| `-SkipWeb` | 不运行 npm，直接使用已有的 `src\futrisk\web_static\dist`（`web\` 下的源码比构建产物新时打印警告） |
| `-SkipTests` | 只构建和打包，不做冒烟测试 |
| `-SkipUp` | 跳过整套系统的 `futrisk up` 测试，其余冒烟测试照做 |
| `-UpSeconds <秒>` | `up` 测试保持系统运行的时长，默认 60 |
| `-Python <路径>` | 运行 PyInstaller 的 Python，默认 `.venv\Scripts\python.exe` |
| `-KeepTemp` | 保留冒烟测试的临时 home 和日志（测试失败时总是保留） |

脚本分 4 步：

1. **仪表盘**：`web\` 下 `npm ci` 和 `npm run build`（`vue-tsc` 类型检查 + `vite build`），产物写入 `src\futrisk\web_static\dist`；
2. **PyInstaller**：按 `packaging\futrisk.spec` 全新构建到 `dist\futrisk\`，完整日志在 `build\pyinstaller.log`；`dist\futrisk\futrisk.exe` 正在运行时脚本拒绝继续（先 `futrisk down`）；
3. **冒烟测试**（§2.2）；
4. **打包**：生成 `dist\futrisk-win64-<版本>.zip` 和 `.zip.sha256`。

成功时最后一行是 `BUILD OK`。任何一步出错时打印 `BUILD FAILED: <原因>`；冒烟测试有失败项时打印 `SMOKE TESTS FAILED: <各项>`，不生成 zip。这两种情况的退出码都是 1，临时 home 和日志保留在 `%TEMP%\futrisk-build-<时间戳>\`。脚本只停止它自己启动的进程（命令行中含有本次临时目录名的 futrisk 进程）。版本号只有一处来源：`src\futrisk\__init__.py` 的 `__version__`（与 `pyproject.toml` 一致）。

### 2.2 冒烟测试

每项测试都使用独立的临时 home：配置从包内默认配置复制，`services.yaml`、`sim.yaml`、`feeds.yaml` 中的端口全部换成空闲的回环端口，所以不会干扰、也不会误认一套已经在默认端口上运行的系统。共 17 项：

| 测试 | 通过条件 |
|---|---|
| `version` | 输出 `futrisk <版本>` |
| `config validate` | 包内默认配置有效（输出以 `OK` 开头） |
| `doctor --json` | `build` 一项注明 frozen exe；`tz Asia/Shanghai`、`dashboard`、`pyzmq`、`sqlite`、`non-ASCII path` 五项为 PASS。端口和时钟同步取决于机器，只报告、不判定 |
| 单独运行 `svc api-gateway`（3 项） | `/api/v1/health` 返回 200；`/` 返回仪表盘页面，页面加载的第一个脚本也返回 200；父进程结束后以退出码 0 停止 |
| `backtest run breaker_synthetic` | 退出码 0（合成剧本的检查全部通过），说明进程池能在 exe 中重新进入；回测清单的 `code_hash` 是 exe 的哈希 |
| `up --headless`，保持 `-UpSeconds` 秒（4 项） | `demo --headless` 建账成功；`ps --json` 显示全部实例 RUNNING 并保持到结束；`status` 退出码 0；`down` 退出码 0、supervisor 正常退出、没有残留进程 |
| 中文路径（6 项） | 把整个 `dist\futrisk` 复制到 `%TEMP%\期货风控测试\futrisk\`，再做 `version`、`config validate`、`doctor --json` 和 api-gateway 的 3 项检查 |

最近一次构建的用时、产物大小和冒烟测试结果见 06 §7.5。

### 2.3 产物

```text
dist\
  futrisk\                         one-folder 包
    futrisk.exe                    一个 exe 带全部子命令；supervisor 用 `futrisk.exe svc <name>` 拉起每个服务
    _internal\                     Python 运行时、扩展模块和 DLL（含 libzmq）、数据文件：
      futrisk\web_static\dist\     仪表盘
      futrisk\services\clearing\templates\   日报模板
      futrisk\sim\bots\bots.yaml   策略机器人默认参数
      config\                      包内默认配置（首次启动时复制到 <home>\config）
    data\hist\                     回测用的历史数据，放在 exe 旁边，可以不重新构建就更新
  futrisk-win64-0.1.0.zip          解压后顶层目录为 futrisk\（不含运行时 home var\）
  futrisk-win64-0.1.0.zip.sha256   一行："<sha256>  futrisk-win64-0.1.0.zip"（小写十六进制）
```

- 不打包：pytest、hypothesis、coverage、ruff、mypy 等开发工具，AkShare、polars 等开发期数据工具，GUI 工具包，以及运行时 home `var\`（首次启动时创建）。
- 分发前后都应当核对哈希：`Get-FileHash futrisk-win64-0.1.0.zip -Algorithm SHA256` 的结果须与 `.sha256` 文件一致（前者输出大写，PowerShell 的 `-eq` 比较不区分大小写，07 §2.2 有现成的命令）。
- 没有代码签名；个别杀毒软件可能误报（07 §7.3）。

### 2.4 安装、升级与卸载

| 操作 | 步骤 |
|---|---|
| 安装 | 把 zip 解压到任意目录（可以含中文）；`futrisk\futrisk.exe doctor` 检查环境，再 `futrisk\futrisk.exe demo`（分步说明见 07 §2.2）。首次启动在 exe 旁边创建 `var\`（不可写时改用 `%LOCALAPPDATA%\futrisk`） |
| 升级 | ① `futrisk down`；② 把新版本解压到新目录；③ 把旧目录的 `var\` 移到新目录（或运行新版本时加 `--home <旧 var>`）；④ 用新版本运行 `futrisk doctor`，看 `config` 和 `config active` 两项；⑤ 启动。数据库结构的迁移在各服务打开数据库时自动执行（03 §9）。home 中已提交的配置保持不变，新版本的默认配置不会覆盖它 |
| 卸载 | `futrisk down`，删除解压目录；使用了 `%LOCALAPPDATA%\futrisk` 时一并删除 |

升级时有两种情况会让旧 home 无法直接启动：

- **配置字段被删除或改名**。配置模型不接受多余的键（11 §1.4），旧 home 的生效版本因此无法加载：`futrisk up` / `futrisk demo` 打印 `configuration error in <home>\config: ...` 并以退出码 2 结束。处理：按报错修改 `<home>\config\` 下的文件，`futrisk config validate --home <home>` 通过后用 `futrisk config commit --home <home>` 离线提交（本文 §4.5），再启动。例如 2026-10-07 的行情网关重写删除了 `feeds.yaml` 中 `arbitration` 下的 6 个参数（`probation_agree_count`、`flapping_window_s`、`flapping_max_switches`、`spike_confirm_ms`、`spike_ticks`、`spike_z`），之前创建的 home 要先删掉这 6 行。
- **数据库被重新基线**。迁移只能向前追加（03 §9）；同一次重写把 md.db 重新基线为版本 1，旧 home 的 md.db 中的版本 1 是重写前的表结构（另外还有代码不认识的版本 2），md-gateway 打开它时报 `checksum mismatch for migration 1 (...): recorded ..., code ...; refusing to start`，反复重启后进入 `GAVE_UP`。md.db 只存行情事件、缺口、结算价输入和接续状态，处理办法是停机后把 `db\md.db`（连同同名的 `-wal`、`-shm`）移走，md-gateway 启动时建一个新库（新的流 ID，订阅方按 02 §8.2 重新核对）。被移走的库中当日的结算价输入随之丢失，所以应当在当日 EOD 完成之后升级。

## 3. docker compose

`deploy/compose.yml` + `deploy/Dockerfile`（构建上下文是仓库根目录，`deploy/Dockerfile.dockerignore` 只放入构建所需的文件）。2026-10-07 在 Windows 11 的 Docker Desktop 上按本章步骤完整跑过一次（06 §7.7）：首次构建约 8 分钟，镜像 896 MB；`up -d` 后 9 个常驻容器全部 RUNNING，一次性的 `config-init` 和 `seed` 正常结束；单独重启一个服务、`down` 后再 `up -d`（配置版本和账本都保留）都正常。同一次运行中出现过两段行情拒收，都自行恢复：开盘集合竞价期间 Feed B 的陈旧快照被判为不在交易时段，Feed B 因此被排除（md-gateway 校验的缺陷，2026-10-08 已修正）；宿主机时钟慢 4 分 11 秒（Windows Time 服务停止）引起时间戳超前的拒收（§3.3）。没有在 Linux 服务器上部署过。

### 3.1 服务与卷

除 `config-init` 和 `seed` 外，每个服务都要等 `config-init` 成功结束、`supervisor` 健康之后才启动，下表的"启动条件"只列出各自另外的条件。

| 服务 | 命令 | 启动条件（`depends_on`） | 健康检查 | 卷 |
|---|---|---|---|---|
| `config-init` | 一次性：`python /opt/futrisk-deploy/init_config.py`；镜像也由它的 `build` 段构建 | — | — | `config`（读写） |
| `supervisor` | `futrisk up --clock-only` | `config-init` 成功结束 | `futrisk ps --json`（一次检查内最多试 3 次） | 自己的 home 卷；`config`（只读） |
| `sim-exchange` | `futrisk svc sim-exchange` | — | Feed B 的 `http://sim-exchange:7002/v1/health` | 自己的 home 卷；`config`（只读） |
| `md-gateway` | `futrisk svc md-gateway` | `sim-exchange` 已启动 | — | home；`config`（只读）；`ticks`（读写） |
| `trade-gateway` | `futrisk svc trade-gateway` | `md-gateway` 已启动 | — | home；`config`（只读） |
| `clearing-worker` | `futrisk svc clearing-worker` | `trade-gateway` 已启动 | — | home；`config`（只读）；`reports`、`backtests`（读写） |
| `risk-engine-0` / `risk-engine-1` | `futrisk svc risk-engine --shard 0` / `1` | `clearing-worker` 已启动 | — | home；`config`（只读） |
| `api-gateway` | `futrisk svc api-gateway` | 两个风控分片已启动 | `/api/v1/health` | home；`config`（读写：热加载在这里生成新版本）；`reports`、`backtests`（只读） |
| `strategy-sim` | `futrisk svc strategy-sim` | `api-gateway` 已启动 | — | home；`config`（只读） |
| `seed` | 一次性：`python /opt/futrisk-deploy/seed_books.py` | `supervisor`、`api-gateway` 健康 | — | `config`（只读）；api-gateway 的 home 卷（只读，读取 bootstrap key） |

- **镜像**（`futrisk:0.1.0`，`FUTRISK_VERSION` 可改标签）：多阶段构建——`node:20-slim` 构建仪表盘；用 Poetry 按 `poetry.lock` 导出 main 组的依赖（不含开发工具和 PyInstaller）；运行镜像基于 `python:3.12-slim`，装中文字体 `fonts-wqy-zenhei`（报表和回测图表用），保留仓库目录结构（`/app/src`、`/app/config`、`/app/data/hist`）并以可编辑方式安装（TD-11），设置 `FUTRISK_HOME=/var/lib/futrisk` 和 `TZ=Asia/Shanghai`，以非 root 用户 `futrisk`（uid 10001）运行，入口为 `futrisk`。所有服务使用同一个镜像，`pull_policy: never`（只在本地构建）。
- **配置**：`config-init` 第一次运行（`config` 卷为空）时，把包内默认配置加上 docker 档位写入 `config` 卷并提交为版本 1：`services.yaml` 换成 `deploy/config/services.yaml`（`profile: docker`，端点主机名为 compose 服务名，api-gateway 的 HTTP 监听 `0.0.0.0:8000`），`sim.yaml` 和 `feeds.yaml` 中的 `127.0.0.1` 换成 `sim-exchange`。以后每次运行只校验生效版本、不覆盖；生效版本无效时它以退出码 1 结束，其余服务都不会启动。
- **身份**：所有容器设置同一个 `FUTRISK_HOME_ID`（默认 `futrisk-compose`），CLI 和 supervisor 因此把这些各自独立的 home 认作同一套部署。
- **网络**：服务在 compose 网络 `bus` 上按服务名互连；只有 api-gateway 的 8000 端口映射到宿主机的 `127.0.0.1`（`FUTRISK_HTTP_PORT` 改宿主机端口）。
- 常驻容器 `restart: unless-stopped`、`init: true`（tini 作为 1 号进程，转发 SIGTERM 实现优雅停机，并回收进程池的子进程）、`stop_grace_period: 30s`；`config-init` 和 `seed` 不重启。

### 3.2 常用命令

```powershell
docker compose -f deploy/compose.yml up -d --build        # 构建镜像并启动全部服务
docker compose -f deploy/compose.yml ps                    # 常驻容器的状态和健康（-a 另外列出已结束的 config-init、seed）
docker compose -f deploy/compose.yml logs -f risk-engine-0 # 某个服务的控制台输出
docker compose -f deploy/compose.yml exec api-gateway cat /var/lib/futrisk/secrets/bootstrap_admin.key
docker compose -f deploy/compose.yml exec api-gateway futrisk status
docker compose -f deploy/compose.yml exec api-gateway futrisk accounts
docker compose -f deploy/compose.yml exec api-gateway futrisk clock jump --to next-open
docker compose -f deploy/compose.yml restart md-gateway    # 重启一个服务（docker 下没有 futrisk kill）
docker compose -f deploy/compose.yml down                  # 停止并删除容器，数据卷保留
docker compose -f deploy/compose.yml down -v               # 同时删除全部数据卷：配置版本、账本、市场时钟、tick、报表
```

- 仪表盘 `http://127.0.0.1:8000`；第一次打开时在设置对话框中粘贴上面 `cat` 出来的 bootstrap key。换宿主机端口：PowerShell 中先设置环境变量 `FUTRISK_HTTP_PORT`（如 18000）再 `up -d`。
- CLI 在 api-gateway 容器中运行：那里有 bootstrap ADMIN key，`FUTRISK_HOME_ID` 与其他容器一致。经总线的 `status`、`ps` 只能在容器内执行；`futrisk logs` 只读得到所在容器自己的日志，其他服务用 `docker compose logs <服务>`。
- 宿主机上装有 futrisk 时，经 REST 的命令（`accounts`、`positions`、`order`、`risk`、`settle`、`report`、`clock` 等）也可以直接发给容器：先把环境变量 `FUTRISK_API_KEY` 设为 bootstrap key，再加 `--api http://127.0.0.1:8000`。显式给出 `FUTRISK_API_KEY` 时 CLI 不核对 home 身份，这是为这种用法设计的（`src/futrisk/cli/toolkit.py` 的 `session`）。

### 3.3 市场时钟

- supervisor 以"只管时钟"模式运行（`futrisk up --clock-only`）：维护并发布 `sys.clock`，状态存在它自己的卷（`supervisor.db`）里，重启后继续随墙钟前进。首次启动时市场时间放在 `app.demo.start_at`（默认为下一个交易日的 08:58），`seed` 容器同时按 `accounts.yaml` 首日建账，机器人从开盘开始交易。隔一段时间再 `up -d`，市场可能已经收盘，用 `futrisk clock jump --to next-open` 跳到下一个交易时段。
- 各服务启动时先从 supervisor 取时钟；sim-exchange 在 `FUTRISK_SIM_CLOCK_WAIT_S`（默认 60s）内取不到就以退出码 1 退出，由重启策略重试，绝不在与其他服务不同的时钟上运行。
- 跳转：`docker compose -f deploy/compose.yml exec api-gateway futrisk clock jump --to 09:30`（也可以是 `next-open`、`pre-close`、`"2026-09-28 13:30"`），或 `POST /api/v1/demo/clock`（ADMIN）。
- 容器内的墙钟是 Docker 虚拟机的时钟。宿主机时钟不准时（Windows Time 服务停止、与 NTP 相差几分钟），Docker Desktop 会周期性地把虚拟机时钟拨回宿主机时间、再由虚拟机自己纠正回来，各进程锚定的时间因此相差 1~2 秒。md-gateway 把交易所时间戳比收到时刻晚 1 秒以上（`future_skew_s`）的 tick 判为 `MD_REJECT_FUTURE_TS`（`BAD_TICK` 行情事件），拒收率超过 `feeds.arbitration.exclude_reject_rate`（0.05）的源被排除，时钟稳定后经试用期自动恢复。2026-10-07 那次运行中，09:03 起两个源都出现了 `MD_REJECT_FUTURE_TS`（Feed A 12182 条、Feed B 1342 条）和随后的可信状态变化，都自行恢复，没有人工干预，也没有服务崩溃（06 §7.7）。同一次运行中 Feed B 在 08:58 被排除是另一个原因：无夜盘交易日的开盘集合竞价（08:55~09:00）期间，Feed B 轮询到的快照还带着竞价前的时间（08:53:53），被判为 `MD_REJECT_OUT_OF_SESSION`。这是 md-gateway 校验的缺陷，2026-10-08 已修正：合约的交易时段正在进行时，轮询记录（Feed B、新浪）和订阅快照的交易所时间如果不在当日任何交易时段内、且不晚于收到时刻，作为陈旧快照跳过，不计入拒收率（`src/futrisk/services/md_gateway/validator.py`）；修正后 2026-10-08 在 docker 上重跑，两个源在 08:55 开始的集合竞价期间都保持 `TRUSTED`。部署前应当先校准宿主机时钟；Windows 上 `futrisk doctor` 的 `clock sync` 一项会指出 Windows Time 服务没有运行（07 §7.5）。

### 3.4 与 exe 部署的差别

| 项 | exe / 源码 | docker |
|---|---|---|
| 进程管理 | supervisor 拉起、崩溃后按退避重启（§4.3） | 容器的重启策略；supervisor 只管时钟，对 `svc.start/stop/restart/kill` 返回 `PRECONDITION_FAILED`（01 ADR-021） |
| `futrisk kill`、仪表盘的"重启"和演示控制中的 Kill 服务 | 可用 | 不可用；用 `docker compose restart <服务>` |
| 启停整套系统 | `futrisk demo` / `futrisk up`；`futrisk down` | `docker compose up -d`；`docker compose down`（不用 `futrisk up`、`futrisk down`） |
| home | 所有服务共用一个 | 每个服务一个卷；`config`、`reports`、`backtests` 三个卷按需共享，`ticks` 卷只由 md-gateway 使用（01 §7.2） |
| 可以热加载的配置改动 | 编辑 `<home>\config\` 后 `POST /api/v1/config/reload`（07 §5.4） | 把文件复制进 api-gateway 容器的 `/var/lib/futrisk/config/`（`docker compose -f deploy/compose.yml cp risk_rules.yaml api-gateway:/var/lib/futrisk/config/risk_rules.yaml`），再在宿主机上调用 `POST http://127.0.0.1:8000/api/v1/config/reload` |
| 只能重启生效的配置改动 | `futrisk down` → 编辑 → `futrisk config commit` → 启动（§4.5） | 没有实测过。思路相同：`docker compose stop` 停下全部容器，把文件复制进 `config` 卷，在一个挂载了该卷的容器中运行 `futrisk config commit`（例如 `docker compose -f deploy/compose.yml run --rm --no-deps api-gateway config commit`），再 `up -d`。改端点时还要同步修改 `deploy/config/services.yaml`（它只在 `config` 卷为空时使用） |
| 日志 | `<home>\logs\` | `docker compose logs`（控制台输出）；JSON 日志在各服务卷的 `logs/` 中 |
| 升级 | §2.4 | 重新 `up -d --build`；§2.4 的两种情况同样适用（`config-init` 校验失败时其余服务都不启动）。演示部署最简单的做法是 `down -v` 后重新开始 |

## 4. 运维

### 4.1 日志

| 项 | 说明 |
|---|---|
| 位置 | `<home>\logs\<实例>_<YYYYMMDD>.log`：JSON Lines，每个实例一个文件（`futrisk data record` 的实例名为 `data-recorder`）。`<实例>.console.log`：supervisor 捕获的子进程标准输出和错误，去掉终端控制符，超过 10MB 时改名为 `<实例>.console.log.1`，只保留一代。`supervisor.console.log`：`futrisk demo` 在后台拉起的 `futrisk up` 的输出。`record_forever.log`：录制循环（07 §5.7） |
| 字段 | `ts`（北京时间，ISO 8601，带 `+08:00` 和微秒，与本机时区无关）、`level`、`instance`、`trading_day`、`corr_id`（关联键：strategy-sim 的委托日志为 `client_order_id`，api-gateway 的告警通知为 `notify_key`，其他服务的日志目前不填）、`event`、`module`，以及 `detail`（其余键值）和 `exception` |
| 滚动与保留 | 北京时间零点切分一次，单个文件超过 `app.yaml` 的 `logging.rotation_mb`（默认 100MB）时再切分；旧文件压缩为 zip，保留 `logging.retention_days`（默认 30 天）。文件名中的日期取文件创建时的本机日期，本机时区不是 UTC+8 时可能与北京日期差一天，以文件内的 `ts` 为准（§5.2） |
| 级别 | `logging.level`，默认 `INFO`；`DEBUG` 另外输出分片写盘等明细。改动在进程重启后生效。`futrisk data record` 不读 `app.yaml`，日志设置固定为默认值 |
| 查看 | `futrisk logs <实例> -n 200`（默认 50 行）、`-f` 跟随（日志轮换时自动接续）、`--console` 看控制台日志、`--raw` 输出原始 JSON。显示的时刻取自 `ts`，是北京时间（墙钟，不是演示的市场时间） |
| 审计 | 人工操作另外写入数据库：风控操作在 `risk-{shard}.db` 的 `audit_events`，REST 访问在 `api.db` 的 `api_audit`，进程事件在 `supervisor.db` 的 `service_events`（03 §4） |

### 4.2 监控

| 手段 | 内容 | 用法 |
|---|---|---|
| 心跳 | 每个服务每 `bus.heartbeat_ms`（1s）一次：状态、配置版本和哈希、时钟版本、队列深度、消息速率、延迟 p50/p99、主循环滞后 `loop_lag_ms`、各 FACT 主题的最新序号 `fact_heads`。同步服务（risk-engine、trade-gateway）由独立线程发送，asyncio 服务由事件循环中的定时任务发送（02 §4.8） | 仪表盘行情与系统页的服务健康表；`GET /api/v1/system/services` |
| `futrisk status` | supervisor、各服务、市场时钟、仪表盘是否健康；不健康时退出码 1 | 放进计划任务定时检查 |
| `futrisk ps` | 进程状态、PID、运行时长、当日重启次数、最近的退出码、心跳状态；有环境变量覆盖或实例不在运行时附 DETAIL 列（覆盖的变量名、最近的错误） | 排查反复重启 |
| `futrisk doctor` | 本机环境：Python、home、中文路径、时区库、时钟同步、pyzmq、SQLite/WAL、配置、端口占用、仪表盘文件、日志、磁盘 | 部署后、换机器后、升级后 |
| webhook 通知 | 严重度不低于 `notify.min_severity`（默认 HIGH）的风险事件，以及已通知告警的解除、确认和重置；行情 `MD_DOWN`、`MD_INTEGRITY`（投票的那一类）、`GAP_UNRECOVERABLE`；EOD 失败与 EOD 警告；服务进入 FAILED；kill switch 没有被全部目标确认；配置提交后 30s 内没有收敛。按 `notify_key` 去重、限流（02 §9.8） | `app.yaml` 的 `notify`（11 §2）；默认渠道 `log` 只写日志 |
| 风控日报 | "数据质量"和"系统运行"两节：数据源可用率、中断和补数次数、服务重启次数和可用率 | 每日检查 |

本期没有把指标导出到监控平台（TD-07），无人值守时依赖 webhook。

### 4.3 重启策略

| 触发 | supervisor 的处理（01 §5.3.6、02 §4.8） |
|---|---|
| 进程退出（`EXITED`） | 立即发现，按退避重启 |
| 连续 3 个心跳周期没有心跳（`HEARTBEAT_LOST`） | 判定挂起，结束进程后重启 |
| 心跳中的 `loop_lag_ms` 连续两次超过 `bus.hang_threshold_ms`（30s）（`LOOP_HANG`） | 判定主循环卡死，结束进程后重启 |
| 启动后 `supervisor.liveness_timeout_s`（20s）内没有存活信号 | 判定启动失败，重启 |
| 5 分钟内失败 5 次（`futrisk kill` 造成的退出不计入） | 停止重试（`GAVE_UP`，状态 FAILED），直到人工 `svc.start` / `svc.restart`（仪表盘"重启"或 `POST /api/v1/services/{instance}/restart`） |

- 退避：1s → 2s → 4s …，上限 30s；连续运行 60s 后退避计数清零（`services.yaml` 的 `supervisor.restart`）。每次拉起前先检查该实例的端口，被别的进程占用时这次启动失败，原因为 `port N in use by pid X (name)`。
- 设计是"崩溃即重启"：内部异常或写库失败后，服务先让自己失效再退出，由 supervisor 拉起，从持久化状态和幂等键确定性地恢复（01 §8.6）。完整版 `v0.1-full` 上测得 risk-engine 带 1 万笔当日成交的冷启动为 1.75s（13 §3）。
- 重启期间下单失败或超时（失效即安全）；风控分片恢复后，锁存的熔断、告警和出入金都保持。
- 停机：`futrisk down` 让 supervisor 按启动的相反顺序停止各服务——向心跳证明确实活着的总线服务发 `ctl.shutdown`，等 `supervisor.shutdown_grace_s`（10s），超时再发 CTRL_BREAK，之后 terminate、kill，最后 supervisor 自己退出。`futrisk down` 本身最多等 `--timeout`（默认 180s）。

### 4.4 备份、保留与恢复

每个交易日 `sys.eod` 到达 `DONE` 之后，各服务对自己的库做维护（03 §8；只有 risk-engine 等到非交易时段才做，其余服务收到 `DONE` 就开始，见本文 §5.2 KD-22）：WAL checkpoint → `VACUUM INTO <home>\backup\<交易日>\<库>.db`（先写临时文件再原子改名，同一天再次备份时覆盖）→ 按保留期分批清理 → 删除 20 个交易日之前的本库备份（api-gateway 先一次清理再备份，也不清理旧备份，本文 §5.2）。

| 库 | 维护者 | 按保留期清理的数据 | 永久保留 |
|---|---|---|---|
| `md.db` | md-gateway | `md_events` 90 天（人工排除或恢复数据源的事件永久保留）及已关闭、不再被引用的 `gaps`；`outbox` 7 天；原始 tick 目录 30 天、规范 tick 文件 365 天 | `settle_inputs`；状态表 `watermarks`、`feed_cursors` |
| `td.db` | trade-gateway | `broker_order_events` 90 天；`outbox` 7 天（`bus.replay_window_days`） | `routed_orders`、`trades`、`route_tombstones` |
| `risk-{shard}.db` | 各风控分片 | `metric_samples` 90 天；`outbox` 7 天 | 委托、成交、告警、风险事件、审计、对账异常、状态机、EOD 状态 |
| `clearing.db` | clearing-worker | `outbox` 7 天 | 结算结果、日初快照、对账、报表索引、回测作业 |
| `api.db` | api-gateway | `api_audit` 180 天；`notifications` 90 天 | API key、配置版本 |
| `supervisor.db` | supervisor | `service_events` 90 天；`outbox` 7 天 | 市场时钟 |
| `sim.db` | —（外部模拟组件，不备份） | — | — |

各库的 `processed_commands` 保留当前和前一个交易日（md.db、td.db、clearing.db 再多保留一个交易日）；outbox 清理后序号也不会回退，序号计数在永不清理的 `outbox_topics` 中。

**恢复**（03 §8）：本期没有恢复命令，按以下步骤手工操作。

1. 停止对应服务（整机恢复时 `futrisk down`）；
2. 用 `backup\<交易日>\<库>.db` 替换 `db\<库>.db`，并删除同名的 `-wal`、`-shm` 文件；
3. **重新生成这个库的流 ID**（03 §4.0 `db_meta`），让订阅方识别出序号已经回退、重新核对（02 §8.2）。只有库函数 `futrisk.db.regenerate_stream_id`，源码环境可以这样调用（已在一个库的副本上验证）；exe 没有 Python 解释器，做不了这一步（§5.2）：

   ```powershell
   # 以 td.db 为例；其他库把模块换成下表中的对应模块
   .venv\Scripts\python -c "from sqlalchemy import create_engine; from futrisk.db import regenerate_stream_id; from futrisk.services.trade_gateway.schema import COMMON; c = create_engine('sqlite:///D:/futrisk/var/db/td.db').connect(); print(regenerate_stream_id(c, COMMON, restored_from='backup/20260928/td.db')); c.commit()"
   ```

   | 库 | 模块 |
   |---|---|
   | `md.db` | `futrisk.services.md_gateway.schema` |
   | `td.db` | `futrisk.services.trade_gateway.schema` |
   | `risk-0.db`、`risk-1.db` | `futrisk.services.risk_engine.schema` |
   | `clearing.db` | `futrisk.services.clearing.schema` |
   | `api.db` | `futrisk.services.api_gateway.schema` |
   | `supervisor.db` | `futrisk.services.supervisor.schema` |

4. 启动服务：它按启动握手从各发布方重放缺失的事实（重放窗口 7 天），并与柜台对账（02 §8.6）。

risk 库丢失时，可以用 clearing 最近一次的日初快照加上 trade-gateway 当日的全部成交重建；告警历史和审计会有缺口，缺口本身要记入审计。

### 4.5 配置版本、两阶段提交与回滚

| 位置 | 内容 |
|---|---|
| `<home>\config\` | 工作目录（编辑这里） |
| `<home>\config\versions\<N>\` | 不可变的版本快照：每次热加载和每次 `futrisk config commit` 各生成一个；被否决的热加载版本目录也保留 |
| `<home>\config\active.json` | 当前生效的版本 `{config_version, path, effective}`；各服务启动时只加载它指向的版本，不读工作目录 |
| `<home>\run\config-2pc\<实例>.json` | 参与方投赞成票后持久化的准备状态 |
| `GET /api/v1/config/versions` | 热加载历史（api.db 的 `config_versions`）：`PREPARING`、`COMMITTING`、`COMMITTED`、`ABORTED`、`FAILED`。`futrisk config commit` 提交的版本不在其中（§5.2） |

提交新版本有两种方式，哪种改动走哪种见 07 §5.4 的表：

- **热加载**（系统运行时）：编辑 → `futrisk config validate --home <home>` → `POST /api/v1/config/reload`（ADMIN，07 §5.4）。api-gateway 把工作目录复制为新版本（编号为已有版本目录和 `config_versions` 中的最大值加 1），先在本地校验，再向 `services.yaml` 中的全部 7 个内部实例发 `ctl.config.prepare`（5s 超时）。全部赞成时先持久化 `COMMITTING`，再改写 `active.json`，再下发提交；有否决、超时或不可达则持久化 `ABORTED` 并下发放弃，消息为 `prepare failed: <实例>: <原因>`，错误码在有否决时为 `BAD_REQUEST`，只是超时或不可达时为 `TIMEOUT` 或 `UNAVAILABLE`（新版本在本地校验就不通过时为 `BAD_REQUEST`，消息 `invalid configuration: ...`）。提交后 30s 内有实例的心跳没有报告新版本时，该版本记为 `FAILED` 并告警；`active.json` 已经指向新版本，落后的实例重启后自动收敛。参与方投赞成票后不会自行放弃，超时或重启后向 api-gateway 查询决定（02 §8.5）。sim-exchange 和 strategy-sim 不参与两阶段提交，只在启动时读取已提交的版本。
- **离线提交**（停机时）：会被热加载否决的改动（端点、数据源、交易参数和交易日历、账户的分片归属）用 `futrisk config commit`：

  ```powershell
  futrisk down --home D:\futrisk\var
  # 编辑 D:\futrisk\var\config\ 下的文件
  futrisk config validate --home D:\futrisk\var
  futrisk config commit --home D:\futrisk\var     # 输出 committed config version N (was M), hash ...: <版本目录>
  futrisk up --home D:\futrisk\var                # 或 futrisk demo --home D:\futrisk\var --headless
  ```

  它先校验工作目录（无效时打印 `INVALID  <目录>: <原因>`，退出码 1，不提交），再确认本 home 的 supervisor 没有在运行（在运行时拒绝；`--force` 强制提交，服务下次启动时才加载），然后把工作目录复制为下一个版本目录并改写 `active.json`。这样提交的版本不经过两阶段提交，也不经过各服务的 prepare 检查（例如风控规则能否编译），`effective` 为空，不写入 `config_versions`。clearing-worker 启动时发现结算相关的文件（`products.yaml`、交易日历）变了，就从当时的交易日起用新版本结算（`src/futrisk/services/clearing/confighist.py`），所以交易参数和交易日历的改动应当在当日 EOD 完成之后提交、在下一个交易时段开始之前启动，新参数从下一个交易日起生效。
- **回滚**：版本号只增不减。把工作目录恢复成旧内容（从 `config\versions\<旧版本>\` 复制回 `config\`），再热加载或离线提交一次，得到一个内容与旧版本相同的新版本。

### 4.6 EOD 之后的检查

| 检查 | 命令或位置 |
|---|---|
| EOD 是否完成、有没有警告 | `futrisk settle days`（各交易日最新的阶段）；警告看仪表盘报表页结算日表的警告数，展开可看明细 |
| 对账检查是否全部通过 | `futrisk settle show --td <交易日>` 的检查结果；日报 |
| 报表是否生成 | `futrisk report list --td <交易日>`；`<home>\reports\<交易日>\` |
| 备份是否生成 | `<home>\backup\<交易日>\` 下每个库一个文件 |
| 交易参数或交易日历要改 | 停机 → 编辑 → `futrisk config commit` → 在下一个交易时段开始前启动（§4.5、TD-09） |
| 磁盘空间 | `futrisk doctor` 的 `disk` 一项 |

## 5. 生产差距与技术债

### 5.1 已知的技术债

下表按 01 §8.17 和 §11 整理，2026-10-08 逐项对照代码核对过。编号沿用 01 §11。

| 能力或问题 | 编号 | 现状（已核对代码） | 上线前应当做什么 |
|---|---|---|---|
| 高可用 | TD-01 | 每个分片、每个服务只有一个实例；主机故障只能重启 | 风控"主备 + 租约"切换；api-gateway 多副本还要先完成 TD-05 |
| 传输加密与身份认证 | TD-02 | 代码中没有 CURVE；总线身份（DEALER identity）是自报的，演示环境只绑定 127.0.0.1 | 跨主机前启用 ZeroMQ CURVE 和白名单、HTTPS |
| 双人复核 | TD-06 | 熔断重置、kill switch 关闭、结算价覆盖只需一个人（一个 key）操作，没有审批流程 | 发起人提交、另一位 RISK_MANAGER 批准后才执行 |
| 真实交易所接入 | 01 §8.17 | `CtpBrokerAdapter` 只有接口，每个操作都抛出 `NotImplementedError`；行情只有模拟源和新浪 | 实现 CTP 行情和交易适配器，接入仿真环境联调 |
| 强制减仓自动执行 | TD-08 | `LIQUIDATE` 只生成减仓计划，确认后需要人工下平仓单 | 由 risk-engine 按计划分批下平仓单，走同一条事前检查 |
| 交易参数和交易日历不能热加载 | TD-09 | 热加载在 prepare 阶段被 md-gateway 否决（它只允许 `feeds.arbitration` 在运行时变更）；risk-engine 本身也只把它们暂存到下次启动（心跳的 `staged_config`）。只能停机后用 `futrisk config commit` 离线提交再启动（§4.5） | 运维上在 EOD 完成之后停机提交、在下一个交易时段开始前启动；以后改为在换日时切换 |
| 查询没有分页 | TD-10 | `QueryOrders`、`QueryFills`、`QueryAlerts` 没有游标或条数限制 | 游标分页 |
| 不支持 wheel 安装 | TD-11 | 默认配置和历史数据按源码目录定位 | 默认配置作为包数据读取，历史数据目录可配置 |
| 指标平台 | TD-07 | 指标只在心跳和仪表盘中 | 导出指标并接入监控告警平台 |
| api-gateway 单实例 | TD-05 | 通知去重和配置协调有状态 | 拆给选主实例，库迁到 PostgreSQL |
| 手续费和保证金是示例值 | TD-04 | 手工维护 `products.yaml`；自动刷新工具没有实现 | 按交易所和经纪商的最新公告维护，上线前逐项核对 |
| 交割、组合保证金、申报费 | TD-03 | 未实现 | 按业务需要实现 |
| live 档新浪转发 | TD-12 | sim-exchange 不转发真实价格（没有 `SinaRelay`） | 生产环境直接接 CTP 行情和厂商源 |
| 补数区间的事后审计 | TD-13 | `RETRO_BREACH` 影子评估未实现（`retro_breaches` 表存在，没有写入方） | — |
| 公司层高水位和锁存 | TD-14 | 公司层高水位只在内存中；公司级锁存没有人工解除命令 | 持久化并增加记审计的重置命令 |
| 研究级回测 | TD-15 | 精简版只有两个方案和确定路径；路径集合、阈值扫描等大规模实验在完整版 `v0.1-full` 中，回放吞吐为 9265 tick/s（13 §3），低于 10 万的目标 | 恢复这些实验时再优化：标量快路径估值、向量化策略 |
| 部分验收度量没有自动化 | TD-16 | QS-05 事件哈希基线、QS-10 仪表盘延迟、性质测试例数 | 补自动化测试 |
| 熔断期间换月开仓 | TD-17 | 锁存期间事前检查拒绝一切开仓（`REJECT_ACCOUNT_HALTED`），不区分换月时在新合约上的开仓 | 识别换月单，按平掉的手数放行新合约上的开仓并写审计 |
| 性能目标 | 01 §10.2 | 下单往返、tick→判定等在负载较高的笔记本上未达标，待安静机器复测（13 文档） | 检出完整版 `v0.1-full`，在生产硬件上用 `futrisk bench` 复测，必要时按 01 §8.12 的扩展顺序优化 |

### 5.2 本次核对新发现的问题

以下问题在核对 07、08、11 与代码时发现（含实际运行中发现的），01 §11 尚未列出，建议补入技术债。2026-10-08 复核时，草稿中的两项已经解决，不再列出：停机状态下提交只能重启生效的配置改动，现在有 `futrisk config commit`（`tests/unit/cli/test_config_commit.py`）；`accounts.yaml` 的 `initial_positions` 在首日建账时成为昨仓（`src/futrisk/cli/ops_cmd.py` 的 `seed_request_body`）。

| # | 问题 | 影响 | 建议 |
|---|---|---|---|
| 1 | 配置热加载、出入金、结算价覆盖、官方结算价导入、日初快照更正、成交导入、对账异常处理、数据源排除只有 REST 入口，没有 CLI 命令，仪表盘也没有（07 §4.12） | 运维要手写带 `Idempotency-Key` 的 HTTP 请求 | 在 CLI 中补齐这些命令 |
| 2 | 03 §8 的恢复流程依赖"恢复脚本"重新生成流 ID，代码只提供了库函数 `regenerate_stream_id`，没有脚本或命令；exe 中没有 Python 解释器，无法调用它 | 从备份恢复只能在源码环境手工调用函数（§4.4），容易漏掉这一步，订阅方识别不出序号回退 | 增加 `futrisk db restore <库> <备份>`：检查服务已停止、替换文件、重新生成流 ID |
| 3 | api-gateway 每天备份 `api.db`，但不清理旧备份（其他服务都只保留 20 个交易日） | `backup\` 目录随时间增长；含 `api.db` 的日期目录永远不会被删除 | 复用其他服务的备份清理 |
| 4 | 具名 API key 的明文从 api-gateway 进程的环境变量读取，进程继承 supervisor 启动时的环境；热加载 `app.yaml` 时 api-gateway 会重新同步 key，但读的仍是自己进程启动时的环境 | 新增或轮换 key 时，改 `app.yaml` 并热加载还不够，要先设置环境变量再重启整个系统（docker 要改 compose 的环境变量） | 支持从 `<home>\secrets\` 下的文件读取 key，或由 supervisor 在重启服务时刷新环境 |
| 5 | `futrisk backtest run <名字>` 只在包内 `config\backtest\` 中查找，`<home>\config\backtest\` 被复制进每个配置版本却从不读取；`futrisk data` 也读包内默认的 `feeds.yaml` | 修改 home 中的这些文件没有任何效果，容易误解 | 先查 home 再查包内，或不再把它们复制进 home |
| 6 | 日志的 `ts` 和每日切分用北京时间，但文件名中的日期取本机日期（loguru 按本机时间格式化文件名） | 本机时区不是 UTC+8 时，文件名的日期可能与北京日期差一天；演示模式下日志时间是墙钟，不是市场时间 | 自定义文件命名；日志同时记录市场时间 |
| 7 | CLI 表格原样打印 API 返回的浮点数（如 `futrisk accounts` 的当日盈亏、`futrisk fault list` 的剩余秒数） | 只影响可读性 | 金额按 0.01 元、时长按 0.1s 格式化显示 |
| 8 | `futrisk config commit` 的缺口：提交的版本不写入 api.db 的 `config_versions`；启动时不提示工作目录有未提交的改动（只有 `futrisk doctor` 分别列出工作目录和生效版本的哈希）；判断"系统是否在运行"时按**编辑后**的工作目录中的 supervisor 端点发 `ctl.ping`，而且任何应答都算（不核对 `home_id`） | 版本历史不完整；运行中的系统如果改了 supervisor 的端点，不加 `--force` 也能提交；另一个 home 的 supervisor 占着这些端口时会误拒 | 提交时同时写入版本历史（或由 api-gateway 启动时补录）；按生效版本的端点探测并核对 `home_id`；启动时比较工作目录与生效版本 |
| 9 | 删除配置字段和重新基线数据库时没有升级路径：配置模型不接受多余的键，md.db 的迁移被重新基线为版本 1（§2.4） | 2026-10-07 之前创建的 home（含 docker 的卷）升级后无法直接启动，要手工删除 `feeds.yaml` 中的 6 个参数并离线提交、移走 md.db | 对已删除的配置键给出警告而不是拒绝，或提供一次性的配置迁移；数据库改动用追加迁移，不再重新基线 |

#### 代码复核确认的缺陷

交付前的独立评审报告过一批疑似缺陷。2026-10-08 在当前代码上逐条复核（阅读代码，并在进程内构造 `EngineCore`、md-gateway 流水线和清算函数做探针实验，不启动服务），其中 12 项仍然存在（KD-01~KD-08、KD-10~KD-13）；KD-09、KD-14 是复核时另外发现的，KD-15~KD-24 是本次按代码重建文档和截图时发现的（KD-19 在进程内复现，其余由阅读代码确认）；另有 2 项已在 md-gateway 精简中消失：冲突保持释放时重放 tick 被当作 LIVE 重新发布（冲突保持已删除，重放和快照不会成为 LIVE），以及 `feeds.feed_b.timeout_ms` 未被使用（现在是每次轮询的读超时）。功能封版期间这些缺陷不修复，按"建议"列入后续工作；相关文档已按代码的实际行为描述。编号 KD（known defect）与 §1 中部署形态的 D1~D3 无关。

| # | 问题 | 影响 | 建议 |
|---|---|---|---|
| KD-01 | 事前检查的 what-if（`core/pretrade.py` 的 `_whatif`）只按"当前持仓 + 本单"估算，不计同账户在途（已报未成）的开仓单 | 多笔各自不越线的开仓单可以先后放行，全部成交后越过禁开线（实测 6 笔 6 手 IF 后杠杆 4.86，超过 4.0；默认规则下 3 笔 2 手 IF 后 `product.utilization` 为 2.7） | what-if 把在途开仓单的名义和保证金并入估值（与资金检查"含在途开仓"的口径一致），并补测试 |
| KD-02 | 账户范围规则的行情质量门控：非 `OK` 持仓占总名义超过 `quality_stale_tolerance`（0.10）或 md-gateway 心跳丢失时，规则状态机完全不动 | 其余 `OK` 合约上的真实亏损即使超过 −3%（实测 −5.8%）也不告警、不熔断、不生成减仓计划，只靠行情质量检查和 what-if 拦截开仓 | 门控期间只暂停"解除"、不暂停"升级"，或只用 `OK` 部分单独评估亏损 |
| KD-03 | 最后一个可信源"连着但不出新行情"（Feed A 只发心跳、`head_seq` 不前进，或 Feed B 每次返回同一份行情、应答中的时间戳照常前进）时，合约质量保持 `OK` | risk-engine 一直按停止更新的标记价估值，规则、what-if 和行情质量检查都不起作用，只有 `MD_QUIET` 事件提示 | 连续交易时段内长时间无成交且没有第二个源佐证时把质量降为 `STALE`，或把 `MD_QUIET` 纳入质量判定 |
| KD-04 | 熔断确认 `confirm: {states: 2, ms: 1000}` 的第二个状态可以来自同账户任一其他持仓合约的 tick（`core/engine.py` 的 `touch_market` 调用），PENDING 回到阈值内立即撤销且没有滞回 | 一个落在涨跌停带内的坏点可以被其他合约的下一笔行情确认并锁存账户；亏损在阈值附近来回时从不锁存，账户在 `HALTED` 与 `RESTRICTED` 之间来回切换（实测 20s 内约 40 次） | 确认只计越线所依赖合约的新状态；PENDING 撤销加滞回或最短保持时间 |
| KD-05 | `risk.reset` 把锁存的熔断改为 `REARMED`（布防线 = min(阈值, 重置时的值 − `extra`)，`core/rules.py` 的 `rearm_line`）后，what-if 仍按级别的原阈值（−0.03）判定 | 当日亏损仍低于 −3% 时，按标记价或更差价格的开仓全部被 `REJECT_WHATIF_BREACH` 拒绝，重置实际上没有恢复开仓 | what-if 对 `REARMED` 及由它进入的 PENDING 级别改用状态机的布防线 |
| KD-06 | `accounts.yaml` 的 `products` 只被 strategy-sim 和回测使用，事前检查不校验账户可交易的品种 | 账户可以开授权以外品种的仓位（实测 ACC4 开 rb、cu，ACC5 开 IF 均被接受） | 在事前检查中加品种白名单（空列表表示不限）和对应的拒单码 |
| KD-07 | `config/products.yaml` 中大商所 `i`、`m` 节前保证金条目的 `effective_from` 是 `20260930`，而交易所通知自 2026-09-29 结算时起调整为 10%（涨跌停 8% 从 20260930 起是对的） | 20260929 的结算按旧费率计算保证金、可用资金和风险度，比期货公司口径偏低 | 保证金条目改为 `effective_from: "20260929"`，同步 `tests/unit/core/test_core_instruments.py` 中的断言 |
| KD-08 | 规则级别的 `params` 是 `dict[str, Any]`，配置模型、规则编译和热加载 prepare 都不校验键名、类型和取值 | 拼错的键（如 `max_order_facter`）静默失效；`block_open: "false"` 反而禁开；类型错误（如 `step: "x"`）在级别触发时抛异常，risk-engine 致命错误并重启 | 按动作定义 `params` 的 pydantic 模型（`extra="forbid"`、类型与范围），在 `config validate` 和 prepare 中校验 |
| KD-09 | `futrisk config validate` 和离线的 `futrisk config commit` 只做 pydantic 校验，不编译规则，也不检查 `metric` 名和 `scope` | 拼错的 `metric` 在冷启动后第一次评估时抛 `KeyError`，risk-engine 致命错误并反复重启；未知 `scope` 启动即失败；只有热加载的 prepare 会拦下这两种错误 | 把规则编译和指标名检查放进配置加载和 `config validate` |
| KD-10 | 资金恒等式 `EQUITY_IDENTITY` 和公司汇总 `FIRM_SUM` 把按持仓取整到分的盈亏与未取整的交易所公式精确比较；导入官方结算价时不校验 tick 网格（人工覆盖会校验） | 官方价 × 乘数 × 手数不是整分时（如 rb 3105.1234）两项检查 FAIL，EOD 进入 `FAILED`、不换日，而明细显示差额 0.00 | 导入官方价时与人工覆盖一样校验 tick 网格；恒等式两边按同一取整口径比较，明细给出未取整的差额 |
| KD-11 | EOD 的出入金核对（`CASH_DIGEST`）只比较 clearing-worker 本地副本与各分片冻结应答中的笔数和带符号净额，是两方核对；成交才是三方核对 | 金额互换等相互抵消的差异核对不出 | 需要更强的核对时为出入金摘要加逐笔哈希 |
| KD-12 | 仪表盘的实时批处理（`web/src/realtime/batcher.ts`）对 `orders:{account}` 主题的委托更新按合约合并 | 同一刷新窗口（200ms）内同一合约两张委托的更新只保留后一张，前一张的最新状态（如 `FILLED`）在委托表里丢失，直到下一次更新或快照 | 委托更新按 `client_order_id` 合并，并补带两张同合约委托的单元测试 |
| KD-13 | 报表页"提交回测"对话框列出 `stress`、`sweep`、`minute`、`ticks` 四个不存在的配置（仓库只有 `default`、`breaker_synthetic`），覆盖参数的占位示例也不合法 | 选这四项或照示例填写覆盖，作业提交后立即 `FAILED` | 从后端获取可用的配置名，占位示例改为 `BacktestConfig` 接受的键 |
| KD-14 | `config/products.yaml` 没有大商所 `i`、`m` 节后恢复原保证金和涨跌停的条目 | 交易所恢复后，10% 保证金和 8% 涨跌停一直生效，直到手工修改 | 按恢复公告追加带 `effective_from` 的条目（自动抓取公告见 TD-04） |
| KD-15 | 行情质量门控期间，规则状态机只清零"解除"的保持计时，PENDING 的确认计时（`pending_since_ns`）不暂停（`core/state_machine.py` 的 `evaluate` 与 `_confirmed`；由代码阅读发现，未做探针） | 越线后行情变为不可信、持续超过 `confirm.ms`，恢复后的第一次评估（包括定时复核）即可按时长确认并锁存，不再等待新的行情状态 | 质量门控期间同时暂停或清零确认计时，恢复后重新计算 |
| KD-16 | supervisor 用市场时间记录服务的启动时刻，`futrisk ps` 用市场时间相减得到 UPTIME（`cli/ops_cmd.py`；supervisor 自身一行用墙钟）；2026-10-08 截图会话中发现 | 演示模式跳转市场时钟后，各服务的 UPTIME 把跳过的时间算进去（例如从 09:30 跳到 15:29 后显示 6 小时以上），只影响显示 | 另记一个墙钟或单调时钟的启动时刻专用于 UPTIME |
| KD-17 | `services.yaml` 的 `bus.replay_window_days` 只有 trade-gateway 和 supervisor 读取；md-gateway（`md_gateway/service.py`）、risk-engine（`risk_engine/store.py`）和 clearing-worker（`clearing/settings.py`）在代码中固定保留 7 天 | 修改这个配置只改变两个服务的 outbox 保留期和重放窗口，其余服务不变 | 各服务统一从配置读取 |
| KD-18 | supervisor.db 的迁移 1 用当前代码中的 `ProcessState`、`ClockMode` 枚举生成表结构和取值约束（其他库把枚举取值固定写在迁移里），也没有测试钉住它的校验和 | 以后修改这些枚举的取值会改变迁移 1 的校验和，已有的 supervisor.db 将拒绝启动；新增的 `ServiceChangeReason` 取值只能写入修改之后新建的库 | 把迁移中的枚举取值固定下来，并像其他库一样用测试钉住校验和 |
| KD-19 | 配置热加载修改一个已锁存、仍然存在的规则级别时，状态机直接采用新定义且不写审计（`core/engine.py` 的 `_reload_machines`）；账户状态由级别的动作和状态机自己的锁存标志推导（`core/engine.py` 的 `_contributions`、`core/actions.py` 的 `action_outcome`），所以只改阈值或 `latch` 时熔断仍然锁存，而把动作从 `CIRCUIT_BREAK` 改成别的动作时账户立即离开 `HALTED`；只有删除或禁用该级别时才保留原规则并写审计 | 例如把锁存中的 `daily_loss/halt` 的动作改为 `ALERT` 后，账户从熔断变为限制下单（`restrict` 级仍然生效）、锁存消失，开仓只受单笔上限减半和禁止亏损加仓的约束（进程内复现）：熔断可以被一次配置变更静默解除 | 锁存期间沿用旧定义，直到 `risk.reset` 或换日（与删除路径一致），并写审计 |
| KD-20 | supervisor（含市场时钟）只在启动时读取配置，没有注册配置提交的回调，但心跳中报告新的配置版本 | 需要重启 supervisor 才生效的改动（11 中已标明）提交后，仪表盘的服务健康表显示 supervisor 已是新版本，容易误以为已经生效 | 心跳报告实际在用的版本，或在 prepare 中否决需要重启才生效的改动 |
| KD-21 | 柜台持仓对账（`core/engine.py` 的对账处理）：账户已有未解决的非阻断 `POSITION_MISMATCH` 时，新的阻断性差异不再登记；账户有任何挂单时，一致的对账既不清零连续不一致的计数，也不自动关闭差异；一次一致的对账会把该账户所有未解决的 `POSITION_MISMATCH`（包括 `UNDECODABLE_FACT`、`CLOSE_EXCEEDS_BOOK`、`LATE_TRADE`）一并标为已解决 | 真实的持仓差异可能被较早的非阻断记录掩盖，或被一次碰巧一致的对账关闭，与 risk-engine 服务代码中注释描述的处理方式不符 | 按差异类型分别登记和解决，阻断性差异不受非阻断记录影响 |
| KD-22 | md-gateway、trade-gateway、clearing-worker、api-gateway、supervisor 的日常维护（含 `VACUUM INTO` 备份）没有限制在非交易时段，只有 risk-engine 做了限制 | 补跑的 EOD 在交易时段内完成时，这些服务会在盘中执行备份和清理，占用各自的写库线程 | 与 risk-engine 一样只在非交易时段执行 |
| KD-23 | 风控分片在账户的日初快照加载之前收到 `order.cancel_all`、`risk.reset`、`risk.halt`、`cash.move` 时，可重试的 `UNAVAILABLE`（no SOD）应答也按幂等键（`command_id`，`cash.move` 为 `movement_id`）记入 `processed_commands`（`core/engine.py` 的 `_record`；`order.submit` 不记录，没有这个问题） | 快照加载后用同一个键重试，只会得到当初保存的 `UNAVAILABLE`（标为重复），要换一个新的键才能执行；只影响启动窗口 | 可重试的错误不写入幂等记录 |
| KD-24 | `products.yaml` 中交易所的 `settle_fallback` 是不做校验的字符串列表（`common/config/products.py`）；结算价回退链依次尝试到未知的名字时才报错（`core/settle_price.py`） | 拼错的回退名（例如把 `FULLDAY` 写成带下划线的形式）能通过 `futrisk config validate` 和离线的 `futrisk config commit`（`products.yaml` 不能热加载），直到某个合约当日无成交、结算走到这一步时才失败，当日 EOD 进入 `FAILED` | 在配置模型中把回退名限定为已实现的取值 |

### 5.3 上线前检查清单

| 类别 | 检查项 |
|---|---|
| 安全 | 启用 CURVE 和白名单（TD-02）；为每个人配置具名 key（先设置环境变量再启动，§5.2 第 4 项）并停用 bootstrap key；HTTPS；双人复核（TD-06） |
| 接入 | CTP 行情与交易适配器；与柜台对账的流程和时点；EOD 调度（`app.eod_start_at`）排在经纪商出入金截止之后 |
| 参数 | `products.yaml` 的费率、保证金率、涨跌停、交易时段按最新公告核对；交易日历使用官方安排（`config/calendar/trading_days.csv` 中 2027 年目前为估计值） |
| 风控 | `risk_rules.yaml` 的阈值和按账户覆盖经风控负责人确认（04 §4）；熔断演练：越线、锁存、重置、换日 |
| 运行 | 主机时钟同步（NTP / PTP；Windows 上 `futrisk doctor` 的 `clock sync` 为 PASS；docker 的宿主机同样要求，§3.3）；进程管理换成 systemd / K8s；日志和指标接入平台（TD-07）；webhook 通道测试 |
| 数据 | 备份复制到另一台主机；演练一次从备份恢复（§4.4）；磁盘容量按 tick 和日志的保留期估算 |
| 变更 | 演练一次离线提交（§4.5）和一次升级（§2.4），确认新版本能加载已有的配置和各个库 |
| 性能 | 检出完整版 `v0.1-full`，在生产硬件上运行 `futrisk bench`，与 01 §10.2 的目标对比 |

---

## 附录 A 修订记录

| 版本 | 日期 | 说明 |
|---|---|---|
| v0.1 | 2026-09-25 | 初版：exe 构建参数、冒烟测试、产物与安装升级；docker compose 的服务、卷、健康检查、时钟与差别；日志、监控、重启策略、备份保留与恢复、配置版本与回滚（含离线提交步骤，已在测试 home 上验证）、EOD 后检查；按 01 §8.17 与 §11 核对代码后的技术债表，新发现问题 10 条，上线前检查清单 |
| v0.2 | 2026-09-26 | 代码冻结后的核对：日志的 `ts` 与每日切分改为北京时间（与本机时区无关），只有文件名中的日期仍取本机日期，§4.1 与生产差距第 7 项随之更正；exe 重新构建，产物大小与冒烟测试结果见 06 §7.5 |
| v0.3 | 2026-09-26 | 范围精简（完整版保留为 git 标签 `v0.1-full`）：clearing-worker 不再挂载 `ticks` 卷；性能复测改为检出完整版；生产差距表中回放吞吐一行按精简版改写 |
| v0.4 | 2026-10-07 | docker compose 首次完整运行（06 §7.7）：本文 §3 开头的验证状态更新；§3.3 增加宿主机时钟不准时虚拟机时钟跳变的说明 |
| v1.0 | 2026-10-08 | 按代码重新核对全文（实现对齐）。§1 增加仪表盘构建产物不在版本库中（`.gitignore` 的 `dist/`），源码方式要先构建。§2 按 `build_exe.ps1` 和 `futrisk.spec` 核对构建步骤、17 项冒烟测试的通过条件和成功 / 失败输出，产物大小改为引用 06 §7.5；§2.4 增加升级时旧 home 无法直接启动的两种情况（`feeds.yaml` 删除 6 个仲裁参数、md.db 重新基线）及处理办法。§3 按 `compose.yml`、`Dockerfile`、`init_config.py`、`seed_books.py` 核对服务、启动条件、健康检查、卷和镜像，写入 2026-10-07 实测要点，并区分那次运行中的两段拒收：08:58 Feed B 被排除源于开盘集合竞价期间的陈旧快照被判为不在交易时段（md-gateway 校验的缺陷，2026-10-08 已修正），时钟不准引起的是 09:03 起的时间戳超前拒收；未来时间戳的判定阈值由"2 秒"更正为 1 秒（`future_skew_s`）；增加宿主机 CLI 经 `--api` 访问容器、docker 下离线提交的思路（未实测）。§4.1 日志按 `common/logging.py` 核对，删除"逐笔行情只在 DEBUG 输出"；§4.2 webhook 来源按 notifier 补全，心跳的发送线程按 02 §4.8 更正；§4.3 冷启动改为引用 13 §3；§4.4 增加已验证的流 ID 重新生成命令；§4.5 改写：只能重启生效的改动用 `futrisk config commit` 离线提交（替代手工改写 `active.json` 的步骤），交易参数和交易日历的热加载现在被 md-gateway 否决；§4.6 的 EOD 警告改看仪表盘（`settle days` 只列阶段）。§5.1 TD-09 按现状改写，补入 TD-17；§5.2 删除已解决的 2 项，新增 `config commit` 的缺口和升级路径 2 项；§5.3 增加时钟同步和变更演练；§5.2 末尾新增"代码复核确认的缺陷"表（KD-01~KD-24：交付前独立评审报告的疑似缺陷在当前代码上复核后仍存在的 12 项，加上复核和本次重建文档、截图时发现的 12 项），2 项已在 md-gateway 精简中消失；独立交叉审查（2026-10-08）对照代码复核全文，更正 18 处（`npm.cmd`、旧 md.db 的实际报错、只有 risk-engine 等到非交易时段做维护、热加载应答的错误码、日志的 `corr_id`、`processed_commands` 的保留期、TD-15 的吞吐，以及 KD-02~KD-05、KD-17、KD-19、KD-22 的用词与细节）；D 表的独立复核（同日）对照代码和进程内探针逐行核对 KD-01~KD-24，改正 6 处（来源计数、KD-03 中 Feed B 的条件、KD-19 的例子按复现结果改为限制下单、KD-21 的挂单条件、KD-23 的幂等键、KD-24 的提交途径） |
