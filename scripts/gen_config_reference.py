"""Generate the configuration tables of docs/11-config-reference.md (FR-DOC-11) from the pydantic models.

Every field of the configuration models - ``futrisk.common.config.*`` (``app.yaml`` ... ``sim.yaml``),
``futrisk.sim.exchange.scenario.ScenarioFile`` (``scenarios/*.yaml``),
``futrisk.backtest.config.BacktestConfig`` (``backtest/*.yaml``) and ``futrisk.sim.bots.config.BotsConfig``
(``bots.yaml``) - becomes one table row: key path, type, required, description, default, hot-reload
effect. Types, required flags and defaults come from the models; the Chinese descriptions and the
hot-reload effects are kept in this script (the models carry no descriptions). A model field without a
description here is an error, so a new configuration key cannot go undocumented. The list of risk
metrics (``@register_metric``) is generated from the registry as well.

Usage (repository root)::

    .venv\\Scripts\\python scripts/gen_config_reference.py             # rewrite the generated blocks
    .venv\\Scripts\\python scripts/gen_config_reference.py --check     # exit 1 when the doc is out of date
    .venv\\Scripts\\python scripts/gen_config_reference.py --print app # print one block to stdout

Generated blocks sit between ``<!-- gen:{name} begin -->`` and ``<!-- gen:{name} end -->`` markers in the
document; everything outside the markers is hand-written and left untouched.
"""

from __future__ import annotations

import argparse
import contextlib
import re
import sys
import types
import typing
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
DOC = ROOT / "docs" / "11-config-reference.md"
sys.path.insert(0, str(ROOT / "src"))

from pydantic import BaseModel  # noqa: E402
from pydantic.fields import FieldInfo  # noqa: E402

from futrisk.backtest.config import BacktestConfig  # noqa: E402
from futrisk.common.config.accounts import AccountsConfig  # noqa: E402
from futrisk.common.config.app import AppConfig  # noqa: E402
from futrisk.common.config.feeds import FeedsConfig  # noqa: E402
from futrisk.common.config.products import ProductsConfig  # noqa: E402
from futrisk.common.config.risk_rules import RiskRulesConfig  # noqa: E402
from futrisk.common.config.services import ServicesConfig  # noqa: E402
from futrisk.common.config.sim import SimConfig  # noqa: E402
from futrisk.sim.bots.config import BotsConfig  # noqa: E402
from futrisk.sim.exchange.scenario import ScenarioFile  # noqa: E402

# ------------------------------------------------------------------ hot-reload effects (docs/11 §1.3)
IMM = "即时"
NTD = "下一交易日"
REJECT = "拒绝热改"
FIRST = "仅首次启动"
PER_RUN = "每次运行时读取"


def restart(who: str) -> str:
    """``重启 supervisor`` / ``重启全部服务`` (a space only before an ASCII name)."""
    return f"重启 {who}" if who[:1].isascii() else f"重启{who}"


@dataclass
class Section:
    """One configuration file and its model."""

    name: str  # marker name: <!-- gen:{name} begin -->
    file: str
    model: type[BaseModel]
    readers: str  # who reads it
    summary: str  # hot-reload summary for the overview table
    default_effect: str
    effects: dict[str, str]  # key-path prefix -> effect (longest prefix wins)
    desc: dict[str, str]
    placeholders: dict[str, str] = field(default_factory=dict)  # dict field path -> key placeholder

    def effect(self, path: str) -> str:
        best, best_len = self.default_effect, -1
        for prefix, eff in self.effects.items():
            if (path == prefix or path.startswith(prefix + ".") or path.startswith(prefix + "[")) and len(prefix) > best_len:
                best, best_len = eff, len(prefix)
        return best


# ------------------------------------------------------------------ descriptions
APP_DESC = {
    "mode": "运行模式。`demo`：市场时间 = 本机时间 + 全系统共享的偏移（可跳转），开放 `/demo/*` 演示接口和虚拟交易所控制接口，"
            "`futrisk demo` 自动建账；`prod`：市场时间就是本机时间，演示接口返回 403",
    "demo": "演示模式参数（`mode=demo` 时使用）",
    "demo.start_at": "首次启动时的市场时间，ISO 格式，不带时区即北京时间；`null` 表示下一个交易日的 08:58（开盘集合竞价之前）。"
                     "只在 supervisor.db 里还没有时钟状态时使用，`futrisk demo --start-at` 可以覆盖",
    "demo.pre_close_at": "`futrisk clock jump --to pre-close`（`PRE_CLOSE`）跳到的当日时刻，北京时间 HH:MM",
    "demo.lead_s": "`futrisk clock jump --to next-open`（`NEXT_OPEN`）跳到下一个交易时段开始前多少秒",
    "demo.auto_seed": "`futrisk demo` 发现本 home 还没有账本时，按 `accounts.yaml` 调用 `POST /settlement/seed-sod` 首日建账",
    "eod_start_at": "EOD 自动开始的市场时间（北京时间 HH:MM）：clearing-worker 的调度器在当日该时刻之后启动结算流程。"
                    "生产环境应当晚于经纪商的出入金截止时间（一般 17:00）",
    "logging": "日志参数（01 §8.7）",
    "logging.level": "日志级别：`TRACE`、`DEBUG`、`INFO`、`WARNING`、`ERROR`",
    "logging.retention_days": "日志文件保留天数，超期的压缩文件自动删除",
    "logging.rotation_mb": "单个日志文件超过该大小（MB）时切分；此外每天切分一次",
    "api_keys": "具名 API key。存在可用的具名 ADMIN key 时，bootstrap key 自动停用并删除文件",
    "api_keys[].name": "key 的名字，即 `key_id`，作为操作者写入审计；不能重复，不能叫 `bootstrap`",
    "api_keys[].role": "角色，权限矩阵见 02 §9.6",
    "api_keys[].key_env": "保存明文 key 的环境变量名。明文至少 16 个字符，api.db 只存 SHA-256；变量缺失或过短时该 key 停用",
    "notify": "告警通知（webhook，02 §9.8）",
    "notify.webhook_url_env": "保存 webhook URL 的环境变量名；`null` 或变量为空时只写日志",
    "notify.channel": "消息格式：企业微信 `wecom`、钉钉 `dingtalk`、飞书 `feishu`，或 `log`（只写日志）",
    "notify.min_severity": "`risk.event` 低于该级别不发通知；CRITICAL 从不被抑制",
}

SERVICES_DESC = {
    "profile": "档位名称（仓库自带 `demo`，docker compose 用 `docker`），只作标识",
    "bus": "内部总线参数（02 §4）",
    "bus.heartbeat_ms": "每个服务发送心跳 `sys.heartbeat.{instance}` 的周期（毫秒）",
    "bus.peer_timeout_missed": "连续丢失多少个心跳周期判定对端 DOWN",
    "bus.hang_threshold_ms": "心跳里的 `loop_lag_ms` 连续两次超过该值，supervisor 判定主循环卡死并重启该进程",
    "bus.pub_hwm": "PUB 套接字的发送高水位（条），超出时按 02 §4.9 丢弃最新值类消息",
    "bus.md_pub_hwm": "md-gateway 的 PUB 发送高水位（条）",
    "bus.router_hwm": "ROUTER 套接字的高水位（条）",
    "bus.replay_window_days": "FACT 主题的重放窗口：outbox 中已发布的消息保留的天数",
    "bus.gc": "服务进程的垃圾回收策略（01 §11 RK-01）",
    "bus.gc.enabled": "是否启用自动垃圾回收",
    "bus.gc.threshold": "启动时设置的 `gc.set_threshold` 三代阈值；调大第 0 代阈值以减少回收停顿",
    "bus.gc.freeze_on_running": "服务报告 RUNNING 时执行一次 `gc.collect()` 和 `gc.freeze()`，启动期创建的对象此后不再被扫描",
    "instances": "每个内部服务实例的端点；实例名见 README §3.3（分片实例为 `risk-engine-{shard}`）",
    "instances.{instance}.pub": "PUB 端点（`tcp://host:port`），发布事件和心跳",
    "instances.{instance}.router": "ROUTER 端点，接收命令",
    "instances.{instance}.http": "HTTP 监听地址，只有 api-gateway 使用（REST、WebSocket、仪表盘）",
    "instances.{instance}.shard": "risk-engine 的分片号，与 `accounts.yaml` 的 `shard` 对应",
    "instances.{instance}.coordinator": "是否兼任 coordinator（汇总公司层指标）；只能有一个分片为 `true`",
    "supervisor": "进程守护参数（01 §5.3.6）",
    "supervisor.startup_order": "启动顺序；停机按相反顺序。每一步只等上一个进程存活，不等它进入 RUNNING",
    "supervisor.external": "外部组件：不在总线上、没有总线心跳，按进程存活和健康检查判断",
    "supervisor.restart": "崩溃后的重启策略：指数退避",
    "supervisor.restart.backoff_initial_ms": "第一次重启前的等待（毫秒），之后每次翻倍",
    "supervisor.restart.backoff_max_ms": "重启等待的上限（毫秒）",
    "supervisor.restart.stable_reset_s": "连续运行满该秒数后，退避计数清零",
    "supervisor.liveness_timeout_s": "进程启动后在该秒数内没有存活信号即判定启动失败；5 分钟内失败 5 次后停止重试（`GAVE_UP`）",
    "supervisor.shutdown_grace_s": "停机时发出 `ctl.shutdown` 后等待的秒数，超时再发 CTRL_BREAK，最后强制结束",
}

PRODUCTS_DESC = {
    "session_templates": "交易时段模板：模板名 → 时段列表，每个时段为 `[开始, 结束, 类型]`（北京时间 HH:MM；类型为 `auction`、"
                         "`auction_no_night`、`continuous`）。结束早于开始表示跨午夜（夜盘）",
    "exchanges": "交易所级规则（01 §8.15），键为交易所代码",
    "exchanges.{exchange}.explicit_close_today": "平仓委托必须显式区分平今和平昨（SHFE/INE 为 `true`）",
    "exchanges.{exchange}.close_style": "平仓时消耗 lot 的顺序：`FIFO` 先开先平；`TODAY_FIRST` 先平今仓",
    "exchanges.{exchange}.close_today_fee_rule": "平今手续费的判定：`LOT_MATCH` 按实际消耗的今仓 lot；`SAME_DAY_OPENS_FIRST` 先平当日新开仓"
                                                 "（CFFEX）；`INTRADAY_BOTH_LEGS` 当日开当日平时两腿都按日内费率（DCE）",
    "exchanges.{exchange}.settle_rounding": "结算价取整：`FLOOR_TICK` 向下取整到 tick（SHFE）、`ROUND_TICK` 四舍五入到 tick、`NONE` 不取整",
    "exchanges.{exchange}.settle_fallback": "当日无成交时结算价的回退链，依次尝试：`MEDIAN`（买一、卖一、昨结三者中位数）、`LIMIT`（封板取涨跌停价）、"
                                            "`HOURLY`（逐小时回溯）、`FULLDAY`（全日 VWAP）、`BENCHMARK`（基准合约涨跌幅）、`PRE_SETTLE`（昨结算）",
    "exchanges.{exchange}.big_side": "单向大边：`NONE` 不实行；`SAME_PRODUCT` 同品种按保证金较大一边（SHFE）；`GROUPS` 按 `big_side_group` 分组（CFFEX）",
    "exchanges.{exchange}.big_side_exit": "合约退出单向大边的时点：`LAST_TRADE_DAY_MINUS_5` 最后交易日前第 5 个交易日；"
                                          "`PHYSICAL_DELIVERY_MONTH_MINUS_1` 实物交割月前一个交易日；`NONE` 不退出",
    "exchanges.{exchange}.cancel_limit": "每个合约每日撤单次数上限（合规指标 `account.compliance_util` 的分母，达到即拒绝撤单）；`null` 不限制",
    "exchanges.{exchange}.large_cancel": "大额撤单限制；`null` 不限制",
    "exchanges.{exchange}.large_cancel.count": "每个合约每日大额撤单次数上限",
    "exchanges.{exchange}.large_cancel.min_qty": "单笔手数不小于该值的撤单算大额撤单",
    "exchanges.{exchange}.self_trade_limit": "每日自成交次数上限（合规指标的分母）",
    "products": "品种规则，键为品种代码（`rb`、`IF` 等）",
    "products.{product}.exchange": "所属交易所，须在 `exchanges` 中配置",
    "products.{product}.name": "中文名称，用于界面和报表",
    "products.{product}.sector": "板块（`black`、`nonferrous`、`precious`、`agri`、`equity_index`、`rates` 等），板块类规则按它汇总",
    "products.{product}.multiplier": "合约乘数",
    "products.{product}.tick_size": "最小变动价位",
    "products.{product}.limit_pct": "涨跌停幅度（小数，0.05 表示 ±5%）",
    "products.{product}.limit_schedule": "带生效日期的涨跌停调整（例如节前放宽）；取生效日不晚于当前交易日的最新一条",
    "products.{product}.limit_schedule[].effective_from": "开始生效的交易日（YYYYMMDD）",
    "products.{product}.limit_schedule[].pct": "新的涨跌停幅度",
    "products.{product}.settle_method": "结算价方法：`VWAP_FULL_DAY` 全日（含夜盘）成交量加权平均；`VWAP_LAST_HOUR` 最后一小时 VWAP（CFFEX）",
    "products.{product}.settle_window": "`VWAP_LAST_HOUR` 的时间窗 `HH:MM-HH:MM`（IF 为 14:00-15:00，T 为 14:15-15:15）",
    "products.{product}.big_side_group": "单向大边分组名，同组合约合并计算大边（如 `CFFEX:INDEX`）",
    "products.{product}.physical_delivery": "实物交割品种（CFFEX 国债）：交割月前一个交易日退出单向大边",
    "products.{product}.sessions": "使用的交易时段模板名（`session_templates` 的键）",
    "products.{product}.fees": "手续费规则，带生效日期；匹配顺序：`contracts` 列表 → `months` → `other`",
    "products.{product}.fees[].months": "适用的合约月份列表，或 `other` 表示其余月份；`null` 表示所有月份",
    "products.{product}.fees[].contracts": "适用的具体合约列表（优先于 `months`）",
    "products.{product}.fees[].effective_from": "开始生效的交易日（YYYYMMDD）",
    "products.{product}.fees[].type": "`rate` 按成交金额的比例；`per_lot` 按手收固定金额（元）",
    "products.{product}.fees[].open": "开仓费率或每手金额",
    "products.{product}.fees[].close_yd": "平昨费率或每手金额",
    "products.{product}.fees[].close_today_mult": "平今手续费 = 平昨费率 × 该倍数（0 表示平今免收）",
    "products.{product}.fees[].intraday": "DCE：当日开当日平时，两腿都按这个费率收取；`null` 表示不适用",
    "products.{product}.margin": "保证金率，带生效日期；可以按合约覆盖。同一时点取生效中的最高费率",
    "products.{product}.margin[].contracts": "适用合约：`*` 表示全部，或合约列表",
    "products.{product}.margin[].effective_from": "开始生效的交易日（YYYYMMDD）",
    "products.{product}.margin[].exchange_rate": "交易所保证金率",
    "products.{product}.margin[].broker_add": "经纪商加收；公司保证金率 = `exchange_rate` + `broker_add`",
    "products.{product}.max_order_qty": "单笔最大手数，按委托类型（`LIMIT`、`MARKET`），事前检查第 1 步使用",
    "products.{product}.position_limit": "单边持仓上限（手），事前检查的持仓步骤使用；`null` 不限制",
    "products.{product}.open_limit_per_day": "每日开仓手数上限（合规指标的分母）；`null` 不限制",
    "products.{product}.notional_face": "国债期货面值：合约价值 = 价格 × 面值 / 100（策略 SDK 使用）；`null` 表示按乘数计算",
    "instruments": "可交易的合约列表",
    "instruments[].instrument_id": "合约代码，按交易所原始大小写（README §3.3）",
    "instruments[].product": "所属品种（`products` 的键）",
    "instruments[].last_trade_day": "最后交易日（YYYYMMDD）；之后不再接受委托，单向大边的退出日也据此推算",
    "instruments[].listed_day": "上市日（YYYYMMDD）；之前不接受委托",
    "instruments[].delivery_month": "交割月（YYYYMM）；`null` 时按合约代码推算",
}

ACCOUNTS_DESC = {
    "accounts": "账户表，键为账户 ID（1~32 个字母、数字、`_`、`-`；演示环境为 `ACC1`~`ACC5`）",
    "accounts.{account_id}.name": "账户名称，用于界面、报表和结算单",
    "accounts.{account_id}.shard": "所属 risk-engine 分片；`services.yaml` 中必须有 `risk-engine-{shard}` 实例",
    "accounts.{account_id}.initial_capital": "初始资金（元）。单位净值和验收口径的当日盈亏比率都以它为分母；首日建账时作为上日结存",
    "accounts.{account_id}.strategy": "strategy-sim 为该账户运行的策略名（`channel_breakout`、`multi_trend`、`counter_trend_add`、"
                                      "`index_directional`、`rb_calendar_spread`）；回测也用它作默认策略",
    "accounts.{account_id}.products": "策略交易的品种",
    "accounts.{account_id}.initial_positions": "预留字段：首日建账的初始持仓。当前版本不读取（首日建账只写资金，持仓为空）",
    "accounts.{account_id}.initial_positions[].instrument_id": "合约代码",
    "accounts.{account_id}.initial_positions[].direction": "持仓方向",
    "accounts.{account_id}.initial_positions[].qty": "手数",
    "accounts.{account_id}.initial_positions[].open_price": "开仓价",
    "accounts.{account_id}.initial_positions[].open_td": "开仓交易日（YYYYMMDD）",
}

RULES_DESC = {
    "rules": "声明式风控规则（01 §8.14），与 02 §7.6 的 `RuleDef` 一一对应",
    "rules[].id": "规则 ID，写入 `alert_key`（`{td}|{rule_id}|{level}|{scope_key}|{episode}`）",
    "rules[].metric": "指标名，须是已注册的指标（见本文 §6.2）",
    "rules[].op": "越线方向：亏损类用 `<=`，占比类用 `>=`",
    "rules[].scope": "评估范围：`account`、`account/product`、`account/sector`、`firm`、`firm/product`、`firm/sector`",
    "rules[].hysteresis": "滞回比例：解除线 = 阈值 × (1 − hysteresis)",
    "rules[].cooldown_s": "回到 OK 后，同一 ALERT 级别在该秒数内不再触发（升级不受限制；限制和熔断类级别不适用）",
    "rules[].hold_ms": "未锁存的级别要在解除线之外连续停留该毫秒数才解除",
    "rules[].pretrade": "是否参与事前检查的 what-if 预估（下单前预估本单会不会让指标越线）",
    "rules[].enabled": "`false` 时不评估该规则",
    "rules[].reset_on_day_roll": "换日时是否解除该规则的锁存",
    "rules[].min_gross_pct": "账户总名义敞口 / 权益低于该值时不评估（视为未越线），`null` 表示总是评估",
    "rules[].levels": "级别列表，每级一个阈值和动作",
    "rules[].levels[].name": "级别名（如 `warn`、`restrict`、`halt`、`derisk`），写入 `alert_key`",
    "rules[].levels[].threshold": "阈值（比率一律为小数）",
    "rules[].levels[].action": "动作：`ALERT` 告警；`RESTRICT` 限制下单；`CIRCUIT_BREAK` 熔断；`LIQUIDATE` 生成减仓计划",
    "rules[].levels[].params": "动作参数，见本文 §6.3",
    "rules[].levels[].latch": "确认后锁存：只能由 `risk.reset` 或换日解除（熔断级别为 `true`）",
    "rules[].levels[].confirm": "确认条件：满足其一即从 PENDING 转为 ACTIVE",
    "rules[].levels[].confirm.states": "需要的不同 LIVE 市场状态数；`0` 表示只按时间确认",
    "rules[].levels[].confirm.ms": "越线持续的毫秒数",
    "rules[].levels[].rearm": "锁存级别经人工重置后的重新布防参数；`null` 表示不重新布防",
    "rules[].levels[].rearm.extra": "重新布防线 = min(阈值, 重置时的值 − extra)，按越线方向取",
    "rules[].overrides": "按账户覆盖：账户 ID → 级别名 → 要覆盖的级别字段，例如 `{ACC2: {halt: {threshold: -0.04}}}`",
    "pretrade": "事前检查参数（01 §5.3.1、02 §11.2）",
    "pretrade.fat_finger_ticks": "限价偏离最新价超过该 tick 数即拒单（与 `fat_finger_pct` 取较宽者）",
    "pretrade.fat_finger_pct": "限价偏离最新价超过该比例即拒单",
    "pretrade.funds_buffer": "资金检查预留的缓冲比例：开仓后可用资金须不低于权益 × 该值",
    "pretrade.throttle_rate_per_s": "每个账户的报单速率上限（笔/秒，令牌桶）",
    "pretrade.throttle_burst": "令牌桶容量（允许的突发笔数）",
    "pretrade.route_ttl_ms": "通过风控后路由的有效期（毫秒）：超过 `expire_ns` 的路由被 trade-gateway 拒绝并写墓碑",
    "pretrade.default_max_order_qty": "`products.yaml` 没有配置 `max_order_qty` 时的单笔最大手数",
    "system": "风控核心的系统参数",
    "system.snapshot_hz": "账户快照 `risk.snapshot.{account_id}` 的最高发布频率（Hz）；另外每秒至少发一次保活",
    "system.metric_sample_s": "账户指标采样周期（秒），写入 `metric_samples`，供日内曲线和日报使用",
    "system.md_unknown_account_block_pct": "账户敞口中行情质量为 UNKNOWN 的比例超过该值时，整个账户禁开",
    "system.recon_interval_s": "盘中与柜台做持仓对账的周期（秒）",
    "system.recon_quiet_s": "对账前要求该账户最近没有成交和在途委托的静默秒数",
    "system.quality_stale_tolerance": "账户范围规则的行情质量门控容忍度：质量不是 OK 的持仓合约占账户总名义敞口不超过该比例时，账户质量仍按 OK 处理"
                                      "（这些合约按最后已知价格估值）；超过时取最差的质量，状态机保持不动。0 表示不容忍",
}

FEEDS_DESC = {
    "feed_a": "Feed A：CTP 风格的 WebSocket 推送源（02 §9.1）",
    "feed_a.enabled": "是否接入",
    "feed_a.url": "WebSocket 地址",
    "feed_a.priority": "优先级（数字小者优先），用于自举和平局时的选择",
    "feed_a.reconnect_backoff_ms": "断线重连的退避序列（毫秒），用完后重复最后一个值",
    "feed_b": "Feed B：厂商风格的 REST 轮询源（02 §9.2）",
    "feed_b.enabled": "是否接入",
    "feed_b.url": "轮询地址",
    "feed_b.poll_ms": "轮询周期（毫秒）；上一次未完成时跳过本次",
    "feed_b.timeout_ms": "单次请求超时（毫秒）",
    "feed_b.priority": "优先级",
    "sina": "新浪期货行情（02 §9.3），live 档使用；sim 档必须关闭（两边价格不是同一个市场）",
    "sina.enabled": "是否接入（md-gateway）",
    "sina.url": "请求地址前缀，合约代码拼在后面",
    "sina.referer": "请求头 Referer（新浪要求）",
    "sina.poll_ms": "轮询周期（毫秒），不应小于 1000",
    "sina.timeout_ms": "单次请求超时（毫秒）",
    "sina.priority": "优先级",
    "sina.trust_env": "是否使用环境变量 `HTTP(S)_PROXY` 中的代理",
    "sina.symbols": "合约代码 → 新浪代码（如 `rb2701: nf_RB2701`，`IF2612: CFF_RE_IF2612`）；`futrisk data record` 也按它录制",
    "arbitration": "多源仲裁参数（01 §8.13）",
    "arbitration.exclude_missed_heartbeats": "Feed A 连续丢失该数量的心跳即排除（EXCLUDED）",
    "arbitration.exclude_failed_polls": "轮询源连续失败该次数即排除",
    "arbitration.exclude_reject_rate": "最近 1 分钟的拒收率超过该值即排除",
    "arbitration.exclude_integrity_votes": "被交叉校验投票该次数即排除",
    "arbitration.probation_healthy_s": "观察期（PROBATION）内健康满该秒数，才可能恢复可信",
    "arbitration.probation_agree_count": "恢复可信还需要与可信源同成交量一致的次数",
    "arbitration.min_dwell_s": "每个可信状态最少停留的秒数（防抖）",
    "arbitration.flapping_window_s": "抖动检测的时间窗（秒）",
    "arbitration.flapping_max_switches": "时间窗内切换超过该次数时告警 `MD_FLAPPING`",
    "arbitration.reorder_buffer_max": "Feed A 乱序缓冲的最大条数",
    "arbitration.reorder_buffer_ms": "Feed A 乱序缓冲的最长等待（毫秒）",
    "arbitration.spike_confirm_ms": "尖刺隔离：等待确认的时限（毫秒），超时未确认即丢弃",
    "arbitration.spike_ticks": "尖刺阈值的下限：max(spike_ticks × tick, spike_z × σ × √Δt × 价格)，上限为涨跌停带宽的 1/4",
    "arbitration.spike_z": "尖刺阈值中波动率的倍数",
    "arbitration.stale_s": "连续交易时段内没有可信源满该秒数，合约质量标为 STALE（禁开）",
    "arbitration.unknown_s": "满该秒数标为 UNKNOWN",
    "arbitration.open_grace_s": "开盘后的宽限秒数，期间不判定陈旧",
    "arbitration.live_max_latency_ms": "接收时间与交易所时间相差超过该毫秒数（双向）的行情不算 LIVE",
    "arbitration.watermark_flush_s": "仲裁水位写入 md.db 的周期（秒）",
}

SIM_DESC = {
    "price_engine": "价格引擎：相关的 GBM 加跳跃过程，价格按 tick 对齐并受涨跌停约束（01 §8.13）",
    "price_engine.seed": "随机种子；同一种子重现同一条价格路径",
    "price_engine.tick_interval_ms": "每个合约产生一个新行情状态的间隔（毫秒），只在连续交易时段内产生",
    "price_engine.sector_correlation": "同板块品种之间的相关系数",
    "price_engine.market_correlation": "全市场公共因子的相关系数",
    "price_engine.instruments": "模拟的合约，键为合约代码（须在 `products.yaml` 中）",
    "price_engine.instruments.{instrument_id}.pre_settle": "昨结算价：价格路径的初值；`futrisk demo` 首日建账也把它作为昨结算",
    "price_engine.instruments.{instrument_id}.vol_annual": "年化波动率",
    "price_engine.instruments.{instrument_id}.jump_intensity_per_day": "每个交易日跳跃次数的期望",
    "price_engine.instruments.{instrument_id}.jump_sigma": "单次跳跃幅度的标准差（对数收益）",
    "price_engine.instruments.{instrument_id}.volume_per_s": "平均每秒成交量（手）",
    "price_engine.instruments.{instrument_id}.open_interest": "初始持仓量（手）",
    "feed_a": "Feed A 服务端（WebSocket）",
    "feed_a.host": "监听地址",
    "feed_a.port": "监听端口",
    "feed_a.ring_size": "重放环形缓冲的容量（条）；缺口早于环形缓冲时只能用快照补齐",
    "feed_a.heartbeat_ms": "心跳间隔（毫秒）",
    "feed_b": "Feed B 服务端（HTTP），同时提供健康检查 `GET /v1/health`",
    "feed_b.host": "监听地址",
    "feed_b.port": "监听端口",
    "broker": "SimBroker 模拟柜台（02 §9.4）",
    "broker.endpoint": "ZeroMQ ROUTER 端点，trade-gateway 连接它",
    "broker.front_id": "前置编号（仿 CTP 的 FrontID）",
    "broker.ack_delay_ms": "委托确认的模拟延迟（毫秒）",
    "broker.fill_delay_ms": "成交回报的模拟延迟（毫秒）",
    "control": "控制接口（02 §9.5），只在 `app.mode=demo` 时开放",
    "control.host": "监听地址",
    "control.port": "监听端口",
}

SCENARIO_DESC = {
    "name": "剧本名，与文件名一致（1~64 个字母、数字、`_`、`-`），`futrisk scenario run <name>` 使用",
    "description": "说明，`futrisk scenario list` 和仪表盘显示",
    "vars": "变量及默认值；步骤中整值为 `${var}` 时按原类型替换，嵌在字符串中时按文本替换；运行时可用 `--var KEY=VALUE` 覆盖",
    "steps": "步骤列表，按 `at_s` 顺序执行",
    "steps[].at_s": "相对剧本开始的秒数；`--speed 2` 时减半",
    "steps[].action": "动作：`shock` 价格冲击、`fault` 注入故障、`clear_fault` 撤销（按 `ref`）、`clear_faults` 撤销全部、"
                      "`pause` / `resume` 暂停和恢复行情、`log` 只记日志",
    "steps[].params": "动作参数，见本文 §9.2",
    "steps[].ref": "给 `fault` 步骤起的名字，供后面的 `clear_fault` 引用",
    "steps[].note": "`log` 步骤的文字",
}

BACKTEST_DESC = {
    "name": "回测名，默认取文件名；输出目录为 `<home>/backtests/<name>-<时间戳>`",
    "description": "说明，`futrisk backtest list` 显示第一行",
    "kind": "类型：`arms` 配对实验；`stress` 每个压力窗口单独运行；`sweep` 阈值 × 动作档位扫描；`scenario` D 层合成剧本（精确断言）",
    "layer": "数据层：`A` 逐合约日线（2015 年至今）；`B` 新浪分钟线；`C` 自录 tick；`D` 合成剧本（01 §8.16）",
    "start": "起始交易日（YYYYMMDD）；`null` 为数据的第一天",
    "end": "结束交易日；`null` 为数据的最后一天",
    "products": "参与回测的品种",
    "accounts": "回测账户，键为账户 ID",
    "accounts.{account_id}.strategy": "策略名（同 `accounts.yaml` 的 `strategy`）",
    "accounts.{account_id}.products": "策略交易的品种；`null` 取 `accounts.yaml` 的 `products`",
    "accounts.{account_id}.params": "策略参数（由策略自己的模型校验）",
    "accounts.{account_id}.capital": "初始资金；`null` 取 `accounts.yaml` 的 `initial_capital`",
    "accounts.{account_id}.enabled": "`false` 时不参与",
    "accounts.{account_id}.role": "`book` 计入 TOTAL；`control` 在每个方案中回放但单独报告（对照组）",
    "accounts.{account_id}.title": "报告中的显示名",
    "arms": "实验方案，名字唯一",
    "arms[].name": "方案名",
    "arms[].tier": "动作档位：`none` 无规则；`alert_only` 只告警；`block_open` 生产规则（熔断禁开）；`halt_only` 只有熔断；`halve` / "
                   "`flatten` 锁存时再减仓 50% / 100%；`placebo` 安慰剂；`production` 全部生产规则",
    "arms[].threshold": "熔断阈值（正数，占初始资金的比例）；`null` 取 `rules.threshold`",
    "arms[].placebo_of": "`placebo` 方案引用的方案名：在熔断没有触发的随机日子重复它的动作",
    "rules": "日亏损规则在各方案中的实例化方式",
    "rules.threshold": "熔断阈值（正数），默认 3%",
    "rules.ladder": "各级别相对熔断阈值的倍数：告警 2/3、限制 5/6、熔断 1、减仓 5/3",
    "rules.include_other_rules": "是否在每个有规则的方案中加入其他生产规则（`production` 档位总是包含）",
    "rules.reduce_step": "`halve` / `flatten` 锁存时平仓的比例",
    "path": "A 层日内路径的合成方式（01 §8.16）",
    "path.rule": "极值顺序：`open_near_extreme` 开盘价靠近哪个极值先走哪个；`high_first`；`low_first`",
    "path.points_per_leg": "每一段（开→极值 1→极值 2→收）至少的价格点数",
    "path.max_step": "自适应加密：每步最大涨跌幅（相对开盘价），0 表示固定点数",
    "path.max_points_per_leg": "每段最多的价格点数",
    "ensemble": "路径集合：用校准的随机路径模型代替确定路径",
    "ensemble.paths": "随机路径的条数 K；0 表示用 `path.rule` 的确定路径",
    "ensemble.calibration_period": "校准极值时间和振荡幅度所用的分钟线周期",
    "ensemble.order_period": "估计先到哪个极值的概率所用的分钟线周期",
    "ensemble.check_periods": "报告振荡拟合优度的周期",
    "ensemble.grid_s": "细路径的时间分辨率（交易时间秒）",
    "ensemble.shrink_days": "品种级顺序概率向合并估计收缩的伪天数",
    "ensemble.min_days": "品种级校准至少需要的天数，不足时按时段形态合并",
    "ensemble.sigma": "固定振荡幅度（敏感性分析），`null` 用校准值",
    "ensemble.rho": "固定板块内相关（敏感性分析），`null` 用校准值",
    "ensemble.bootstrap": "路径 × 日块联合 bootstrap 的次数",
    "sensitivity": "敏感性变体：在本配置上叠加点号覆盖后重跑，与主结果并列报告",
    "sensitivity[].name": "变体名（唯一）",
    "sensitivity[].title": "报告中的标题",
    "sensitivity[].set": "点号覆盖，如 `{path.rule: low_first}`",
    "sensitivity[].paths": "变体的随机路径条数；`null` 为确定路径",
    "sensitivity[].arms": "变体要跑的方案名；`null` 为全部非安慰剂方案",
    "data": "数据位置",
    "data.dir": "历史数据目录；相对路径相对应用目录（exe 所在目录，或源码仓库根目录）；`null` 为 `data/hist`",
    "data.minute_period": "B 层分钟线周期：5、15 或 60",
    "data.minute_window": "A 层只回放该周期分钟线文件共同覆盖的交易日（与 B 层在相同的日子上对照）；`null` 为日线的全部交易日",
    "data.ticks_home": "C 层 tick 所在的 home（含 `ticks/raw/<td>/sina`）；`null` 为当前 home",
    "data.tick_days": "C 层回放的交易日；空表示全部已录制的交易日",
    "analysis": "统计分析选项",
    "analysis.horizons": "效果观察期（交易日）：d..d+h",
    "analysis.bootstrap_samples": "配对块 bootstrap 的抽样次数",
    "analysis.bootstrap_block": "块长度（交易日）",
    "analysis.exclude_roll_days": "换月日不计入真假阳性统计（单独计数）",
    "analysis.focus_days": "保留日内路径的交易日",
    "analysis.intraday_trigger_days": "最多保留多少个触发日的日内路径",
    "analysis.hit_rate": "是否用分钟线统计\"开盘价靠近极值\"规则的命中率",
    "analysis.bounding": "是否对静态持仓做必然 / 不确定 / 不可能触发的界定分类",
    "analysis.primary_basis": "衡量效果的价格口径：`close` 收盘价（主口径）、`settle` 结算价、`open` 次日开盘价",
    "analysis.primary_horizon": "主观察期 h（d..d+h）",
    "analysis.leave_one_out": "逐年、逐压力窗口剔除后重算效果",
    "analysis.multiplicity": "多账户 p 值的校正方法：`holm` 或 `bh`",
    "stress": "压力窗口（`kind=stress` 时每个窗口单独运行；`arms` 时从全期结果中切片）",
    "stress.windows": "窗口列表",
    "stress.windows[].id": "窗口 ID",
    "stress.windows[].title": "标题",
    "stress.windows[].start": "起始交易日",
    "stress.windows[].end": "结束交易日",
    "stress.windows[].focus_day": "重点日（保留日内路径）",
    "stress.windows[].products": "窗口关注的品种",
    "stress.windows[].note": "备注",
    "stress.windows[].auto": "自动选日：`largest_drops` 取 `auto_product` 跌幅最大的 `top_n` 天",
    "stress.windows[].auto_product": "自动选日所用的品种",
    "stress.windows[].top_n": "自动选取的天数",
    "stress.windows[].pad_days": "自动选取日前后各加的交易日数",
    "stress.skipped": "跳过的窗口及原因（只写进报告）",
    "stress.warmup_days": "窗口前用于指标预热的交易日数（不交易）",
    "sweep": "参数扫描（`kind=sweep`）",
    "sweep.thresholds": "扫描的熔断阈值",
    "sweep.tiers": "扫描的动作档位",
    "sweep.periods": "样本期：名称 → [起, 止]（`null` 为数据末尾），默认样本内 2015-2020、样本外 2021 至今",
    "sweep.objectives": "平台 / 最优分析的目标：`total_ret`、`max_dd`、`calmar`、`cvar95`",
    "sweep.focus_accounts": "热力图重点展示的账户（`TOTAL` 为合计）",
    "sweep.center": "被检验邻域的生产阈值",
    "sweep.warmup_days": "每个样本期前的预热交易日数",
    "scenarios": "D 层合成剧本（`kind=scenario`），结果已知、可精确断言",
    "scenarios[].name": "剧本名",
    "scenarios[].description": "说明",
    "scenarios[].trading_day": "剧本所在的交易日",
    "scenarios[].start": "第 0 步的北京时间（须在所有合约的连续交易时段内）",
    "scenarios[].reference": "合约 → 参考价（同时作为昨结算）",
    "scenarios[].book": "第 0 步按参考价买入的初始持仓（合约 → 手数）",
    "scenarios[].account": "剧本账户",
    "scenarios[].steps": "市场状态序列",
    "scenarios[].steps[].label": "步骤标签（断言按它引用）",
    "scenarios[].steps[].at_s": "相对开始的秒数（市场时间）",
    "scenarios[].steps[].move": "合约、品种或板块 → 相对参考价的涨跌幅",
    "scenarios[].steps[].orders": "该步提交的脚本委托",
    "scenarios[].expect": "期望结果（由回测和单测逐条检查）",
    "scenarios[].use_strategy": "`true` 时运行账户的策略，而不是脚本委托",
    "seed": "随机种子",
    "workers": "进程池大小；`auto` 为 min(任务数, CPU 核数 − 2, 14)，`futrisk backtest run --workers` 可覆盖",
    "warmup_days": "`start` 之前喂给策略的历史交易日数（不交易）",
    "sizing": "策略的仓位基准：`initial` 初始资金（不复利，各方案在下次调仓时重新对齐）；`equity` 当前权益（复利）",
    "base_config": "基础配置目录（品种、账户、风控规则、日历）；`null` 为包内的 `config/`，相对路径相对应用目录",
}

BOTS_DESC = {
    "identity": "strategy-sim 在总线上的身份（DEALER identity），必须在 `order.*` 的白名单中（02 §4.11）",
    "seed": "随机种子；每个账户的随机数发生器为 `Random(\"{seed}:{account}\")`",
    "aggressive": "激进（剧本）模式：ACC3 一次建满多头持仓，供熔断演示使用；等同于 `FUTRISK_BOTS_MODE=scenario`",
    "accounts_filter": "只运行这些账户；`null` 为全部已配置账户",
    "order_tag": "写进自己 `client_order_id` 的标记，重启后据此找回前一个进程的委托",
    "status_log_s": "每个账户输出一条状态日志的周期（秒）",
    "resync_interval_s": "空闲时向风控查询持仓和权益的周期（秒）；0 关闭",
    "ready_timeout_s": "等待风控分片 RUNNING 的最长秒数，超时后照常开始交易",
    "startup_cancel_own": "启动时撤销前一个进程留下的本机器人挂单（不动人工单和减仓单）",
    "startup_sync": "启动时从风控分片同步持仓、权益和状态",
    "shutdown_cancel_own": "正常停止时撤销自己的挂单",
    "snapshot_resync": "空闲且与 `risk.snapshot` 不一致时，按快照重新同步持仓",
    "drift_confirm": "连续多少个不一致的快照才重新同步",
    "execution": "下单执行参数（所有账户的默认值，账户级 `execution` 覆盖）",
    "execution.max_orders_per_min": "每分钟最多报单数（令牌桶，市场时间）",
    "execution.burst": "令牌桶容量",
    "execution.order_ttl_s": "自己的挂单超过该秒数即撤销",
    "execution.aggression_ticks": "可成交限价单越过对手价的 tick 数（随机取其一）",
    "execution.passive_prob": "开仓单改为挂在本方价位的概率",
    "execution.slice_notional": "单笔委托的最大名义金额（元）",
    "execution.max_slice_lots": "单笔委托的最大手数（同时受 `max_order_qty` 限制）",
    "execution.deadband_frac": "目标仓位变化小于目标的该比例时不调仓",
    "execution.probe_qty": "开仓限制解除后第一笔试探开仓的手数",
    "execution.in_doubt_resubmit_s": "结果不明（超时）的委托，隔该秒数用同一个 `client_order_id` 重发",
    "execution.in_doubt_giveup_s": "超过该秒数放弃（不再计为在途）",
    "execution.cancel_retry_s": "结果不明的撤单隔该秒数重发",
    "execution.halt_probe_s": "熔断期间每隔该秒数发一笔 1 手的开仓试探（被拒 `REJECT_ACCOUNT_HALTED`）；0 关闭",
    "gates": "被拒后的退避（市场时间秒，重复被拒时翻倍直到上限）",
    "gates.halt_backoff_s": "熔断或 kill switch 拒单后的退避",
    "gates.halt_backoff_max_s": "上述退避的上限",
    "gates.block_backoff_s": "禁开、禁止亏损加仓、敞口上限、what-if、资金不足等拒单后的退避",
    "gates.block_backoff_max_s": "上述退避的上限",
    "gates.transport_backoff_s": "TIMEOUT / UNAVAILABLE / BUSY / RISK_BUSY 重试失败后的退避",
    "gates.transport_backoff_max_s": "上述退避的上限",
    "gates.size_restore_s": "单笔上限系数限制（ORDER_SIZE_FACTOR）安静该秒数后恢复全量",
    "accounts": "按账户的覆盖，键为账户 ID",
    "accounts.{account_id}.enabled": "`false` 时不运行该账户的机器人",
    "accounts.{account_id}.strategy": "策略名；`null` 取 `accounts.yaml` 的 `strategy`",
    "accounts.{account_id}.params": "策略参数（由策略自己的模型校验，如 `instruments`、`bar_s`、`notional_frac`）",
    "accounts.{account_id}.execution": "覆盖全局 `execution` 的字段",
    "accounts.{account_id}.aggressive": "只在激进（剧本）模式下叠加的 `params` / `execution`",
}

# ------------------------------------------------------------------ sections
RE = "risk-engine"
SECTIONS: list[Section] = [
    Section(
        "app", "app.yaml", AppConfig,
        readers="全部服务、CLI",
        summary="大部分重启生效；`eod_start_at`、`api_keys`、`notify` 即时",
        default_effect=restart("相关进程"),
        effects={
            "mode": restart("全部服务"),
            "demo.start_at": FIRST,
            "demo.pre_close_at": restart("supervisor"),
            "demo.lead_s": restart("supervisor"),
            "demo.auto_seed": "下次 `futrisk demo`",
            "eod_start_at": IMM,
            "logging": restart("各进程"),
            "api_keys": f"{IMM}（需环境变量）",
            "notify": f"{IMM}（需环境变量）",
        },
        desc=APP_DESC,
    ),
    Section(
        "services", "services.yaml", ServicesConfig,
        readers="全部内部服务、CLI",
        summary="端点、总线参数拒绝热改；`supervisor` 段重启 supervisor 生效",
        default_effect=REJECT,
        effects={"supervisor": restart("supervisor")},
        desc=SERVICES_DESC,
        placeholders={"instances": "instance"},
    ),
    Section(
        "products", "products.yaml", ProductsConfig,
        readers="md-gateway、risk-engine、clearing-worker、strategy-sim、回测",
        summary="下一交易日（risk-engine 重启后才用新值，TD-09）",
        default_effect=f"{NTD}（{RE} 需重启）",
        effects={},
        desc=PRODUCTS_DESC,
        placeholders={"session_templates": "template", "exchanges": "exchange", "products": "product"},
    ),
    Section(
        "accounts", "accounts.yaml", AccountsConfig,
        readers="risk-engine、api-gateway、clearing-worker、strategy-sim、`futrisk demo`",
        summary="分片归属拒绝热改；其余见各字段",
        default_effect=IMM,
        effects={
            "accounts": REJECT,
            "accounts.{account_id}.name": IMM,
            "accounts.{account_id}.initial_capital": "首日建账时读取",
            "accounts.{account_id}.strategy": restart("strategy-sim"),
            "accounts.{account_id}.products": restart("strategy-sim"),
            "accounts.{account_id}.initial_positions": "不读取",
        },
        desc=ACCOUNTS_DESC,
        placeholders={"accounts": "account_id"},
    ),
    Section(
        "risk_rules", "risk_rules.yaml", RiskRulesConfig,
        readers="risk-engine（各分片）、api-gateway、仪表盘",
        summary="即时（已锁存的熔断保持到重置或换日）",
        default_effect=IMM,
        effects={},
        desc=RULES_DESC,
    ),
    Section(
        "feeds", "feeds.yaml", FeedsConfig,
        readers="md-gateway；`futrisk data`（读包内默认配置）",
        summary="`arbitration` 即时；数据源段拒绝热改",
        default_effect=REJECT,
        effects={"arbitration": IMM},
        desc=FEEDS_DESC,
    ),
    Section(
        "sim", "sim.yaml", SimConfig,
        readers="sim-exchange、`futrisk demo`（首日建账的昨结算）、trade-gateway（柜台端点）",
        summary="重启 sim-exchange（外部组件，不参与两阶段提交）",
        default_effect=restart("sim-exchange"),
        effects={"broker.endpoint": restart("sim-exchange、trade-gateway")},
        desc=SIM_DESC,
        placeholders={"price_engine.instruments": "instrument_id"},
    ),
    Section(
        "scenario", "scenarios/*.yaml", ScenarioFile,
        readers="sim-exchange（每次 `scenario run`）",
        summary="每次运行时读取 sim-exchange 启动时的配置版本目录",
        default_effect=PER_RUN,
        effects={},
        desc=SCENARIO_DESC,
    ),
    Section(
        "backtest", "backtest/*.yaml", BacktestConfig,
        readers="`futrisk backtest`、clearing-worker 的回测作业",
        summary="每次运行时读取（包内 `config/backtest/` 或给定的 YAML 路径）",
        default_effect=PER_RUN,
        effects={},
        desc=BACKTEST_DESC,
        placeholders={"accounts": "account_id"},
    ),
    Section(
        "bots", "bots.yaml", BotsConfig,
        readers="strategy-sim（可选文件）",
        summary="重启 strategy-sim",
        default_effect=restart("strategy-sim"),
        effects={},
        desc=BOTS_DESC,
        placeholders={"accounts": "account_id"},
    ),
]

# ------------------------------------------------------------------ model walking


def is_model(tp: Any) -> bool:
    return isinstance(tp, type) and issubclass(tp, BaseModel)


def _union_args(tp: Any) -> tuple[Any, ...] | None:
    origin = typing.get_origin(tp)
    if origin is typing.Union or origin is types.UnionType:
        return typing.get_args(tp)
    return None


def sub_models(tp: Any) -> list[tuple[str, type[BaseModel]]]:
    """``(suffix, model)`` for the models nested in an annotation: ``""`` (a model), ``"[]"`` (list), ``"{}"`` (dict)."""
    if is_model(tp):
        return [("", tp)]
    args = _union_args(tp)
    if args is not None:
        out: list[tuple[str, type[BaseModel]]] = []
        for a in args:
            out += sub_models(a)
        return out
    origin = typing.get_origin(tp)
    targs = typing.get_args(tp)
    if origin is list and targs:
        return [("[]", m) for suffix, m in sub_models(targs[0]) if suffix == ""]
    if origin is dict and len(targs) == 2:
        return [("{}", m) for suffix, m in sub_models(targs[1]) if suffix == ""]
    return []


@dataclass
class Row:
    path: str
    type_md: str
    required: bool
    default_md: str
    desc: str
    effect: str


def walk(model: type[BaseModel], sec: Section, prefix: str = "") -> Iterator[tuple[str, Any, FieldInfo]]:
    for name, f in model.model_fields.items():
        path = f"{prefix}{name}"
        yield path, f.annotation, f
        for suffix, sub in sub_models(f.annotation):
            if suffix == "{}":
                key = sec.placeholders.get(path.replace("{", "").replace("}", "")) or sec.placeholders.get(path) or "name"
                child = f"{path}.{{{key}}}."
            elif suffix == "[]":
                child = f"{path}[]."
            else:
                child = f"{path}."
            yield from walk(sub, sec, child)


def code(text: str) -> str:
    return f"`{text}`"


def type_md(tp: Any, sec: Section, path: str) -> str:
    if tp is type(None):
        return code("null")
    args = _union_args(tp)
    if args is not None:
        parts = [type_md(a, sec, path) for a in args if a is not type(None)]
        text = " \\| ".join(parts)
        return text + (" \\| " + code("null") if type(None) in args else "")
    origin = typing.get_origin(tp)
    targs = typing.get_args(tp)
    if origin is typing.Literal:
        return " \\| ".join(code(str(v)) for v in targs)
    if origin is list:
        inner = targs[0] if targs else Any
        if is_model(inner):
            return "对象列表"
        inner_md = type_md(inner, sec, path)
        return code(f"list[{inner_md.strip('`')}]") if inner_md.count("`") == 2 else f"list[{inner_md}]"
    if origin is dict:
        k, v = targs if len(targs) == 2 else (str, Any)
        if is_model(v):
            key = sec.placeholders.get(path, "name")
            return f"{{{key}: 对象}}"
        v_md = type_md(v, sec, path)
        k_md = type_md(k, sec, path).strip("`")
        return code(f"dict[{k_md}, {v_md.strip('`')}]") if v_md.count("`") == 2 else f"dict[{k_md}, {v_md}]"
    if is_model(tp):
        return "对象"
    if isinstance(tp, type) and issubclass(tp, Enum):
        return " \\| ".join(code(str(m.value)) for m in tp)
    if tp is Decimal:
        return code("decimal")
    if tp is Any:
        return code("any")
    if isinstance(tp, type):
        return code(tp.__name__)
    return code(str(tp))


def _scalar(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, Enum):
        return str(v.value)
    if isinstance(v, float):
        return str(int(v)) if v.is_integer() and abs(v) < 1e15 else f"{v:.6g}"
    if isinstance(v, (int, Decimal)):
        return str(v)
    if isinstance(v, str):
        return v if v else '""'
    if isinstance(v, BaseModel):
        return _flow(v.model_dump(mode="json"))
    if isinstance(v, (list, tuple, dict)):
        return _flow(v)
    return str(v)


def _flow(v: Any) -> str:
    if isinstance(v, dict):
        return "{" + ", ".join(f"{k}: {_flow(x)}" for k, x in v.items()) + "}"
    if isinstance(v, (list, tuple)):
        return "[" + ", ".join(_flow(x) for x in v) + "]"
    return _scalar(v)


def default_md(f: FieldInfo) -> tuple[bool, str]:
    if f.is_required():
        return True, "—"
    value = f.default_factory() if f.default_factory is not None else f.default  # type: ignore[call-arg]
    if isinstance(value, BaseModel):
        return False, "见子字段"
    return False, code(_scalar(value))


def escape_cell(text: str) -> str:
    return re.sub(r"(?<!\\)\|", r"\\|", text)


def subtree_effect(sec: Section, path: str, paths: list[str]) -> str:
    """The effect of ``path``; for a container, ``见子字段`` when its sub-keys differ."""
    own = sec.effect(path)
    subs = {sec.effect(p) for p in paths if p.startswith(path + ".") or p.startswith(path + "[")}
    if subs and subs != {own}:
        return "见子字段"
    return own


def rows(sec: Section) -> tuple[list[Row], list[str]]:
    out: list[Row] = []
    missing: list[str] = []
    walked = list(walk(sec.model, sec))
    all_paths = [p for p, _, _ in walked]
    for path, ann, f in walked:
        desc = sec.desc.get(path)
        if desc is None:
            missing.append(f"{sec.name}: {path}")
            desc = "（缺说明）"
        required, dflt = default_md(f)
        container = path.rsplit(".", 1)[0] if "." in path else path
        out.append(Row(path, type_md(ann, sec, container if container in sec.placeholders else path),
                       required, dflt, desc, subtree_effect(sec, path, all_paths)))
    extra = sorted(set(sec.desc) - {r.path for r in out})
    missing += [f"{sec.name}: description for a key that no longer exists: {p}" for p in extra]
    return out, missing


def table(sec: Section) -> tuple[str, list[str]]:
    rs, missing = rows(sec)
    lines = ["| 字段 | 类型 | 必填 | 说明 | 默认值 | 热加载 |", "|---|---|---|---|---|---|"]
    for r in rs:
        lines.append(
            f"| {code(r.path)} | {r.type_md} | {'是' if r.required else '否'} | {escape_cell(r.desc)} | "
            f"{escape_cell(r.default_md)} | {escape_cell(r.effect)} |"
        )
    return "\n".join(lines), missing


def overview() -> str:
    lines = ["| 文件 | pydantic 模型 | 读取方 | 热加载（概要） |", "|---|---|---|---|"]
    for s in SECTIONS:
        model = f"{s.model.__module__}.{s.model.__name__}"
        lines.append(f"| `{s.file}` | `{model}` | {escape_cell(s.readers)} | {escape_cell(s.summary)} |")
    lines.append("| `calendar/trading_days.csv` | —（CSV，`futrisk.common.calendar.TradingCalendar`） | 全部服务、回测 | "
                 "下一交易日（risk-engine 需重启） |")
    return "\n".join(lines)


def metrics_table() -> str:
    import futrisk.core.engine  # noqa: F401  (imports every module that registers metrics)
    from futrisk.core.metrics import registered_metrics

    by_scope: dict[str, list[str]] = {}
    bases: dict[str, str] = {}
    for name, m in sorted(registered_metrics().items()):
        by_scope.setdefault(m.scope, []).append(name)
        if m.money_base:
            bases[name] = m.money_base
    scope_rules = {
        "account": "`account`",
        "product": "`account/product`、`firm/product`",
        "sector": "`account/sector`、`firm/sector`",
        "firm": "`firm`",
    }
    lines = ["| 指标类别 | 可用于的 `scope` | 指标名 = 前缀 + 字段 |", "|---|---|---|"]
    for scope in ("account", "product", "sector", "firm"):
        names = by_scope.get(scope, [])
        # "前缀 + 字段" instead of full dotted names: `firm.x` would read like a bus topic / command name (02 §4.4)
        fields = "、".join(code(n.split(".", 1)[1]) + (f"（分母 `{bases[n]}`）" if n in bases else "") for n in names)
        lines.append(f"| `{scope}` | {scope_rules.get(scope, code(scope))} | 前缀 `{scope}.`，字段：{fields} |")
    return "\n".join(lines)


BLOCKS: dict[str, Callable[[], tuple[str, list[str]]]] = {
    "files": lambda: (overview(), []),
    "metrics": lambda: (metrics_table(), []),
    **{s.name: (lambda s=s: table(s)) for s in SECTIONS},
}


def render_all() -> tuple[dict[str, str], list[str]]:
    out: dict[str, str] = {}
    missing: list[str] = []
    for name, fn in BLOCKS.items():
        text, miss = fn()
        out[name] = text
        missing += miss
    return out, missing


def apply(doc: str, blocks: dict[str, str]) -> tuple[str, list[str]]:
    """Replace every ``<!-- gen:{name} begin -->`` ... ``end`` block; returns the new text and absent markers."""
    absent: list[str] = []
    for name, text in blocks.items():
        pat = re.compile(rf"(<!-- gen:{re.escape(name)} begin -->\n)(.*?)(<!-- gen:{re.escape(name)} end -->)", re.S)
        if not pat.search(doc):
            absent.append(name)
            continue
        doc = pat.sub(lambda m, t=text: m.group(1) + t + "\n" + m.group(3), doc)
    return doc, absent


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="exit 1 when docs/11-config-reference.md is out of date")
    ap.add_argument("--print", dest="show", metavar="BLOCK", help=f"print one block: {', '.join(BLOCKS)}")
    ap.add_argument("--doc", type=Path, default=DOC, help="document to update (default: %(default)s)")
    args = ap.parse_args(argv)
    for stream in (sys.stdout, sys.stderr):
        with contextlib.suppress(Exception):
            stream.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    blocks, missing = render_all()
    if args.show:
        if args.show not in blocks:
            print(f"unknown block {args.show!r}; choose from {', '.join(blocks)}", file=sys.stderr)
            return 2
        print(blocks[args.show])
        return 0
    if missing:
        print("undocumented configuration keys (add them to the *_DESC tables of this script):", file=sys.stderr)
        for m in missing:
            print(f"  {m}", file=sys.stderr)
    doc = args.doc.read_text(encoding="utf-8")
    new, absent = apply(doc, blocks)
    if absent:
        print(f"markers missing in {args.doc}: {', '.join(absent)}", file=sys.stderr)
    if args.check:
        stale = new != doc
        if stale:
            print(f"{args.doc} is out of date: run python scripts/gen_config_reference.py", file=sys.stderr)
        return 1 if (stale or missing or absent) else 0
    if new != doc:
        args.doc.write_text(new, encoding="utf-8", newline="\n")
        print(f"updated {args.doc} ({len(blocks) - len(absent)} blocks)")
    else:
        print(f"{args.doc} is up to date")
    return 1 if (missing or absent) else 0


if __name__ == "__main__":
    sys.exit(main())
