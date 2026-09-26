"""Section titles of the daily risk report and headings of the statements (01 §5.3.4, 03 §5.3)."""

from __future__ import annotations

# 01 §5.3.4 日报章节（表头 … 压力测试），顺序即页面顺序
SECTION_TITLES: tuple[str, ...] = (
    "表头",
    "摘要",
    "资金表（CTP 口径）",
    "盈亏与敞口",
    "限额使用",
    "事件与动作",
    "数据质量",
    "系统运行",
    "图表",
    "压力测试",
)

# 03 §5.3 STATEMENT_TXT：资金状况、成交记录、平仓明细、持仓明细、持仓汇总
STATEMENT_HEADINGS: tuple[str, ...] = ("资金状况", "成交记录", "平仓明细", "持仓明细", "持仓汇总")

__all__ = ["SECTION_TITLES", "STATEMENT_HEADINGS"]
