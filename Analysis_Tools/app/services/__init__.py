# =============================================================
# SERVICES PACKAGE
# Contains shared business logic services
# =============================================================

from .signal_service import (
    BEARISH_SECTIONS,
    BULLISH_SECTIONS,
    TOP_N_ITEMS,
    build_screener_data_structure,
    compute_signals_simple,
    compute_signals_with_breakdown,
    get_signal_for_ticker,
)

__all__ = [
    "compute_signals_with_breakdown",
    "compute_signals_simple",
    "get_signal_for_ticker",
    "build_screener_data_structure",
    "BULLISH_SECTIONS",
    "BEARISH_SECTIONS",
    "TOP_N_ITEMS",
]
