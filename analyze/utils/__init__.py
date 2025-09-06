# encoding: utf-8

"""
工具层模块
提供配置管理和其他实用工具
"""

from .config import Config
from .compatibility import (
    analyze_j_under_13, 
    analyze_j13_volume_pattern,
    StockAnalyzer as LegacyStockAnalyzer
)

__all__ = [
    'Config',
    'analyze_j_under_13',
    'analyze_j13_volume_pattern',
    'LegacyStockAnalyzer'
]
