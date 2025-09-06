# encoding: utf-8

"""
股票分析系统
重构后的模块化股票分析框架
"""

# 核心模块
from .core import StockAnalyzer, ScoringEngine, StockScore, RankedStock

# 数据层
from .data import StockDataLoader, StockRepository

# 策略层
from .strategies import (
    BaseStrategy, StrategyResult, StrategyRegistry,
    JValueStrategy, VolumePatternStrategy
)

# 输出层
from .output import (
    ConsoleFormatter, TableFormatter,
    CSVExporter, ExcelExporter,
    AnalysisReporter
)

# 工具层
from .utils import Config

# 向后兼容
from .utils.compatibility import (
    analyze_j_under_13, 
    analyze_j13_volume_pattern,
    StockAnalyzer as LegacyStockAnalyzer
)

__version__ = "2.0.0"
__all__ = [
    # 核心类
    'StockAnalyzer',
    'ScoringEngine', 
    'StockScore',
    'RankedStock',
    
    # 数据层
    'StockDataLoader',
    'StockRepository',
    
    # 策略层
    'BaseStrategy',
    'StrategyResult', 
    'StrategyRegistry',
    'JValueStrategy',
    'VolumePatternStrategy',
    
    # 输出层
    'ConsoleFormatter',
    'TableFormatter',
    'CSVExporter',
    'ExcelExporter', 
    'AnalysisReporter',
    
    # 工具
    'Config',
    
    # 兼容性
    'analyze_j_under_13',
    'analyze_j13_volume_pattern',
    'LegacyStockAnalyzer'
]
