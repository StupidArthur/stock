# encoding: utf-8

"""
策略基类定义
所有分析策略都应该继承这个基类
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional
import pandas as pd


@dataclass
class StrategyResult:
    """策略分析结果"""
    
    # 基本信息
    stock_code: str
    stock_name: str
    strategy_name: str
    
    # 分析结果
    is_qualified: bool  # 是否符合策略条件
    score: float        # 策略得分 (0-100)
    confidence: float   # 置信度 (0-1)
    
    # 详细数据
    details: Dict[str, Any]  # 详细分析数据
    reason: str             # 分析理由
    
    # 市场数据
    current_price: float = 0.0
    trade_date: str = ""
    
    def __post_init__(self):
        """验证数据有效性"""
        if not 0 <= self.score <= 100:
            raise ValueError("score 必须在 0-100 之间")
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence 必须在 0-1 之间")


class BaseStrategy(ABC):
    """分析策略基类"""
    
    def __init__(self, name: str = None, weight: float = 1.0, 
                 enabled: bool = True, is_filter_strategy: bool = False,
                 threshold_score: float = 100.0, **kwargs):
        """
        初始化策略
        Args:
            name: 策略名称
            weight: 策略权重 (用于综合评分)
            enabled: 是否启用
            is_filter_strategy: 是否为筛选策略（必须满足的条件）
            threshold_score: 筛选策略的阈值得分，得分达到此值才算通过筛选
            **kwargs: 策略特定参数
        """
        self.name = name or self.get_default_name()
        self.weight = weight
        self.enabled = enabled
        self.is_filter_strategy = is_filter_strategy
        self.threshold_score = threshold_score
        self.params = kwargs
    
    @abstractmethod
    def analyze(self, stock_code: str, stock_name: str, 
                stock_data: pd.DataFrame) -> StrategyResult:
        """
        分析单只股票
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            stock_data: 股票历史数据
        Returns:
            策略分析结果
        """
        pass
    
    @abstractmethod
    def get_default_name(self) -> str:
        """获取策略默认名称"""
        pass
    
    def get_name(self) -> str:
        """获取策略名称"""
        return self.name
    
    def get_weight(self) -> float:
        """获取策略权重"""
        return self.weight
    
    def is_enabled(self) -> bool:
        """策略是否启用"""
        return self.enabled
    
    def set_enabled(self, enabled: bool):
        """设置策略启用状态"""
        self.enabled = enabled
    
    def set_weight(self, weight: float):
        """设置策略权重"""
        if weight < 0:
            raise ValueError("权重不能为负数")
        self.weight = weight
    
    def is_filter(self) -> bool:
        """是否为筛选策略"""
        return self.is_filter_strategy
    
    def set_filter_strategy(self, is_filter: bool, threshold_score: float = None):
        """设置为筛选策略"""
        self.is_filter_strategy = is_filter
        if threshold_score is not None:
            self.threshold_score = threshold_score
    
    def get_threshold_score(self) -> float:
        """获取筛选阈值得分"""
        return self.threshold_score
    
    def passes_filter(self, result: 'StrategyResult') -> bool:
        """
        检查分析结果是否通过筛选
        Args:
            result: 策略分析结果
        Returns:
            是否通过筛选
        """
        if not self.is_filter_strategy:
            return True  # 非筛选策略默认通过
        
        # 筛选策略需要满足两个条件：is_qualified=True 且 score >= threshold_score
        return result.is_qualified and result.score >= self.threshold_score
    
    def get_params(self) -> Dict[str, Any]:
        """获取策略参数"""
        return self.params.copy()
    
    def set_param(self, key: str, value: Any):
        """设置策略参数"""
        self.params[key] = value
    
    def validate_data(self, stock_data: pd.DataFrame, min_length: int = 5) -> bool:
        """
        验证股票数据是否满足分析要求
        Args:
            stock_data: 股票数据
            min_length: 最小数据长度
        Returns:
            是否满足要求
        """
        if stock_data is None or stock_data.empty:
            return False
        
        if len(stock_data) < min_length:
            return False
        
        # 检查必要的列是否存在
        required_columns = ['trade_date', 'close', 'vol']
        for col in required_columns:
            if col not in stock_data.columns:
                return False
        
        return True
    
    def get_latest_data(self, stock_data: pd.DataFrame) -> Optional[pd.Series]:
        """
        获取最新交易日数据
        Args:
            stock_data: 股票数据
        Returns:
            最新交易日数据
        """
        if stock_data is None or stock_data.empty:
            return None
        
        # 按日期排序，获取最新数据
        df_sorted = stock_data.sort_values('trade_date', ascending=True)
        return df_sorted.iloc[-1]
    
    def create_failed_result(self, stock_code: str, stock_name: str, 
                           reason: str = "数据不足") -> StrategyResult:
        """
        创建失败的分析结果
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            reason: 失败原因
        Returns:
            失败的策略结果
        """
        return StrategyResult(
            stock_code=stock_code,
            stock_name=stock_name,
            strategy_name=self.name,
            is_qualified=False,
            score=0.0,
            confidence=0.0,
            details={},
            reason=reason
        )
    
    def __str__(self):
        return f"{self.name}(weight={self.weight}, enabled={self.enabled})"
    
    def __repr__(self):
        return f"{self.__class__.__name__}(name='{self.name}', weight={self.weight})"
