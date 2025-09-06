# encoding: utf-8

"""
策略注册器
管理所有分析策略的注册、查找和执行
"""

from typing import Dict, List, Optional, Type
from .base_strategy import BaseStrategy, StrategyResult
import pandas as pd


class StrategyRegistry:
    """策略注册器"""
    
    def __init__(self):
        """初始化策略注册器"""
        self._strategies: Dict[str, BaseStrategy] = {}
        self._strategy_classes: Dict[str, Type[BaseStrategy]] = {}
    
    def register_strategy(self, strategy: BaseStrategy):
        """
        注册策略实例
        Args:
            strategy: 策略实例
        """
        if not isinstance(strategy, BaseStrategy):
            raise TypeError("策略必须继承自 BaseStrategy")
        
        name = strategy.get_name()
        if name in self._strategies:
            print(f"警告: 策略 '{name}' 已存在，将被覆盖")
        
        self._strategies[name] = strategy
        print(f"策略 '{name}' 注册成功")
    
    def register_strategy_class(self, strategy_class: Type[BaseStrategy], name: str = None):
        """
        注册策略类
        Args:
            strategy_class: 策略类
            name: 策略名称，如果为None则使用类的默认名称
        """
        if not issubclass(strategy_class, BaseStrategy):
            raise TypeError("策略类必须继承自 BaseStrategy")
        
        if name is None:
            # 创建临时实例获取默认名称
            temp_instance = strategy_class()
            name = temp_instance.get_default_name()
        
        self._strategy_classes[name] = strategy_class
    
    def unregister_strategy(self, name: str):
        """
        取消注册策略
        Args:
            name: 策略名称
        """
        if name in self._strategies:
            del self._strategies[name]
            print(f"策略 '{name}' 已取消注册")
        else:
            print(f"策略 '{name}' 不存在")
    
    def get_strategy(self, name: str) -> Optional[BaseStrategy]:
        """
        获取策略实例
        Args:
            name: 策略名称
        Returns:
            策略实例，如果不存在返回None
        """
        return self._strategies.get(name)
    
    def get_all_strategies(self) -> List[BaseStrategy]:
        """
        获取所有注册的策略
        Returns:
            策略列表
        """
        return list(self._strategies.values())
    
    def get_enabled_strategies(self) -> List[BaseStrategy]:
        """
        获取所有启用的策略
        Returns:
            启用的策略列表
        """
        return [strategy for strategy in self._strategies.values() 
                if strategy.is_enabled()]
    
    def get_filter_strategies(self) -> List[BaseStrategy]:
        """
        获取所有启用的筛选策略
        Returns:
            启用的筛选策略列表
        """
        return [strategy for strategy in self._strategies.values() 
                if strategy.is_enabled() and strategy.is_filter()]
    
    def get_scoring_strategies(self) -> List[BaseStrategy]:
        """
        获取所有启用的评分策略（非筛选策略）
        Returns:
            启用的评分策略列表
        """
        return [strategy for strategy in self._strategies.values() 
                if strategy.is_enabled() and not strategy.is_filter()]
    
    def get_strategy_names(self) -> List[str]:
        """
        获取所有策略名称
        Returns:
            策略名称列表
        """
        return list(self._strategies.keys())
    
    def has_strategy(self, name: str) -> bool:
        """
        检查策略是否存在
        Args:
            name: 策略名称
        Returns:
            是否存在
        """
        return name in self._strategies
    
    def create_strategy_from_class(self, name: str, **kwargs) -> Optional[BaseStrategy]:
        """
        从注册的策略类创建实例
        Args:
            name: 策略名称
            **kwargs: 策略参数
        Returns:
            策略实例
        """
        if name not in self._strategy_classes:
            return None
        
        strategy_class = self._strategy_classes[name]
        return strategy_class(**kwargs)
    
    def analyze_stock(self, stock_code: str, stock_name: str, 
                     stock_data: pd.DataFrame, 
                     strategy_names: List[str] = None) -> Dict[str, StrategyResult]:
        """
        使用指定策略分析股票
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            stock_data: 股票数据
            strategy_names: 要使用的策略名称列表，如果为None则使用所有启用的策略
        Returns:
            {策略名称: 分析结果} 字典
        """
        results = {}
        
        if strategy_names is None:
            strategies = self.get_enabled_strategies()
        else:
            strategies = [self.get_strategy(name) for name in strategy_names 
                         if self.get_strategy(name) is not None]
        
        for strategy in strategies:
            try:
                result = strategy.analyze(stock_code, stock_name, stock_data)
                results[strategy.get_name()] = result
            except Exception as e:
                print(f"策略 '{strategy.get_name()}' 分析 {stock_code} 时出错: {e}")
                # 创建错误结果
                error_result = strategy.create_failed_result(
                    stock_code, stock_name, f"分析出错: {str(e)}"
                )
                results[strategy.get_name()] = error_result
        
        return results
    
    def analyze_stock_with_filtering(self, stock_code: str, stock_name: str, 
                                   stock_data: pd.DataFrame, 
                                   strategy_names: List[str] = None) -> Dict[str, StrategyResult]:
        """
        使用分层策略分析股票：先执行筛选策略，再执行评分策略
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            stock_data: 股票数据
            strategy_names: 要使用的策略名称列表，如果为None则使用所有启用的策略
        Returns:
            {策略名称: 分析结果} 字典，如果未通过筛选则返回空字典
        """
        results = {}
        
        # 确定要使用的策略
        if strategy_names is None:
            filter_strategies = self.get_filter_strategies()
            scoring_strategies = self.get_scoring_strategies()
            all_strategies = filter_strategies + scoring_strategies
        else:
            all_strategies = [self.get_strategy(name) for name in strategy_names 
                             if self.get_strategy(name) is not None]
            filter_strategies = [s for s in all_strategies if s.is_filter()]
            scoring_strategies = [s for s in all_strategies if not s.is_filter()]
        
        # 第一步：执行筛选策略
        for strategy in filter_strategies:
            try:
                result = strategy.analyze(stock_code, stock_name, stock_data)
                results[strategy.get_name()] = result
                
                # 检查是否通过筛选
                if not strategy.passes_filter(result):
                    # 如果未通过筛选，直接返回筛选结果，不执行评分策略
                    return results
                    
            except Exception as e:
                print(f"筛选策略 '{strategy.get_name()}' 分析 {stock_code} 时出错: {e}")
                error_result = strategy.create_failed_result(
                    stock_code, stock_name, f"分析出错: {str(e)}"
                )
                results[strategy.get_name()] = error_result
                # 筛选策略出错也视为未通过筛选
                return results
        
        # 第二步：通过筛选后，执行评分策略
        for strategy in scoring_strategies:
            try:
                result = strategy.analyze(stock_code, stock_name, stock_data)
                results[strategy.get_name()] = result
            except Exception as e:
                print(f"评分策略 '{strategy.get_name()}' 分析 {stock_code} 时出错: {e}")
                error_result = strategy.create_failed_result(
                    stock_code, stock_name, f"分析出错: {str(e)}"
                )
                results[strategy.get_name()] = error_result
        
        return results
    
    def batch_analyze_stocks(self, stocks_data: List[tuple], 
                           strategy_names: List[str] = None) -> Dict[str, Dict[str, StrategyResult]]:
        """
        批量分析股票
        Args:
            stocks_data: [(stock_code, stock_name, stock_data), ...] 列表
            strategy_names: 要使用的策略名称列表
        Returns:
            {stock_code: {strategy_name: result}} 字典
        """
        all_results = {}
        
        for stock_code, stock_name, stock_data in stocks_data:
            results = self.analyze_stock(stock_code, stock_name, stock_data, strategy_names)
            all_results[stock_code] = results
        
        return all_results
    
    def set_strategy_weight(self, name: str, weight: float):
        """
        设置策略权重
        Args:
            name: 策略名称
            weight: 权重值
        """
        strategy = self.get_strategy(name)
        if strategy:
            strategy.set_weight(weight)
        else:
            print(f"策略 '{name}' 不存在")
    
    def set_strategy_enabled(self, name: str, enabled: bool):
        """
        设置策略启用状态
        Args:
            name: 策略名称
            enabled: 是否启用
        """
        strategy = self.get_strategy(name)
        if strategy:
            strategy.set_enabled(enabled)
        else:
            print(f"策略 '{name}' 不存在")
    
    def get_total_weight(self) -> float:
        """
        获取所有启用策略的权重总和
        Returns:
            权重总和
        """
        return sum(strategy.get_weight() for strategy in self.get_enabled_strategies())
    
    def clear(self):
        """清空所有注册的策略"""
        self._strategies.clear()
        self._strategy_classes.clear()
    
    def __len__(self):
        """返回注册策略数量"""
        return len(self._strategies)
    
    def __contains__(self, name: str):
        """检查策略是否存在"""
        return name in self._strategies
    
    def __str__(self):
        enabled_count = len(self.get_enabled_strategies())
        total_count = len(self._strategies)
        return f"StrategyRegistry(总数: {total_count}, 启用: {enabled_count})"
