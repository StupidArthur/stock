# encoding: utf-8

"""
核心股票分析器
协调各个组件，提供统一的分析接口
"""

from typing import List, Dict, Optional, Tuple
from pathlib import Path
import pandas as pd

from ..data import StockDataLoader, StockRepository
from ..strategies import StrategyRegistry, BaseStrategy, StrategyResult
from ..strategies import JValueStrategy, VolumePatternStrategy
from .scoring_engine import ScoringEngine, StockScore, RankedStock


class AnalysisResults:
    """分析结果集合"""
    
    def __init__(self, stock_scores: List[StockScore], 
                 ranked_stocks: List[RankedStock] = None,
                 analysis_date: str = "",
                 strategy_performance: Dict = None):
        self.stock_scores = stock_scores
        self.ranked_stocks = ranked_stocks or []
        self.analysis_date = analysis_date
        self.strategy_performance = strategy_performance or {}
    
    @property
    def total_stocks(self) -> int:
        """总股票数"""
        return len(self.stock_scores)
    
    @property
    def qualified_stocks(self) -> int:
        """符合条件的股票数"""
        return len([score for score in self.stock_scores if score.qualified_count > 0])
    
    def get_top_stocks(self, n: int = 10) -> List[RankedStock]:
        """获取前N名股票"""
        return self.ranked_stocks[:n]
    
    def get_stocks_by_strategy(self, strategy_name: str) -> List[StockScore]:
        """获取通过指定策略的股票"""
        return [
            score for score in self.stock_scores
            if strategy_name in score.qualified_strategies
        ]


class StockAnalyzer:
    """股票分析器 - 协调各个组件的主分析器"""
    
    def __init__(self, data_date: str = None, 
                 base_data_dir: Path = None,
                 stock_list_file: Path = None):
        """
        初始化分析器
        Args:
            data_date: 分析日期，格式YYYYMMDD，如果为None则使用最新日期
            base_data_dir: 数据根目录
            stock_list_file: 股票列表文件路径
        """
        # 初始化数据层
        self.data_loader = StockDataLoader(base_data_dir, stock_list_file)
        self.repository = StockRepository(self.data_loader)
        
        # 确定分析日期
        if data_date is None:
            self.data_date = self.data_loader.get_latest_date()
        else:
            self.data_date = data_date
        
        # 初始化策略层
        self.strategy_registry = StrategyRegistry()
        self._register_default_strategies()
        
        # 初始化评分引擎
        self.scoring_engine = ScoringEngine()
        
        # 输出信息
        latest_date = self.data_loader.get_latest_date()
        if self.data_date == latest_date:
            print(f"分析日期: {self.data_date} (最新数据)")
        else:
            print(f"分析日期: {self.data_date} (历史数据)")
            print(f"数据来源: {latest_date} 目录（包含完整历史数据）")
        
        print(f"数据目录: {self.data_loader.get_data_dir()}")
    
    def _register_default_strategies(self):
        """注册默认策略"""
        # 注册J值策略
        j_strategy = JValueStrategy(max_j_value=13.0, weight=1.0)
        self.strategy_registry.register_strategy(j_strategy)
        
        # 注册量价关系策略
        volume_strategy = VolumePatternStrategy(
            days_to_analyze=20, 
            min_volume_contrast=1.2,
            weight=1.0
        )
        self.strategy_registry.register_strategy(volume_strategy)
    
    def add_strategy(self, strategy: BaseStrategy):
        """
        添加分析策略
        Args:
            strategy: 策略实例
        """
        self.strategy_registry.register_strategy(strategy)
    
    def add_filter_strategy(self, strategy: BaseStrategy, threshold_score: float = 100.0):
        """
        添加筛选策略（必须满足的条件）
        Args:
            strategy: 策略实例
            threshold_score: 筛选阈值得分，默认100.0表示必须is_qualified=True
        """
        strategy.set_filter_strategy(True, threshold_score)
        self.strategy_registry.register_strategy(strategy)
    
    def add_scoring_strategy(self, strategy: BaseStrategy):
        """
        添加评分策略（用于排序的策略）
        Args:
            strategy: 策略实例
        """
        strategy.set_filter_strategy(False)
        self.strategy_registry.register_strategy(strategy)
    
    def remove_strategy(self, strategy_name: str):
        """
        移除分析策略
        Args:
            strategy_name: 策略名称
        """
        self.strategy_registry.unregister_strategy(strategy_name)
    
    def get_strategies(self) -> List[BaseStrategy]:
        """获取所有策略"""
        return self.strategy_registry.get_all_strategies()
    
    def get_enabled_strategies(self) -> List[BaseStrategy]:
        """获取启用的策略"""
        return self.strategy_registry.get_enabled_strategies()
    
    def get_filter_strategies(self) -> List[BaseStrategy]:
        """获取筛选策略"""
        return self.strategy_registry.get_filter_strategies()
    
    def get_scoring_strategies(self) -> List[BaseStrategy]:
        """获取评分策略"""
        return self.strategy_registry.get_scoring_strategies()
    
    def set_strategy_weight(self, strategy_name: str, weight: float):
        """设置策略权重"""
        self.strategy_registry.set_strategy_weight(strategy_name, weight)
    
    def set_strategy_enabled(self, strategy_name: str, enabled: bool):
        """设置策略启用状态"""
        self.strategy_registry.set_strategy_enabled(strategy_name, enabled)
    
    def analyze_stocks(self, stock_codes: List[str] = None,
                      markets: List[str] = None,
                      strategy_names: List[str] = None,
                      min_data_length: int = 30) -> AnalysisResults:
        """
        执行股票分析
        Args:
            stock_codes: 指定股票代码列表，如果为None则分析所有股票
            markets: 市场列表，如['主板', '创业板']
            strategy_names: 使用的策略名称列表，如果为None则使用所有启用的策略
            min_data_length: 最小数据长度要求
        Returns:
            分析结果
        """
        print("=== 开始股票分析 ===")
        
        # 获取要分析的股票
        if stock_codes is not None:
            # 分析指定股票
            stocks_data = self._get_specified_stocks_data(stock_codes)
        else:
            # 分析所有符合条件的股票
            if markets is None:
                markets = ['主板', '创业板']
            stocks_data = self._get_stocks_by_markets(markets, min_data_length)
        
        if not stocks_data:
            print("没有找到符合条件的股票")
            return AnalysisResults([], [], self.data_date)
        
        print(f"开始分析 {len(stocks_data)} 只股票...")
        
        # 批量分析
        all_strategy_results = {}
        processed_count = 0
        
        for stock_info, stock_data in stocks_data:
            # 筛选数据到指定日期
            filtered_data = self._filter_data_by_date(stock_data, self.data_date)
            if filtered_data is None or len(filtered_data) < 5:
                continue
            
            # 分析股票 - 使用分层分析（先筛选，再评分）
            strategy_results = self.strategy_registry.analyze_stock_with_filtering(
                stock_info.ts_code, 
                stock_info.name, 
                filtered_data,
                strategy_names
            )
            
            if strategy_results:
                all_strategy_results[stock_info.ts_code] = strategy_results
            
            processed_count += 1
            if processed_count % 100 == 0:
                print(f"已处理 {processed_count}/{len(stocks_data)} 只股票...")
        
        print(f"分析完成，共处理 {processed_count} 只股票，获得有效结果 {len(all_strategy_results)} 只")
        
        # 计算评分
        strategy_weights = self._get_strategy_weights(strategy_names)
        filter_strategy_names = [s.get_name() for s in self.get_filter_strategies()]
        stock_scores = self.scoring_engine.batch_calculate_scores(
            all_strategy_results, strategy_weights, filter_strategy_names
        )
        
        # 排序
        ranked_stocks = self.scoring_engine.rank_stocks(
            stock_scores, 
            sort_by="weighted_score",
            min_score=0.0,
            min_qualified_strategies=1
        )
        
        # 策略表现分析
        strategy_performance = self.scoring_engine.get_strategy_performance(stock_scores)
        
        return AnalysisResults(
            stock_scores=stock_scores,
            ranked_stocks=ranked_stocks,
            analysis_date=self.data_date,
            strategy_performance=strategy_performance
        )
    
    def analyze_j_under_value(self, max_j_value: float = 13.0) -> AnalysisResults:
        """
        分析J值小于指定值的股票
        Args:
            max_j_value: J值上限
        Returns:
            分析结果
        """
        # 临时创建J值策略
        j_strategy = JValueStrategy(max_j_value=max_j_value)
        
        # 临时注册策略
        original_strategies = self.strategy_registry.get_strategy_names()
        self.strategy_registry.clear()
        self.strategy_registry.register_strategy(j_strategy)
        
        try:
            results = self.analyze_stocks(strategy_names=[j_strategy.get_name()])
        finally:
            # 恢复原有策略
            self.strategy_registry.clear()
            self._register_default_strategies()
        
        return results
    
    def analyze_j_with_volume_pattern(self, max_j_value: float = 13.0,
                                    days_to_analyze: int = 20) -> AnalysisResults:
        """
        分析J值小于指定值且符合量价关系的股票
        Args:
            max_j_value: J值上限
            days_to_analyze: 量价分析天数
        Returns:
            分析结果
        """
        # 创建组合策略
        j_strategy = JValueStrategy(max_j_value=max_j_value, weight=0.6)
        volume_strategy = VolumePatternStrategy(days_to_analyze=days_to_analyze, weight=0.4)
        
        # 临时替换策略
        original_strategies = self.strategy_registry.get_strategy_names()
        self.strategy_registry.clear()
        self.strategy_registry.register_strategy(j_strategy)
        self.strategy_registry.register_strategy(volume_strategy)
        
        try:
            results = self.analyze_stocks()
            # 只保留同时通过两个策略的股票
            filtered_ranked = [
                ranked for ranked in results.ranked_stocks
                if ranked.stock_score.qualified_count >= 2
            ]
            results.ranked_stocks = filtered_ranked
        finally:
            # 恢复原有策略
            self.strategy_registry.clear()
            self._register_default_strategies()
        
        return results
    
    def _get_specified_stocks_data(self, stock_codes: List[str]) -> List[Tuple]:
        """获取指定股票的数据"""
        stocks_data = []
        
        for stock_code in stock_codes:
            stock_info = self.repository.get_stock_info(stock_code)
            if stock_info is None:
                continue
            
            stock_data = self.data_loader.load_stock_data(stock_info.code_6digit)
            if stock_data is not None:
                stocks_data.append((stock_info, stock_data))
        
        return stocks_data
    
    def _get_stocks_by_markets(self, markets: List[str], 
                              min_data_length: int) -> List[Tuple]:
        """根据市场获取股票数据"""
        return self.repository.get_stocks_by_criteria(
            markets=markets,
            min_data_length=min_data_length
        )
    
    def _filter_data_by_date(self, stock_data: pd.DataFrame, 
                           end_date: str) -> Optional[pd.DataFrame]:
        """筛选数据到指定日期"""
        if stock_data is None or stock_data.empty:
            return None
        
        # 确保trade_date是字符串类型
        stock_data = stock_data.copy()
        stock_data['trade_date'] = stock_data['trade_date'].astype(str)
        
        # 筛选数据
        filtered_data = stock_data[stock_data['trade_date'] <= end_date]
        
        return filtered_data if not filtered_data.empty else None
    
    def _get_strategy_weights(self, strategy_names: List[str] = None) -> Dict[str, float]:
        """获取策略权重"""
        if strategy_names is None:
            strategies = self.strategy_registry.get_enabled_strategies()
        else:
            strategies = [
                self.strategy_registry.get_strategy(name) 
                for name in strategy_names
                if self.strategy_registry.get_strategy(name) is not None
            ]
        
        return {strategy.get_name(): strategy.get_weight() for strategy in strategies}
    
    def get_analysis_summary(self, results: AnalysisResults) -> Dict:
        """获取分析总结"""
        return {
            'analysis_date': self.data_date,
            'total_stocks_analyzed': results.total_stocks,
            'qualified_stocks': results.qualified_stocks,
            'top_10_stocks': [
                {
                    'rank': ranked.rank,
                    'stock_code': ranked.stock_code,
                    'stock_name': ranked.stock_name,
                    'weighted_score': round(ranked.weighted_score, 2),
                    'qualified_strategies': len(ranked.stock_score.qualified_strategies)
                }
                for ranked in results.get_top_stocks(10)
            ],
            'strategy_performance': results.strategy_performance,
            'enabled_strategies': [s.get_name() for s in self.get_enabled_strategies()]
        }
    
    def set_scoring_method(self, method: str):
        """设置评分方法"""
        self.scoring_engine.set_scoring_method(method)
    
    def set_data_date(self, data_date: str):
        """设置分析日期"""
        self.data_date = data_date
