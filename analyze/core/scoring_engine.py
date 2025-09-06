# encoding: utf-8

"""
评分引擎
根据多种策略的分析结果计算综合评分
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from ..strategies.base_strategy import StrategyResult


@dataclass
class StockScore:
    """股票综合评分"""
    
    stock_code: str
    stock_name: str
    total_score: float          # 总得分 (0-100)
    weighted_score: float       # 加权得分 (0-100)
    strategy_scores: Dict[str, float] = field(default_factory=dict)  # 各策略得分
    strategy_weights: Dict[str, float] = field(default_factory=dict) # 各策略权重
    strategy_results: Dict[str, StrategyResult] = field(default_factory=dict) # 详细结果
    qualified_strategies: List[str] = field(default_factory=list)    # 符合条件的策略
    confidence: float = 0.0     # 整体置信度
    
    # 市场数据
    current_price: float = 0.0
    trade_date: str = ""
    
    def __post_init__(self):
        """计算衍生指标"""
        if self.strategy_results:
            # 计算符合条件的策略数量
            self.qualified_strategies = [
                name for name, result in self.strategy_results.items()
                if result.is_qualified
            ]
            
            # 计算整体置信度 (所有策略置信度的加权平均)
            if self.strategy_weights:
                total_weight = sum(self.strategy_weights.values())
                if total_weight > 0:
                    weighted_confidence = sum(
                        result.confidence * self.strategy_weights.get(name, 0)
                        for name, result in self.strategy_results.items()
                    )
                    self.confidence = weighted_confidence / total_weight
    
    @property
    def qualified_count(self) -> int:
        """符合条件的策略数量"""
        return len(self.qualified_strategies)
    
    @property
    def total_strategies(self) -> int:
        """总策略数量"""
        return len(self.strategy_results)
    
    @property
    def qualification_rate(self) -> float:
        """策略通过率"""
        if self.total_strategies == 0:
            return 0.0
        return self.qualified_count / self.total_strategies


@dataclass
class RankedStock:
    """排序后的股票"""
    
    rank: int
    stock_score: StockScore
    
    @property
    def stock_code(self) -> str:
        return self.stock_score.stock_code
    
    @property
    def stock_name(self) -> str:
        return self.stock_score.stock_name
    
    @property
    def total_score(self) -> float:
        return self.stock_score.total_score
    
    @property
    def weighted_score(self) -> float:
        return self.stock_score.weighted_score


class ScoringEngine:
    """评分引擎"""
    
    def __init__(self, scoring_method: str = "weighted_average"):
        """
        初始化评分引擎
        Args:
            scoring_method: 评分方法 ("weighted_average", "multiplicative", "max_score")
        """
        self.scoring_method = scoring_method
        self.min_qualified_strategies = 1  # 最少需要通过的策略数
    
    def calculate_stock_score(self, stock_code: str, stock_name: str,
                            strategy_results: Dict[str, StrategyResult],
                            strategy_weights: Dict[str, float] = None,
                            filter_strategy_names: List[str] = None) -> StockScore:
        """
        计算单只股票的综合评分
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            strategy_results: 策略分析结果
            strategy_weights: 策略权重，如果为None则使用策略自身权重
            filter_strategy_names: 筛选策略名称列表，用于区分筛选策略和评分策略
        Returns:
            股票综合评分
        """
        if not strategy_results:
            return StockScore(
                stock_code=stock_code,
                stock_name=stock_name,
                total_score=0.0,
                weighted_score=0.0
            )
        
        # 分离筛选策略和评分策略
        if filter_strategy_names is None:
            filter_strategy_names = []
        
        scoring_results = {
            name: result for name, result in strategy_results.items()
            if name not in filter_strategy_names
        }
        
        # 处理策略权重（只对评分策略计算权重）
        if strategy_weights is None:
            # 从策略结果中获取权重（需要从策略实例获取，这里先设为1.0）
            strategy_weights = {name: 1.0 for name in scoring_results.keys()}
        else:
            # 只保留评分策略的权重
            strategy_weights = {
                name: weight for name, weight in strategy_weights.items()
                if name in scoring_results
            }
        
        # 提取各策略得分（只计算评分策略的得分）
        strategy_scores = {
            name: result.score for name, result in scoring_results.items()
        }
        
        # 计算总得分和加权得分
        total_score, weighted_score = self._calculate_scores(
            strategy_scores, strategy_weights
        )
        
        # 获取市场数据（从第一个有效结果中获取）
        current_price = 0.0
        trade_date = ""
        for result in strategy_results.values():
            if result.current_price > 0:
                current_price = result.current_price
                trade_date = result.trade_date
                break
        
        return StockScore(
            stock_code=stock_code,
            stock_name=stock_name,
            total_score=total_score,
            weighted_score=weighted_score,
            strategy_scores=strategy_scores,
            strategy_weights=strategy_weights,
            strategy_results=strategy_results,  # 包含所有策略结果（筛选+评分）
            current_price=current_price,
            trade_date=trade_date
        )
    
    def _calculate_scores(self, strategy_scores: Dict[str, float],
                         strategy_weights: Dict[str, float]) -> Tuple[float, float]:
        """
        计算得分
        Args:
            strategy_scores: 策略得分
            strategy_weights: 策略权重
        Returns:
            (总得分, 加权得分)
        """
        if not strategy_scores:
            return 0.0, 0.0
        
        scores = list(strategy_scores.values())
        weights = [strategy_weights.get(name, 1.0) for name in strategy_scores.keys()]
        
        # 计算简单平均得分
        total_score = sum(scores) / len(scores)
        
        # 计算加权得分
        if self.scoring_method == "weighted_average":
            total_weight = sum(weights)
            if total_weight > 0:
                weighted_score = sum(score * weight for score, weight in zip(scores, weights)) / total_weight
            else:
                weighted_score = total_score
        
        elif self.scoring_method == "multiplicative":
            # 乘积法：所有得分的几何平均
            if all(score > 0 for score in scores):
                product = 1.0
                for score, weight in zip(scores, weights):
                    product *= (score / 100) ** weight
                weighted_score = (product ** (1 / sum(weights))) * 100
            else:
                weighted_score = 0.0
        
        elif self.scoring_method == "max_score":
            # 最高分法：取权重最高策略的得分
            max_weight_idx = weights.index(max(weights))
            weighted_score = scores[max_weight_idx]
        
        else:
            weighted_score = total_score
        
        return total_score, weighted_score
    
    def batch_calculate_scores(self, stocks_strategy_results: Dict[str, Dict[str, StrategyResult]],
                             strategy_weights: Dict[str, float] = None,
                             filter_strategy_names: List[str] = None) -> List[StockScore]:
        """
        批量计算股票评分
        Args:
            stocks_strategy_results: {stock_code: {strategy_name: result}}
            strategy_weights: 策略权重
            filter_strategy_names: 筛选策略名称列表
        Returns:
            股票评分列表
        """
        stock_scores = []
        
        for stock_code, strategy_results in stocks_strategy_results.items():
            if not strategy_results:
                continue
            
            # 从第一个结果获取股票名称
            stock_name = list(strategy_results.values())[0].stock_name
            
            stock_score = self.calculate_stock_score(
                stock_code, stock_name, strategy_results, strategy_weights, filter_strategy_names
            )
            stock_scores.append(stock_score)
        
        return stock_scores
    
    def rank_stocks(self, stock_scores: List[StockScore],
                   sort_by: str = "weighted_score",
                   min_score: float = 0.0,
                   min_qualified_strategies: int = None) -> List[RankedStock]:
        """
        对股票进行排序
        Args:
            stock_scores: 股票评分列表
            sort_by: 排序依据 ("total_score", "weighted_score", "qualified_count")
            min_score: 最低得分要求
            min_qualified_strategies: 最少通过策略数要求
        Returns:
            排序后的股票列表
        """
        if min_qualified_strategies is None:
            min_qualified_strategies = self.min_qualified_strategies
        
        # 筛选符合条件的股票
        filtered_stocks = []
        for stock_score in stock_scores:
            if sort_by == "total_score" and stock_score.total_score < min_score:
                continue
            elif sort_by == "weighted_score" and stock_score.weighted_score < min_score:
                continue
            
            if stock_score.qualified_count < min_qualified_strategies:
                continue
            
            filtered_stocks.append(stock_score)
        
        # 排序
        if sort_by == "total_score":
            filtered_stocks.sort(key=lambda x: x.total_score, reverse=True)
        elif sort_by == "weighted_score":
            filtered_stocks.sort(key=lambda x: x.weighted_score, reverse=True)
        elif sort_by == "qualified_count":
            filtered_stocks.sort(key=lambda x: (x.qualified_count, x.weighted_score), reverse=True)
        elif sort_by == "confidence":
            filtered_stocks.sort(key=lambda x: x.confidence, reverse=True)
        else:
            # 默认按加权得分排序
            filtered_stocks.sort(key=lambda x: x.weighted_score, reverse=True)
        
        # 创建排序结果
        ranked_stocks = []
        for i, stock_score in enumerate(filtered_stocks, 1):
            ranked_stocks.append(RankedStock(rank=i, stock_score=stock_score))
        
        return ranked_stocks
    
    def get_strategy_performance(self, stock_scores: List[StockScore]) -> Dict[str, Dict]:
        """
        分析各策略的表现
        Args:
            stock_scores: 股票评分列表
        Returns:
            策略表现统计
        """
        strategy_stats = {}
        
        # 收集所有策略名称
        all_strategies = set()
        for stock_score in stock_scores:
            all_strategies.update(stock_score.strategy_results.keys())
        
        # 统计各策略表现
        for strategy_name in all_strategies:
            scores = []
            qualified_count = 0
            total_count = 0
            
            for stock_score in stock_scores:
                if strategy_name in stock_score.strategy_results:
                    result = stock_score.strategy_results[strategy_name]
                    scores.append(result.score)
                    if result.is_qualified:
                        qualified_count += 1
                    total_count += 1
            
            if scores:
                strategy_stats[strategy_name] = {
                    'average_score': sum(scores) / len(scores),
                    'max_score': max(scores),
                    'min_score': min(scores),
                    'qualified_count': qualified_count,
                    'total_count': total_count,
                    'qualification_rate': qualified_count / total_count if total_count > 0 else 0.0
                }
        
        return strategy_stats
    
    def set_scoring_method(self, method: str):
        """设置评分方法"""
        valid_methods = ["weighted_average", "multiplicative", "max_score"]
        if method not in valid_methods:
            raise ValueError(f"不支持的评分方法: {method}，可选: {valid_methods}")
        self.scoring_method = method
    
    def set_min_qualified_strategies(self, count: int):
        """设置最少通过策略数要求"""
        if count < 0:
            raise ValueError("最少通过策略数不能为负数")
        self.min_qualified_strategies = count
