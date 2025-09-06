# encoding: utf-8

"""
结果格式化器
将分析结果格式化为不同的显示形式
"""

from abc import ABC, abstractmethod
from typing import List, Dict
from ..core.scoring_engine import RankedStock, StockScore
from ..core.stock_analyzer import AnalysisResults


class BaseFormatter(ABC):
    """格式化器基类"""
    
    @abstractmethod
    def format_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           title: str = "股票排名") -> str:
        """格式化排名股票列表"""
        pass
    
    @abstractmethod
    def format_analysis_summary(self, results: AnalysisResults) -> str:
        """格式化分析总结"""
        pass


class ConsoleFormatter(BaseFormatter):
    """控制台输出格式化器"""
    
    def format_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           title: str = "股票排名") -> str:
        """格式化排名股票列表为控制台输出"""
        if not ranked_stocks:
            return f"\n=== {title} ===\n没有符合条件的股票\n"
        
        output = [f"\n=== {title} ==="]
        output.append(f"共找到 {len(ranked_stocks)} 只股票")
        output.append("")
        
        # 表头
        header = f"{'排名':>4} {'股票代码':>10} {'股票名称':>15} {'综合得分':>8} {'加权得分':>8} {'通过策略':>8} {'置信度':>8} {'收盘价':>8}"
        output.append(header)
        output.append("-" * len(header))
        
        # 数据行
        for ranked in ranked_stocks:
            score = ranked.stock_score
            line = (f"{ranked.rank:4d} "
                   f"{score.stock_code:>10} "
                   f"{score.stock_name:>15} "
                   f"{score.total_score:8.1f} "
                   f"{score.weighted_score:8.1f} "
                   f"{score.qualified_count:8d} "
                   f"{score.confidence:8.2f} "
                   f"{score.current_price:8.2f}")
            output.append(line)
        
        return "\n".join(output)
    
    def format_strategy_details(self, ranked_stocks: List[RankedStock], 
                              strategy_name: str) -> str:
        """格式化特定策略的详细结果"""
        if not ranked_stocks:
            return f"\n=== {strategy_name} 详细结果 ===\n没有符合条件的股票\n"
        
        output = [f"\n=== {strategy_name} 详细结果 ==="]
        
        # 只显示通过该策略的股票
        qualified_stocks = [
            ranked for ranked in ranked_stocks
            if strategy_name in ranked.stock_score.qualified_strategies
        ]
        
        if not qualified_stocks:
            output.append("没有股票通过该策略")
            return "\n".join(output)
        
        output.append(f"通过该策略的股票: {len(qualified_stocks)} 只")
        output.append("")
        
        for ranked in qualified_stocks:
            score = ranked.stock_score
            strategy_result = score.strategy_results.get(strategy_name)
            
            if strategy_result:
                output.append(f"{ranked.rank:2d}. {score.stock_name} ({score.stock_code})")
                output.append(f"    策略得分: {strategy_result.score:.1f}")
                output.append(f"    置信度: {strategy_result.confidence:.2f}")
                output.append(f"    分析理由: {strategy_result.reason}")
                if strategy_result.details:
                    for key, value in strategy_result.details.items():
                        if isinstance(value, (int, float)):
                            output.append(f"    {key}: {value:.2f}")
                        elif not isinstance(value, dict):
                            output.append(f"    {key}: {value}")
                output.append("")
        
        return "\n".join(output)
    
    def format_analysis_summary(self, results: AnalysisResults) -> str:
        """格式化分析总结"""
        output = [f"\n=== 分析总结 ({results.analysis_date}) ==="]
        
        output.append(f"总分析股票数: {results.total_stocks}")
        output.append(f"符合条件股票数: {results.qualified_stocks}")
        output.append(f"符合条件比例: {results.qualified_stocks/results.total_stocks*100:.1f}%")
        output.append("")
        
        # 策略表现
        if results.strategy_performance:
            output.append("策略表现:")
            for strategy_name, stats in results.strategy_performance.items():
                output.append(f"  {strategy_name}:")
                output.append(f"    平均得分: {stats['average_score']:.1f}")
                output.append(f"    通过率: {stats['qualification_rate']*100:.1f}%")
                output.append(f"    通过数量: {stats['qualified_count']}/{stats['total_count']}")
            output.append("")
        
        # 前5名股票
        top_stocks = results.get_top_stocks(5)
        if top_stocks:
            output.append("前5名股票:")
            for ranked in top_stocks:
                score = ranked.stock_score
                strategies = ", ".join(score.qualified_strategies)
                output.append(f"  {ranked.rank}. {score.stock_name} ({score.stock_code}) - "
                            f"得分: {score.weighted_score:.1f}, 策略: {strategies}")
        
        return "\n".join(output)
    
    def format_j_value_results(self, ranked_stocks: List[RankedStock]) -> str:
        """格式化J值筛选结果"""
        if not ranked_stocks:
            return "\n=== J值筛选结果 ===\n没有符合条件的股票\n"
        
        output = ["\n=== J值筛选结果 ==="]
        output.append(f"共找到 {len(ranked_stocks)} 只股票")
        output.append("")
        
        # 表头
        header = f"{'序号':>4} {'股票代码':>10} {'股票名称':>15} {'J值':>8} {'得分':>8} {'收盘价':>8}"
        output.append(header)
        output.append("-" * len(header))
        
        # 数据行
        for ranked in ranked_stocks:
            score = ranked.stock_score
            j_strategy_result = None
            
            # 查找J值策略结果
            for result in score.strategy_results.values():
                if "J值" in result.strategy_name:
                    j_strategy_result = result
                    break
            
            if j_strategy_result and 'j_value' in j_strategy_result.details:
                j_value = j_strategy_result.details['j_value']
                line = (f"{ranked.rank:4d} "
                       f"{score.stock_code:>10} "
                       f"{score.stock_name:>15} "
                       f"{j_value:8.2f} "
                       f"{j_strategy_result.score:8.1f} "
                       f"{score.current_price:8.2f}")
                output.append(line)
        
        return "\n".join(output)
    
    def format_volume_pattern_results(self, ranked_stocks: List[RankedStock]) -> str:
        """格式化量价关系结果"""
        if not ranked_stocks:
            return "\n=== 量价关系分析结果 ===\n没有符合条件的股票\n"
        
        output = ["\n=== 量价关系分析结果 ==="]
        output.append(f"共找到 {len(ranked_stocks)} 只股票")
        output.append("")
        
        # 表头
        header = f"{'序号':>4} {'股票代码':>10} {'股票名称':>15} {'量比对比':>8} {'涨日量比':>8} {'跌日量比':>8} {'5日涨幅%':>8} {'得分':>8}"
        output.append(header)
        output.append("-" * len(header))
        
        # 数据行
        for ranked in ranked_stocks:
            score = ranked.stock_score
            volume_strategy_result = None
            
            # 查找量价策略结果
            for result in score.strategy_results.values():
                if "量价" in result.strategy_name:
                    volume_strategy_result = result
                    break
            
            if volume_strategy_result and volume_strategy_result.details:
                details = volume_strategy_result.details
                line = (f"{ranked.rank:4d} "
                       f"{score.stock_code:>10} "
                       f"{score.stock_name:>15} "
                       f"{details.get('volume_contrast', 0):8.1f} "
                       f"{details.get('avg_vol_ratio_up', 0):8.1f} "
                       f"{details.get('avg_vol_ratio_down', 0):8.1f} "
                       f"{details.get('recent_return_5d', 0):8.1f} "
                       f"{volume_strategy_result.score:8.1f}")
                output.append(line)
        
        return "\n".join(output)


class TableFormatter(BaseFormatter):
    """表格格式化器"""
    
    def format_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           title: str = "股票排名") -> str:
        """格式化为简单表格"""
        if not ranked_stocks:
            return f"{title}: 没有符合条件的股票"
        
        lines = [f"{title} (共{len(ranked_stocks)}只)"]
        for ranked in ranked_stocks[:10]:  # 只显示前10名
            score = ranked.stock_score
            lines.append(f"{ranked.rank}. {score.stock_name}({score.stock_code}) - 得分:{score.weighted_score:.1f}")
        
        return "\n".join(lines)
    
    def format_analysis_summary(self, results: AnalysisResults) -> str:
        """格式化分析总结"""
        summary = []
        summary.append(f"分析日期: {results.analysis_date}")
        summary.append(f"分析股票: {results.total_stocks}只")
        summary.append(f"符合条件: {results.qualified_stocks}只")
        
        if results.ranked_stocks:
            top1 = results.ranked_stocks[0]
            summary.append(f"最佳股票: {top1.stock_score.stock_name}({top1.stock_score.stock_code})")
        
        return " | ".join(summary)
