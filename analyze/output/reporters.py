# encoding: utf-8

"""
报告生成器
整合格式化器和导出器，生成完整的分析报告
"""

from pathlib import Path
from typing import List, Optional
from .formatters import BaseFormatter, ConsoleFormatter
from .exporters import BaseExporter, CSVExporter
from ..core.scoring_engine import RankedStock
from ..core.stock_analyzer import AnalysisResults


class AnalysisReporter:
    """分析报告生成器"""
    
    def __init__(self, formatter: BaseFormatter = None, 
                 exporter: BaseExporter = None,
                 output_dir: Path = None):
        """
        初始化报告生成器
        Args:
            formatter: 格式化器
            exporter: 导出器
            output_dir: 输出目录
        """
        self.formatter = formatter or ConsoleFormatter()
        self.exporter = exporter or CSVExporter()
        
        if output_dir is None:
            self.output_dir = Path(__file__).parent.parent.parent / "analysis_results"
        else:
            self.output_dir = Path(output_dir)
        
        # 确保输出目录存在
        self.output_dir.mkdir(exist_ok=True)
    
    def generate_full_report(self, results: AnalysisResults, 
                           report_name: str = None,
                           print_to_console: bool = True,
                           export_to_file: bool = True) -> str:
        """
        生成完整的分析报告
        Args:
            results: 分析结果
            report_name: 报告名称，用于文件命名
            print_to_console: 是否打印到控制台
            export_to_file: 是否导出到文件
        Returns:
            控制台输出文本
        """
        if report_name is None:
            report_name = f"stock_analysis_{results.analysis_date}"
        
        # 生成控制台输出
        console_output = []
        
        # 1. 分析总结
        console_output.append(self.formatter.format_analysis_summary(results))
        
        # 2. 排名股票列表
        console_output.append(self.formatter.format_ranked_stocks(
            results.ranked_stocks, "综合排名 (前20名)"
        ))
        
        # 3. 各策略详细结果
        if results.strategy_performance:
            for strategy_name in results.strategy_performance.keys():
                strategy_stocks = [
                    ranked for ranked in results.ranked_stocks
                    if strategy_name in ranked.stock_score.qualified_strategies
                ]
                if strategy_stocks:
                    console_output.append(
                        self.formatter.format_strategy_details(strategy_stocks, strategy_name)
                    )
        
        output_text = "\n".join(console_output)
        
        # 打印到控制台
        if print_to_console:
            print(output_text)
        
        # 导出到文件
        if export_to_file:
            # 导出CSV
            csv_filepath = self.output_dir / f"{report_name}.csv"
            self.exporter.export_analysis_results(results, str(csv_filepath), include_details=True)
            
            # 如果是Excel导出器，也生成Excel文件
            if hasattr(self.exporter, 'export_analysis_results'):
                excel_filepath = self.output_dir / f"{report_name}.xlsx"
                try:
                    self.exporter.export_analysis_results(results, str(excel_filepath))
                except:
                    pass  # Excel导出失败不影响主流程
        
        return output_text
    
    def generate_j_value_report(self, results: AnalysisResults,
                              max_j_value: float = 13.0,
                              print_to_console: bool = True,
                              export_to_file: bool = True) -> str:
        """
        生成J值分析专项报告
        Args:
            results: 分析结果
            max_j_value: J值阈值
            print_to_console: 是否打印到控制台
            export_to_file: 是否导出到文件
        Returns:
            控制台输出文本
        """
        # 筛选包含J值策略的股票
        j_value_stocks = []
        for ranked in results.ranked_stocks:
            for strategy_name in ranked.stock_score.strategy_results.keys():
                if "J值" in strategy_name:
                    j_value_stocks.append(ranked)
                    break
        
        # 生成控制台输出
        output_text = self.formatter.format_j_value_results(j_value_stocks)
        
        if print_to_console:
            print(output_text)
        
        # 导出到文件
        if export_to_file and hasattr(self.exporter, 'export_j_value_results'):
            filename = f"j_under_{max_j_value}_{results.analysis_date}.csv"
            filepath = self.output_dir / filename
            self.exporter.export_j_value_results(j_value_stocks, str(filepath))
        
        return output_text
    
    def generate_volume_pattern_report(self, results: AnalysisResults,
                                     print_to_console: bool = True,
                                     export_to_file: bool = True) -> str:
        """
        生成量价关系分析专项报告
        Args:
            results: 分析结果
            print_to_console: 是否打印到控制台
            export_to_file: 是否导出到文件
        Returns:
            控制台输出文本
        """
        # 筛选包含量价策略的股票
        volume_pattern_stocks = []
        for ranked in results.ranked_stocks:
            for strategy_name in ranked.stock_score.strategy_results.keys():
                if "量价" in strategy_name:
                    volume_pattern_stocks.append(ranked)
                    break
        
        # 生成控制台输出
        output_text = self.formatter.format_volume_pattern_results(volume_pattern_stocks)
        
        if print_to_console:
            print(output_text)
        
        # 导出到文件
        if export_to_file and hasattr(self.exporter, 'export_volume_pattern_results'):
            filename = f"volume_pattern_{results.analysis_date}.csv"
            filepath = self.output_dir / filename
            self.exporter.export_volume_pattern_results(volume_pattern_stocks, str(filepath))
        
        return output_text
    
    def generate_combined_report(self, results: AnalysisResults,
                               print_to_console: bool = True,
                               export_to_file: bool = True) -> str:
        """
        生成综合策略报告（同时通过多个策略的股票）
        Args:
            results: 分析结果
            print_to_console: 是否打印到控制台
            export_to_file: 是否导出到文件
        Returns:
            控制台输出文本
        """
        # 筛选通过多个策略的股票
        multi_strategy_stocks = [
            ranked for ranked in results.ranked_stocks
            if ranked.stock_score.qualified_count >= 2
        ]
        
        output = []
        
        if multi_strategy_stocks:
            output.append(f"\n=== 综合策略分析结果 ({results.analysis_date}) ===")
            output.append(f"同时通过多个策略的股票: {len(multi_strategy_stocks)} 只")
            output.append("")
            
            # 详细列表
            header = f"{'排名':>4} {'股票代码':>10} {'股票名称':>15} {'通过策略数':>8} {'加权得分':>8} {'通过策略':>30}"
            output.append(header)
            output.append("-" * len(header))
            
            for ranked in multi_strategy_stocks:
                score = ranked.stock_score
                strategies_str = ", ".join(score.qualified_strategies)
                if len(strategies_str) > 28:
                    strategies_str = strategies_str[:25] + "..."
                
                line = (f"{ranked.rank:4d} "
                       f"{score.stock_code:>10} "
                       f"{score.stock_name:>15} "
                       f"{score.qualified_count:8d} "
                       f"{score.weighted_score:8.1f} "
                       f"{strategies_str:>30}")
                output.append(line)
            
            # 最佳股票详情
            if multi_strategy_stocks:
                best_stock = multi_strategy_stocks[0]
                output.append(f"\n最佳综合股票: {best_stock.stock_score.stock_name} ({best_stock.stock_score.stock_code})")
                output.append(f"  - 综合得分: {best_stock.stock_score.weighted_score:.1f}")
                output.append(f"  - 通过策略: {', '.join(best_stock.stock_score.qualified_strategies)}")
                output.append(f"  - 收盘价: {best_stock.stock_score.current_price:.2f}")
        else:
            output.append(f"\n=== 综合策略分析结果 ({results.analysis_date}) ===")
            output.append("暂时没有发现同时通过多个策略的股票")
        
        output_text = "\n".join(output)
        
        if print_to_console:
            print(output_text)
        
        # 导出到文件
        if export_to_file and multi_strategy_stocks:
            filename = f"combined_strategies_{results.analysis_date}.csv"
            filepath = self.output_dir / filename
            self.exporter.export_ranked_stocks(multi_strategy_stocks, str(filepath), include_details=True)
        
        return output_text
    
    def set_output_dir(self, output_dir: Path):
        """设置输出目录"""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
    
    def set_formatter(self, formatter: BaseFormatter):
        """设置格式化器"""
        self.formatter = formatter
    
    def set_exporter(self, exporter: BaseExporter):
        """设置导出器"""
        self.exporter = exporter
