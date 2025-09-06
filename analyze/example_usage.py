# encoding: utf-8

"""
新架构使用示例
展示如何使用重构后的股票分析系统
"""

from core.stock_analyzer import StockAnalyzer
from strategies import JValueStrategy, VolumePatternStrategy
from output.reporters import AnalysisReporter
from output.formatters import ConsoleFormatter
from output.exporters import CSVExporter, ExcelExporter


def example_1_basic_usage():
    """示例1: 基本使用 - 使用默认策略分析"""
    print("=== 示例1: 基本使用 ===")
    
    # 创建分析器（自动加载默认策略）
    analyzer = StockAnalyzer()
    
    # 执行分析
    results = analyzer.analyze_stocks()
    
    # 创建报告生成器并输出结果
    reporter = AnalysisReporter()
    reporter.generate_full_report(results)


def example_2_j_value_only():
    """示例2: 只使用J值策略"""
    print("=== 示例2: J值策略分析 ===")
    
    analyzer = StockAnalyzer()
    
    # 分析J值小于13的股票
    results = analyzer.analyze_j_under_value(max_j_value=13.0)
    
    # 生成专项报告
    reporter = AnalysisReporter()
    reporter.generate_j_value_report(results, max_j_value=13.0)


def example_3_volume_pattern_only():
    """示例3: 只使用量价关系策略"""
    print("=== 示例3: 量价关系策略分析 ===")
    
    analyzer = StockAnalyzer()
    
    # 清除默认策略，只添加量价策略
    analyzer.strategy_registry.clear()
    volume_strategy = VolumePatternStrategy(days_to_analyze=20, weight=1.0)
    analyzer.add_strategy(volume_strategy)
    
    # 执行分析
    results = analyzer.analyze_stocks()
    
    # 生成专项报告
    reporter = AnalysisReporter()
    reporter.generate_volume_pattern_report(results)


def example_4_combined_strategies():
    """示例4: 组合策略 - J值 + 量价关系"""
    print("=== 示例4: 组合策略分析 ===")
    
    analyzer = StockAnalyzer()
    
    # 分析同时满足J值和量价关系的股票
    results = analyzer.analyze_j_with_volume_pattern(max_j_value=13.0, days_to_analyze=20)
    
    # 生成综合报告
    reporter = AnalysisReporter()
    reporter.generate_combined_report(results)


def example_5_custom_strategy():
    """示例5: 自定义策略"""
    print("=== 示例5: 自定义策略 ===")
    
    from strategies.base_strategy import BaseStrategy, StrategyResult
    import pandas as pd
    
    class RSIStrategy(BaseStrategy):
        """RSI策略示例"""
        
        def __init__(self, rsi_threshold: float = 30.0, **kwargs):
            super().__init__(**kwargs)
            self.rsi_threshold = rsi_threshold
        
        def get_default_name(self) -> str:
            return "RSI超卖策略"
        
        def analyze(self, stock_code: str, stock_name: str, 
                   stock_data: pd.DataFrame) -> StrategyResult:
            # 简化的RSI计算示例
            if not self.validate_data(stock_data, min_length=15):
                return self.create_failed_result(stock_code, stock_name)
            
            latest_data = self.get_latest_data(stock_data)
            
            # 这里应该有真正的RSI计算逻辑
            # 为了示例，我们假设有RSI列
            if 'rsi' in latest_data:
                rsi_value = latest_data['rsi']
                is_qualified = rsi_value < self.rsi_threshold
                score = max(0, 100 - rsi_value) if rsi_value < 100 else 0
            else:
                # 如果没有RSI数据，简单基于价格变化估算
                recent_data = stock_data.tail(10)
                price_change = (recent_data['close'].iloc[-1] / recent_data['close'].iloc[0] - 1) * 100
                rsi_value = 50 + price_change  # 简化估算
                is_qualified = rsi_value < self.rsi_threshold
                score = max(0, 100 - rsi_value) if is_qualified else 0
            
            return StrategyResult(
                stock_code=stock_code,
                stock_name=stock_name,
                strategy_name=self.name,
                is_qualified=is_qualified,
                score=score,
                confidence=0.7,
                details={'rsi_value': rsi_value, 'rsi_threshold': self.rsi_threshold},
                reason=f"RSI值{rsi_value:.1f}{'<' if is_qualified else '>='}{self.rsi_threshold}",
                current_price=latest_data['close'],
                trade_date=str(latest_data['trade_date'])
            )
    
    # 使用自定义策略
    analyzer = StockAnalyzer()
    
    # 添加自定义策略
    rsi_strategy = RSIStrategy(rsi_threshold=30.0, weight=0.8)
    analyzer.add_strategy(rsi_strategy)
    
    # 执行分析
    results = analyzer.analyze_stocks()
    
    # 输出结果
    reporter = AnalysisReporter()
    reporter.generate_full_report(results)


def example_6_advanced_configuration():
    """示例6: 高级配置"""
    print("=== 示例6: 高级配置 ===")
    
    analyzer = StockAnalyzer()
    
    # 配置策略权重
    analyzer.set_strategy_weight("J值筛选策略", 0.6)
    analyzer.set_strategy_weight("量价关系策略", 0.4)
    
    # 配置评分方法
    analyzer.set_scoring_method("weighted_average")  # 或 "multiplicative", "max_score"
    
    # 分析指定股票
    specific_stocks = ["000001.SZ", "000002.SZ", "600036.SH"]
    results = analyzer.analyze_stocks(stock_codes=specific_stocks)
    
    # 使用Excel导出器
    reporter = AnalysisReporter(
        formatter=ConsoleFormatter(),
        exporter=ExcelExporter()
    )
    
    reporter.generate_full_report(results, "advanced_analysis")
    
    # 获取分析总结
    summary = analyzer.get_analysis_summary(results)
    print("\n=== 分析总结 ===")
    for key, value in summary.items():
        print(f"{key}: {value}")


def example_7_backward_compatibility():
    """示例7: 向后兼容性 - 使用原有接口"""
    print("=== 示例7: 向后兼容性 ===")
    
    # 使用原有的接口方式
    from utils.compatibility import analyze_j_under_13, analyze_j13_volume_pattern
    
    # 原有的J值分析
    print("使用原有接口分析J值:")
    j_results = analyze_j_under_13()
    
    print("\n使用原有接口分析J值+量价关系:")
    volume_results = analyze_j13_volume_pattern()


if __name__ == "__main__":
    print("股票分析系统新架构使用示例")
    print("=" * 50)
    
    # 运行不同的示例
    try:
        example_1_basic_usage()
        print("\n" + "="*50 + "\n")
        
        example_2_j_value_only()
        print("\n" + "="*50 + "\n")
        
        # 其他示例可以根据需要运行
        # example_3_volume_pattern_only()
        # example_4_combined_strategies()
        # example_5_custom_strategy()
        # example_6_advanced_configuration()
        # example_7_backward_compatibility()
        
    except Exception as e:
        print(f"示例运行出错: {e}")
        import traceback
        traceback.print_exc()
