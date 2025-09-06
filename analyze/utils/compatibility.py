# encoding: utf-8

"""
向后兼容层
保持与原有代码的兼容性
"""

from typing import List, Tuple, Dict
from ..core.stock_analyzer import StockAnalyzer as NewStockAnalyzer
from ..output.reporters import AnalysisReporter
from ..output.formatters import ConsoleFormatter
from ..output.exporters import CSVExporter


# 为了向后兼容，导出新的StockAnalyzer类，但保持相同的接口
class StockAnalyzer(NewStockAnalyzer):
    """
    兼容性包装的股票分析器
    保持原有接口的同时使用新架构
    """
    
    def __init__(self, data_date: str = None):
        """保持原有初始化接口"""
        super().__init__(data_date=data_date)
        self.reporter = AnalysisReporter(
            formatter=ConsoleFormatter(),
            exporter=CSVExporter()
        )
    
    def get_j_under_value_stocks(self, max_j_value: float = 13.0) -> List[Tuple[str, str, float]]:
        """
        兼容原有的J值筛选接口
        返回: [(股票代码, 股票名称, J值), ...]
        """
        results = self.analyze_j_under_value(max_j_value)
        
        j_stocks = []
        for ranked in results.ranked_stocks:
            stock_score = ranked.stock_score
            
            # 查找J值策略结果
            for result in stock_score.strategy_results.values():
                if "J值" in result.strategy_name and result.is_qualified:
                    j_value = result.details.get('j_value', 0)
                    j_stocks.append((stock_score.stock_code, stock_score.stock_name, j_value))
                    break
        
        # 按J值排序
        j_stocks.sort(key=lambda x: x[2])
        return j_stocks
    
    def analyze_volume_pattern(self, stock_codes: List[str], 
                             days_to_analyze: int = 20) -> List[Tuple[str, str, Dict]]:
        """
        兼容原有的量价关系分析接口
        返回: [(股票代码, 股票名称, 分析结果), ...]
        """
        results = self.analyze_stocks(stock_codes=stock_codes)
        
        volume_stocks = []
        for ranked in results.ranked_stocks:
            stock_score = ranked.stock_score
            
            # 查找量价策略结果
            for result in stock_score.strategy_results.values():
                if "量价" in result.strategy_name and result.is_qualified:
                    # 转换为原有格式
                    analysis_result = {
                        'up_days_count': result.details.get('up_days_count', 0),
                        'down_days_count': result.details.get('down_days_count', 0),
                        'avg_vol_ratio_up': result.details.get('avg_vol_ratio_up', 0),
                        'avg_vol_ratio_down': result.details.get('avg_vol_ratio_down', 0),
                        'volume_contrast': result.details.get('volume_contrast', 0),
                        'recent_return_5d': result.details.get('recent_return_5d', 0),
                        'current_price': stock_score.current_price,
                        'j_value': result.details.get('j_value', 0),
                        'reason': result.reason
                    }
                    volume_stocks.append((stock_score.stock_code, stock_score.stock_name, analysis_result))
                    break
        
        # 按量比对比度排序
        volume_stocks.sort(key=lambda x: x[2]['volume_contrast'], reverse=True)
        return volume_stocks
    
    def analyze_j_under_13_with_volume_pattern(self) -> List[Tuple[str, str, Dict]]:
        """
        兼容原有的J值+量价关系组合分析接口
        返回: [(股票代码, 股票名称, 分析结果), ...]
        """
        results = self.analyze_j_with_volume_pattern(max_j_value=13.0)
        
        combined_stocks = []
        for ranked in results.ranked_stocks:
            stock_score = ranked.stock_score
            
            # 查找量价策略结果
            for result in stock_score.strategy_results.values():
                if "量价" in result.strategy_name and result.is_qualified:
                    # 转换为原有格式
                    analysis_result = {
                        'up_days_count': result.details.get('up_days_count', 0),
                        'down_days_count': result.details.get('down_days_count', 0),
                        'avg_vol_ratio_up': result.details.get('avg_vol_ratio_up', 0),
                        'avg_vol_ratio_down': result.details.get('avg_vol_ratio_down', 0),
                        'volume_contrast': result.details.get('volume_contrast', 0),
                        'recent_return_5d': result.details.get('recent_return_5d', 0),
                        'current_price': stock_score.current_price,
                        'j_value': result.details.get('j_value', 0),
                        'reason': result.reason
                    }
                    combined_stocks.append((stock_score.stock_code, stock_score.stock_name, analysis_result))
                    break
        
        # 按量比对比度排序
        combined_stocks.sort(key=lambda x: x[2]['volume_contrast'], reverse=True)
        return combined_stocks
    
    def print_j_results(self, results: List[Tuple], title: str = "J值筛选结果"):
        """兼容原有的J值结果打印接口"""
        print(f"\n=== {title} ===")
        print(f"共找到 {len(results)} 只股票")
        
        if not results:
            print("没有符合条件的股票")
            return
            
        print(f"{'序号':>4} {'股票代码':>10} {'股票名称':>15} {'J值':>8}")
        print("-" * 50)
        
        for i, (ts_code, name, j_value) in enumerate(results, 1):
            print(f"{i:4d} {ts_code:>10} {name:>15} {j_value:8.2f}")
    
    def print_volume_pattern_results(self, results: List[Tuple], title: str = "放量上涨缩量下跌股票"):
        """兼容原有的量价关系结果打印接口"""
        print(f"\n=== {title} ===")
        print(f"共找到 {len(results)} 只股票")
        
        if not results:
            print("没有符合条件的股票")
            return
            
        print(f"{'序号':>4} {'股票代码':>10} {'股票名称':>15} {'量比对比':>8} {'涨日量比':>8} {'跌日量比':>8} {'5日涨幅%':>8} {'J值':>8} {'收盘价':>8}")
        print("-" * 110)
        
        for i, (ts_code, name, analysis) in enumerate(results, 1):
            print(f"{i:4d} {ts_code:>10} {name:>15} {analysis['volume_contrast']:8.1f} {analysis['avg_vol_ratio_up']:8.1f} {analysis['avg_vol_ratio_down']:8.1f} {analysis['recent_return_5d']:8.1f} {analysis['j_value']:8.1f} {analysis['current_price']:8.2f}")
    
    def save_results_to_csv(self, results: List[Tuple], filename: str, result_type: str = "j_analysis"):
        """兼容原有的CSV保存接口 - 统一格式，股票代码作为第一列"""
        import pandas as pd
        from pathlib import Path
        
        if not results:
            print("没有结果需要保存")
            return
        
        # 构建完整的文件路径
        output_dir = Path(__file__).parent.parent.parent / "analysis_results"
        output_dir.mkdir(exist_ok=True)
        file_path = output_dir / filename
        
        if result_type == "j_analysis":
            # J值分析结果 - 统一格式
            df_result = pd.DataFrame(results, columns=['股票代码', '股票名称', 'J值'])
        else:
            # 量价关系分析结果 - 统一格式
            output_data = []
            for ts_code, name, analysis in results:
                output_data.append({
                    '股票代码': ts_code,
                    '股票名称': name,
                    '上涨日数': analysis['up_days_count'],
                    '下跌日数': analysis['down_days_count'],
                    '涨日平均量比': analysis['avg_vol_ratio_up'],
                    '跌日平均量比': analysis['avg_vol_ratio_down'],
                    '量比对比度': analysis['volume_contrast'],
                    '近5日涨跌幅%': analysis['recent_return_5d'],
                    'J值': analysis['j_value'],
                    '收盘价': analysis['current_price'],
                    '分析理由': analysis['reason']
                })
            df_result = pd.DataFrame(output_data)
        
        df_result.to_csv(file_path, index=False, encoding='utf-8-sig')
        print(f"\n📁 结果已保存到: {file_path}")


def analyze_j_under_13():
    """兼容原有的J值分析入口函数"""
    try:
        analyzer = StockAnalyzer()
        
        # 筛选J值小于13的股票
        j_results = analyzer.get_j_under_value_stocks(max_j_value=13.0)
        
        # 打印结果
        analyzer.print_j_results(j_results, "J值小于13的股票")
        
        # 保存结果
        if j_results:
            filename = f"j_under_13_{analyzer.data_date}.csv"
            analyzer.save_results_to_csv(j_results, filename, "j_analysis")
        
        return j_results
        
    except Exception as e:
        print(f"程序运行出错: {e}")
        return []


def analyze_j13_volume_pattern():
    """兼容原有的J值+量价关系分析入口函数"""
    try:
        analyzer = StockAnalyzer()
        
        # 分析J值小于13且符合量价关系的股票
        volume_pattern_results = analyzer.analyze_j_under_13_with_volume_pattern()
        
        # 打印结果
        analyzer.print_volume_pattern_results(volume_pattern_results, "J值<13且符合放量上涨缩量下跌的股票")
        
        # 保存结果
        if volume_pattern_results:
            filename = f"volume_pattern_j_under_13_{analyzer.data_date}.csv"
            analyzer.save_results_to_csv(volume_pattern_results, filename, "volume_pattern")
            
            # 总结
            print(f"\n=== 分析总结 ({analyzer.data_date}) ===")
            print(f"符合J值<13且放量上涨缩量下跌模式: {len(volume_pattern_results)} 只")
            
            if volume_pattern_results:
                best_stock = volume_pattern_results[0]
                print(f"量价关系最佳: {best_stock[1]} ({best_stock[0]})")
                print(f"  - 量比对比度: {best_stock[2]['volume_contrast']:.1f}")
                print(f"  - 涨日量比: {best_stock[2]['avg_vol_ratio_up']:.1f}")
                print(f"  - 跌日量比: {best_stock[2]['avg_vol_ratio_down']:.1f}")
                print(f"  - 近5日涨跌幅: {best_stock[2]['recent_return_5d']:.1f}%")
                print(f"  - J值: {best_stock[2]['j_value']:.1f}")
        else:
            print("\n在J值小于13的股票中，暂时没有发现明显的放量上涨缩量下跌模式")
        
        return volume_pattern_results
        
    except Exception as e:
        print(f"程序运行出错: {e}")
        return []
