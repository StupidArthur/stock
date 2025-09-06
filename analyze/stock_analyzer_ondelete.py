# encoding: utf-8

"""
精简版股票分析器
从old_code重构而来，专注于核心分析功能：J值筛选和量价关系分析
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Tuple, Dict, Optional
import numpy as np


class StockAnalyzer:
    """股票分析器 - 集成J值筛选和量价关系分析"""
    
    def __init__(self, data_date: str = None):
        """
        初始化分析器
        Args:
            data_date: 分析日期，格式YYYYMMDD，如果为None则使用最新日期
                      注意：数据总是从最新的数据目录读取，data_date只用于标识分析的目标日期
        """
        self.base_data_dir = Path(__file__).parent.parent / "data"
        self.stock_list_file = Path(__file__).parent.parent / "old_code" / "stock_list.csv"
        
        # 加载股票基本信息
        self.stocks_df = pd.read_csv(self.stock_list_file)
        
        # 确定分析日期（用于输出文件命名和数据筛选）
        if data_date is None:
            self.data_date = self._get_latest_date()
        else:
            self.data_date = data_date
        
        # 数据目录始终使用最新日期的目录（因为包含完整历史数据）
        self.latest_data_date = self._get_latest_date()
        self.data_dir = self.base_data_dir / self.latest_data_date
        
        if not self.data_dir.exists():
            raise ValueError(f"数据目录不存在: {self.data_dir}")
        
        # 创建分析结果输出目录
        self.output_dir = Path(__file__).parent.parent / "analysis_results"
        self.output_dir.mkdir(exist_ok=True)
        
        # 输出信息
        if self.data_date == self.latest_data_date:
            print(f"分析日期: {self.data_date} (最新数据)")
        else:
            print(f"分析日期: {self.data_date} (历史数据)")
            print(f"数据来源: {self.latest_data_date} 目录（包含完整历史数据）")
        print(f"数据目录: {self.data_dir}")
        print(f"分析结果保存目录: {self.output_dir}")
    
    def _get_latest_date(self) -> str:
        """获取最新的数据日期"""
        if not self.base_data_dir.exists():
            raise ValueError("数据目录不存在")
            
        date_dirs = [d.name for d in self.base_data_dir.iterdir() if d.is_dir() and d.name.isdigit()]
        if not date_dirs:
            raise ValueError("没有找到任何数据日期目录")
            
        return max(date_dirs)
    
    def _load_stock_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        """
        加载单只股票的数据，并筛选到指定分析日期
        Args:
            stock_code: 股票代码（6位数字，如000001）
        Returns:
            股票数据DataFrame，如果文件不存在返回None
        """
        file_path = self.data_dir / f"{stock_code}.parquet"
        if not file_path.exists():
            return None
            
        try:
            df = pd.read_parquet(file_path)
            
            # 筛选到指定分析日期（包含该日期及之前的数据）
            df['trade_date'] = df['trade_date'].astype(str)
            filtered_df = df[df['trade_date'] <= self.data_date].copy()
            
            if filtered_df.empty:
                return None
                
            return filtered_df
        except Exception as e:
            print(f"读取文件 {file_path} 失败: {e}")
            return None
    
    def get_j_under_value_stocks(self, max_j_value: float = 13.0) -> List[Tuple[str, str, float]]:
        """
        筛选J值小于指定值的股票
        Args:
            max_j_value: J值上限，默认13.0
        Returns:
            [(股票代码, 股票名称, J值), ...] 的列表
        """
        results = []
        processed_count = 0
        
        # 获取所有主板和创业板股票
        main_stocks = self.stocks_df[self.stocks_df['market'].isin(['主板', '创业板'])]
        
        print(f"开始筛选J值小于{max_j_value}的股票，总共 {len(main_stocks)} 只股票...")
        
        for _, stock_info in main_stocks.iterrows():
            ts_code = stock_info['ts_code']
            stock_code = ts_code[:6]  # 提取6位数字代码
            stock_name = stock_info['name']
            
            # 加载股票数据
            df = self._load_stock_data(stock_code)
            if df is None or len(df) < 5:
                continue
                
            # 获取指定分析日期的交易日数据（或最接近的交易日）
            df_sorted = df.sort_values('trade_date', ascending=True)
            latest_data = df_sorted.iloc[-1]
            
            # 检查是否有J值数据
            if 'J' not in latest_data or pd.isna(latest_data['J']):
                continue
                
            j_value = latest_data['J']
            
            # 筛选J值小于指定值的股票
            if j_value < max_j_value:
                results.append((ts_code, stock_name, j_value))
            
            processed_count += 1
            if processed_count % 500 == 0:
                print(f"已处理 {processed_count}/{len(main_stocks)} 只股票，找到符合条件股票 {len(results)} 只")
        
        print(f"筛选完成，共处理 {processed_count} 只股票，找到J值小于{max_j_value}的股票: {len(results)} 只")
        return results
    
    def analyze_volume_pattern(self, stock_codes: List[str], days_to_analyze: int = 20) -> List[Tuple[str, str, Dict]]:
        """
        分析指定股票的量价关系 - 寻找放量上涨、缩量下跌的股票
        
        Args:
            stock_codes: 要分析的股票代码列表（6位数字格式或完整ts_code）
            days_to_analyze: 分析的天数
            
        Returns:
            符合条件的股票列表
        """
        results = []
        processed_count = 0
        
        print(f"开始分析量价关系，总共 {len(stock_codes)} 只股票...")
        
        for stock_code in stock_codes:
            # 从ts_code中提取6位数字代码
            if '.' in stock_code:
                code_6digit = stock_code[:6]
                ts_code = stock_code
            else:
                code_6digit = stock_code
                # 查找完整的ts_code
                stock_info = self.stocks_df[self.stocks_df['ts_code'].str.startswith(code_6digit)]
                if stock_info.empty:
                    continue
                ts_code = stock_info.iloc[0]['ts_code']
                
            df = self._load_stock_data(code_6digit)
            if df is None or len(df) < days_to_analyze + 5:
                continue
                
            # 按日期排序，获取最近的数据
            df_sorted = df.sort_values('trade_date', ascending=True)
            recent_data = df_sorted.tail(days_to_analyze + 5)  # 多取5天用于计算移动平均
            
            if len(recent_data) < days_to_analyze:
                continue
                
            # 计算价格变化和成交量
            analysis_data = recent_data.tail(days_to_analyze).copy()
            analysis_data['price_change'] = analysis_data['close'].pct_change()
            analysis_data['volume_ma'] = analysis_data['vol'].rolling(window=5).mean()
            analysis_data['volume_ratio'] = analysis_data['vol'] / analysis_data['volume_ma']
            
            # 分类涨跌日
            up_days = analysis_data[analysis_data['price_change'] > 0.01]  # 涨幅超过1%
            down_days = analysis_data[analysis_data['price_change'] < -0.01]  # 跌幅超过1%
            
            if len(up_days) < 3 or len(down_days) < 3:  # 至少要有3个涨跌日样本
                continue
                
            # 计算上涨日和下跌日的平均量比
            avg_vol_ratio_up = up_days['volume_ratio'].mean()
            avg_vol_ratio_down = down_days['volume_ratio'].mean()
            
            # 判断是否符合放量上涨、缩量下跌的条件
            conditions_met = True
            reason = []
            
            # 1. 上涨日平均量比 > 下跌日平均量比
            volume_contrast = avg_vol_ratio_up / avg_vol_ratio_down
            if volume_contrast < 1.2:  # 上涨日成交量至少比下跌日大20%
                conditions_met = False
            else:
                reason.append(f"量比对比{volume_contrast:.1f}")
                
            # 2. 上涨日平均量比 > 1.0 (相对5日均量放大)
            if avg_vol_ratio_up < 1.0:
                conditions_met = False
            else:
                reason.append(f"涨日量比{avg_vol_ratio_up:.1f}")
                
            # 3. 下跌日平均量比 < 1.0 (相对5日均量缩减)
            if avg_vol_ratio_down > 1.0:
                conditions_met = False
            else:
                reason.append(f"跌日量比{avg_vol_ratio_down:.1f}")
                
            # 4. 计算最近表现
            latest_data = analysis_data.iloc[-1]
            recent_5days = analysis_data.tail(5)
            
            # 最近5天累计涨跌幅
            recent_return = (recent_5days['close'].iloc[-1] / recent_5days['close'].iloc[0] - 1) * 100
            
            # 获取当前J值
            j_value = latest_data.get('J', 0)
            
            if conditions_met:
                # 获取股票名称
                stock_info = self.stocks_df[self.stocks_df['ts_code'].str.startswith(code_6digit)]
                if not stock_info.empty:
                    stock_name = stock_info.iloc[0]['name']
                    
                    analysis_result = {
                        'up_days_count': len(up_days),
                        'down_days_count': len(down_days),
                        'avg_vol_ratio_up': avg_vol_ratio_up,
                        'avg_vol_ratio_down': avg_vol_ratio_down,
                        'volume_contrast': volume_contrast,
                        'recent_return_5d': recent_return,
                        'current_price': latest_data['close'],
                        'j_value': j_value,
                        'reason': ', '.join(reason)
                    }
                    results.append((ts_code, stock_name, analysis_result))
            
            processed_count += 1
            if processed_count % 50 == 0:
                print(f"已处理 {processed_count}/{len(stock_codes)} 只股票...")
        
        print(f"量价关系分析完成，共处理 {processed_count} 只股票，找到符合条件股票 {len(results)} 只")
        return results
    
    def analyze_j_under_13_with_volume_pattern(self) -> List[Tuple[str, str, Dict]]:
        """分析J值小于13且符合量价关系的股票"""
        print("=== 筛选J值小于13的股票并分析量价关系 ===")
        
        # 首先筛选J值小于13的股票
        j_under_13_stocks = self.get_j_under_value_stocks(max_j_value=13.0)
        
        if not j_under_13_stocks:
            print("没有找到J值小于13的股票")
            return []
        
        # 提取股票代码进行量价关系分析
        stock_codes = [stock[0] for stock in j_under_13_stocks]  # 使用完整的ts_code
        
        # 分析这些股票的量价关系
        volume_pattern_results = self.analyze_volume_pattern(stock_codes, days_to_analyze=20)
        
        return volume_pattern_results
    
    def print_j_results(self, results: List[Tuple], title: str = "J值筛选结果"):
        """打印J值筛选结果"""
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
        """打印量价关系分析结果"""
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
        """
        保存结果到CSV文件
        Args:
            results: 结果列表
            filename: 文件名
            result_type: 结果类型，"j_analysis"或"volume_pattern"
        """
        if not results:
            print("没有结果需要保存")
            return
        
        # 构建完整的文件路径，保存到analysis_results目录
        file_path = self.output_dir / filename
        
        if result_type == "j_analysis":
            # J值分析结果
            df_result = pd.DataFrame(results, columns=['股票代码', '股票名称', 'J值'])
        else:
            # 量价关系分析结果
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
    """分析J值小于13的股票 - 主程序入口"""
    try:
        analyzer = StockAnalyzer()
        
        # 筛选J值小于13的股票
        j_results = analyzer.get_j_under_value_stocks(max_j_value=13.0)
        
        # 按J值排序
        j_results.sort(key=lambda x: x[2])
        
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
    """分析J值小于13且符合量价关系的股票 - 主程序入口"""
    try:
        analyzer = StockAnalyzer()
        
        # 分析J值小于13且符合量价关系的股票
        volume_pattern_results = analyzer.analyze_j_under_13_with_volume_pattern()
        
        # 按量比对比度排序
        volume_pattern_results.sort(key=lambda x: x[2]['volume_contrast'], reverse=True)
        
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


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "j13":
        # 分析J值小于13且符合量价关系的股票
        analyze_j13_volume_pattern()
    else:
        # 只分析J值小于13的股票
        analyze_j_under_13()
