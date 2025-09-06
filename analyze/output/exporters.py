# encoding: utf-8

"""
数据导出器
将分析结果导出为不同格式的文件
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional
from abc import ABC, abstractmethod
from ..core.scoring_engine import RankedStock, StockScore
from ..core.stock_analyzer import AnalysisResults


class BaseExporter(ABC):
    """导出器基类"""
    
    @abstractmethod
    def export_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           filepath: str, **kwargs):
        """导出排名股票数据"""
        pass
    
    @abstractmethod
    def export_analysis_results(self, results: AnalysisResults, 
                              filepath: str, **kwargs):
        """导出完整分析结果"""
        pass


class CSVExporter(BaseExporter):
    """CSV导出器"""
    
    def export_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           filepath: str, include_details: bool = False):
        """
        导出排名股票到CSV文件
        Args:
            ranked_stocks: 排名股票列表
            filepath: 输出文件路径
            include_details: 是否包含详细信息
        """
        if not ranked_stocks:
            print("没有数据需要导出")
            return
        
        # 构建基础数据 - 统一格式，股票代码作为第一列
        data = []
        for ranked in ranked_stocks:
            score = ranked.stock_score
            row = {
                '股票代码': score.stock_code,
                '股票名称': score.stock_name,
                '排名': ranked.rank,
                '综合得分': round(score.total_score, 2),
                '加权得分': round(score.weighted_score, 2),
                '通过策略数': score.qualified_count,
                '总策略数': score.total_strategies,
                '通过率%': round(score.qualification_rate * 100, 1),
                '置信度': round(score.confidence, 2),
                '收盘价': score.current_price,
                '交易日期': score.trade_date,
                '通过策略': ', '.join(score.qualified_strategies)
            }
            
            # 添加各策略得分
            for strategy_name, strategy_score in score.strategy_scores.items():
                row[f'{strategy_name}_得分'] = round(strategy_score, 2)
            
            # 如果包含详细信息，添加策略详情
            if include_details:
                for strategy_name, result in score.strategy_results.items():
                    row[f'{strategy_name}_通过'] = '是' if result.is_qualified else '否'
                    row[f'{strategy_name}_理由'] = result.reason
                    
                    # 添加策略特定的详细数据
                    if result.details:
                        for key, value in result.details.items():
                            if not isinstance(value, dict) and key != 'analysis_params':
                                if isinstance(value, float):
                                    row[f'{strategy_name}_{key}'] = round(value, 3)
                                else:
                                    row[f'{strategy_name}_{key}'] = value
            
            data.append(row)
        
        # 创建DataFrame并导出
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"📁 数据已导出到: {filepath}")
    
    def export_j_value_results(self, ranked_stocks: List[RankedStock], filepath: str):
        """导出J值分析结果"""
        if not ranked_stocks:
            print("没有J值分析数据需要导出")
            return
        
        data = []
        for ranked in ranked_stocks:
            score = ranked.stock_score
            
            # 查找J值策略结果
            j_result = None
            for result in score.strategy_results.values():
                if "J值" in result.strategy_name:
                    j_result = result
                    break
            
            if j_result:
                row = {
                    '股票代码': score.stock_code,
                    '股票名称': score.stock_name,
                    'J值': round(j_result.details.get('j_value', 0), 2),
                    '排名': ranked.rank,
                    'J值阈值': j_result.details.get('max_j_value', 0),
                    '策略得分': round(j_result.score, 2),
                    '置信度': round(j_result.confidence, 2),
                    '是否通过': '是' if j_result.is_qualified else '否',
                    '分析理由': j_result.reason,
                    '收盘价': score.current_price,
                    '交易日期': score.trade_date
                }
                data.append(row)
        
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"📁 J值分析结果已导出到: {filepath}")
    
    def export_volume_pattern_results(self, ranked_stocks: List[RankedStock], filepath: str):
        """导出量价关系分析结果"""
        if not ranked_stocks:
            print("没有量价关系分析数据需要导出")
            return
        
        data = []
        for ranked in ranked_stocks:
            score = ranked.stock_score
            
            # 查找量价策略结果
            volume_result = None
            for result in score.strategy_results.values():
                if "量价" in result.strategy_name:
                    volume_result = result
                    break
            
            if volume_result and volume_result.details:
                details = volume_result.details
                row = {
                    '股票代码': score.stock_code,
                    '股票名称': score.stock_name,
                    '上涨日数': details.get('up_days_count', 0),
                    '下跌日数': details.get('down_days_count', 0),
                    '涨日平均量比': round(details.get('avg_vol_ratio_up', 0), 2),
                    '跌日平均量比': round(details.get('avg_vol_ratio_down', 0), 2),
                    '量比对比度': round(details.get('volume_contrast', 0), 2),
                    '近5日涨跌幅%': round(details.get('recent_return_5d', 0), 2),
                    'J值': round(details.get('j_value', 0), 2),
                    '收盘价': score.current_price,
                    '分析理由': volume_result.reason,
                    '排名': ranked.rank,
                    '策略得分': round(volume_result.score, 2),
                    '置信度': round(volume_result.confidence, 2),
                    '是否通过': '是' if volume_result.is_qualified else '否',
                    '交易日期': score.trade_date
                }
                data.append(row)
        
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"📁 量价关系分析结果已导出到: {filepath}")
    
    def export_analysis_results(self, results: AnalysisResults, 
                              filepath: str, include_details: bool = True):
        """导出完整分析结果"""
        self.export_ranked_stocks(results.ranked_stocks, filepath, include_details)
    
    def export_strategy_performance(self, strategy_performance: Dict, filepath: str):
        """导出策略表现统计"""
        if not strategy_performance:
            print("没有策略表现数据需要导出")
            return
        
        data = []
        for strategy_name, stats in strategy_performance.items():
            row = {
                '策略名称': strategy_name,
                '平均得分': round(stats['average_score'], 2),
                '最高得分': round(stats['max_score'], 2),
                '最低得分': round(stats['min_score'], 2),
                '通过数量': stats['qualified_count'],
                '总数量': stats['total_count'],
                '通过率%': round(stats['qualification_rate'] * 100, 1)
            }
            data.append(row)
        
        df = pd.DataFrame(data)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        print(f"📁 策略表现统计已导出到: {filepath}")


class ExcelExporter(BaseExporter):
    """Excel导出器"""
    
    def export_ranked_stocks(self, ranked_stocks: List[RankedStock], 
                           filepath: str, **kwargs):
        """导出排名股票到Excel文件"""
        if not ranked_stocks:
            print("没有数据需要导出")
            return
        
        # 使用CSV导出器的逻辑创建DataFrame
        csv_exporter = CSVExporter()
        
        # 创建临时CSV文件路径
        temp_csv = filepath.replace('.xlsx', '.csv').replace('.xls', '.csv')
        csv_exporter.export_ranked_stocks(ranked_stocks, temp_csv, 
                                        kwargs.get('include_details', False))
        
        # 读取CSV并转换为Excel
        try:
            df = pd.read_csv(temp_csv, encoding='utf-8-sig')
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, sheet_name='股票排名', index=False)
                
                # 调整列宽
                worksheet = writer.sheets['股票排名']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            # 删除临时CSV文件
            Path(temp_csv).unlink()
            print(f"📁 数据已导出到: {filepath}")
            
        except Exception as e:
            print(f"导出Excel文件失败: {e}")
            print(f"CSV文件已保存在: {temp_csv}")
    
    def export_analysis_results(self, results: AnalysisResults, 
                              filepath: str, **kwargs):
        """导出完整分析结果到Excel（多工作表）"""
        if not results.ranked_stocks:
            print("没有数据需要导出")
            return
        
        try:
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                # 工作表1: 股票排名
                csv_exporter = CSVExporter()
                temp_csv = "temp_ranked.csv"
                csv_exporter.export_ranked_stocks(results.ranked_stocks, temp_csv, True)
                df_ranked = pd.read_csv(temp_csv, encoding='utf-8-sig')
                df_ranked.to_excel(writer, sheet_name='股票排名', index=False)
                Path(temp_csv).unlink()
                
                # 工作表2: 策略表现
                if results.strategy_performance:
                    temp_csv = "temp_strategy.csv"
                    csv_exporter.export_strategy_performance(results.strategy_performance, temp_csv)
                    df_strategy = pd.read_csv(temp_csv, encoding='utf-8-sig')
                    df_strategy.to_excel(writer, sheet_name='策略表现', index=False)
                    Path(temp_csv).unlink()
                
                # 工作表3: 分析总结
                summary_data = [{
                    '分析日期': results.analysis_date,
                    '总股票数': results.total_stocks,
                    '符合条件股票数': results.qualified_stocks,
                    '符合条件比例%': round(results.qualified_stocks/results.total_stocks*100, 1) if results.total_stocks > 0 else 0
                }]
                df_summary = pd.DataFrame(summary_data)
                df_summary.to_excel(writer, sheet_name='分析总结', index=False)
            
            print(f"📁 完整分析结果已导出到: {filepath}")
            
        except Exception as e:
            print(f"导出Excel文件失败: {e}")
            # 回退到CSV导出
            csv_filepath = filepath.replace('.xlsx', '.csv').replace('.xls', '.csv')
            self.export_ranked_stocks(results.ranked_stocks, csv_filepath, **kwargs)
