# encoding: utf-8

"""
J值筛选策略
筛选J值小于指定阈值的股票
"""

import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategyResult


class JValueStrategy(BaseStrategy):
    """J值筛选策略"""
    
    def __init__(self, max_j_value: float = 13.0, weight: float = 1.0, 
                 is_filter_strategy: bool = False, threshold_score: float = 100.0, **kwargs):
        """
        初始化J值策略
        Args:
            max_j_value: J值上限阈值
            weight: 策略权重
            is_filter_strategy: 是否为筛选策略（必须满足的条件）
            threshold_score: 筛选策略的阈值得分
        """
        super().__init__(weight=weight, is_filter_strategy=is_filter_strategy, 
                        threshold_score=threshold_score, max_j_value=max_j_value, **kwargs)
        self.max_j_value = max_j_value
    
    def get_default_name(self) -> str:
        """获取策略默认名称"""
        return "J值筛选策略"
    
    def analyze(self, stock_code: str, stock_name: str, 
                stock_data: pd.DataFrame) -> StrategyResult:
        """
        分析单只股票的J值
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            stock_data: 股票历史数据
        Returns:
            策略分析结果
        """
        # 数据验证
        if not self.validate_data(stock_data, min_length=5):
            return self.create_failed_result(stock_code, stock_name, "数据不足或缺少必要列")
        
        # 获取最新交易日数据
        latest_data = self.get_latest_data(stock_data)
        if latest_data is None:
            return self.create_failed_result(stock_code, stock_name, "无法获取最新数据")
        
        # 检查J值是否存在
        if 'J' not in latest_data or pd.isna(latest_data['J']):
            return self.create_failed_result(stock_code, stock_name, "缺少J值数据")
        
        j_value = float(latest_data['J'])
        current_price = float(latest_data['close'])
        trade_date = str(latest_data['trade_date'])
        
        # 判断是否符合条件
        is_qualified = j_value < self.max_j_value
        
        # 计算得分 (J值越低，得分越高)
        if j_value <= 0:
            score = 100.0  # J值为0或负数时给满分
        elif j_value >= self.max_j_value:
            score = 0.0    # J值大于等于阈值时得分为0
        else:
            # 线性映射: J值从0到max_j_value，得分从100到10
            score = 100 - (j_value / self.max_j_value) * 90
        
        # 计算置信度 (基于J值的可靠性)
        if j_value <= self.max_j_value * 0.5:
            confidence = 0.9  # J值很低时置信度高
        elif j_value <= self.max_j_value * 0.8:
            confidence = 0.7  # J值较低时置信度中等
        else:
            confidence = 0.5  # J值接近阈值时置信度较低
        
        # 构建详细信息
        details = {
            'j_value': j_value,
            'max_j_value': self.max_j_value,
            'j_percentile': j_value / self.max_j_value * 100,
            'analysis_params': {
                'max_j_value': self.max_j_value
            }
        }
        
        # 生成分析理由
        if is_qualified:
            reason = f"J值{j_value:.2f}小于阈值{self.max_j_value}，处于超卖区域"
        else:
            reason = f"J值{j_value:.2f}大于阈值{self.max_j_value}，不在超卖区域"
        
        return StrategyResult(
            stock_code=stock_code,
            stock_name=stock_name,
            strategy_name=self.name,
            is_qualified=is_qualified,
            score=score,
            confidence=confidence,
            details=details,
            reason=reason,
            current_price=current_price,
            trade_date=trade_date
        )
    
    def get_j_distribution_stats(self, stock_data: pd.DataFrame) -> dict:
        """
        获取J值分布统计信息
        Args:
            stock_data: 股票数据
        Returns:
            J值统计信息字典
        """
        if 'J' not in stock_data.columns:
            return {}
        
        j_values = stock_data['J'].dropna()
        if j_values.empty:
            return {}
        
        return {
            'mean': float(j_values.mean()),
            'std': float(j_values.std()),
            'min': float(j_values.min()),
            'max': float(j_values.max()),
            'q25': float(j_values.quantile(0.25)),
            'q50': float(j_values.quantile(0.50)),
            'q75': float(j_values.quantile(0.75)),
            'below_threshold_count': int((j_values < self.max_j_value).sum()),
            'total_count': len(j_values)
        }
    
    def analyze_j_trend(self, stock_data: pd.DataFrame, window: int = 10) -> dict:
        """
        分析J值趋势
        Args:
            stock_data: 股票数据
            window: 分析窗口期
        Returns:
            J值趋势分析结果
        """
        if 'J' not in stock_data.columns or len(stock_data) < window:
            return {}
        
        # 按日期排序
        df_sorted = stock_data.sort_values('trade_date', ascending=True)
        recent_j = df_sorted['J'].tail(window).dropna()
        
        if len(recent_j) < 3:
            return {}
        
        # 计算趋势
        j_values = recent_j.values
        x = np.arange(len(j_values))
        trend = np.polyfit(x, j_values, 1)[0]  # 线性趋势斜率
        
        # 计算变化率
        change_rate = (j_values[-1] - j_values[0]) / j_values[0] * 100 if j_values[0] != 0 else 0
        
        return {
            'trend_slope': float(trend),
            'trend_direction': 'up' if trend > 0 else 'down' if trend < 0 else 'flat',
            'change_rate_percent': float(change_rate),
            'recent_j_mean': float(recent_j.mean()),
            'recent_j_std': float(recent_j.std()),
            'window_size': window
        }
    
    def set_max_j_value(self, max_j_value: float):
        """设置J值阈值"""
        if max_j_value <= 0:
            raise ValueError("J值阈值必须大于0")
        self.max_j_value = max_j_value
        self.set_param('max_j_value', max_j_value)
    
    def get_max_j_value(self) -> float:
        """获取J值阈值"""
        return self.max_j_value
