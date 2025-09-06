# encoding: utf-8

"""
量价关系策略
分析股票的量价关系模式，寻找放量上涨、缩量下跌的股票
"""

import pandas as pd
import numpy as np
from .base_strategy import BaseStrategy, StrategyResult


class VolumePatternStrategy(BaseStrategy):
    """量价关系策略"""
    
    def __init__(self, days_to_analyze: int = 20, 
                 min_price_change: float = 0.01,
                 min_volume_contrast: float = 1.2,
                 weight: float = 1.0, **kwargs):
        """
        初始化量价关系策略
        Args:
            days_to_analyze: 分析的天数
            min_price_change: 最小价格变化阈值（1%）
            min_volume_contrast: 最小量比对比度（上涨日量比/下跌日量比）
            weight: 策略权重
        """
        super().__init__(weight=weight, 
                        days_to_analyze=days_to_analyze,
                        min_price_change=min_price_change,
                        min_volume_contrast=min_volume_contrast,
                        **kwargs)
        self.days_to_analyze = days_to_analyze
        self.min_price_change = min_price_change
        self.min_volume_contrast = min_volume_contrast
    
    def get_default_name(self) -> str:
        """获取策略默认名称"""
        return "量价关系策略"
    
    def analyze(self, stock_code: str, stock_name: str, 
                stock_data: pd.DataFrame) -> StrategyResult:
        """
        分析单只股票的量价关系
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            stock_data: 股票历史数据
        Returns:
            策略分析结果
        """
        # 数据验证
        required_length = self.days_to_analyze + 10  # 额外需要一些数据计算移动平均
        if not self.validate_data(stock_data, min_length=required_length):
            return self.create_failed_result(stock_code, stock_name, "数据不足")
        
        # 检查必要的列
        required_columns = ['close', 'vol', 'trade_date']
        for col in required_columns:
            if col not in stock_data.columns:
                return self.create_failed_result(stock_code, stock_name, f"缺少必要列: {col}")
        
        try:
            # 按日期排序，获取最近的数据
            df_sorted = stock_data.sort_values('trade_date', ascending=True)
            recent_data = df_sorted.tail(self.days_to_analyze + 5)  # 多取5天用于计算移动平均
            
            if len(recent_data) < self.days_to_analyze:
                return self.create_failed_result(stock_code, stock_name, "近期数据不足")
            
            # 分析量价关系
            analysis_result = self._analyze_volume_price_pattern(recent_data)
            
            if analysis_result is None:
                return self.create_failed_result(stock_code, stock_name, "量价分析失败")
            
            # 获取最新数据信息
            latest_data = recent_data.iloc[-1]
            current_price = float(latest_data['close'])
            trade_date = str(latest_data['trade_date'])
            
            # 判断是否符合条件
            is_qualified = analysis_result['is_qualified']
            
            # 计算得分
            score = self._calculate_score(analysis_result)
            
            # 计算置信度
            confidence = self._calculate_confidence(analysis_result)
            
            # 生成分析理由
            reason = self._generate_reason(analysis_result)
            
            return StrategyResult(
                stock_code=stock_code,
                stock_name=stock_name,
                strategy_name=self.name,
                is_qualified=is_qualified,
                score=score,
                confidence=confidence,
                details=analysis_result,
                reason=reason,
                current_price=current_price,
                trade_date=trade_date
            )
            
        except Exception as e:
            return self.create_failed_result(stock_code, stock_name, f"分析出错: {str(e)}")
    
    def _analyze_volume_price_pattern(self, data: pd.DataFrame) -> dict:
        """
        分析量价关系模式
        Args:
            data: 股票数据
        Returns:
            分析结果字典
        """
        try:
            # 计算价格变化和成交量
            analysis_data = data.tail(self.days_to_analyze).copy()
            analysis_data['price_change'] = analysis_data['close'].pct_change()
            analysis_data['volume_ma'] = analysis_data['vol'].rolling(window=5).mean()
            analysis_data['volume_ratio'] = analysis_data['vol'] / analysis_data['volume_ma']
            
            # 分类涨跌日
            up_days = analysis_data[analysis_data['price_change'] > self.min_price_change]
            down_days = analysis_data[analysis_data['price_change'] < -self.min_price_change]
            
            if len(up_days) < 3 or len(down_days) < 3:
                return None  # 涨跌日样本不足
            
            # 计算上涨日和下跌日的平均量比
            avg_vol_ratio_up = up_days['volume_ratio'].mean()
            avg_vol_ratio_down = down_days['volume_ratio'].mean()
            
            # 计算量比对比度
            volume_contrast = avg_vol_ratio_up / avg_vol_ratio_down if avg_vol_ratio_down != 0 else 0
            
            # 判断是否符合条件
            conditions_met = True
            condition_scores = {}
            
            # 条件1: 上涨日平均量比 > 下跌日平均量比
            condition_scores['volume_contrast'] = volume_contrast >= self.min_volume_contrast
            if volume_contrast < self.min_volume_contrast:
                conditions_met = False
            
            # 条件2: 上涨日平均量比 > 1.0 (相对5日均量放大)
            condition_scores['up_day_volume'] = avg_vol_ratio_up >= 1.0
            if avg_vol_ratio_up < 1.0:
                conditions_met = False
            
            # 条件3: 下跌日平均量比 < 1.0 (相对5日均量缩减)
            condition_scores['down_day_volume'] = avg_vol_ratio_down <= 1.0
            if avg_vol_ratio_down > 1.0:
                conditions_met = False
            
            # 计算最近表现
            recent_5days = analysis_data.tail(5)
            recent_return = (recent_5days['close'].iloc[-1] / recent_5days['close'].iloc[0] - 1) * 100
            
            # 获取当前J值（如果有的话）
            latest_data = analysis_data.iloc[-1]
            j_value = latest_data.get('J', 0) if 'J' in latest_data else 0
            
            return {
                'is_qualified': conditions_met,
                'up_days_count': len(up_days),
                'down_days_count': len(down_days),
                'avg_vol_ratio_up': avg_vol_ratio_up,
                'avg_vol_ratio_down': avg_vol_ratio_down,
                'volume_contrast': volume_contrast,
                'recent_return_5d': recent_return,
                'j_value': j_value,
                'condition_scores': condition_scores,
                'analysis_params': {
                    'days_to_analyze': self.days_to_analyze,
                    'min_price_change': self.min_price_change,
                    'min_volume_contrast': self.min_volume_contrast
                }
            }
            
        except Exception as e:
            print(f"量价关系分析出错: {e}")
            return None
    
    def _calculate_score(self, analysis_result: dict) -> float:
        """
        计算策略得分
        Args:
            analysis_result: 分析结果
        Returns:
            得分 (0-100)
        """
        if not analysis_result['is_qualified']:
            return 0.0
        
        score = 0.0
        
        # 量比对比度得分 (40分)
        volume_contrast = analysis_result['volume_contrast']
        if volume_contrast >= 2.0:
            score += 40.0
        elif volume_contrast >= 1.5:
            score += 30.0
        elif volume_contrast >= self.min_volume_contrast:
            score += 20.0
        
        # 上涨日量比得分 (30分)
        avg_vol_ratio_up = analysis_result['avg_vol_ratio_up']
        if avg_vol_ratio_up >= 1.5:
            score += 30.0
        elif avg_vol_ratio_up >= 1.2:
            score += 20.0
        elif avg_vol_ratio_up >= 1.0:
            score += 10.0
        
        # 下跌日量比得分 (20分)
        avg_vol_ratio_down = analysis_result['avg_vol_ratio_down']
        if avg_vol_ratio_down <= 0.8:
            score += 20.0
        elif avg_vol_ratio_down <= 0.9:
            score += 15.0
        elif avg_vol_ratio_down <= 1.0:
            score += 10.0
        
        # 近期表现得分 (10分)
        recent_return = analysis_result['recent_return_5d']
        if recent_return > 0:
            score += min(10.0, recent_return)  # 最多10分
        
        return min(100.0, score)
    
    def _calculate_confidence(self, analysis_result: dict) -> float:
        """
        计算置信度
        Args:
            analysis_result: 分析结果
        Returns:
            置信度 (0-1)
        """
        if not analysis_result['is_qualified']:
            return 0.1
        
        confidence = 0.5  # 基础置信度
        
        # 样本数量影响置信度
        total_samples = analysis_result['up_days_count'] + analysis_result['down_days_count']
        if total_samples >= 10:
            confidence += 0.2
        elif total_samples >= 6:
            confidence += 0.1
        
        # 量比对比度影响置信度
        volume_contrast = analysis_result['volume_contrast']
        if volume_contrast >= 2.0:
            confidence += 0.2
        elif volume_contrast >= 1.5:
            confidence += 0.1
        
        # 条件满足情况影响置信度
        conditions_met = sum(analysis_result['condition_scores'].values())
        confidence += conditions_met * 0.05
        
        return min(1.0, confidence)
    
    def _generate_reason(self, analysis_result: dict) -> str:
        """
        生成分析理由
        Args:
            analysis_result: 分析结果
        Returns:
            分析理由字符串
        """
        if not analysis_result['is_qualified']:
            reasons = []
            if not analysis_result['condition_scores']['volume_contrast']:
                reasons.append(f"量比对比度{analysis_result['volume_contrast']:.1f}不足{self.min_volume_contrast}")
            if not analysis_result['condition_scores']['up_day_volume']:
                reasons.append(f"涨日量比{analysis_result['avg_vol_ratio_up']:.1f}未放量")
            if not analysis_result['condition_scores']['down_day_volume']:
                reasons.append(f"跌日量比{analysis_result['avg_vol_ratio_down']:.1f}未缩量")
            return "不符合条件: " + ", ".join(reasons)
        else:
            return (f"符合放量上涨缩量下跌模式: 量比对比{analysis_result['volume_contrast']:.1f}, "
                   f"涨日量比{analysis_result['avg_vol_ratio_up']:.1f}, "
                   f"跌日量比{analysis_result['avg_vol_ratio_down']:.1f}")
    
    def set_analysis_params(self, days_to_analyze: int = None, 
                           min_price_change: float = None,
                           min_volume_contrast: float = None):
        """设置分析参数"""
        if days_to_analyze is not None:
            self.days_to_analyze = days_to_analyze
            self.set_param('days_to_analyze', days_to_analyze)
        
        if min_price_change is not None:
            self.min_price_change = min_price_change
            self.set_param('min_price_change', min_price_change)
        
        if min_volume_contrast is not None:
            self.min_volume_contrast = min_volume_contrast
            self.set_param('min_volume_contrast', min_volume_contrast)
