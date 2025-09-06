# encoding: utf-8

"""
配置管理模块
"""

from pathlib import Path
from typing import Dict, Any


class Config:
    """配置管理类"""
    
    def __init__(self):
        self.base_path = Path(__file__).parent.parent.parent
        self._config = self._load_default_config()
    
    def _load_default_config(self) -> Dict[str, Any]:
        """加载默认配置"""
        return {
            # 数据配置
            'data': {
                'base_data_dir': self.base_path / "data",
                'stock_list_file': self.base_path / "old_code" / "stock_list.csv",
                'output_dir': self.base_path / "analysis_results"
            },
            
            # 分析配置
            'analysis': {
                'default_markets': ['主板', '创业板'],
                'min_data_length': 30,
                'j_value_threshold': 13.0,
                'volume_analysis_days': 20,
                'min_volume_contrast': 1.2,
                'min_price_change': 0.01
            },
            
            # 评分配置
            'scoring': {
                'method': 'weighted_average',  # weighted_average, multiplicative, max_score
                'min_qualified_strategies': 1,
                'min_score_threshold': 0.0
            },
            
            # 输出配置
            'output': {
                'console_output': True,
                'file_export': True,
                'include_details': True,
                'max_display_count': 20
            }
        }
    
    def get(self, key: str, default=None):
        """获取配置值"""
        keys = key.split('.')
        value = self._config
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any):
        """设置配置值"""
        keys = key.split('.')
        config = self._config
        
        for k in keys[:-1]:
            if k not in config:
                config[k] = {}
            config = config[k]
        
        config[keys[-1]] = value
    
    def get_data_config(self) -> Dict[str, Any]:
        """获取数据配置"""
        return self.get('data', {})
    
    def get_analysis_config(self) -> Dict[str, Any]:
        """获取分析配置"""
        return self.get('analysis', {})
    
    def get_scoring_config(self) -> Dict[str, Any]:
        """获取评分配置"""
        return self.get('scoring', {})
    
    def get_output_config(self) -> Dict[str, Any]:
        """获取输出配置"""
        return self.get('output', {})
