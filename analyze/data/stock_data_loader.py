# encoding: utf-8

"""
股票数据加载器
负责从本地文件系统加载股票数据
"""

import pandas as pd
from pathlib import Path
from typing import Optional, List
from datetime import datetime


class StockDataLoader:
    """股票数据加载器"""
    
    def __init__(self, base_data_dir: Path = None, stock_list_file: Path = None):
        """
        初始化数据加载器
        Args:
            base_data_dir: 数据根目录
            stock_list_file: 股票列表文件路径
        """
        if base_data_dir is None:
            self.base_data_dir = Path(__file__).parent.parent.parent / "data"
        else:
            self.base_data_dir = Path(base_data_dir)
            
        if stock_list_file is None:
            self.stock_list_file = Path(__file__).parent.parent.parent / "old_code" / "stock_list.csv"
        else:
            self.stock_list_file = Path(stock_list_file)
        
        # 缓存股票列表
        self._stock_list_df = None
        self._latest_date_cache = None
    
    def get_latest_date(self) -> str:
        """获取最新的数据日期"""
        if self._latest_date_cache is not None:
            return self._latest_date_cache
            
        if not self.base_data_dir.exists():
            raise ValueError(f"数据目录不存在: {self.base_data_dir}")
            
        date_dirs = [d.name for d in self.base_data_dir.iterdir() 
                    if d.is_dir() and d.name.isdigit()]
        if not date_dirs:
            raise ValueError("没有找到任何数据日期目录")
            
        self._latest_date_cache = max(date_dirs)
        return self._latest_date_cache
    
    def get_data_dir(self, data_date: str = None) -> Path:
        """
        获取指定日期的数据目录
        Args:
            data_date: 数据日期，如果为None则使用最新日期
        Returns:
            数据目录路径
        """
        if data_date is None:
            data_date = self.get_latest_date()
        
        data_dir = self.base_data_dir / data_date
        if not data_dir.exists():
            raise ValueError(f"数据目录不存在: {data_dir}")
        
        return data_dir
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        if self._stock_list_df is None:
            if not self.stock_list_file.exists():
                raise ValueError(f"股票列表文件不存在: {self.stock_list_file}")
            self._stock_list_df = pd.read_csv(self.stock_list_file)
        
        return self._stock_list_df.copy()
    
    def load_stock_data(self, stock_code: str, end_date: str = None, 
                       data_source_date: str = None) -> Optional[pd.DataFrame]:
        """
        加载单只股票的数据
        Args:
            stock_code: 股票代码（6位数字，如000001）
            end_date: 数据截止日期，格式YYYYMMDD
            data_source_date: 数据源日期，如果为None则使用最新日期
        Returns:
            股票数据DataFrame，如果文件不存在返回None
        """
        try:
            data_dir = self.get_data_dir(data_source_date)
            file_path = data_dir / f"{stock_code}.parquet"
            
            if not file_path.exists():
                return None
            
            df = pd.read_parquet(file_path)
            
            # 如果指定了截止日期，筛选数据
            if end_date is not None:
                df['trade_date'] = df['trade_date'].astype(str)
                df = df[df['trade_date'] <= end_date].copy()
                
                if df.empty:
                    return None
            
            return df
            
        except Exception as e:
            print(f"读取文件 {stock_code} 失败: {e}")
            return None
    
    def batch_load_stocks(self, stock_codes: List[str], end_date: str = None,
                         data_source_date: str = None) -> dict:
        """
        批量加载股票数据
        Args:
            stock_codes: 股票代码列表
            end_date: 数据截止日期
            data_source_date: 数据源日期
        Returns:
            {stock_code: DataFrame} 字典
        """
        results = {}
        
        for stock_code in stock_codes:
            # 提取6位数字代码
            if '.' in stock_code:
                code_6digit = stock_code[:6]
            else:
                code_6digit = stock_code
                
            df = self.load_stock_data(code_6digit, end_date, data_source_date)
            if df is not None:
                results[stock_code] = df
        
        return results
    
    def get_available_stock_codes(self, data_source_date: str = None) -> List[str]:
        """
        获取可用的股票代码列表
        Args:
            data_source_date: 数据源日期
        Returns:
            股票代码列表（6位数字格式）
        """
        try:
            data_dir = self.get_data_dir(data_source_date)
            parquet_files = list(data_dir.glob("*.parquet"))
            return [f.stem for f in parquet_files]
        except Exception as e:
            print(f"获取股票代码列表失败: {e}")
            return []
