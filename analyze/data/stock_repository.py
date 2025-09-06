# encoding: utf-8

"""
股票数据仓库
提供高级查询功能和数据缓存
"""

import pandas as pd
from typing import List, Dict, Optional, Tuple
from .stock_data_loader import StockDataLoader


class StockInfo:
    """股票信息数据类"""
    
    def __init__(self, ts_code: str, name: str, market: str, list_date: str = None):
        self.ts_code = ts_code
        self.code_6digit = ts_code[:6] if '.' in ts_code else ts_code
        self.name = name
        self.market = market
        self.list_date = list_date
    
    def __repr__(self):
        return f"StockInfo({self.ts_code}, {self.name}, {self.market})"


class StockRepository:
    """股票数据仓库"""
    
    def __init__(self, data_loader: StockDataLoader = None):
        """
        初始化数据仓库
        Args:
            data_loader: 数据加载器实例
        """
        self.data_loader = data_loader or StockDataLoader()
        self._stock_info_cache = {}
        self._data_cache = {}
    
    def get_stock_info_list(self, markets: List[str] = None) -> List[StockInfo]:
        """
        获取股票信息列表
        Args:
            markets: 市场列表，如['主板', '创业板']
        Returns:
            股票信息列表
        """
        stock_list_df = self.data_loader.get_stock_list()
        
        if markets:
            stock_list_df = stock_list_df[stock_list_df['market'].isin(markets)]
        
        stock_infos = []
        for _, row in stock_list_df.iterrows():
            stock_info = StockInfo(
                ts_code=row['ts_code'],
                name=row['name'],
                market=row['market'],
                list_date=row.get('list_date')
            )
            stock_infos.append(stock_info)
        
        return stock_infos
    
    def get_stock_info(self, stock_code: str) -> Optional[StockInfo]:
        """
        获取单只股票信息
        Args:
            stock_code: 股票代码（支持6位数字或完整ts_code）
        Returns:
            股票信息，如果不存在返回None
        """
        if stock_code in self._stock_info_cache:
            return self._stock_info_cache[stock_code]
        
        stock_list_df = self.data_loader.get_stock_list()
        
        # 支持6位数字代码或完整ts_code查询
        if '.' in stock_code:
            match = stock_list_df[stock_list_df['ts_code'] == stock_code]
        else:
            match = stock_list_df[stock_list_df['ts_code'].str.startswith(stock_code)]
        
        if match.empty:
            return None
        
        row = match.iloc[0]
        stock_info = StockInfo(
            ts_code=row['ts_code'],
            name=row['name'],
            market=row['market'],
            list_date=row.get('list_date')
        )
        
        # 缓存结果
        self._stock_info_cache[stock_code] = stock_info
        return stock_info
    
    def get_stocks_by_criteria(self, markets: List[str] = None, 
                              min_data_length: int = 0) -> List[Tuple[StockInfo, pd.DataFrame]]:
        """
        根据条件筛选股票
        Args:
            markets: 市场列表
            min_data_length: 最小数据长度要求
        Returns:
            (股票信息, 股票数据) 元组列表
        """
        stock_infos = self.get_stock_info_list(markets)
        results = []
        
        for stock_info in stock_infos:
            df = self.data_loader.load_stock_data(stock_info.code_6digit)
            if df is not None and len(df) >= min_data_length:
                results.append((stock_info, df))
        
        return results
    
    def batch_get_stock_data(self, stock_codes: List[str], 
                           end_date: str = None) -> Dict[str, Tuple[StockInfo, pd.DataFrame]]:
        """
        批量获取股票数据（包含股票信息）
        Args:
            stock_codes: 股票代码列表
            end_date: 数据截止日期
        Returns:
            {stock_code: (股票信息, 股票数据)} 字典
        """
        results = {}
        
        for stock_code in stock_codes:
            stock_info = self.get_stock_info(stock_code)
            if stock_info is None:
                continue
            
            df = self.data_loader.load_stock_data(stock_info.code_6digit, end_date)
            if df is not None:
                results[stock_code] = (stock_info, df)
        
        return results
    
    def get_latest_trading_data(self, stock_code: str, 
                               end_date: str = None) -> Optional[pd.Series]:
        """
        获取股票最新交易日数据
        Args:
            stock_code: 股票代码
            end_date: 数据截止日期
        Returns:
            最新交易日数据Series，如果不存在返回None
        """
        df = self.data_loader.load_stock_data(
            stock_code[:6] if '.' in stock_code else stock_code, 
            end_date
        )
        
        if df is None or df.empty:
            return None
        
        # 按日期排序，获取最新数据
        df_sorted = df.sort_values('trade_date', ascending=True)
        return df_sorted.iloc[-1]
    
    def clear_cache(self):
        """清空缓存"""
        self._stock_info_cache.clear()
        self._data_cache.clear()
