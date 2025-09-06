# encoding: utf-8

"""
精简版股票数据下载器
从old_code重构而来，专注于核心下载功能

使用方法:
1. 完整下载: python data_downloader.py
   - 下载所有股票的历史数据（从2023年开始到今天）
   - 下载成功后会清理旧数据，只保留当天的数据
   
2. 增量更新: python data_downloader.py update
   - 检查本地最新数据日期
   - 只下载缺失的最新数据（按工作日计算）
   - 更新成功后保留最近3天的数据
   - 适合日常数据更新使用
"""

import time
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import tushare as ts
from typing import List, Optional
import threading
import numpy as np
import shutil

# 初始化tushare
TOKEN = '0bb5c055c25ec5ccb72bac19aef5440e181aec495d80f03e92a358c1'
ts.set_token(TOKEN)
TUSHARE_API = ts.pro_api()

# 数据存储路径
BASE_DATA_DIR = Path(__file__).parent.parent / "data"
STOCK_LIST_FILE = Path(__file__).parent.parent / "stock_list.csv"

# API频率限制配置
API_CALLS_PER_MINUTE = 40
MIN_CALL_INTERVAL = 60.0 / API_CALLS_PER_MINUTE  # 每次调用最小间隔（1.5秒）


class APIRateLimiter:
    """API频率限制器"""
    def __init__(self, calls_per_minute=API_CALLS_PER_MINUTE):
        self.min_interval = 60.0 / calls_per_minute
        self.last_call_time = 0
        self.call_count = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """如果需要，等待到下次可以调用的时间"""
        with self.lock:
            current_time = time.time()
            time_since_last_call = current_time - self.last_call_time
            
            if time_since_last_call < self.min_interval:
                sleep_time = self.min_interval - time_since_last_call
                print(f"⏳ API频率限制：等待 {sleep_time:.2f} 秒")
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()
            self.call_count += 1
            if self.call_count % 10 == 0:
                print(f"📊 已调用API {self.call_count} 次")


class StockDataDownloader:
    """股票数据下载器"""
    
    def __init__(self, target_date: str = None):
        """
        初始化下载器
        Args:
            target_date: 目标日期，格式YYYYMMDD，如果为None则使用当前日期
        """
        if target_date is None:
            target_date = datetime.now().strftime("%Y%m%d")
        
        self.target_date = target_date
        self.data_dir = BASE_DATA_DIR / target_date
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # 加载股票列表
        self.stocks_df = pd.read_csv(STOCK_LIST_FILE)
        self.rate_limiter = APIRateLimiter()
        
        print(f"下载器初始化完成，目标日期: {target_date}")
        print(f"数据保存目录: {self.data_dir}")
    
    def _calculate_kdj(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算KDJ指标"""
        n = 9
        data = data.sort_values('trade_date', ascending=True).reset_index(drop=True)
        
        close_prices = data['close'].values
        high_prices = data['high'].values
        low_prices = data['low'].values
        
        k_list, d_list, j_list = [], [], []
        k_previous, d_previous = 50, 50
        
        for i in range(len(close_prices)):
            # 计算RSV值
            low_min = np.min(low_prices[max(0, i - n + 1):i + 1])
            high_max = np.max(high_prices[max(0, i - n + 1):i + 1])
            rsv = (close_prices[i] - low_min) / (high_max - low_min) * 100
            
            # 计算K值
            k = 2 / 3 * k_previous + 1 / 3 * rsv
            k_list.append(k)
            
            # 计算D值
            d = 2 / 3 * d_previous + 1 / 3 * k
            d_list.append(d)
            
            # 计算J值
            j = 3 * k - 2 * d
            j_list.append(j)
            
            k_previous, d_previous = k, d
        
        data['K'] = k_list
        data['D'] = d_list
        data['J'] = j_list
        return data
    
    def _calculate_bbi(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算BBI指标"""
        data['SMA5'] = data['close'].rolling(window=5).mean()
        data['SMA10'] = data['close'].rolling(window=10).mean()
        data['SMA20'] = data['close'].rolling(window=20).mean()
        data['SMA60'] = data['close'].rolling(window=60).mean()
        data['BBI'] = (data['SMA5'] + data['SMA10'] + data['SMA20'] + data['SMA60']) / 4
        return data
    
    def _calculate_white(self, data: pd.DataFrame) -> pd.DataFrame:
        """计算知行短期趋势线 (white)：EMA(EMA(C,10),10)"""
        # 第一层：对收盘价计算10日EMA
        data['EMA10'] = data['close'].ewm(span=10, adjust=False).mean()
        # 第二层：对EMA10再计算10日EMA
        data['white'] = data['EMA10'].ewm(span=10, adjust=False).mean()
        # 删除中间变量
        data = data.drop('EMA10', axis=1)
        return data
    
    def _calculate_yellow(self, data: pd.DataFrame, m1=5, m2=10, m3=20, m4=60) -> pd.DataFrame:
        """
        计算知行多空线 (yellow)：(MA(CLOSE,M1)+MA(CLOSE,M2)+MA(CLOSE,M3)+MA(CLOSE,M4))/4
        Args:
            data: 股票数据
            m1, m2, m3, m4: 四个移动平均线的周期，默认为5, 10, 20, 60
        """
        ma1 = data['close'].rolling(window=m1).mean()
        ma2 = data['close'].rolling(window=m2).mean()
        ma3 = data['close'].rolling(window=m3).mean()
        ma4 = data['close'].rolling(window=m4).mean()
        
        # 计算四个MA的平均值
        data['yellow'] = (ma1 + ma2 + ma3 + ma4) / 4
        return data
    
    def download_stock_batch(self, stock_codes: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        批量下载股票数据
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            下载的数据DataFrame
        """
        batch_size = 8  # 每批8只股票
        all_data = []
        
        print(f"开始下载 {len(stock_codes)} 只股票的数据...")
        
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i+batch_size]
            codes_str = ",".join(batch_codes)
            
            print(f"正在下载第 {i//batch_size + 1} 批数据 ({len(batch_codes)} 只股票)...")
            
            try:
                # API频率控制
                self.rate_limiter.wait_if_needed()
                
                # 获取数据
                data = TUSHARE_API.daily(ts_code=codes_str, start_date=start_date, end_date=end_date)
                
                if not data.empty:
                    all_data.append(data)
                    print(f"第 {i//batch_size + 1} 批成功获取 {len(data)} 条记录")
                else:
                    print(f"第 {i//batch_size + 1} 批没有数据")
                    
            except Exception as e:
                print(f"第 {i//batch_size + 1} 批下载失败: {e}")
                continue
        
        if all_data:
            combined_data = pd.concat(all_data, ignore_index=True)
            print(f"✅ 批量下载完成，总共获取了 {len(combined_data)} 条数据记录")
            return combined_data
        else:
            print("❌ 没有成功获取到任何数据")
            return pd.DataFrame()
    
    def process_and_save_data(self, data: pd.DataFrame):
        """处理数据并保存到本地文件"""
        if data.empty:
            print("没有数据需要处理")
            return
        
        # 按股票代码分组处理
        grouped = data.groupby('ts_code')
        saved_count = 0
        
        for ts_code, group_data in grouped:
            try:
                # 计算技术指标
                processed_data = self._calculate_kdj(group_data.copy())
                processed_data = self._calculate_bbi(processed_data)
                processed_data = self._calculate_white(processed_data)
                processed_data = self._calculate_yellow(processed_data)
                
                # 文件名使用股票代码的前6位数字
                file_name = f"{ts_code[:6]}.parquet"
                file_path = self.data_dir / file_name
                
                # 如果文件已存在，需要合并数据
                if file_path.exists():
                    existing_data = pd.read_parquet(file_path)
                    
                    # 只合并基础交易数据，不包含技术指标
                    base_columns = ['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 
                                  'pre_close', 'change', 'pct_chg', 'vol', 'amount']
                    
                    # 确保新数据只包含基础列
                    new_base_data = group_data[base_columns].copy()
                    existing_base_data = existing_data[base_columns].copy()
                    
                    # 合并基础数据并去重
                    combined_base_data = pd.concat([existing_base_data, new_base_data])
                    combined_base_data = combined_base_data.drop_duplicates(subset=['trade_date']).sort_values('trade_date')
                    
                    # 为合并后的完整数据重新计算技术指标
                    final_data = self._calculate_kdj(combined_base_data)
                    final_data = self._calculate_bbi(final_data)
                    final_data = self._calculate_white(final_data)
                    final_data = self._calculate_yellow(final_data)
                    
                    final_data.to_parquet(file_path, index=False)
                else:
                    processed_data.to_parquet(file_path, index=False)
                
                saved_count += 1
                print(f"已保存 {ts_code} 的数据到 {file_name}")
                
            except Exception as e:
                print(f"处理 {ts_code} 数据时出错: {e}")
                continue
        
        print(f"成功保存了 {saved_count} 只股票的数据")
    
    def cleanup_old_data(self):
        """
        清理旧的数据目录，只保留当天的数据
        """
        if not BASE_DATA_DIR.exists():
            return
        
        print(f"\n🧹 开始清理旧数据，只保留当天数据 ({self.target_date})...")
        
        # 获取所有日期目录
        date_dirs = []
        for item in BASE_DATA_DIR.iterdir():
            if item.is_dir() and item.name.isdigit() and len(item.name) == 8:
                try:
                    # 验证是否为有效日期
                    datetime.strptime(item.name, "%Y%m%d")
                    date_dirs.append(item)
                except ValueError:
                    continue
        
        # 找出需要删除的目录（所有非当天的目录）
        dirs_to_delete = [d for d in date_dirs if d.name != self.target_date]
        
        if not dirs_to_delete:
            print(f"没有发现旧数据目录，当前只有今天的数据 ({self.target_date})")
            return
        
        print(f"保留目录: {self.target_date}")
        print(f"将删除目录: {[d.name for d in dirs_to_delete]}")
        
        deleted_count = 0
        total_size_freed = 0
        
        for dir_to_delete in dirs_to_delete:
            try:
                # 计算目录大小
                dir_size = sum(f.stat().st_size for f in dir_to_delete.rglob('*') if f.is_file())
                
                # 删除目录
                shutil.rmtree(dir_to_delete)
                deleted_count += 1
                total_size_freed += dir_size
                
                print(f"✅ 已删除目录: {dir_to_delete.name} (大小: {dir_size/1024/1024:.1f}MB)")
                
            except Exception as e:
                print(f"❌ 删除目录 {dir_to_delete.name} 失败: {e}")
        
        if deleted_count > 0:
            print(f"\n🎯 清理完成:")
            print(f"   删除了 {deleted_count} 个旧数据目录")
            print(f"   释放磁盘空间: {total_size_freed/1024/1024:.1f}MB")
            print(f"   现在只保留当天数据: {self.target_date}")
        else:
            print("\n❌ 没有成功删除任何目录")
    
    def verify_download_success(self) -> bool:
        """
        验证今天的数据是否下载成功
        Returns:
            True if successful, False otherwise
        """
        if not self.data_dir.exists():
            return False
        
        # 检查parquet文件数量
        parquet_files = list(self.data_dir.glob("*.parquet"))
        expected_count = len(self.stocks_df[self.stocks_df['market'].isin(['主板', '创业板'])])
        
        success_rate = len(parquet_files) / expected_count if expected_count > 0 else 0
        
        print(f"\n📊 下载验证:")
        print(f"   预期文件数: {expected_count}")
        print(f"   实际文件数: {len(parquet_files)}")
        print(f"   成功率: {success_rate*100:.1f}%")
        
        # 如果成功率超过95%，认为下载成功
        return success_rate >= 0.95
    
    def download_all_stocks(self, start_date: str = "20230101", end_date: str = None):
        """
        下载所有主板和创业板股票数据
        Args:
            start_date: 开始日期，默认"20230101"
            end_date: 结束日期，如果为None则使用target_date
        """
        if end_date is None:
            end_date = self.target_date
        
        print(f"=== 股票数据下载器 ===")
        print(f"下载范围: {start_date} 到 {end_date}")
        print(f"股票范围: 主板和创业板所有股票")
        print(f"保存目录: {self.data_dir}")
        print("🚦 API频率控制: 每分钟最多40次调用（每1.5秒一次）")
        
        start_time = time.time()
        
        # 获取所有主板和创业板股票代码
        main_stocks = self.stocks_df[self.stocks_df['market'].isin(['主板', '创业板'])]
        all_codes = main_stocks['ts_code'].tolist()
        
        print(f"需要下载 {len(all_codes)} 只股票的数据")
        
        # 下载数据
        data = self.download_stock_batch(all_codes, start_date, end_date)
        
        # 处理并保存数据
        self.process_and_save_data(data)
        
        end_time = time.time()
        duration = end_time - start_time
        print(f"\n=== 下载完成 ===")
        print(f"总运行时间: {duration:.2f} 秒 ({duration/60:.2f} 分钟)")
        print(f"数据已保存到: {self.data_dir}")
        print("每只股票的数据保存为单独的parquet文件，文件名为股票代码前6位.parquet")
        
        # 验证下载是否成功，如果成功则清理旧数据
        if self.verify_download_success():
            print(f"\n✅ 今日数据下载成功！开始清理旧数据...")
            self.cleanup_old_data()  # 只保留当天的数据
        else:
            print(f"\n⚠️ 今日数据下载可能不完整，跳过清理旧数据")


def download_today_data():
    """下载今天的数据 - 主程序入口"""
    today = datetime.now().strftime("%Y%m%d")
    downloader = StockDataDownloader(target_date=today)
    downloader.download_all_stocks()


if __name__ == "__main__":
    download_today_data()
    