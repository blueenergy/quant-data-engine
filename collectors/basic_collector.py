# quant_data_engine/collectors/base_collector.py
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime, timedelta

class BaseDataCollector(ABC):
    """数据采集器基类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.rate_limiter = self._setup_rate_limiter()
        self.cache = self._setup_cache()
        self.retry_config = config.get('retry', {})
    
    @abstractmethod
    async def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        pass
    
    @abstractmethod
    async def get_stock_basic_info(self, symbols: List[str]) -> pd.DataFrame:
        """获取股票基本信息"""
        pass
    
    @abstractmethod
    async def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        pass
    
    @abstractmethod
    async def get_minute_data(self, symbol: str, start_time: str, end_time: str) -> pd.DataFrame:
        """获取分钟级数据"""
        pass
    
    @abstractmethod
    async def get_financial_data(self, symbol: str, period: str = 'Q') -> pd.DataFrame:
        """获取财务数据"""
        pass
    
    @abstractmethod
    async def get_money_flow_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取资金流向数据"""
        pass
    
    async def collect_batch(self, symbols: List[str], data_types: List[str], **kwargs) -> Dict[str, pd.DataFrame]:
        """批量采集数据"""
        results = {}
        for symbol in symbols:
            for data_type in data_types:
                try:
                    data = await self._collect_single(symbol, data_type, **kwargs)
                    results[f"{symbol}_{data_type}"] = data
                except Exception as e:
                    self.logger.error(f"采集 {symbol} {data_type} 失败: {e}")
        return results

# 🔧 Tushare 采集器实现
class TushareCollector(BaseDataCollector):
    """Tushare 数据采集器"""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        import tushare as ts
        self.pro = ts.pro_api(config['tushare_token'])
    
    async def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        try:
            # 获取基础信息
            basic_info = self.pro.stock_basic(
                exchange='',
                list_status='L',
                fields='ts_code,symbol,name,area,industry,market,list_date'
            )
            return basic_info
        except Exception as e:
            self.logger.error(f"获取股票列表失败: {e}")
            raise
    
    async def get_daily_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """获取日线数据"""
        try:
            # 转换股票代码格式
            ts_code = self._convert_symbol_to_tushare(symbol)
            
            # 获取日线数据
            daily_data = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date,
                fields='ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount'
            )
            
            return daily_data
        except Exception as e:
            self.logger.error(f"获取 {symbol} 日线数据失败: {e}")
            raise
    
    async def get_financial_data(self, symbol: str, period: str = 'Q') -> pd.DataFrame:
        """获取财务数据"""
        try:
            ts_code = self._convert_symbol_to_tushare(symbol)
            
            # 获取利润表
            income = self.pro.income(ts_code=ts_code, period=period)
            # 获取资产负债表
            balancesheet = self.pro.balancesheet(ts_code=ts_code, period=period)
            # 获取现金流量表
            cashflow = self.pro.cashflow(ts_code=ts_code, period=period)
            
            return {
                'income': income,
                'balancesheet': balancesheet,
                'cashflow': cashflow
            }
        except Exception as e:
            self.logger.error(f"获取 {symbol} 财务数据失败: {e}")
            raise