# quant_data_engine/processors/data_cleaner.py
import pandas as pd
import numpy as np
from typing import Dict, List, Optional

class DataCleaner:
    """数据清洗器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.outlier_methods = {
            'iqr': self._remove_outliers_iqr,
            'zscore': self._remove_outliers_zscore,
            'isolation_forest': self._remove_outliers_isolation_forest
        }
    
    def clean_stock_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """清洗股票数据"""
        # 1. 删除重复数据
        data = self._remove_duplicates(data)
        
        # 2. 处理缺失值
        data = self._handle_missing_values(data)
        
        # 3. 数据类型转换
        data = self._convert_data_types(data)
        
        # 4. 异常值处理
        data = self._handle_outliers(data)
        
        # 5. 数据范围验证
        data = self._validate_ranges(data)
        
        return data
    
    def _remove_duplicates(self, data: pd.DataFrame) -> pd.DataFrame:
        """删除重复数据"""
        before_count = len(data)
        
        # 根据股票代码和日期去重
        if 'symbol' in data.columns and 'date' in data.columns:
            data = data.drop_duplicates(subset=['symbol', 'date'], keep='last')
        else:
            data = data.drop_duplicates()
        
        after_count = len(data)
        if before_count != after_count:
            print(f"🧹 删除重复数据: {before_count - after_count} 条")
        
        return data
    
    def _handle_missing_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理缺失值"""
        # 价格数据前向填充
        price_columns = ['open', 'high', 'low', 'close', 'pre_close']
        for col in price_columns:
            if col in data.columns:
                data[col] = data[col].fillna(method='ffill')
        
        # 成交量为0的用前一日数据
        if 'volume' in data.columns:
            data['volume'] = data['volume'].fillna(0)
        
        # 财务数据用0填充
        financial_columns = ['revenue', 'profit', 'eps', 'roe']
        for col in financial_columns:
            if col in data.columns:
                data[col] = data[col].fillna(0)
        
        return data
    
    def _handle_outliers(self, data: pd.DataFrame) -> pd.DataFrame:
        """处理异常值"""
        method = self.config.get('outlier_method', 'iqr')
        
        if method in self.outlier_methods:
            return self.outlier_methods[method](data)
        
        return data
    
    def _remove_outliers_iqr(self, data: pd.DataFrame) -> pd.DataFrame:
        """使用IQR方法删除异常值"""
        numeric_columns = data.select_dtypes(include=[np.number]).columns
        
        for col in numeric_columns:
            if col in ['open', 'high', 'low', 'close', 'volume']:
                Q1 = data[col].quantile(0.25)
                Q3 = data[col].quantile(0.75)
                IQR = Q3 - Q1
                
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # 标记异常值而不是删除
                outlier_mask = (data[col] < lower_bound) | (data[col] > upper_bound)
                data.loc[outlier_mask, col] = np.nan
                
                # 用前后值的平均值填充
                data[col] = data[col].interpolate()
        
        return data

# 🔧 技术指标计算器
class TechnicalCalculator:
    """技术指标计算器"""
    
    @staticmethod
    def calculate_ma(data: pd.Series, window: int) -> pd.Series:
        """计算移动平均线"""
        return data.rolling(window=window).mean()
    
    @staticmethod
    def calculate_ema(data: pd.Series, span: int) -> pd.Series:
        """计算指数移动平均线"""
        return data.ewm(span=span).mean()
    
    @staticmethod
    def calculate_rsi(data: pd.Series, window: int = 14) -> pd.Series:
        """计算RSI指标"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi
    
    @staticmethod
    def calculate_macd(data: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, pd.Series]:
        """计算MACD指标"""
        ema_fast = data.ewm(span=fast).mean()
        ema_slow = data.ewm(span=slow).mean()
        macd = ema_fast - ema_slow
        signal_line = macd.ewm(span=signal).mean()
        histogram = macd - signal_line
        
        return {
            'macd': macd,
            'signal': signal_line,
            'histogram': histogram
        }
    
    @staticmethod
    def calculate_bollinger_bands(data: pd.Series, window: int = 20, std_dev: int = 2) -> Dict[str, pd.Series]:
        """计算布林带"""
        ma = data.rolling(window=window).mean()
        std = data.rolling(window=window).std()
        
        return {
            'upper': ma + (std * std_dev),
            'middle': ma,
            'lower': ma - (std * std_dev)
        }