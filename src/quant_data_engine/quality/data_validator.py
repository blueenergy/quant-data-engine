# quant_data_engine/quality/data_validator.py
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime, timedelta

class DataValidator:
    """数据质量验证器"""
    
    def __init__(self):
        self.rules = {
            'stock_data': self._validate_stock_data,
            'financial_data': self._validate_financial_data,
            'technical_data': self._validate_technical_data
        }
    
    def validate_data(self, data: pd.DataFrame, data_type: str) -> Dict[str, Any]:
        """验证数据质量"""
        if data_type not in self.rules:
            raise ValueError(f"不支持的数据类型: {data_type}")
        
        return self.rules[data_type](data)
    
    def _validate_stock_data(self, data: pd.DataFrame) -> Dict[str, Any]:
        """验证股票数据"""
        issues = []
        
        # 1. 检查必需字段
        required_fields = ['open', 'high', 'low', 'close', 'volume']
        missing_fields = [field for field in required_fields if field not in data.columns]
        if missing_fields:
            issues.append(f"缺少必需字段: {missing_fields}")
        
        # 2. 检查价格逻辑
        if 'high' in data.columns and 'low' in data.columns:
            invalid_high_low = data[data['high'] < data['low']]
            if not invalid_high_low.empty:
                issues.append(f"最高价小于最低价: {len(invalid_high_low)} 条记录")
        
        # 3. 检查价格范围
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if col in data.columns:
                negative_prices = data[data[col] <= 0]
                if not negative_prices.empty:
                    issues.append(f"{col} 存在非正数: {len(negative_prices)} 条记录")
        
        # 4. 检查成交量
        if 'volume' in data.columns:
            negative_volume = data[data['volume'] < 0]
            if not negative_volume.empty:
                issues.append(f"成交量为负数: {len(negative_volume)} 条记录")
        
        # 5. 检查数据连续性
        if 'date' in data.columns:
            data_sorted = data.sort_values('date')
            date_gaps = self._check_date_gaps(data_sorted['date'])
            if date_gaps:
                issues.append(f"日期不连续: {len(date_gaps)} 个缺口")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'total_records': len(data),
            'validated_at': datetime.now()
        }
    
    def _check_date_gaps(self, dates: pd.Series) -> List[str]:
        """检查日期缺口"""
        gaps = []
        dates = pd.to_datetime(dates).sort_values()
        
        for i in range(1, len(dates)):
            current_date = dates.iloc[i]
            prev_date = dates.iloc[i-1]
            
            # 检查是否有超过3天的缺口（排除周末）
            if (current_date - prev_date).days > 3:
                gaps.append(f"{prev_date.strftime('%Y-%m-%d')} 到 {current_date.strftime('%Y-%m-%d')}")
        
        return gaps

# 🔧 质量监控器
class QualityMonitor:
    """数据质量监控器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.validator = DataValidator()
        self.thresholds = config.get('quality_thresholds', {
            'missing_rate': 0.05,  # 缺失率阈值
            'outlier_rate': 0.01,  # 异常值率阈值
            'delay_hours': 2       # 数据延迟阈值（小时）
        })
    
    async def monitor_data_quality(self, symbol: str, data_type: str) -> Dict[str, Any]:
        """监控数据质量"""
        try:
            # 获取最新数据
            latest_data = await self._get_latest_data(symbol, data_type)
            
            # 验证数据质量
            validation_result = self.validator.validate_data(latest_data, data_type)
            
            # 检查数据时效性
            timeliness_check = self._check_data_timeliness(latest_data)
            
            # 检查数据完整性
            completeness_check = self._check_data_completeness(latest_data)
            
            # 汇总质量评分
            quality_score = self._calculate_quality_score(
                validation_result,
                timeliness_check,
                completeness_check
            )
            
            return {
                'symbol': symbol,
                'data_type': data_type,
                'quality_score': quality_score,
                'validation': validation_result,
                'timeliness': timeliness_check,
                'completeness': completeness_check,
                'checked_at': datetime.now()
            }
            
        except Exception as e:
            return {
                'symbol': symbol,
                'data_type': data_type,
                'error': str(e),
                'checked_at': datetime.now()
            }