# quant_data_engine/storage/database_manager.py
from typing import Dict, List, Optional, Any
import pandas as pd
from datetime import datetime
import asyncio

class DataStorageManager:
    """数据存储管理器"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.handlers = {}
        self._initialize_handlers()
    
    def _initialize_handlers(self):
        """初始化存储处理器"""
        if self.config.get('mongodb'):
            from .mongodb_handler import MongoDBHandler
            self.handlers['mongodb'] = MongoDBHandler(self.config['mongodb'])
        
        if self.config.get('postgresql'):
            from .postgresql_handler import PostgreSQLHandler
            self.handlers['postgresql'] = PostgreSQLHandler(self.config['postgresql'])
        
        if self.config.get('redis'):
            from .redis_handler import RedisHandler
            self.handlers['redis'] = RedisHandler(self.config['redis'])
    
    async def save_stock_data(self, symbol: str, data: pd.DataFrame, data_type: str, storage_type: str = 'mongodb'):
        """保存股票数据"""
        handler = self.handlers.get(storage_type)
        if not handler:
            raise ValueError(f"不支持的存储类型: {storage_type}")
        
        # 添加元数据
        metadata = {
            'symbol': symbol,
            'data_type': data_type,
            'created_at': datetime.now(),
            'record_count': len(data)
        }
        
        return await handler.save_data(data, metadata)
    
    async def get_stock_data(self, symbol: str, data_type: str, start_date: str = None, end_date: str = None, storage_type: str = 'mongodb') -> pd.DataFrame:
        """获取股票数据"""
        handler = self.handlers.get(storage_type)
        if not handler:
            raise ValueError(f"不支持的存储类型: {storage_type}")
        
        query = {'symbol': symbol, 'data_type': data_type}
        
        if start_date and end_date:
            query['date'] = {'$gte': start_date, '$lte': end_date}
        
        return await handler.get_data(query)
    
    async def batch_save(self, data_batch: List[Dict], storage_type: str = 'mongodb'):
        """批量保存数据"""
        handler = self.handlers.get(storage_type)
        if not handler:
            raise ValueError(f"不支持的存储类型: {storage_type}")
        
        return await handler.batch_save(data_batch)

# 🔧 MongoDB 处理器
class MongoDBHandler:
    """MongoDB 数据处理器"""
    
    def __init__(self, config: Dict):
        from pymongo import MongoClient
        self.client = MongoClient(config['uri'])
        self.db = self.client[config['database']]
        self.collections = {
            'daily_data': self.db.daily_data,
            'minute_data': self.db.minute_data,
            'financial_data': self.db.financial_data,
            'technical_indicators': self.db.technical_indicators,
            'money_flow': self.db.money_flow,
            'stock_info': self.db.stock_info
        }
    
    async def save_data(self, data: pd.DataFrame, metadata: Dict) -> bool:
        """保存数据到 MongoDB"""
        try:
            collection_name = self._get_collection_name(metadata['data_type'])
            collection = self.collections[collection_name]
            
            # 转换DataFrame为字典列表
            records = data.to_dict('records')
            
            # 添加元数据到每条记录
            for record in records:
                record.update({
                    'symbol': metadata['symbol'],
                    'data_type': metadata['data_type'],
                    'created_at': metadata['created_at']
                })
            
            # 批量插入
            if records:
                result = collection.insert_many(records)
                print(f"✅ 保存 {len(result.inserted_ids)} 条 {metadata['data_type']} 数据")
                return True
            
            return False
            
        except Exception as e:
            print(f"❌ 保存数据失败: {e}")
            return False
    
    def _get_collection_name(self, data_type: str) -> str:
        """根据数据类型获取集合名称"""
        mapping = {
            'daily': 'daily_data',
            'minute': 'minute_data',
            'financial': 'financial_data',
            'technical': 'technical_indicators',
            'money_flow': 'money_flow',
            'basic_info': 'stock_info'
        }
        
        return mapping.get(data_type, 'daily_data')