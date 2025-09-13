# quant_data_engine/__init__.py
from .collectors import TushareCollector, AKShareCollector
from .processors import DataCleaner, TechnicalCalculator
from .storage import DataStorageManager
from .quality import DataValidator, QualityMonitor
from .apis import DataAPI

class QuantDataEngine:
    """量化数据引擎主类"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        
        # 初始化各个模块
        self.collectors = self._init_collectors()
        self.processors = self._init_processors()
        self.storage = DataStorageManager(config.get('storage', {}))
        self.quality = QualityMonitor(config.get('quality', {}))
        
        # API 服务
        if config.get('enable_api', False):
            self.api = DataAPI(self)
    
    def _init_collectors(self) -> Dict[str, Any]:
        """初始化数据采集器"""
        collectors = {}
        
        if self.config.get('tushare'):
            collectors['tushare'] = TushareCollector(self.config['tushare'])
        
        if self.config.get('akshare'):
            collectors['akshare'] = AKShareCollector(self.config['akshare'])
        
        return collectors
    
    async def collect_and_store(self, symbols: List[str], data_types: List[str], collector_type: str = 'tushare'):
        """采集并存储数据"""
        collector = self.collectors.get(collector_type)
        if not collector:
            raise ValueError(f"未配置采集器: {collector_type}")
        
        results = []
        
        for symbol in symbols:
            for data_type in data_types:
                try:
                    # 1. 采集数据
                    raw_data = await collector.collect_data(symbol, data_type)
                    
                    # 2. 数据处理
                    processed_data = self.processors['cleaner'].clean_stock_data(raw_data)
                    
                    # 3. 数据验证
                    validation = await self.quality.monitor_data_quality(symbol, data_type)
                    
                    # 4. 存储数据
                    if validation.get('quality_score', 0) > 0.8:
                        await self.storage.save_stock_data(symbol, processed_data, data_type)
                        results.append({
                            'symbol': symbol,
                            'data_type': data_type,
                            'status': 'success',
                            'records': len(processed_data)
                        })
                    else:
                        results.append({
                            'symbol': symbol,
                            'data_type': data_type,
                            'status': 'quality_failed',
                            'quality_score': validation.get('quality_score', 0)
                        })
                        
                except Exception as e:
                    results.append({
                        'symbol': symbol,
                        'data_type': data_type,
                        'status': 'error',
                        'error': str(e)
                    })
        
        return results

# 🔧 工厂函数
def create_data_engine(config_path: str = None) -> QuantDataEngine:
    """创建数据引擎实例"""
    if config_path:
        import json
        with open(config_path, 'r') as f:
            config = json.load(f)
    else:
        config = {
            'tushare': {'token': os.getenv('TUSHARE_TOKEN')},
            'storage': {'mongodb': {'uri': os.getenv('MONGO_URI')}},
            'enable_api': True
        }
    
    return QuantDataEngine(config)