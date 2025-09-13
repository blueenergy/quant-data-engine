# quant_data_engine/apis/data_api.py
from fastapi import FastAPI, HTTPException, Depends, Query
from typing import List, Optional, Dict
import pandas as pd
from datetime import datetime, timedelta

app = FastAPI(title="Quant Data Engine API", version="1.0.0")

class DataAPI:
    """数据 API 服务"""
    
    def __init__(self, data_engine):
        self.data_engine = data_engine
        self.setup_routes()
    
    def setup_routes(self):
        """设置 API 路由"""
        
        @app.get("/api/stocks/list")
        async def get_stock_list(
            market: Optional[str] = Query(None, description="市场类型"),
            industry: Optional[str] = Query(None, description="行业"),
            page: int = Query(1, ge=1),
            limit: int = Query(100, ge=1, le=1000)
        ):
            """获取股票列表"""
            try:
                stocks = await self.data_engine.get_stock_list(
                    market=market,
                    industry=industry,
                    page=page,
                    limit=limit
                )
                return {
                    "code": 200,
                    "data": stocks.to_dict('records'),
                    "total": len(stocks),
                    "page": page,
                    "limit": limit
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @app.get("/api/stocks/{symbol}/daily")
        async def get_daily_data(
            symbol: str,
            start_date: Optional[str] = Query(None, description="开始日期 YYYY-MM-DD"),
            end_date: Optional[str] = Query(None, description="结束日期 YYYY-MM-DD"),
            indicators: Optional[List[str]] = Query(None, description="技术指标列表")
        ):
            """获取日线数据"""
            try:
                # 设置默认日期范围
                if not end_date:
                    end_date = datetime.now().strftime('%Y-%m-%d')
                if not start_date:
                    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
                
                data = await self.data_engine.get_daily_data(
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    indicators=indicators
                )
                
                return {
                    "code": 200,
                    "data": data.to_dict('records'),
                    "symbol": symbol,
                    "start_date": start_date,
                    "end_date": end_date
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @app.get("/api/stocks/{symbol}/financial")
        async def get_financial_data(
            symbol: str,
            period: str = Query("Q", description="报告期类型: Q-季报, A-年报"),
            years: int = Query(5, ge=1, le=10, description="获取年数")
        ):
            """获取财务数据"""
            try:
                data = await self.data_engine.get_financial_data(
                    symbol=symbol,
                    period=period,
                    years=years
                )
                
                return {
                    "code": 200,
                    "data": data,
                    "symbol": symbol,
                    "period": period
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        
        @app.post("/api/data/batch-collect")
        async def batch_collect_data(
            request: Dict[str, Any]
        ):
            """批量采集数据"""
            try:
                symbols = request.get('symbols', [])
                data_types = request.get('data_types', ['daily'])
                date_range = request.get('date_range', {})
                
                result = await self.data_engine.batch_collect(
                    symbols=symbols,
                    data_types=data_types,
                    **date_range
                )
                
                return {
                    "code": 200,
                    "message": "批量采集完成",
                    "result": result
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))