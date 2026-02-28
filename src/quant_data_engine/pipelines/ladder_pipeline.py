"""Pipeline for limit-up ladder data collection and aggregation.

Fetches limit-up stocks from 东方财富, calculates streak heights,
aggregates by sector, and computes sentiment indicators.
"""

import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict
from dataclasses import dataclass

from quant_data_engine.repositories.ladder_repository import LadderRepository
from quant_data_engine.adapters.tushare_adapter import TushareLadderAdapter
from quant_data_engine.config.settings import get_settings

logger = logging.getLogger(__name__)


@dataclass
class LadderResult:
    """Result of a ladder sync operation."""
    date: str
    total_stocks: int
    saved_stocks: int
    sectors: int
    sentiment_score: int
    issues: List[str]


class EastMoneyAPI:
    """东方财富数据接口"""
    
    LIMIT_UP_URL = "https://push2ex.eastmoney.com/getTopicZTPool"
    LIMIT_DOWN_URL = "https://push2ex.eastmoney.com/getTopicDTPool"
    
    @staticmethod
    def get_limit_up_stocks(date: str) -> List[Dict]:
        """获取涨停股票列表
        
        Args:
            date: 日期，格式 YYYYMMDD
            
        Returns:
            涨停股票列表
        """
        try:
            params = {
                "ut": "7eea3edcaed734bea9cbfc24409ed989",
                "dpt": "wz.ztzt",
                "Pageindex": 0,
                "pagesize": 500,
                "sort": "fbt:asc",
                "date": date,
                "_": int(datetime.now().timestamp() * 1000)
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            }
            
            resp = requests.get(EastMoneyAPI.LIMIT_UP_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("data") and data["data"].get("pool"):
                stocks = []
                # Date Validation: Check if the returned data is actually for the requested date
                # We check the first item's fbt (First Board Time) or any time field
                # Format of fbt is usually HH:MM:SS, but let's check if we can trust it blindly?
                # Actually, real-time API usually doesn't return date field in item.
                # However, if today is holiday, it returns last trading day's data.
                # We should verify if the date matches. 
                # Difficulity: 'fbt' is HH:MM:SS, no YYYYMMDD. 
                # But 'lbt' (Last Board Time) is also HH:MM:SS.
                # Wait, looking at the debug output (if available) would be best.
                # Assuming the debug script showed 'fbt' is just time.
                # BUT, if we are running this on a non-trading day, we can expect the API to return old data.
                # We need a way to detect "stale" data.
                
                # Update: The debug output (Step 148) will show us the structure.
                # If fbt/lbt doesn't have date, maybe we can check another field?
                # If not, we might need to rely on Tushare calendar check in run_ladder_sync.py instead.
                
                # Let's assume for now we will add a filter.
                # Actually, let's wait for Step 148 output to be sure.
                pass 
                
                for item in data["data"]["pool"]:
                    # ... processing ...
                    # Determine exchange suffix
                    code = item['c']
                    if code.startswith('6'):
                        suffix = 'SH'
                    elif code.startswith(('0', '3')):
                        suffix = 'SZ'
                    else:
                        suffix = 'BJ'
                    
                    # 连板数的正确字段是 zttj.ct (近N天涨停次数)
                    # lb 字段通常为空或1，不准确
                    # lbc 是连板次数，zttj.ct 是更可靠的连板统计
                    zttj = item.get("zttj", {})
                    streak = zttj.get("ct", 1)  # ct = 连续涨停次数
                    if streak == 0:
                        streak = item.get("lbc", 1)  # 备选：lbc
                    if streak == 0:
                        streak = 1  # 至少是首板
                    
                    stock = {
                        "symbol": f"{code}.{suffix}",
                        "code": code,
                        "name": item["n"],
                        "streak": streak,
                        "limit_up_time": item.get("fbt", ""),
                        "last_seal_time": item.get("lbt", ""),
                        "open_count": item.get("zbc", 0),  # zbc = 炸板次数
                        "seal_amount": item.get("fund", 0),
                        "turnover_ratio": item.get("hs", 0) / 100 if item.get("hs") else 0,
                        "limit_reason": item.get("hybk", ""),
                    }
                    stocks.append(stock)
                
                logger.info(f"Fetched {len(stocks)} limit-up stocks for {date}")
                return stocks
            else:
                logger.warning(f"No limit-up data for {date}: {data}")
                return []
                
        except Exception as e:
            logger.error(f"Failed to fetch limit-up data: {e}")
            return []

    @staticmethod
    def get_limit_down_stocks(date: str) -> List[Dict]:
        """获取跌停股票列表"""
        try:
            params = {
                "ut": "7eea3edfded0837c88f9a8f282852274",
                "dpt": "wz.dtdt",
                "Pageindex": 0,
                "pagesize": 500,
                "sort": "f18:1",
                "date": date,
                "_": int(datetime.now().timestamp() * 1000)
            }
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            }
            resp = requests.get(EastMoneyAPI.LIMIT_DOWN_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            if data.get("data") and data["data"].get("pool"):
                stocks = []
                for item in data["data"]["pool"]:
                    code = item['c']
                    suffix = 'SH' if code.startswith('6') else 'SZ' if code.startswith(('0', '3')) else 'BJ'
                    
                    # 跌停统计通常比涨停简单，这里主要关注跌停原因和板块
                    stocks.append({
                        "symbol": f"{code}.{suffix}",
                        "code": code,
                        "name": item["n"],
                        "streak": item.get("lbc", 1), # 连续跌停次数
                        "limit_down_time": item.get("fbt", ""),
                        "last_seal_time": item.get("lbt", ""),
                        "open_count": item.get("zbc", 0),
                        "seal_amount": item.get("fund", 0),
                        "turnover_ratio": item.get("hs", 0) / 100 if item.get("hs") else 0,
                        "limit_reason": item.get("hybk", "跌停"),
                    })
                return stocks
            return []
        except Exception as e:
            logger.error(f"Failed to fetch limit-down data: {e}")
            return []
    
    @staticmethod
    def get_stock_sectors(symbol: str) -> List[str]:
        """获取股票所属概念板块"""
        try:
            code = symbol.split(".")[0]
            market = "0" if symbol.endswith("SZ") else "1" if symbol.endswith("SH") else "0"
            
            url = "https://push2.eastmoney.com/api/qt/slist/get"
            params = {
                "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                "fltt": 2,
                "invt": 2,
                "fields": "f14",
                "secid": f"{market}.{code}",
                "np": 1,
                "spt": 1,
                "_": int(datetime.now().timestamp() * 1000)
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            }
            
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
            
            sectors = []
            if data.get("data") and data["data"].get("diff"):
                for item in data["data"]["diff"]:
                    if item.get("f14"):
                        sectors.append(item["f14"])
            
            return sectors[:5]
            
            
        except Exception as e:
            logger.debug(f"Failed to get sectors for {symbol}: {e}")
            return []

    @staticmethod
    def get_latest_market_date() -> Optional[str]:
        """获取最新市场交易日期 (YYYYMMDD)"""
        try:
            # Check 1.000001 (SH Index)
            url = "https://push2.eastmoney.com/api/qt/stock/get"
            params = {
                "secid": "1.000001",
                "fields": "f86",  # f86 = update timestamp
                "_": int(datetime.now().timestamp() * 1000)
            }
            
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Referer": "https://quote.eastmoney.com/"
            }
            
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            data = resp.json()
            
            if data.get("data") and data["data"].get("f86"):
                ts = data["data"]["f86"]
                dt = datetime.fromtimestamp(ts)
                return dt.strftime("%Y%m%d")
            
            return None
        except Exception as e:
            logger.error(f"Failed to get market date: {e}")
            return None


class LadderPipeline:
    """Pipeline for limit-up ladder data collection."""
    
    def __init__(self, repository: LadderRepository):
        self.repository = repository
    
    def run(self, date: str, fetch_sectors: bool = True, source: str = "tushare") -> LadderResult:
        """Run the ladder collection pipeline for a single date.
        
        Args:
            date: Date in YYYYMMDD format
            fetch_sectors: Whether to fetch sector info for each stock (slower)
            source: Data source ("eastmoney" or "tushare")
            
        Returns:
            LadderResult with statistics
        """
    
    def process_external_data(self, date: str, stocks: List[Dict], source: str = "eastmoney", fetch_sectors: bool = True) -> LadderResult:
        """Process a list of normalized stock data."""
        issues = []
        
        if not stocks:
            return LadderResult(
                date=date,
                total_stocks=0,
                saved_stocks=0,
                sectors=0,
                sentiment_score=50,
                issues=[f"No limit-up data available from {source}"]
            )
        
        # 2. Get yesterday's data for streak comparison
        yesterday_stocks = self.repository.get_yesterday_stocks(date)
        
        # 3. Process and save each stock
        saved_count = 0
        sector_counts = defaultdict(int)
        
        # Track current symbols to identify broken plates
        current_symbols = set()
        
        for stock in stocks:
            try:
                current_symbols.add(stock["symbol"])
                
                # Fetch sectors if enabled
                sectors = []
                if fetch_sectors and source == "eastmoney":
                    sectors = EastMoneyAPI.get_stock_sectors(stock["symbol"])
                
                primary_sector = sectors[0] if sectors else stock.get("limit_reason", "其他")
                
                doc = {
                    "date": date,
                    "symbol": stock["symbol"],
                    "name": stock["name"],
                    "streak": stock["streak"],
                    "prev_streak": yesterday_stocks.get(stock["symbol"], 0),
                    "limit_up_time": stock["limit_up_time"],
                    "last_seal_time": stock["last_seal_time"],
                    "open_count": stock["open_count"],
                    "seal_amount": stock["seal_amount"],
                    "turnover_ratio": stock["turnover_ratio"],
                    "sectors": sectors,
                    "primary_sector": primary_sector,
                    "limit_reason": stock.get("limit_reason", ""),
                    "industry": stock.get("industry", ""),
                    "close": stock.get("close", 0.0),
                    "pct_chg": stock.get("pct_chg", 0.0),
                    "up_stat": stock.get("up_stat", ""),
                    "limit_times": stock.get("limit_times", 0),
                    "data_source": source,
                    "stock_type": self._classify_stock_type(stock),
                    "is_sector_linked": len(sectors) > 0,
                    "status": "limit_up", # Explicitly set status
                    "limit_up_status": "sealed",
                    "created_at": datetime.now()
                }
                
                if self.repository.save_daily_stock(doc):
                    saved_count += 1
                    sector_counts[primary_sector] += 1
                    
            except Exception as e:
                issues.append(f"Failed to save {stock['symbol']}: {e}")

        # 3.5 Check for broken plates (stocks in DB for today but not in current list)
        existing_stocks = self.repository.get_daily_stocks(date)
        broken_count = 0
        for db_stock in existing_stocks:
            if db_stock["symbol"] not in current_symbols and db_stock.get("status") != "broken":
                logger.info(f"Marking {db_stock['symbol']} ({db_stock.get('name')}) as broken")
                if self.repository.mark_as_broken(date, db_stock["symbol"]):
                    broken_count += 1
        
        if broken_count > 0:
            logger.info(f"Marked {broken_count} stocks as broken limit-up")
        
        # 4. Calculate and save ladder aggregation
        ladder_doc = self._calculate_ladder(date, stocks, yesterday_stocks)
        self.repository.save_ladder(ladder_doc)
        
        # 5. Calculate and save sector flow
        self._save_sector_flow(date, stocks)
        
        return LadderResult(
            date=date,
            total_stocks=len(stocks),
            saved_stocks=saved_count,
            sectors=len(sector_counts),
            sentiment_score=ladder_doc.get("indicators", {}).get("sentiment_score", 50),
            issues=issues
        )

    def run(self, date: str, fetch_sectors: bool = True, source: str = "eastmoney") -> LadderResult:
        """Run the ladder collection pipeline for a single date."""
        # 1. Fetch limit-up stocks based on source
        if source == "tushare":
            stocks = self._fetch_from_tushare(date)
        else:
            stocks = EastMoneyAPI.get_limit_up_stocks(date)
            
        return self.process_external_data(date, stocks, source, fetch_sectors)
    
    def run_limit_down(self, date: str, fetch_sectors: bool = True, source: str = "tushare") -> LadderResult:
        """Run the limit-down collection pipeline."""
        if source == "tushare":
            stocks = self._fetch_down_from_tushare(date)
        else:
            stocks = EastMoneyAPI.get_limit_down_stocks(date)
            
        if not stocks:
            return LadderResult(date, 0, 0, 0, 50, [f"No limit-down data from {source}"])
            
        saved_count = 0
        sector_counts = defaultdict(int)
        for stock in stocks:
            try:
                sectors = EastMoneyAPI.get_stock_sectors(stock["symbol"]) if fetch_sectors else []
                primary_sector = sectors[0] if sectors else stock.get("limit_reason", "其他")
                
                doc = {
                    "date": date,
                    "symbol": stock["symbol"],
                    "name": stock["name"],
                    "streak": stock["streak"],
                    "limit_down_time": stock.get("limit_down_time", ""),
                    "last_seal_time": stock.get("last_seal_time", ""),
                    "open_count": stock.get("open_count", 0),
                    "seal_amount": stock.get("seal_amount", 0),
                    "turnover_ratio": stock.get("turnover_ratio", 0),
                    "sectors": sectors,
                    "primary_sector": primary_sector,
                    "status": "limit_down",
                    "data_source": source,
                    "updated_at": datetime.now()
                }
                if self.repository.save_daily_limit_down(doc):
                    saved_count += 1
                    sector_counts[primary_sector] += 1
            except Exception as e:
                logger.error(f"Failed to save limit-down {stock['symbol']}: {e}")

        # Basic aggregation for limit-down
        self.repository.save_limit_down_ladder({
            "date": date,
            "total_count": len(stocks),
            "sector_distribution": dict(sector_counts),
            "created_at": datetime.now()
        })
        
        return LadderResult(date, len(stocks), saved_count, len(sector_counts), 50, [])

    def _fetch_down_from_tushare(self, date: str) -> List[Dict]:
        """Fetch limit-down stocks from Tushare."""
        settings = get_settings()
        if not settings.tushare_token:
            logger.error("Tushare token not found in settings")
            return []
        try:
            adapter = TushareLadderAdapter(settings.tushare_token)
            limit_list = adapter.get_limit_down_stocks(date)
            if not limit_list:
                return []
            return self.convert_tushare_list(limit_list, status="limit_down")
        except Exception as e:
            logger.error(f"Failed to fetch limit-down from Tushare: {e}")
            return []
    
    def _classify_stock_type(self, stock: Dict) -> str:
        """Classify stock type (normal, st, etc.)"""
        name = stock.get("name", "")
        if "ST" in name or "*ST" in name:
            return "st"
        return "normal"
    
    def _calculate_ladder(self, date: str, stocks: List[Dict], 
                          yesterday_stocks: Dict[str, int]) -> Dict:
        """Calculate ladder aggregation data."""
        # Group by tier
        tiers = defaultdict(list)
        sector_stats = defaultdict(lambda: defaultdict(list))
        
        for stock in stocks:
            streak = stock["streak"]
            tier_key = "4+" if streak >= 4 else str(streak)
            tiers[tier_key].append(stock)
            
            sector = stock.get("limit_reason", "其他")
            sector_stats[tier_key][sector].append(stock)
        
        # Build tier data
        tier_data = {}
        for tier_key in ["1", "2", "3", "4+"]:
            stocks_in_tier = tiers.get(tier_key, [])
            
            sectors = {}
            for sector, sector_stocks in sector_stats.get(tier_key, {}).items():
                seal_amounts = [s.get("seal_amount", 0) for s in sector_stocks]
                avg_seal = sum(seal_amounts) / len(seal_amounts) if seal_amounts else 0
                
                sectors[sector] = {
                    "count": len(sector_stocks),
                    "stocks": [s["symbol"] for s in sector_stocks],
                    "avg_seal_amount": avg_seal
                }
            
            tier_data[tier_key] = {
                "count": len(stocks_in_tier),
                "stocks": [s["symbol"] for s in stocks_in_tier],
                "sectors": sectors
            }
        
        # Calculate indicators
        high_tier_stocks = [s for s in stocks if s["streak"] >= 3]
        max_streak = max((s["streak"] for s in stocks), default=0)
        
        # Promotion rate (yesterday tier 1 -> today tier 2+)
        tier1_yesterday = [sym for sym, streak in yesterday_stocks.items() if streak == 1]
        promoted = sum(1 for s in stocks if s["streak"] >= 2 and 
                      yesterday_stocks.get(s["symbol"], 0) == 1)
        promotion_rate = promoted / len(tier1_yesterday) if tier1_yesterday else 0
        
        sentiment_score = self._calculate_sentiment_score(
            total=len(stocks),
            high_tier=len(high_tier_stocks),
            promotion_rate=promotion_rate,
            max_streak=max_streak
        )
        
        indicators = {
            "total_limit_up": len(stocks),
            "promotion_rate_1to2": round(promotion_rate, 4),
            "high_tier_count": len(high_tier_stocks),
            "max_streak": max_streak,
            "sentiment_score": sentiment_score
        }
        
        # Sector ranking
        sector_ranking = self._calculate_sector_ranking(stocks)
        
        return {
            "date": date,
            "tiers": tier_data,
            "indicators": indicators,
            "sector_ranking": sector_ranking,
            "created_at": datetime.now()
        }
    
    def _calculate_sentiment_score(self, total: int, high_tier: int,
                                   promotion_rate: float, max_streak: int) -> int:
        """Calculate market sentiment score (0-100).
        
        Scoring components:
        - Base: 40
        - Total limit-up: +3 per 15 stocks over 50, max +20
        - High tier (3板+): +4 per stock, max +15
        - Promotion rate: +40 * rate
        - Max streak: +8 for >=5板, +4 for >=4板
        
        Result: 68 for typical good day (75 涨停, 8 high-tier, 6% promotion, 7板 max)
        """
        score = 40  # Conservative base score
        
        # Total limit-up count (+3 per 15 stocks over 50, max +20)
        score += min(20, max(0, (total - 50)) // 15 * 3)
        
        # High tier count (+4 per stock, max +15)
        score += min(15, high_tier * 4)
        
        # Promotion rate (+40 * rate, more impact than before)
        score += int(promotion_rate * 40)
        
        # Max streak bonus
        if max_streak >= 5:
            score += 8
        elif max_streak >= 4:
            score += 4
        
        return max(0, min(100, score))
    
    def _calculate_sector_ranking(self, stocks: List[Dict]) -> List[Dict]:
        """Calculate sector ranking."""
        sector_data = defaultdict(lambda: {"count": 0, "total_streak": 0, "stocks": []})
        
        for stock in stocks:
            sector = stock.get("limit_reason", "其他")
            sector_data[sector]["count"] += 1
            sector_data[sector]["total_streak"] += stock["streak"]
            sector_data[sector]["stocks"].append(stock["symbol"])
        
        ranking = []
        for sector, data in sector_data.items():
            avg_streak = data["total_streak"] / data["count"] if data["count"] else 0
            score = data["count"] * 10 + avg_streak * 20
            
            ranking.append({
                "sector": sector,
                "count": data["count"],
                "avg_streak": round(avg_streak, 2),
                "score": round(score, 1)
            })
        
        ranking.sort(key=lambda x: x["score"], reverse=True)
        return ranking[:15]
    
    def _save_sector_flow(self, date: str, stocks: List[Dict]):
        """Calculate and save sector flow data."""
        sector_data = defaultdict(lambda: {
            "limit_up_count": 0,
            "streak_distribution": defaultdict(int),
            "promotions": 0,
            "stocks": []
        })
        
        for stock in stocks:
            sector = stock.get("limit_reason", "其他")
            streak = stock["streak"]
            prev_streak = stock.get("prev_streak", 0)
            
            sector_data[sector]["limit_up_count"] += 1
            
            tier_key = "4+" if streak >= 4 else str(streak)
            sector_data[sector]["streak_distribution"][tier_key] += 1
            sector_data[sector]["stocks"].append(stock["symbol"])
            
            if prev_streak > 0 and streak > prev_streak:
                sector_data[sector]["promotions"] += 1
        
        for sector, data in sector_data.items():
            promotion_rate = 0
            if data["limit_up_count"] > 0:
                promotion_rate = data["promotions"] / data["limit_up_count"]
            
            doc = {
                "date": date,
                "sector": sector,
                "limit_up_count": data["limit_up_count"],
                "streak_distribution": dict(data["streak_distribution"]),
                "promotions": data["promotions"],
                "promotion_rate": round(promotion_rate, 4),
                "is_mainline": data["limit_up_count"] >= 5 and promotion_rate >= 0.3,
                "created_at": datetime.now()
            }
            
            self.repository.save_sector_flow(doc)
            
    def _fetch_from_tushare(self, date: str) -> List[Dict]:
        """Fetch and merge data from Tushare."""
        settings = get_settings()
        if not settings.tushare_token:
            logger.error("Tushare token not found in settings")
            return []
            
        try:
            adapter = TushareLadderAdapter(settings.tushare_token)
            
            
            # 1. Fetch daily limit list (limit_list_d)
            limit_list = adapter.get_limit_up_stocks(date)
            if not limit_list:
                return []
                
            return self.convert_tushare_list(limit_list)
            
        except Exception as e:
            logger.error(f"Failed to fetch from Tushare: {e}")
            return []

    def convert_tushare_list(self, limit_list: List[Dict], status: str = "limit_up") -> List[Dict]:
        """Convert raw Tushare limit_list_d items to internal schema."""
        stocks = []
        for item in limit_list:
            # TS code format: 000001.SZ -> 000001.SZ (already compatible)
            
            # Parse streak from up_stat "2/4" -> 2
            up_stat = item.get("up_stat", "1/1")
            streak = 1
            if "/" in str(up_stat):
                try:
                    streak = int(up_stat.split("/")[0])
                except:
                    pass
            
            stock = {
                "symbol": item["ts_code"],
                "code": item["ts_code"].split(".")[0],
                "name": item["name"],
                "streak": streak,
                "limit_up_time": "" if status == "limit_up" else None,
                "limit_down_time": "" if status == "limit_down" else None,
                "last_seal_time": item.get("last_time", ""), 
                "open_count": int(item.get("open_times", 0)),
                "seal_amount": 0, # Not available
                "turnover_ratio": 0, # Not directly in limit_list_d
                "limit_reason": item.get("industry", ""), # Use industry as reason
                "industry": item.get("industry", ""),
                "close": float(item.get("close", 0)),
                "pct_chg": float(item.get("pct_chg", 0)),
                "up_stat": up_stat,
                "limit_times": int(item.get("limit_times", 0)),
            }
            stocks.append(stock)
        return stocks
