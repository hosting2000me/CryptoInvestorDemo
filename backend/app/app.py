import os
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse, FileResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
import asyncio
import json
import time
from typing import AsyncGenerator, Dict, Set, Optional
from datetime import date

from config import settings
from schemas import HealthCheckResponse, UserListResponse, UserCreateRequest, User

# Import crypto analytics modules
from core.db import PostgreSQLController, DeltaTableController
from core.services.crypto_analytics import CryptoAnalytics
from core.models.schemas import AddressStats, BenchmarkMetrics, AddressFilter

# Определяем окружение для документации
# Документация доступна только в dev окружении для безопасности

if settings.is_dev:
    docs_url = "/docs"
    redoc_url = "/redoc"
    openapi_url = "/openapi.json"
else:  # prod или любое другое значение
    docs_url = None
    redoc_url = None
    openapi_url = None

app = FastAPI(
    title="Crypto Investor Analytics API",
    description="API for analyzing Bitcoin addresses with financial metrics including profit/loss, Sharpe ratio, and drawdown",
    version="1.0.0",
    docs_url=docs_url,
    redoc_url=redoc_url,
    openapi_url=openapi_url,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https://(.+\.)?yourdomain\.com",
    allow_methods=["*"],  # Allow all methods
    allow_headers=["*"],  # Allow all headers
)

# Mount static files directory
app.mount("/assets", StaticFiles(directory="./static/assets"), name="assets")

# Initialize crypto analytics controllers and service
# These are initialized at startup for better performance
if PostgreSQLController is not None:
    postgres_controller = PostgreSQLController(settings.postgres_connection_params)
else:
    postgres_controller = None
    print("WARNING: PostgreSQLController is not available (psycopg2 not installed)")
delta_controller = DeltaTableController(settings.table_paths)
if postgres_controller is not None:
    analytics = CryptoAnalytics(postgres_controller, delta_controller)
else:
    analytics = None
    print("WARNING: CryptoAnalytics service is not available (PostgreSQLController missing)")

@app.get("/")
def read_root():
    return FileResponse("./static/index.html")

@app.get("/health", response_model=HealthCheckResponse)
def health_check():
    return HealthCheckResponse(status="healthy")

@app.get("/api/example")
def example_endpoint():
    return {"message": "This is an example endpoint"}

# Additional example endpoints
@app.get("/api/users", response_model=UserListResponse)
def get_users():
    return UserListResponse(users=[], total=0)

@app.post("/api/users", response_model=User)
def create_user(user_data: UserCreateRequest):
    # In a real application, this would create a user in the database
    new_user = User(id=1, name=user_data.name, email=user_data.email)
    return new_user

@app.get("/api/items/{item_id}")
def get_item(item_id: int, q: str = None):
    return {"item_id": item_id, "q": q}

# ============ Crypto Analytics Endpoints ============

@app.get("/api/crypto/address/{address}/stats")
def get_address_stats(address: str):
    """
    Get comprehensive statistics for a Bitcoin address.
    
    Returns profit/loss, Sharpe ratio, drawdown, exposure, and benchmark metrics.
    """
    if analytics is None:
        raise HTTPException(status_code=503, detail="CryptoAnalytics service is not available (PostgreSQLController missing)")
    try:
        result = analytics.get_address_transactions(address)
        # Return only the statistics, not the full balance history
        return {
            "address": result["address"],
            "profit_pct": result["profit_pct"],
            "sharpe_ratio": result["sharpe_ratio"],
            "drawdown": result["drawdown"],
            "exposure": result["exposure"],
            "count_days_in_market": result["count_days_in_market"],
            "benchmark_profit": result["benchmark_profit"],
            "benchmark_sharpe": result["benchmark_sharpe"],
            "benchmark_drawdown": result["benchmark_drawdown"]
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.get("/api/crypto/address/{address}/balance")
def get_address_balance(address: str):
    """
    Get balance history for a Bitcoin address.
    
    Returns a DataFrame with date and value columns.
    """
    if analytics is None:
        raise HTTPException(status_code=503, detail="CryptoAnalytics service is not available (PostgreSQLController missing)")
    try:
        df = analytics.get_address_balance(address)
        # Convert Polars DataFrame to dict for JSON serialization
        return {
            "address": address,
            "data": df.to_dict(as_series=False)
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")



@app.get("/api/crypto/top-addresses")
def get_top_addresses(
    date: str,
    profit2btc_min: Optional[float] = None,
    max_btc_min: Optional[int] = None,
    btcvalue_ratio_min: Optional[float] = None,
    count_out_min: Optional[int] = None,
    first_in_after: Optional[str] = None
):
    """
    Get top addresses by profit for a specific date.
    
    Query parameters:
    - date: Date in format YYYY-MM-DD (required)
    - profit2btc_min: Minimum profit in BTC
    - max_btc_min: Minimum maximum BTC held
    - btcvalue_ratio_min: Minimum current BTC value ratio to max (0-1)
    - count_out_min: Minimum number of output transactions
    - first_in_after: First input after this date (YYYY-MM-DD)
    """
    if analytics is None:
        raise HTTPException(status_code=503, detail="CryptoAnalytics service is not available (PostgreSQLController missing)")
    try:
        filters = {}
        if profit2btc_min is not None:
            filters["profit2btc_min"] = profit2btc_min
        if max_btc_min is not None:
            filters["max_btc_min"] = max_btc_min
        if btcvalue_ratio_min is not None:
            filters["btcvalue_ratio_min"] = btcvalue_ratio_min
        if count_out_min is not None:
            filters["count_out_min"] = count_out_min
        if first_in_after is not None:
            filters["first_in_after"] = date.fromisoformat(first_in_after)
        
        addresses = analytics.get_top_addresses_by_profit(date, filters)
        return {
            "date": date,
            "count": len(addresses),
            "addresses": addresses
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

@app.get("/api/crypto/address/{address}/full")
def get_address_full(address: str):
    """
    Get full data for a Bitcoin address including statistics and balance history.
    
    Returns comprehensive data including balance history DataFrame.
    """
    if analytics is None:
        raise HTTPException(status_code=503, detail="CryptoAnalytics service is not available (PostgreSQLController missing)")
    try:
        result = analytics.get_address_transactions(address)
        # Convert balance history DataFrame to dict
        balance_history_dict = result["balance_history"].to_dict(as_series=False)
        
        return {
            "address": result["address"],
            "profit_pct": result["profit_pct"],
            "sharpe_ratio": result["sharpe_ratio"],
            "drawdown": result["drawdown"],
            "exposure": result["exposure"],
            "count_days_in_market": result["count_days_in_market"],
            "benchmark_profit": result["benchmark_profit"],
            "benchmark_sharpe": result["benchmark_sharpe"],
            "benchmark_drawdown": result["benchmark_drawdown"],
            "balance_history": balance_history_dict
        }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing request: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        workers=1,
        loop="auto",
        timeout_keep_alive=180,  
        timeout_graceful_shutdown=5,
        log_level="debug",
        access_log=True
    )       