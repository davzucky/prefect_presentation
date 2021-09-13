from prefect import task, Flow, Parameter, unmapped
import pandas as pd
from datetime import timedelta
from typing import List
import time
import pathlib

@task
def index_to_stooq_id(index_name:str)-> str:
    i_2_s = {"DJI": "578",
            "NDX":"580",
            "HSI":"616"}
    return i_2_s[index_name]

@task(max_retries=4, retry_delay=timedelta(seconds=2), timeout=timedelta(seconds=4))
def get_index_composition(index_id: str) -> pd.DataFrame:
    return pd.read_html(f"https://stooq.com/t/?i={index_id}", attrs = {"id": "fth1"})[0]

@task
def get_symbols(df_index_composition: pd.DataFrame) -> List[str]:
    return [symbol for symbol in df_index_composition["Symbol"][:6]]

@task(max_retries=4, retry_delay=timedelta(seconds=2), timeout=timedelta(seconds=6))
def get_historical_data(symbol: str, time_frame: str="d") -> pd.DataFrame:
    df = pd.read_csv(f"https://stooq.com/q/d/l/?s={symbol}&i={time_frame}", parse_dates=True)
    df['Date'] =  pd.to_datetime(df['Date'], format='%Y-%m-%d')
    return df


@task
def resample_candle(df_daily:pd.DataFrame, resampling: str = "W") -> pd.DataFrame:
    agg_dict = {'Open': 'first',
          'High': 'max',
          'Low': 'min',
          'Close': 'last',
          'Volume': 'mean'}
    df_with_index = df_daily.set_index("Date")
    return df_with_index.resample(resampling).agg(agg_dict).reset_index()

@task
def get_save_path(root:str, index_name: str, symbol:str, timeframe:str)-> str:
    return f"{root}/{index_name}/{symbol}_{timeframe}.parquet"

@task
def save_to_parquet(df: pd.DataFrame, path: str):
    pathlib.Path(path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path)


def get_flow() -> Flow:
    with Flow("extract_market_data") as f:
        # Define the flow input parameters
        index_name = Parameter("index", default="DJI")
        root_market_data = Parameter("root_folder", "~/git/prefect_presentation/market_data")
        
        # Retrieve the symbols for the index
        index_id = index_to_stooq_id(index_name)
        df_composition = get_index_composition(index_id)
        symbols = get_symbols(df_composition)
        
        # Download the market data for the index
        df_OHLC_daily = get_historical_data.map(symbols)
        daily_path = get_save_path.map(unmapped(root_market_data),unmapped(index_name), symbols, unmapped("d") )
        save_to_parquet.map(df_OHLC_daily, daily_path)
        
        # Resample the market data to Weekly
        df_weekly = resample_candle.map(df_OHLC_daily, unmapped("W"))
        weekly_path = get_save_path.map(unmapped(root_market_data),unmapped(index_name), symbols, unmapped("W") )
        save_to_parquet.map(df_weekly, weekly_path)
        
        # Resample the market data to Monthly
        df_monthly = resample_candle.map(df_OHLC_daily, unmapped("M"))
        monthly_path = get_save_path.map(unmapped(root_market_data),unmapped(index_name), symbols, unmapped("M") )
        save_to_parquet.map(df_monthly, monthly_path)

    return f