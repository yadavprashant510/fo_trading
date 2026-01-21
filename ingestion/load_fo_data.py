"""
ETL Script: Load NSE Futures & Options Data into PostgreSQL
Author: Prashant
Purpose: Normalize high-volume F&O data into relational schema
"""

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from datetime import datetime

# -----------------------------
# CONFIGURATION
# -----------------------------
DB_URI = "postgresql://prashantyadav:Terminus#123@localhost:5432/fo_trading"
CSV_PATH = "3mfanddo.csv"
CHUNK_SIZE = 200_000
DEFAULT_EXCHANGE = "NSE"

# -----------------------------
# DATABASE CONNECTION
# -----------------------------
engine = create_engine(DB_URI, pool_pre_ping=True)
Session = sessionmaker(bind=engine)
session = Session()

# -----------------------------
# HELPER FUNCTIONS
# -----------------------------

def get_exchange_id(exchange_name: str) -> int:
    """Fetch exchange_id for given exchange"""
    result = session.execute(
        text("SELECT exchange_id FROM exchanges WHERE exchange_name = :name"),
        {"name": exchange_name}
    ).fetchone()
    return result[0]


def load_instruments(df: pd.DataFrame, exchange_id: int) -> dict:
    """
    Insert distinct instruments and return mapping:
    (symbol, instrument_type) -> instrument_id
    """
    instruments = (
        df[["SYMBOL", "INSTRUMENT"]]
        .drop_duplicates()
        .rename(columns={"SYMBOL": "symbol", "INSTRUMENT": "instrument_type"})
    )

    instruments["exchange_id"] = exchange_id

    instruments.to_sql(
        "instruments",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    result = session.execute(
        text("""
            SELECT instrument_id, symbol, instrument_type
            FROM instruments
            WHERE exchange_id = :eid
        """),
        {"eid": exchange_id}
    ).fetchall()

    return {(r[1], r[2]): r[0] for r in result}


def load_expiries(df: pd.DataFrame, instrument_map: dict) -> dict:
    """
    Insert expiries and return mapping:
    (instrument_id, expiry_dt, strike_pr, option_typ) -> expiry_id
    """
    expiry_df = (
        df[["SYMBOL", "INSTRUMENT", "EXPIRY_DT", "STRIKE_PR", "OPTION_TYP"]]
        .drop_duplicates()
        .rename(columns={
            "EXPIRY_DT": "expiry_dt",
            "STRIKE_PR": "strike_pr",
            "OPTION_TYP": "option_typ"
        })
    )

    expiry_df["instrument_id"] = expiry_df.apply(
        lambda r: instrument_map.get((r["SYMBOL"], r["INSTRUMENT"])),
        axis=1
    )

    expiry_df = expiry_df[
        ["instrument_id", "expiry_dt", "strike_pr", "option_typ"]
    ]

    expiry_df.to_sql(
        "expiries",
        engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    result = session.execute(
        text("""
            SELECT expiry_id, instrument_id, expiry_dt, strike_pr, option_typ
            FROM expiries
        """)
    ).fetchall()

    return {
        (r[1], r[2], r[3], r[4]): r[0]
        for r in result
    }


def load_trades(df: pd.DataFrame, instrument_map: dict, expiry_map: dict):
    """Insert trades (fact table)"""
    trades = df.rename(columns={
        "TIMESTAMP": "trade_date",
        "OPEN": "open",
        "HIGH": "high",
        "LOW": "low",
        "CLOSE": "close",
        "OPEN_INT": "open_int",
        "SETTLE_PR": "settle_pr",
        "EXPIRY_DT": "expiry_dt",
        "STRIKE_PR": "strike_pr",
        "OPTION_TYP": "option_typ",
        "CONTRACTS": "contracts",
        "VAL_INLAKH": "val_inlakh",
        "CHG_IN_OI": "chg_in_oi",
    })

    trades["instrument_id"] = trades.apply(
        lambda r: instrument_map.get((r["SYMBOL"], r["INSTRUMENT"])),
        axis=1
    )

    trades["expiry_id"] = trades.apply(
        lambda r: expiry_map.get(
            (r["instrument_id"], r["expiry_dt"], r["strike_pr"], r["option_typ"])
        ),
        axis=1
    )

    trades = trades[
        [
            "instrument_id",
            "expiry_id",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "val_inlakh",
            "open_int",
            "settle_pr",
            "chg_in_oi",
            "contracts",
        ]
    ]

    trades.to_sql(
        "trades",
        engine,
        if_exists="append",
        index=False,
        chunksize=CHUNK_SIZE,
        method="multi",
    )


# -----------------------------
# MAIN ETL PIPELINE
# -----------------------------
def main():
    start_time = datetime.now()
    print("🚀 ETL started at:", start_time)

    exchange_id = get_exchange_id(DEFAULT_EXCHANGE)

    print("📥 Loading CSV...")
    for i, chunk in enumerate(pd.read_csv(CSV_PATH, chunksize=CHUNK_SIZE)):
        print(f"➡ Processing chunk {i + 1}")

        # Clean & standardize
        chunk["EXPIRY_DT"] = pd.to_datetime(chunk["EXPIRY_DT"]).dt.date
        chunk["TIMESTAMP"] = pd.to_datetime(chunk["TIMESTAMP"]).dt.date
        chunk["OPTION_TYP"] = chunk["OPTION_TYP"].replace(r"^\s*$", None, regex=True)

        instrument_map = load_instruments(chunk, exchange_id)
        expiry_map = load_expiries(chunk, instrument_map)
        load_trades(chunk, instrument_map, expiry_map)

        session.commit()

    end_time = datetime.now()
    print("✅ ETL completed at:", end_time)
    print("⏱ Total duration:", end_time - start_time)


if __name__ == "__main__":
    main()
