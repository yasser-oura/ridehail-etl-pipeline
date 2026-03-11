import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime
import numpy as np

#CONFIG
load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

VALID_CITIES = ["Nairobi", "Lagos", "Casablanca", "Dakar", "Cairo", "Abidjan"]
VALID_STATUSES = ["active", "inactive", "suspended"]

CITY_MAPPING = {
    "Nairobi":    {"country_code": "KE", "currency_code": "KES"},
    "Lagos":      {"country_code": "NG", "currency_code": "NGN"},
    "Casablanca": {"country_code": "MA", "currency_code": "MAD"},
    "Dakar":      {"country_code": "SN", "currency_code": "XOF"},
    "Cairo":      {"country_code": "EG", "currency_code": "EGP"},
    "Abidjan":    {"country_code": "CI", "currency_code": "XOF"},
}


#EXTRACT
def extract():
    drivers_df = pd.read_csv("raw_drivers.csv")
    rides_df = pd.read_csv("raw_rides.csv")
    payments_df = pd.read_csv("raw_payments.csv")

    print(f"  drivers: {drivers_df.shape[0]} rows, {drivers_df.shape[1]} columns")
    print(f"  rides: {rides_df.shape[0]} rows, {rides_df.shape[1]} columns")
    print(f"  payments: {payments_df.shape[0]} rows, {payments_df.shape[1]} columns")

    return drivers_df, rides_df, payments_df

#TRANSFORM
# #helper function since same cleaning patern is applied to all 3 dataframes
def normalize_city(city):
    if pd.isna(city):
        return None
    city = str(city).strip().title()
    if city in VALID_CITIES:
        return city
    else:
        return None

def generate_id(series, prefix):
    cleaned_series = series.copy()
    cleaned_series = cleaned_series.replace(r'^\s*$', np.nan, regex=True)

    existing_nums = pd.to_numeric(
        cleaned_series.astype(str).str.replace(f"{prefix}-", "", regex=False), 
        errors='coerce'
    )
    last_id = existing_nums.max()
    start_num = int(last_id) + 1 if pd.notna(last_id) else 1
    missing_count = cleaned_series.isna().sum()

    if missing_count > 0:
        new_ids = [f"{prefix}-{str(n).zfill(4)}" for n in range(start_num, start_num + missing_count)]
        missing_indexes = cleaned_series[cleaned_series.isna()].index
        fill_series = pd.Series(new_ids, index=missing_indexes)
        cleaned_series = cleaned_series.fillna(fill_series)
    return cleaned_series

#transform drivers 
def transform_drivers(drivers_df):
    print("Cleaning DRIVERS...")
    df = drivers_df.copy()
    original_count = len(df)
    df = df.rename(columns={"city": "city_name"})

#fill missing id 
    df["driver_id"]=generate_id(df["driver_id"],"DRV")
#normalize city names
    df["city_name"]=df["city_name"].apply(normalize_city)
#normalize vehicle type
    df["vehicle_type"] = df["vehicle_type"].astype(str).str.strip().str.lower()
    vehicle_fixes = {
        "moto": "motorcycle",
        "motor cycle": "motorcycle"
    }
    df["vehicle_type"] = df["vehicle_type"].replace(vehicle_fixes)
    valid_vehicles = ["motorcycle", "car", "van", "bicycle"]
    df.loc[~df["vehicle_type"].isin(valid_vehicles), "vehicle_type"] = np.nan
#clean rating
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    out_of_range=((df["rating"] < 1.0) | (df["rating"] > 5.0)).sum()
    df.loc[(df["rating"] < 1.0) | (df["rating"] > 5.0), "rating"] = None
#clean phone numbers
    phone_placeholders = ["n/a","unknown","none","nan", ""]
    df["phone"] = df["phone"].astype(str).str.strip()
    df.loc[df["phone"].str.lower().isin(phone_placeholders), "phone"] = None
#date formatting
    df["joined_date"] = pd.to_datetime(df["joined_date"], format="mixed", errors="coerce")
    #clean status
    df["status"] = df["status"].astype(str).str.strip().str.lower()
    invalid_status = ~df["status"].isin(VALID_STATUSES)
    df.loc[invalid_status, "status"] = None

    print(f"  DONE: {original_count} rows → {len(df)} rows (cleaned,none dropped)")
    return df

#transform rides
def transform_rides(rides_df):
    print("Cleaning RIDES...")
    original_count = len(rides_df)
    df = rides_df.copy()
    df = df.rename(columns={"city": "city_name"})

    # fill missing id
    df["ride_id"] = generate_id(df["ride_id"], "RIDE")

    # clean driver_id
    df["driver_id"] = df["driver_id"].astype(str).str.upper().str.replace(" ", "").str.replace("_", "-")
    invalid_ids = ["UNKNOWN", "NAN", "NONE", "N/A", ""]
    df.loc[df["driver_id"].isin(invalid_ids), "driver_id"] = np.nan
    valid_mask = df["driver_id"].notna()
    nums_only = df.loc[valid_mask, "driver_id"].str.replace("DRV", "").str.replace("-", "")
    df.loc[valid_mask, "driver_id"] = "DRV-" + nums_only.str.zfill(4)
    # normalize city names
    df["city_name"] = df["city_name"].apply(normalize_city)
    # date formatting
    df["requested_at"] = pd.to_datetime(df["requested_at"], format="mixed", errors="coerce")
    # clean fare_amount
    df["fare_amount"] = df["fare_amount"].astype(str).str.replace(",", "", regex=False)
    df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce").abs()
    df.loc[df["fare_amount"] == 0, "fare_amount"] = np.nan
    # clean distance_km
    df["distance_km"] = pd.to_numeric(df["distance_km"], errors="coerce")
    df.loc[df["distance_km"] <= 0, "distance_km"] = np.nan
    # clean duration_minutes
    df["duration_minutes"] = pd.to_numeric(df["duration_minutes"], errors="coerce")
    df.loc[(df["duration_minutes"] <= 0) | (df["duration_minutes"] == 999), "duration_minutes"] = np.nan
    # normalize payment_method
    df["payment_method"] = df["payment_method"].astype(str).str.strip().str.lower()
    df["ride_status"] = df["ride_status"].astype(str).str.strip().str.lower()
    payment_fixes = {
        "m-pesa": "mobile_money", "mpesa": "mobile_money", "momo": "mobile_money", 
        "mobile money": "mobile_money", "orange money": "mobile_money",
        "credit card": "card", "debit card": "card"
    }
    status_fixes = {
        "cancel": "cancelled_by_rider", "canceled": "cancelled_by_rider", 
        "cancelled": "cancelled_by_rider", "in progress": "in_progress"
    }
    df["payment_method"] = df["payment_method"].replace(payment_fixes)
    df["ride_status"] = df["ride_status"].replace(status_fixes)
    valid_payments = ["mobile_money", "cash", "card", "wallet"]
    valid_statuses = ["completed", "cancelled_by_rider", "cancelled_by_driver", "in_progress", "requested", "no_show"]
    df.loc[~df["payment_method"].isin(valid_payments), "payment_method"] = np.nan
    df.loc[~df["ride_status"].isin(valid_statuses), "ride_status"] = np.nan
    # clean surge_multiplier
    df["surge_multiplier"] = pd.to_numeric(df["surge_multiplier"], errors="coerce")
    df.loc[df["surge_multiplier"] < 1.0, "surge_multiplier"] = 1.0
    df["surge_multiplier"] = df["surge_multiplier"].fillna(1.0)

    # clean rider_rating
    df["rider_rating"] = pd.to_numeric(df["rider_rating"], errors="coerce")
    df.loc[(df["rider_rating"] < 1) | (df["rider_rating"] > 5), "rider_rating"] = None

    print(f"    DONE: {original_count} rows → {len(df)} rows (cleaned)")
    return df
#transform payments
def transform_payments(payments_df, rides_df):
    print("Cleaning PAYMENTS...")
    df = payments_df.copy()
    original_count = len(df)
    # deduplicate
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["payment_id"], keep="first")
    dupes_removed = before_dedup - len(df)
    # fill missing payment IDs
    df["payment_id"] = generate_id(df["payment_id"], "PAY")
    # clean ride_id
    df["ride_id"] = df["ride_id"].astype(str).str.upper().str.strip()
    invalid_rides = ["RIDE_UNKNOWN", "N/A", "NONE", "NAN", ""]
    df.loc[df["ride_id"].isin(invalid_rides), "ride_id"] = np.nan
    valid_mask = df["ride_id"].notna()
    nums_only = df.loc[valid_mask, "ride_id"].str.replace("RIDE", "").str.replace("-", "")
    df.loc[valid_mask, "ride_id"] = "RIDE-" + nums_only.str.zfill(4)
    # clean amount
    df["amount"] = df["amount"].astype(str).str.replace(",", "", regex=False)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").abs()
    df.loc[df["amount"] == 0, "amount"] = np.nan
    # clean tip
    df["tip"] = pd.to_numeric(df["tip"], errors="coerce")
    df.loc[df["tip"] < 0, "tip"] = 0
    df["tip"] = df["tip"].fillna(0)
    # validate commission_rate
    df["commission_rate"] = pd.to_numeric(df["commission_rate"], errors="coerce")
    invalid_comm = (df["commission_rate"] < 0.01) | (df["commission_rate"] > 1.0)
    df.loc[invalid_comm, "commission_rate"] = None
    # recalculate commission_amount
    df["commission_amount"] = (df["amount"] * df["commission_rate"]).round(2)
    # clean driver_payout
    df["driver_payout"] = pd.to_numeric(df["driver_payout"], errors="coerce").abs()
    # date formatting
    df["paid_at"] = pd.to_datetime(df["paid_at"], format="mixed", errors="coerce")
    # normalize & derive currency
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    invalid_currencies = ["N/A", "NAN", "NONE", "", "UNKNOWN"]
    df.loc[df["currency"].isin(invalid_currencies), "currency"] = None

    city_currency_map = {city: info["currency_code"] for city, info in CITY_MAPPING.items()}
    valid_currencies = list(city_currency_map.values())
    df.loc[~df["currency"].isin(valid_currencies), "currency"] = np.nan

    ride_to_city = rides_df.dropna(subset=["ride_id", "city_name"]).set_index("ride_id")["city_name"]
    ride_to_currency = ride_to_city.map(city_currency_map)

    before_derive = df["currency"].isna().sum()
    df["currency"] = df["currency"].fillna(df["ride_id"].map(ride_to_currency))
    after_derive = df["currency"].isna().sum()
    # normalize payment_status
    df["payment_status"] = df["payment_status"].astype(str).str.strip().str.lower()

    print(f"DONE: {original_count} → {len(df)} rows (cleaned & deduped)")
    return df
# the main transform function that return cleaned dataframes
def transform(drivers_df, rides_df, payments_df):
    clean_drivers = transform_drivers(drivers_df)
    clean_rides = transform_rides(rides_df)
    clean_payments = transform_payments(payments_df, clean_rides)

    clean_rides.loc[~clean_rides["driver_id"].isin(clean_drivers["driver_id"]), "driver_id"] = np.nan
    clean_payments.loc[~clean_payments["ride_id"].isin(clean_rides["ride_id"]), "ride_id"] = np.nan

    return clean_drivers, clean_rides, clean_payments
#build city reference
def build_cities():
    cities = pd.DataFrame([
        {"city_name": "Nairobi","country_code": "KE", "currency_code": "KES"},
        {"city_name": "Lagos","country_code": "NG", "currency_code": "NGN"},
        {"city_name": "Casablanca", "country_code": "MA", "currency_code": "MAD"},
        {"city_name": "Dakar",   "country_code": "SN", "currency_code": "XOF"},
        {"city_name": "Cairo",   "country_code": "EG", "currency_code": "EGP"},
        {"city_name": "Abidjan",  "country_code": "CI", "currency_code": "XOF"},
    ])
    print(f"  built cities reference: {len(cities)} cities")
    return cities



#load part
#schema creation

def create_schema(conn):
    cur=conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS ridehailing CASCADE;")
    cur.execute("CREATE SCHEMA ridehailing;")
    print("  created ridehailing schema")

    cur.execute("""
        CREATE TABLE ridehailing.cities (
            city_name VARCHAR(50) PRIMARY KEY,
            country_code CHAR(2) NOT NULL,
            currency_code CHAR(3) NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE ridehailing.drivers (
            driver_id VARCHAR(10) PRIMARY KEY,
            driver_name VARCHAR(100),
            city_name VARCHAR(50) REFERENCES ridehailing.cities(city_name),
            vehicle_type VARCHAR(20),
            rating NUMERIC(2,1) CHECK (rating BETWEEN 1.0 AND 5.0),
            joined_date DATE,
            phone VARCHAR(20),
            status VARCHAR(15) CHECK (status IN ('active', 'inactive', 'suspended'))
        );
    """)

    cur.execute("""
        CREATE TABLE ridehailing.rides (
            ride_id VARCHAR(15) PRIMARY KEY,
            driver_id VARCHAR(10) REFERENCES ridehailing.drivers(driver_id),
            city_name VARCHAR(50) REFERENCES ridehailing.cities(city_name),
            requested_at TIMESTAMP,
            duration_minutes INTEGER CHECK (duration_minutes > 0),
            distance_km NUMERIC(6,1) CHECK (distance_km > 0),
            fare_amount NUMERIC(10,2) CHECK (fare_amount > 0),
            surge_multiplier NUMERIC(3,1) CHECK (surge_multiplier >= 1.0),
            payment_method VARCHAR(20),
            ride_status VARCHAR(25),
            rider_rating NUMERIC(2,1) CHECK (rider_rating BETWEEN 1 AND 5)
        );
    """)

    cur.execute("""
        CREATE TABLE ridehailing.payments (
            payment_id VARCHAR(15) PRIMARY KEY,
            ride_id VARCHAR(15) REFERENCES ridehailing.rides(ride_id),
            amount NUMERIC(10,2) CHECK (amount > 0),
            tip NUMERIC(10,2) DEFAULT 0,
            commission_rate NUMERIC(4,3) CHECK (commission_rate BETWEEN 0.01 AND 1.0),
            commission_amount NUMERIC(10,2),
            driver_payout NUMERIC(10,2),
            payment_status VARCHAR(15),
            paid_at TIMESTAMP,
            currency CHAR(3)
        );
    """)

    cur.execute("CREATE INDEX idx_rides_driver ON ridehailing.rides(driver_id);")
    cur.execute("CREATE INDEX idx_rides_city ON ridehailing.rides(city_name);")
    cur.execute("CREATE INDEX idx_rides_date ON ridehailing.rides(requested_at);")
    cur.execute("CREATE INDEX idx_payments_ride ON ridehailing.payments(ride_id);")
    cur.execute("CREATE INDEX idx_payments_status ON ridehailing.payments(payment_status);")

    cur.close()          
    conn.commit()       
    print("  created all tables and indexes")

#LOAD
def load_data(conn, cities_df, drivers_df, rides_df, payments_df):
    """Bulk insert cleaned data into the database tables."""
    cur = conn.cursor()

    cities_df = cities_df.replace({np.nan: None, pd.NaT: None})
    drivers_df = drivers_df.replace({np.nan: None, pd.NaT: None})
    rides_df = rides_df.replace({np.nan: None, pd.NaT: None})
    payments_df = payments_df.replace({np.nan: None, pd.NaT: None})

    def bulk_insert(table_name, df, columns):
        if df.empty:
            return
        cols_str = ", ".join(columns)
        query = f"INSERT INTO ridehailing.{table_name} ({cols_str}) VALUES %s ON CONFLICT DO NOTHING"
        
        values = [tuple(row) for row in df[columns].to_numpy()]
        
        execute_values(cur, query, values)
        print(f"  loaded {len(df)} rows into {table_name}")

    # Load Cities
    bulk_insert("cities", cities_df, ["city_name", "country_code", "currency_code"])
    # Load Drivers
    bulk_insert("drivers", drivers_df, [
        "driver_id", "driver_name", "city_name", "vehicle_type", 
        "rating", "joined_date", "phone", "status"
    ])

    # Load Rides
    bulk_insert("rides", rides_df, [
        "ride_id", "driver_id", "city_name", "requested_at", "duration_minutes",
        "distance_km", "fare_amount", "surge_multiplier", "payment_method",
        "ride_status", "rider_rating"
    ])

    # Load Payments
    bulk_insert("payments", payments_df, [
        "payment_id", "ride_id", "amount", "tip", "commission_rate",
        "commission_amount", "driver_payout", "payment_status", "paid_at", "currency"
    ])
    cur.close()
    conn.commit()
    print("All data loaded successfully")
#validate 
def validate(conn):
    cur=conn.cursor()
    cur.execute("SELECT COUNT(*) FROM ridehailing.cities;")
    cities_count=cur.fetchone()[0]
    print(f"  VALIDATION: cities count = {cities_count}")

    cur.execute("SELECT COUNT(*) FROM ridehailing.drivers;")
    drivers_count=cur.fetchone()[0]
    print(f"  VALIDATION: drivers count = {drivers_count}")

    cur.execute("SELECT COUNT(*) FROM ridehailing.rides;")
    rides_count=cur.fetchone()[0]
    print(f"  VALIDATION: rides count = {rides_count}")

    cur.execute("SELECT COUNT(*) FROM ridehailing.payments;")
    payments_count=cur.fetchone()[0]
    print(f"  VALIDATION: payments count = {payments_count}")
#main function to run the whole pipeline
def main():
    #extract 
    drivers_df, rides_df, payments_df = extract()
    #transform
    cities_df = build_cities()
    drivers_df, rides_df, payments_df = transform(drivers_df, rides_df, payments_df)
    #load and validate
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        print("connection established ")
        create_schema(conn)
        load_data(conn, cities_df, drivers_df, rides_df, payments_df)
        validate(conn)
        print("pipeline created")
    except psycopg2.Error as e:
        print(f" DATABASE ERROR: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"ERROR: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            print("connection closed")
if __name__ == "__main__":
    main()