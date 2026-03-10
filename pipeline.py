import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
import numpy as np
# ─── Config ───────────────────────────────────────────────────────────────────
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


# ─── Extract ──────────────────────────────────────────────────────────────────
def extract():
    drivers_df = pd.read_csv("raw_drivers.csv")
    rides_df = pd.read_csv("raw_rides.csv")
    payments_df = pd.read_csv("raw_payments.csv")

    print(f"  drivers: {drivers_df.shape[0]} rows, {drivers_df.shape[1]} columns")
    print(f"  rides: {rides_df.shape[0]} rows, {rides_df.shape[1]} columns")
    print(f"  payments: {payments_df.shape[0]} rows, {payments_df.shape[1]} columns")

    return drivers_df, rides_df, payments_df

# ─── Transform ───────────────────────────────────────────────────────────────
#helper function since same cleaning patern is applied to all 3 dataframes
def normalize_city(city):
    if pd.isna(city):
        return None
    city = str(city).strip().title()
    if city in VALID_CITIES:
        return city
    return None


def flexible_date(date_value):
    if pd.isna(date_value):
        return None
    date_str = str(date_value).strip()
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%d-%m-%Y %H:%M:%S",
        "%d-%m-%Y %H:%M",
        "%d-%m-%Y",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    try:
        return pd.to_datetime(date_str)
    except Exception:
        return None

def clean_numeric(value,must_be_positive=True,remove_commas=True):
    if pd.isna(value):
        return None
    s=str(value).strip()
    if remove_commas:
        s=s.replace(",","")
    try:
        num=float(s)
    except ValueError:
        return None
    if must_be_positive and num<=0:
        num=abs(num)
    if num==0:
        return None 
    return num

def generate_id(series,prefix):
# find the highest existing number for the prefix (to avoid duplicates)
    existing_nums = []
    for val in series.dropna():
        val_str=str(val).strip().upper()
        if val_str.startswith(prefix.upper()+"-"):
            try:
                num = int(val_str.split("-")[1])
                existing_nums.append(num)
            except (ValueError, IndexError):
                pass
    
    next_num=max(existing_nums) + 1 if existing_nums else 1
    
#fill the missing value 
    new_series = series.copy()
    for i in range(len(new_series)):
        if pd.isna(new_series.iloc[i]) or str(new_series.iloc[i]).strip() == "":
            new_series.iloc[i] = f"{prefix}-{next_num:04d}"
            next_num += 1
    return new_series  

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
    vehicle_map = {
        "moto": "motorcycle",
        "motor cycle": "motorcycle",
        "motorcycle": "motorcycle",
        "car": "car",
        "van": "van",
        "bicycle": "bicycle",}
    df["vehicle_type"] = (
        df["vehicle_type"]
        .astype(str)        
        .str.strip()        
        .str.lower()        
        .map(vehicle_map) 
    )
#clean rating
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    out_of_range=((df["rating"] < 1.0) | (df["rating"] > 5.0)).sum()
    df.loc[(df["rating"] < 1.0) | (df["rating"] > 5.0), "rating"] = None
#clean phone numbers
    phone_placeholders = ["n/a","unknown","none","nan", ""]
    df["phone"] = df["phone"].astype(str).str.strip()
    df.loc[df["phone"].str.lower().isin(phone_placeholders), "phone"] = None
#date formatting
    df["joined_date"]=df["joined_date"].apply(flexible_date)
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
    def clean_driver_id(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s == "" or s.upper() == "UNKNOWN":
            return None
        s = s.upper().replace(" ", "")
        s = s.replace("_", "-")
        if s.startswith("DRV") and "-" not in s:
            s = "DRV-" + s[3:]
        if s.startswith("DRV-"):
            try:
                num = int(s.split("-")[1])
                return f"DRV-{num:04d}"
            except (ValueError, IndexError):
                return None
        return None

    df["driver_id"] = df["driver_id"].apply(clean_driver_id)

    # normalize city names
    df["city_name"] = df["city_name"].apply(normalize_city)

    # parse dates
    df["requested_at"] = df["requested_at"].apply(flexible_date)

    # clean fare_amount
    df["fare_amount"] = df["fare_amount"].apply(
        lambda x: clean_numeric(x, must_be_positive=True, remove_commas=True)
    )

    # clean distance_km
    def clean_distance(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s.upper() == "N/A" or s == "":
            return None
        try:
            num = float(s)
        except ValueError:
            return None
        if num <= 0:
            return None
        return num

    df["distance_km"] = df["distance_km"].apply(clean_distance)

    # clean duration_minutes
    def clean_duration(val):
        if pd.isna(val):
            return None
        try:
            num = int(float(str(val).strip()))
        except (ValueError, TypeError):
            return None
        if num <= 0 or num == 999:
            return None
        return num

    df["duration_minutes"] = df["duration_minutes"].apply(clean_duration)

    # normalize payment_method
    payment_map = {
        "m-pesa": "mobile_money",
        "mpesa": "mobile_money",
        "momo": "mobile_money",
        "mobile money": "mobile_money",
        "mobile_money": "mobile_money",
        "orange money": "mobile_money",
        "cash": "cash",
        "card": "card",
        "credit card": "card",
        "debit card": "card",
        "wallet": "wallet",
    }
    df["payment_method"] = (
        df["payment_method"]
        .astype(str).str.strip().str.lower()
        .map(payment_map)
    )

    # normalize ride_status
    status_map = {
        "completed": "completed",
        "cancelled": "cancelled_by_rider",
        "cancelled_by_rider": "cancelled_by_rider",
        "cancel": "cancelled_by_rider",
        "canceled": "cancelled_by_rider",
        "cancelled_by_driver": "cancelled_by_driver",
        "in_progress": "in_progress",
        "in progress": "in_progress",
        "requested": "requested",
        "no_show": "no_show",
    }
    df["ride_status"] = (
        df["ride_status"]
        .astype(str).str.strip().str.lower()
        .map(status_map)
    )

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
    invalid_ride_ids = ["ride_unknown", "n/a", "none", "nan", ""]

    def validate_ride_id(val):
        if pd.isna(val):
            return None
        s = str(val).strip()
        if s.lower() in invalid_ride_ids:
            return None
        s_upper = s.upper()
        if s_upper.startswith("RIDE-"):
            try:
                num = int(s_upper.split("-")[1])
                return f"RIDE-{num:04d}"
            except (ValueError, IndexError):
                return None
        return None

    df["ride_id"] = df["ride_id"].apply(validate_ride_id)

    # clean amount
    df["amount"] = df["amount"].apply(
        lambda x: clean_numeric(x, must_be_positive=True, remove_commas=True)
    )

    # clean tip
    df["tip"] = pd.to_numeric(df["tip"], errors="coerce")
    df.loc[df["tip"] < 0, "tip"] = 0
    df["tip"] = df["tip"].fillna(0)

    # validate commission_rate
    df["commission_rate"] = pd.to_numeric(df["commission_rate"], errors="coerce")
    invalid_comm = (df["commission_rate"] < 0.01) | (df["commission_rate"] > 1.0)
    df.loc[invalid_comm, "commission_rate"] = None

    # recalculate commission_amount
    df["commission_amount"] = df.apply(
        lambda row: round(row["amount"] * row["commission_rate"], 2)
        if pd.notna(row["amount"]) and pd.notna(row["commission_rate"])
        else None,
        axis=1,
    )

    # clean driver_payout
    df["driver_payout"] = pd.to_numeric(df["driver_payout"], errors="coerce").abs()

    # parse paid_at dates
    df["paid_at"] = df["paid_at"].apply(flexible_date)

    # normalize & derive currency
    df["currency"] = df["currency"].astype(str).str.strip().str.upper()
    invalid_currencies = ["N/A", "NAN", "NONE", "", "UNKNOWN"]
    df.loc[df["currency"].isin(invalid_currencies), "currency"] = None

    ride_city_map = (
        rides_df
        .dropna(subset=["ride_id", "city_name"])
        .set_index("ride_id")["city_name"]
        .to_dict()
    )
    city_currency_map = {
        city: info["currency_code"]
        for city, info in CITY_MAPPING.items()
    }
    valid_currencies = set(city_currency_map.values())

    def derive_currency(row):
        if pd.notna(row["currency"]) and row["currency"] in valid_currencies:
            return row["currency"]
        ride_id = row.get("ride_id")
        if ride_id and ride_id in ride_city_map:
            city = ride_city_map[ride_id]
            if city in city_currency_map:
                return city_currency_map[city]
        return None

    before_derive = df["currency"].isna().sum()
    df["currency"] = df.apply(derive_currency, axis=1)
    after_derive = df["currency"].isna().sum()
    print(f"    Currency: derived {before_derive - after_derive} missing values from ride→city")

    # normalize payment_status
    df["payment_status"] = df["payment_status"].astype(str).str.strip().str.lower()

    print(f"DONE: {original_count} → {len(df)} rows (cleaned & deduped)")
    return df


#the main transform function that return cleaned dataframes
def transform(drivers_df, rides_df, payments_df):
    clean_drivers = transform_drivers(drivers_df)
    clean_rides = transform_rides(rides_df)
    #rides.driver_id must exist in drivers
    valid_driver_ids = set(clean_drivers["driver_id"].dropna().unique())
    orphan_mask = clean_rides["driver_id"].notna() & ~clean_rides["driver_id"].isin(valid_driver_ids)
    orphan_count = orphan_mask.sum()
    clean_rides.loc[orphan_mask, "driver_id"] = None

    clean_payments = transform_payments(payments_df, clean_rides)

    # payments.ride_id must exist in rides
    valid_ride_ids = set(clean_rides["ride_id"].dropna().unique())
    orphan_mask = clean_payments["ride_id"].notna() & ~clean_payments["ride_id"].isin(valid_ride_ids)
    orphan_count = orphan_mask.sum()
    clean_payments.loc[orphan_mask, "ride_id"] = None
    print(f"FK cleanup: {orphan_count} payment ride_ids not in rides → NULL")

    return clean_drivers, clean_rides, clean_payments


#build city ref 
def build_cities():
    cities = pd.DataFrame([
        {"city_name": "Nairobi",    "country_code": "KE", "currency_code": "KES"},
        {"city_name": "Lagos",      "country_code": "NG", "currency_code": "NGN"},
        {"city_name": "Casablanca", "country_code": "MA", "currency_code": "MAD"},
        {"city_name": "Dakar",      "country_code": "SN", "currency_code": "XOF"},
        {"city_name": "Cairo",      "country_code": "EG", "currency_code": "EGP"},
        {"city_name": "Abidjan",    "country_code": "CI", "currency_code": "XOF"},
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


# ─── Load: Insert Data ───────────────────────────────────────────────────────
def load_data(conn, cities_df, drivers_df, rides_df, payments_df):
    """Insert cleaned data into the database tables."""
    cur = conn.cursor()

    # NaN → None conversion right before inserting
    cities_df = cities_df.replace({np.nan: None})
    drivers_df = drivers_df.replace({np.nan: None})
    rides_df = rides_df.replace({np.nan: None})
    payments_df = payments_df.replace({np.nan: None})

    # Cities
    for i, row in cities_df.iterrows():
        cur.execute("""
            INSERT INTO ridehailing.cities (city_name, country_code, currency_code)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (row["city_name"], row["country_code"], row["currency_code"]))
    print(f"  loaded {len(cities_df)} cities")

    # Drivers
    for i, row in drivers_df.iterrows():
        cur.execute("""
            INSERT INTO ridehailing.drivers
            (driver_id, driver_name, city_name, vehicle_type, rating,
             joined_date, phone, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row["driver_id"], row["driver_name"], row["city_name"],
            row["vehicle_type"], row["rating"], row["joined_date"],
            row["phone"], row["status"]
        ))
    print(f"  loaded {len(drivers_df)} drivers")

    # Rides
    for i, row in rides_df.iterrows():
        cur.execute("""
            INSERT INTO ridehailing.rides
            (ride_id, driver_id, city_name, requested_at, duration_minutes,
             distance_km, fare_amount, surge_multiplier, payment_method,
             ride_status, rider_rating)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row["ride_id"], row["driver_id"], row["city_name"],
            row["requested_at"], row["duration_minutes"],
            row["distance_km"], row["fare_amount"],
            row["surge_multiplier"], row["payment_method"],
            row["ride_status"], row["rider_rating"]
        ))
    print(f"  loaded {len(rides_df)} rides")

    # Payments
    for i, row in payments_df.iterrows():
        cur.execute("""
            INSERT INTO ridehailing.payments
            (payment_id, ride_id, amount, tip, commission_rate,
             commission_amount, driver_payout, payment_status, paid_at, currency)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            row["payment_id"], row["ride_id"], row["amount"],
            row["tip"], row["commission_rate"],
            row["commission_amount"], row["driver_payout"],
            row["payment_status"], row["paid_at"], row["currency"]
        ))
    print(f"  loaded {len(payments_df)} payments")

    cur.close()
    conn.commit()
    print("all data loaded successfully")


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


