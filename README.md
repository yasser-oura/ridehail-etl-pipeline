#  Ride-Hail ETL Pipeline

An end-to-end Python ETL pipeline that **extracts**, **cleans**, and **loads** messy ride-hailing CSV data into a normalized **PostgreSQL** database (Supabase).

Built to demonstrate real-world data engineering skills — handling dirty data, inconsistent formats, missing values, and referential integrity across multiple related datasets.

---


## Overview

This pipeline simulates a ride-hailing platform operating across **6 African cities** (Nairobi, Lagos, Casablanca, Dakar, Cairo, and Abidjan). It ingests three raw CSV files containing intentionally messy data, applies comprehensive cleaning and transformation logic, and loads the results into a normalized PostgreSQL schema hosted on Supabase.

---

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│   EXTRACT   │ ──▶ │    TRANSFORM     │ ──▶ │       LOAD          │
│             │     │                  │     │                     │
│ raw_drivers │     │ • Normalize IDs  │     │ ridehailing.cities  │
│ raw_rides   │     │ • Clean cities   │     │ ridehailing.drivers │
│ raw_payments│     │ • Fix formats    │     │ ridehailing.rides   │
│   (.csv)    │     │ • Validate data  │     │ ridehailing.payments│
│             │     │ • Enforce refs   │     │   (PostgreSQL)      │
└─────────────┘     └──────────────────┘     └─────────────────────┘
```

---

## Data Sources

| File | Records | Description |
|---|---|---|
| `raw_drivers.csv` | 40 drivers | Driver profiles — IDs, names, cities, vehicle types, ratings, phone numbers, status |
| `raw_rides.csv` | 250 rides | Ride records — timestamps, distances, fares, payment methods, statuses, ratings |
| `raw_payments.csv` | 135 payments | Payment transactions — amounts, tips, commissions, currencies, settlement statuses |

---

## Data Quality Issues Handled

The raw data contains **real-world messiness** that the pipeline resolves:

### Identifiers
- ✅ Missing IDs auto-generated (`DRV-XXXX`, `RIDE-XXXX`, `PAY-XXXX`)
- ✅ Inconsistent ID formats normalized (`DRV_1005` → `DRV-1005`, `drv-1010` → `DRV-1010`)
- ✅ Invalid references (`UNKNOWN`, `RIDE_UNKNOWN`, `N/A`) set to `NULL`

### City & Location Data
- ✅ Case normalization (`nairobi` → `Nairobi`, `NAIROBI` → `Nairobi`, `DAKAR` → `Dakar`)
- ✅ Whitespace trimming (` Lagos`, ` DAKAR `, ` Nairobi `)
- ✅ Invalid cities rejected

### Dates & Timestamps
- ✅ Mixed date formats parsed (`2025-06-06 09:15:00`, `18-07-2025 11:13`, `16/09/2024`, `25/05/2024`)
- ✅ Invalid dates coerced to `NULL`

### Numeric Fields
- ✅ Negative fares converted to absolute values (`-40.69` → `40.69`)
- ✅ Zero fares/distances set to `NULL`
- ✅ Out-of-range ratings (< 1.0 or > 5.0) set to `NULL`
- ✅ Sentinel values removed (`999` duration → `NULL`)
- ✅ Comma-separated numbers cleaned (`1,234` → `1234`)
- ✅ Invalid surge multipliers floored to `1.0`

### Categorical Fields
- ✅ Payment method variants unified (`M-Pesa`, `mpesa`, `momo`, `Orange Money` → `mobile_money`; `credit card`, `debit card` → `card`)
- ✅ Ride status variants unified (`cancel`, `canceled`, `CANCELLED` → `cancelled_by_rider`; `Completed` → `completed`)
- ✅ Vehicle type normalization (`moto`, `motor cycle` → `motorcycle`; `VAN` → `van`, `Car` → `car`)
- ✅ Driver status normalization and validation

### Phone Numbers
- ✅ Placeholder values removed (`N/A`, `unknown`, `none` → `NULL`)

### Payments
- ✅ Duplicate `payment_id` records deduplicated
- ✅ Negative amounts converted to absolute values
- ✅ Negative tips floored to `0`
- ✅ Invalid commission rates (< 0.01 or > 1.0) set to `NULL`
- ✅ Commission amounts recalculated from `amount × commission_rate`
- ✅ Missing currency codes derived from ride → city → currency mapping

### Referential Integrity
- ✅ `rides.driver_id` validated against existing drivers
- ✅ `payments.ride_id` validated against existing rides
- ✅ Orphaned foreign keys set to `NULL`

---

## Database Schema

The pipeline creates a **normalized schema** (`ridehailing`) with 4 tables, foreign keys, check constraints, and indexes:

```
ridehailing.cities (reference table)
├── city_name       VARCHAR(50)  PK
├── country_code    CHAR(2)      NOT NULL
└── currency_code   CHAR(3)      NOT NULL

ridehailing.drivers
├── driver_id       VARCHAR(10)  PK
├── driver_name     VARCHAR(100)
├── city_name       VARCHAR(50)  FK → cities
├── vehicle_type    VARCHAR(20)
├── rating          NUMERIC(2,1) CHECK 1.0–5.0
├── joined_date     DATE
├── phone           VARCHAR(20)
└── status          VARCHAR(15)  CHECK (active|inactive|suspended)

ridehailing.rides
├── ride_id            VARCHAR(15)  PK
├── driver_id          VARCHAR(10)  FK → drivers
├── city_name          VARCHAR(50)  FK → cities
├── requested_at       TIMESTAMP
├── duration_minutes   INTEGER      CHECK > 0
├── distance_km        NUMERIC(6,1) CHECK > 0
├── fare_amount        NUMERIC(10,2) CHECK > 0
├── surge_multiplier   NUMERIC(3,1) CHECK ≥ 1.0
├── payment_method     VARCHAR(20)
├── ride_status        VARCHAR(25)
└── rider_rating       NUMERIC(2,1) CHECK 1–5

ridehailing.payments
├── payment_id       VARCHAR(15)   PK
├── ride_id          VARCHAR(15)   FK → rides
├── amount           NUMERIC(10,2) CHECK > 0
├── tip              NUMERIC(10,2) DEFAULT 0
├── commission_rate  NUMERIC(4,3)  CHECK 0.01–1.0
├── commission_amount NUMERIC(10,2)
├── driver_payout    NUMERIC(10,2)
├── payment_status   VARCHAR(15)
├── paid_at          TIMESTAMP
└── currency         CHAR(3)
```

**Indexes** are created on `rides.driver_id`, `rides.city_name`, `rides.requested_at`, `payments.ride_id`, and `payments.payment_status` for query performance.

---



## Getting Started

### Prerequisites

- Python 3.8+
- A PostgreSQL database (e.g., [Supabase](https://supabase.com/) free tier)

### 1. Clone the repository

```bash
git clone https://github.com/yasser-oura/ridehail-etl-pipeline.git
cd ridehail-etl-pipeline
```

### 2. Install dependencies

```bash
pip install pandas psycopg2-binary python-dotenv numpy
```

### 3. Configure environment variables

Create a `.env` file in the project root:

```env
DB_HOST=your-supabase-host.supabase.co
DB_PORT=5432
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=your-password
```

### 4. Run the pipeline

```bash
python pipeline.py
```

## How It Works

The pipeline runs in **4 sequential stages**:

### 1. Extract
Reads all three CSV files into pandas DataFrames and logs row/column counts.

### 2. Transform
Each dataset goes through its own cleaning function:

- **`transform_drivers()`** — Normalizes cities, vehicle types, ratings, phone numbers, dates, and statuses.
- **`transform_rides()`** — Cleans driver references, fares, distances, durations, payment methods, ride statuses, and surge multipliers.
- **`transform_payments()`** — Deduplicates records, cleans amounts/tips, validates commission rates, recalculates commissions, derives missing currencies from city mapping, and normalizes statuses.

Cross-table referential integrity is enforced after individual transforms (orphaned foreign keys → `NULL`).

### 3. Load
- Drops and recreates the `ridehailing` schema (idempotent runs)
- Creates tables with proper constraints, foreign keys, and indexes
- Bulk inserts data using `psycopg2.extras.execute_values` with `ON CONFLICT DO NOTHING`

### 4. Validate
Queries row counts from all tables to confirm data was loaded successfully.

---

