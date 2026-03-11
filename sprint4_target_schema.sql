-- ============================================================
-- Sprint 4 — Target Schema (for reference only)
-- Your pipeline should CREATE these tables and LOAD clean data
-- ============================================================

-- Run this in Supabase ONLY if you want to pre-create the tables.
-- Or let your Python script create them.

CREATE SCHEMA IF NOT EXISTS ridehailing;

CREATE TABLE ridehailing.cities (
    city_name VARCHAR(50) PRIMARY KEY,
    country_code CHAR(2) NOT NULL,
    currency_code CHAR(3) NOT NULL
);

-- Reference data your pipeline should insert:
-- Nairobi  → KE, KES
-- Lagos    → NG, NGN
-- Casablanca → MA, MAD
-- Dakar    → SN, XOF
-- Cairo    → EG, EGP
-- Abidjan  → CI, XOF

CREATE TABLE ridehailing.drivers (
    driver_id VARCHAR(10) PRIMARY KEY,
    driver_name VARCHAR(100) NOT NULL,
    city_name VARCHAR(50) REFERENCES ridehailing.cities(city_name),
    vehicle_type VARCHAR(20) NOT NULL,  -- motorcycle, car, van, bicycle
    rating NUMERIC(2,1) CHECK (rating BETWEEN 1.0 AND 5.0),
    joined_date DATE NOT NULL,
    phone VARCHAR(20),
    status VARCHAR(15) NOT NULL CHECK (status IN ('active', 'inactive', 'suspended'))
);

CREATE TABLE ridehailing.rides (
    ride_id VARCHAR(15) PRIMARY KEY,
    driver_id VARCHAR(10) REFERENCES ridehailing.drivers(driver_id),
    city_name VARCHAR(50) REFERENCES ridehailing.cities(city_name),
    requested_at TIMESTAMP NOT NULL,
    duration_minutes INTEGER CHECK (duration_minutes > 0),
    distance_km NUMERIC(6,1) CHECK (distance_km > 0),
    fare_amount NUMERIC(10,2) CHECK (fare_amount > 0),
    surge_multiplier NUMERIC(3,1) CHECK (surge_multiplier >= 1.0),
    payment_method VARCHAR(20),  -- cash, mobile_money, card, wallet
    ride_status VARCHAR(25) NOT NULL,  -- completed, cancelled_by_rider, cancelled_by_driver, no_show
    rider_rating INTEGER CHECK (rider_rating BETWEEN 1 AND 5)
);

CREATE TABLE ridehailing.payments (
    payment_id VARCHAR(15) PRIMARY KEY,
    ride_id VARCHAR(15) REFERENCES ridehailing.rides(ride_id),
    amount NUMERIC(10,2) CHECK (amount > 0),
    tip NUMERIC(10,2) DEFAULT 0,
    commission_rate NUMERIC(4,2) CHECK (commission_rate BETWEEN 0.01 AND 1.0),
    commission_amount NUMERIC(10,2),
    driver_payout NUMERIC(10,2) CHECK (driver_payout > 0),
    payment_status VARCHAR(15) NOT NULL CHECK (payment_status IN ('settled', 'pending', 'failed', 'refunded')),
    paid_at TIMESTAMP NOT NULL,
    currency CHAR(3) NOT NULL
);

-- Indexes
CREATE INDEX idx_rides_driver ON ridehailing.rides(driver_id);
CREATE INDEX idx_rides_city ON ridehailing.rides(city_name);
CREATE INDEX idx_rides_date ON ridehailing.rides(requested_at);
CREATE INDEX idx_payments_ride ON ridehailing.payments(ride_id);
CREATE INDEX idx_payments_status ON ridehailing.payments(payment_status);
