#!/usr/bin/env python3

import os
import psycopg2

# Replace these environment variables with your actual RDS connection info
DB_HOST = os.getenv("DB_HOST", "YOUR_RDS_ENDPOINT")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "SECRET_PASSWORD")

def get_connection():
    """
    Creates and returns a new psycopg2 connection to your Postgres RDS instance.
    """
    return psycopg2.connect(
        host=DB_HOST,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def create_sellers_table():
    """
    Ensures the 'sellers' table (for SP-API refresh tokens) exists.
    Adjust columns as needed for your project.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sellers (
            id SERIAL PRIMARY KEY,
            selling_partner_id VARCHAR(50) UNIQUE,
            refresh_token TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def create_ads_tokens_table():
    """
    Ensures the 'ads_tokens' table (for Amazon Ads refresh tokens) exists.
    Adjust columns as needed (e.g., if you don't want advertiser_id UNIQUE).
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS ads_tokens (
            id SERIAL PRIMARY KEY,
            advertiser_id VARCHAR(50) UNIQUE,
            refresh_token TEXT,
            created_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def store_seller_refresh_token(selling_partner_id, refresh_token):
    """
    Inserts or updates the SP-API refresh token for a given seller.
    Uses a Postgres 'upsert' approach if 'selling_partner_id' is UNIQUE.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = """
    INSERT INTO sellers (selling_partner_id, refresh_token)
    VALUES (%s, %s)
    ON CONFLICT (selling_partner_id)
    DO UPDATE SET refresh_token = EXCLUDED.refresh_token,
                  updated_at = NOW();
    """
    cur.execute(sql, (selling_partner_id, refresh_token))
    conn.commit()
    cur.close()
    conn.close()

def fetch_seller_refresh_token(selling_partner_id):
    """
    Retrieves the SP-API refresh token for a given seller, or None if not found.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = "SELECT refresh_token FROM sellers WHERE selling_partner_id = %s"
    cur.execute(sql, (selling_partner_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]
    return None

def store_ads_refresh_token(advertiser_id, refresh_token):
    """
    Inserts or updates the Amazon Ads refresh token for a given advertiser.
    Uses a Postgres 'upsert' approach if 'advertiser_id' is UNIQUE.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = """
    INSERT INTO ads_tokens (advertiser_id, refresh_token)
    VALUES (%s, %s)
    ON CONFLICT (advertiser_id)
    DO UPDATE SET refresh_token = EXCLUDED.refresh_token,
                  updated_at = NOW();
    """
    cur.execute(sql, (advertiser_id, refresh_token))
    conn.commit()
    cur.close()
    conn.close()

def fetch_ads_refresh_token(advertiser_id):
    """
    Retrieves the Amazon Ads refresh token for a given advertiser, or None if not found.
    """
    conn = get_connection()
    cur = conn.cursor()
    sql = "SELECT refresh_token FROM ads_tokens WHERE advertiser_id = %s"
    cur.execute(sql, (advertiser_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return row[0]
    return None

if __name__ == "__main__":
    # If you run `python db.py` directly, it will create/ensure both tables exist.
    create_sellers_table()
    create_ads_tokens_table()
    print("Created/ensured 'sellers' and 'ads_tokens' tables exist.")
