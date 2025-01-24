#!/usr/bin/env python3

import json
import psycopg2
import boto3
from botocore.exceptions import ClientError

#####################################
# Retrieve DB Credentials from AWS Secrets Manager
#####################################

def get_db_secret():
    """
    Fetches database credentials from AWS Secrets Manager.

    Secret name: MyRDSSecret
    Region: us-east-2

    The secret should have JSON structure like:
    {
      "username": "postgres",
      "password": "YOUR_DB_PASSWORD",
      "host": "mydb.xxxx.us-east-2.rds.amazonaws.com",
      "port": 5432,
      "dbname": "postgres"
    }
    """
    secret_name = "MyRDSSecret"  # Your secret name
    region_name = "us-east-2"    # Update if your secret is in another region

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        raise Exception(f"Error retrieving secret from Secrets Manager: {e}")

    secret_str = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret_str)
    return secret_dict

#####################################
# Connection Logic
#####################################

def get_connection():
    """
    Builds a psycopg2 connection using credentials from Secrets Manager.
    Uses sslmode='require' for RDS encryption if needed.
    """
    secret = get_db_secret()
    db_user = secret["username"]
    db_pass = secret["password"]
    db_host = secret["host"]
    db_port = secret.get("port", 5432)
    db_name = secret.get("dbname", "postgres")

    try:
        conn = psycopg2.connect(
            user=db_user,
            password=db_pass,
            host=db_host,
            port=db_port,
            dbname=db_name,
            sslmode='require'
        )
        return conn
    except psycopg2.OperationalError as e:
        raise Exception(f"An error occurred: {e}")

#####################################
# Table Creation
#####################################

def create_sellers_table():
    """
    Ensures the 'sellers' table (for SP-API refresh tokens) exists.
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

#####################################
# SP-API: Store & Fetch
#####################################

def store_refresh_token(selling_partner_id, refresh_token):
    """
    Inserts or updates the SP-API refresh token for a given seller (selling_partner_id).
    (Matches your usage in app.py)
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

def get_refresh_token(selling_partner_id):
    """
    Returns the SP-API refresh token for the given seller, or None if not found.
    (Matches your usage in app.py)
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

#####################################
# Amazon Ads: Store & Fetch
#####################################

def store_ads_refresh_token(advertiser_id, refresh_token):
    """
    Inserts or updates the Amazon Ads refresh token for a given advertiser_id.
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
    Returns the Amazon Ads refresh token for the given advertiser, or None if not found.
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

#####################################
# Main Entry Point
#####################################

if __name__ == "__main__":
    create_sellers_table()
    create_ads_tokens_table()
    print("Created/ensured 'sellers' and 'ads_tokens' tables exist.")
