#!/usr/bin/env python3

import json
import boto3
import psycopg2
from botocore.exceptions import ClientError

def get_db_secret():
    """
    Retrieves RDS credentials from AWS Secrets Manager under the name 'MyRDSSecret'.
    The secret JSON should look like:
    {
      "username": "postgres",
      "password": "...",
      "host": "your-rds-host.us-east-2.rds.amazonaws.com",
      "port": 5432,
      "dbname": "postgres"
    }
    """
    secret_name = "MyRDSSecret"  # or your actual secret name
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print("Error retrieving DB secret:", e)
        raise e

    secret_str = get_secret_value_response['SecretString']
    secret_dict = json.loads(secret_str)
    return secret_dict

def connect_db():
    """
    Connects to the PostgreSQL DB using the credentials from 'MyRDSSecret'.
    """
    secret_dict = get_db_secret()

    username = secret_dict['username']
    password = secret_dict['password']
    host = secret_dict['host']
    port = secret_dict['port']
    dbname = secret_dict.get('dbname', 'postgres')

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=username,
        password=password,
        dbname=dbname
    )
    return conn

def create_sellers_table():
    """
    Creates a 'sellers' table if it doesn't exist.
    We'll store each seller's refresh token from OAuth flow here.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sellers (
            id SERIAL PRIMARY KEY,
            selling_partner_id VARCHAR(100) UNIQUE NOT NULL,
            refresh_token TEXT NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def store_refresh_token(selling_partner_id, refresh_token):
    """
    Insert or update a seller's refresh token in the 'sellers' table.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sellers (selling_partner_id, refresh_token)
        VALUES (%s, %s)
        ON CONFLICT (selling_partner_id)
        DO UPDATE SET refresh_token = EXCLUDED.refresh_token, updated_at = NOW();
    """, (selling_partner_id, refresh_token))
    conn.commit()
    cur.close()
    conn.close()

def get_refresh_token(selling_partner_id):
    """
    Retrieve the refresh token for a given seller, or None if not found.
    """
    conn = connect_db()
    cur = conn.cursor()
    cur.execute("SELECT refresh_token FROM sellers WHERE selling_partner_id = %s", (selling_partner_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row[0] if row else None

if __name__ == "__main__":
    # Quick test: ensure table exists
    create_sellers_table()
    print("Sellers table ensured to exist.")

