#!/usr/bin/env python3

import json
import requests
import urllib.parse
import datetime
from flask import Flask, request, redirect, jsonify

from db import create_sellers_table, store_refresh_token, get_refresh_token
from sp_api.api import Sellers, Orders
from sp_api.base import Marketplaces, SellingApiException

import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

def get_spapi_secrets():
    """
    Pull SP-API LWA credentials from 'sp-api-credentials' in AWS Secrets Manager.
    JSON structure:
    {
      "CLIENT_ID": "...",  # LWA Client ID (amzn1.application-oa2-client...)
      "CLIENT_SECRET": "...",
      "AWS_ACCESS_KEY_ID": "...",
      "AWS_SECRET_ACCESS_KEY": "..."
    }
    """
    secret_name = "sp-api-credentials"  # Or your actual secret name
    region_name = "us-east-2"          # Adjust if in a different region

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']
    return json.loads(secret_str)

@app.route("/start")
def auth_start():
    """
    1) OAuth Login URL -> e.g. https://auth.cohortanalysis.ai/start
    2) Builds Amazon consent URL & redirects user to Amazon.

    NOTE: We add 'version=beta' if our app is DRAFT in Seller Central,
    because Amazon's draft flow uses 'spapi_oauth_code' param. 
    """
    spapi_secrets = get_spapi_secrets()

    # Your SP-API "Solution ID" from Seller Central's Developer Console
    # e.g. amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4
    spapi_solution_id = "amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4"

    # Must be https for Amazon
    redirect_uri = "https://auth.cohortanalysis.ai/callback"
    state = "randomState123"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": spapi_solution_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "version": "beta"  # required for DRAFT apps
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    1) OAuth Redirect URL -> https://auth.cohortanalysis.ai/callback
    2) Amazon sends 'spapi_oauth_code' & 'selling_partner_id' if we're in DRAFT flow.
    3) Exchange that code for a refresh_token & store it in the DB.
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    # In draft mode, Amazon sends spapi_oauth_code, not authorization_code
    auth_code = request.args.get('spapi_oauth_code')
    selling_partner_id = request.args.get('selling_partner_id')

    if not auth_code:
        return "Missing spapi_oauth_code param", 400

    token_url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,  # from spapi_oauth_code
        "redirect_uri": "https://auth.cohortanalysis.ai/callback",
        "client_id": lwa_client_id,
        "client_secret": lwa_client_secret
    }

    resp = requests.post(token_url, data=data)
    if resp.status_code != 200:
        return f"Error exchanging code: {resp.text}", 400

    tokens = resp.json()
    refresh_token = tokens.get('refresh_token')
    if not refresh_token:
        return "No refresh token returned", 400

    if not selling_partner_id:
        selling_partner_id = "UNKNOWN_PARTNER"

    # Save the refresh token
    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    Example -> https://auth.cohortanalysis.ai/test_sp_api?seller_id=XYZ
    A quick route showing how to call SP-API with that seller's refresh token.
    Calls get_marketplace_participation for demonstration.
    """
    spapi_secrets = get_spapi_secrets()
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id param", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    credentials_dict = {
        'lwa_app_id': spapi_secrets['CLIENT_ID'],
        'lwa_client_secret': spapi_secrets['CLIENT_SECRET'],
        'refresh_token': token,
        'aws_access_key': spapi_secrets['AWS_ACCESS_KEY_ID'],
        'aws_secret_key': spapi_secrets['AWS_SECRET_ACCESS_KEY']
    }

    sellers_client = Sellers(credentials=credentials_dict, marketplace=Marketplaces.US)
    try:
        response = sellers_client.get_marketplace_participation()
        return {
            "seller_id": seller_id,
            "marketplace_participation": response.payload
        }
    except SellingApiException as exc:
        return {"error": str(exc)}, 400

@app.route("/sales")
def get_sales():
    """
    Example -> https://auth.cohortanalysis.ai/sales?seller_id=XYZ
    Fetch last 7 days of orders from SP-API for that seller.
    """
    spapi_secrets = get_spapi_secrets()
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    credentials_dict = {
        'lwa_app_id': spapi_secrets['CLIENT_ID'],
        'lwa_client_secret': spapi_secrets['CLIENT_SECRET'],
        'refresh_token': token,
        'aws_access_key': spapi_secrets['AWS_ACCESS_KEY_ID'],
        'aws_secret_key': spapi_secrets['AWS_SECRET_ACCESS_KEY']
    }

    # Use the Orders client
    orders_client = Orders(credentials=credentials_dict, marketplace=Marketplaces.US)

    # Compute "created after" timestamp
    seven_days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()

    try:
        response = orders_client.get_orders(
            CreatedAfter=seven_days_ago,
            MarketplaceIds=["ATVPDKIKX0DER"]  # US marketplace ID
        )
        orders_data = response.payload.get("Orders", [])
        return jsonify({
            "seller_id": seller_id,
            "orders_last_7_days": orders_data
        })
    except SellingApiException as exc:
        return jsonify({"error": str(exc)}), 400

if __name__ == "__main__":
    create_sellers_table()
    # We run on 0.0.0.0:5000; Nginx proxies HTTPS -> port 5000
    app.run(host="0.0.0.0", port=5000, debug=True)
