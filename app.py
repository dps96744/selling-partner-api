#!/usr/bin/env python3

import json
import requests
import urllib.parse
from flask import Flask, request, redirect

from db import create_sellers_table, store_refresh_token, get_refresh_token
from sp_api.api import Sellers
from sp_api.base import Marketplaces, SellingApiException

import boto3
from botocore.exceptions import ClientError

app = Flask(__name__)

def get_spapi_secrets():
    """
    Fetches SP-API LWA credentials from a secret named 'sp-api-credentials'.
    The JSON might look like:
    {
      "CLIENT_ID": "...",
      "CLIENT_SECRET": "...",
      "AWS_ACCESS_KEY_ID": "...",
      "AWS_SECRET_ACCESS_KEY": "..."
    }
    """
    secret_name = "sp-api-credentials"
    region_name = "us-east-2"

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']
    return json.loads(secret_str)

@app.route("/start")
def auth_start():
    """
    1. Builds the Amazon OAuth consent URL
    2. Redirects the seller to login & authorize your SP-API application
    URL -> https://auth.cohortanalysis.ai/start
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    # Typically we use the LWA client ID as the 'application_id' for SP-API OAuth
    application_id = lwa_client_id
    state = "randomState123"

    # The callback must match what's in Seller Central: "https://auth.cohortanalysis.ai/callback"
    redirect_uri = "https://auth.cohortanalysis.ai/callback"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": application_id,
        "redirect_uri": redirect_uri,
        "state": state
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    1. Amazon redirects here after the seller logs in & clicks "Authorize"
    2. We get authorization_code
    3. Exchange for refresh_token
    4. Store refresh_token in the DB
    URL -> https://auth.cohortanalysis.ai/callback
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    auth_code = request.args.get('authorization_code')
    selling_partner_id = request.args.get('selling_partner_id')  # sometimes included

    if not auth_code:
        return "Missing authorization_code", 400

    token_url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
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

    # Store the refresh token
    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    Example SP-API call using a seller's refresh token from DB.
    GET https://auth.cohortanalysis.ai/test_sp_api?seller_id=XXX
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']
    aws_access_key = spapi_secrets['AWS_ACCESS_KEY_ID']
    aws_secret_key = spapi_secrets['AWS_SECRET_ACCESS_KEY']

    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id param", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    credentials_dict = {
        'lwa_app_id': lwa_client_id,
        'lwa_client_secret': lwa_client_secret,
        'refresh_token': token,
        'aws_access_key': aws_access_key,
        'aws_secret_key': aws_secret_key
    }

    sellers_client = Sellers(credentials=credentials_dict, marketplace=Marketplaces.US)
    try:
        response = sellers_client.get_marketplace_participation()
        return {
            "seller_id": seller_id,
            "marketplace_participation": response.payload
        }
    except SellingApiException as exc:
        return {
            "error": str(exc)
        }, 400

if __name__ == "__main__":
    create_sellers_table()
    # If behind Nginx or ALB with SSL, you might use port=80 or port=5000, etc.
    # For direct HTTP, open port 80 in EC2 security group, or if using 5000, do port=5000 and open that.
    # Example:
    app.run(host="0.0.0.0", port=80, debug=True)
    # Then in your domain DNS, "auth.cohortanalysis.ai" -> EC2 IP,
    # and in Seller Central: 
    #   OAuth Login URI = https://auth.cohortanalysis.ai/start
    #   OAuth Redirect URI = https://auth.cohortanalysis.ai/callback
