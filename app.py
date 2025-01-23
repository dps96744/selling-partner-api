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
    Fetches your SP-API LWA credentials from a secret named 'sp-api-credentials'.
    {
      "CLIENT_ID": "...",          # LWA client ID (amzn1.application-oa2-client...)
      "CLIENT_SECRET": "...",      # LWA client secret
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
    OAuth Login URI -> http://<EC2-IP>:5000/start
    Builds the Amazon consent URL & redirects the seller to Amazon.

    IMPORTANT: We add 'version=beta' to support a DRAFT app workflow.
    Once your app is published, remove "version": "beta".
    """
    spapi_secrets = get_spapi_secrets()

    # This is your SP-API "Solution ID"/"App ID" from Seller Central, something like:
    # amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4
    spapi_solution_id = "amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4"

    # The redirect URI must match what's in your Developer Console.
    redirect_uri = "https://auth.cohortanalysis.ai/callback"
    state = "randomState123"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": spapi_solution_id,     # SP-API solution ID
        "redirect_uri": redirect_uri,
        "state": state,
        "version": "beta"   # <-- The key addition allowing Draft mode OAuth
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    OAuth Redirect URI -> https://auth.cohortanalysis.ai/callback
    Amazon sends the user here after authorization.
    We exchange the auth_code for a refresh_token.
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
        "client_id": lwa_client_id,       # LWA client ID
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

    # Store the refresh token in DB
    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    Example -> http://<EC2-IP>:5000/test_sp_api?seller_id=SOMETHING
    Uses the stored refresh token to call SP-API for that seller.
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
        return {"error": str(exc)}, 400

if __name__ == "__main__":
    # Create the table if not exists
    create_sellers_table()

    # Listen on 0.0.0.0:5000 for external access without needing sudo
    app.run(host="0.0.0.0", port=5000, debug=True)
