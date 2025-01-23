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
    Pull SP-API LWA credentials from 'sp-api-credentials' in Secrets Manager:
    {
      "CLIENT_ID": "...",  # LWA client ID: amzn1.application-oa2-client.…
      "CLIENT_SECRET": "...",
      "AWS_ACCESS_KEY_ID": "...",
      "AWS_SECRET_ACCESS_KEY": "..."
    }
    """
    secret_name = "sp-api-credentials"
    region_name = "us-east-2"

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']
    return json.loads(secret_str)

@app.route("/start")
def auth_start():
    """
    OAuth Login URI -> https://auth.cohortanalysis.ai/start
    Builds the Amazon consent URL & redirects the seller to Amazon.

    We use 'version=beta' since the app is in DRAFT mode, and Amazon 
    might send 'spapi_oauth_code' instead of 'authorization_code'.
    """
    spapi_secrets = get_spapi_secrets()

    # Your SP-API Solution ID (the "App ID" in Seller Central).
    # e.g. amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4
    spapi_solution_id = "amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4"

    # Must be HTTPS for Amazon
    redirect_uri = "https://auth.cohortanalysis.ai/callback"
    state = "randomState123"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": spapi_solution_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "version": "beta"  # Required for Draft-mode apps
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    OAuth Redirect URI -> https://auth.cohortanalysis.ai/callback
    Now Amazon sends 'spapi_oauth_code' instead of 'authorization_code'.

    We exchange spapi_oauth_code for a refresh_token.
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']         # amzn1.application-oa2-client...
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    # The new Draft flow param from Amazon is 'spapi_oauth_code'
    auth_code = request.args.get('spapi_oauth_code')
    selling_partner_id = request.args.get('selling_partner_id')  # optional

    if not auth_code:
        return "Missing spapi_oauth_code param", 400

    # Exchange the spapi_oauth_code for tokens
    token_url = "https://api.amazon.com/auth/o2/token"
    data = {
        "grant_type": "authorization_code",
        # The 'code' field in the token request is still the LWA param name,
        # but we supply the spapi_oauth_code value from the callback.
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

    # Store the refresh token in your DB
    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    Example -> https://auth.cohortanalysis.ai/test_sp_api?seller_id=YOUR_SELLER_ID
    Uses stored refresh token to call SP-API for that seller.
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
    create_sellers_table()
    # Listen on 0.0.0.0:5000 behind Nginx
    app.run(host="0.0.0.0", port=5000, debug=True)
