#!/usr/bin/env python3

import json
import requests
import urllib.parse
import datetime
import time
from flask import Flask, request, redirect, jsonify

from db import create_sellers_table, store_refresh_token, get_refresh_token
from sp_api.api import Sellers, Orders, Reports
from sp_api.base import Marketplaces, SellingApiException
from botocore.exceptions import ClientError
import boto3

app = Flask(__name__)

def get_spapi_secrets():
    """
    Pull SP-API LWA credentials from 'sp-api-credentials' in AWS Secrets Manager.
    {
      "CLIENT_ID": "...",  # LWA Client ID (amzn1.application-oa2-client...)
      "CLIENT_SECRET": "...",
      "AWS_ACCESS_KEY_ID": "...",
      "AWS_SECRET_ACCESS_KEY": "..."
    }
    """
    secret_name = "sp-api-credentials"  # or your actual secret name
    region_name = "us-east-2"          # adjust if in a different region

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret_str = response['SecretString']
    return json.loads(secret_str)

@app.route("/start")
def auth_start():
    """
    1) OAuth Login URI -> https://auth.cohortanalysis.ai/start
    2) Builds the Amazon consent URL & redirects the seller to Amazon.

    - We add 'version=beta' if the app is in DRAFT mode, so Amazon uses spapi_oauth_code
      instead of authorization_code.
    """
    spapi_secrets = get_spapi_secrets()

    # Your SP-API "Solution ID" (the "App ID" from Seller Central).
    spapi_solution_id = "amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4"

    redirect_uri = "https://auth.cohortanalysis.ai/callback"  # Must be HTTPS
    state = "randomState123"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": spapi_solution_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "version": "beta"  # for DRAFT apps
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    1) OAuth Redirect URI -> https://auth.cohortanalysis.ai/callback
    2) If 'version=beta', Amazon sends spapi_oauth_code => we exchange for a refresh token.
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    auth_code = request.args.get('spapi_oauth_code')  # for draft flow
    selling_partner_id = request.args.get('selling_partner_id')

    if not auth_code:
        return "Missing spapi_oauth_code param", 400

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

    # Store refresh token in DB
    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    Example -> https://auth.cohortanalysis.ai/test_sp_api?seller_id=XYZ
    Basic SP-API call to Sellers.get_marketplace_participation()
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
    Pulls last 7 days of orders from Orders API (may not reach beyond ~2 yrs).
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

    orders_client = Orders(credentials=credentials_dict, marketplace=Marketplaces.US)
    seven_days_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()

    try:
        response = orders_client.get_orders(
            CreatedAfter=seven_days_ago,
            MarketplaceIds=["ATVPDKIKX0DER"]
        )
        orders_data = response.payload.get("Orders", [])
        return jsonify({
            "seller_id": seller_id,
            "orders_last_7_days": orders_data
        })
    except SellingApiException as exc:
        return jsonify({"error": str(exc)}), 400

@app.route("/long_term_sales")
def get_long_term_sales():
    """
    Example -> https://auth.cohortanalysis.ai/long_term_sales?seller_id=XYZ

    Demonstrates requesting older data (>2 yrs) via the Reports API (ALL_ORDERS).
    We create a 'GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL' report for ~10 yrs,
    then poll for completion, and finally retrieve the report doc.

    In reality, this can produce huge data & take time. A real production approach
    would do this asynchronously, storing the file or partial results somewhere
    rather than returning them in one request.
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

    reports_client = Reports(credentials=credentials_dict, marketplace=Marketplaces.US)

    now_utc = datetime.datetime.utcnow().isoformat()
    # 10 years => 3650 days
    ten_years_ago = (datetime.datetime.utcnow() - datetime.timedelta(days=3650)).isoformat()

    try:
        # 1) Create the ALL_ORDERS report
        create_resp = reports_client.create_report(
            reportType="GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            dataStartTime=ten_years_ago,
            dataEndTime=now_utc,
            marketplaceIds=["ATVPDKIKX0DER"]
        )
        report_id = create_resp.get("reportId")
        if not report_id:
            return jsonify({"error": "No reportId returned"}), 400

        # 2) Poll for report completion
        #   This is a naive approach that may block for minutes if the report is large.
        while True:
            status_resp = reports_client.get_report(reportId=report_id)
            report_info = status_resp.payload
            if report_info.get("processingStatus") == "DONE":
                # 3) Once DONE, get the reportDocumentId
                doc_id = report_info.get("reportDocumentId")
                if not doc_id:
                    return jsonify({"error": "No reportDocumentId returned"}), 400
                break
            elif report_info.get("processingStatus") in ("CANCELLED", "FATAL"):
                return jsonify({"error": f"Report cancelled or fatal. {report_info}"}), 400
            time.sleep(5)  # wait 5s before checking again

        # 4) Retrieve the report document
        doc_resp = reports_client.get_report_document(doc_id)
        # doc_resp.payload has details; doc_resp.file is the actual file content
        # python-amazon-sp-api includes a method to decode
        # Example: doc_resp.decode() if it's text
        content = doc_resp.decode()

        # We'll just return the entire text content here (might be huge).
        return jsonify({
            "seller_id": seller_id,
            "report_id": report_id,
            "report_document_id": doc_id,
            "file_length": len(content),
            "report_file_preview": content[:1000]  # first 1000 chars
        })

    except SellingApiException as exc:
        return jsonify({"error": str(exc)}), 400


if __name__ == "__main__":
    create_sellers_table()
    # Listen on 0.0.0.0:5000 behind Nginx+SSL => https://auth.cohortanalysis.ai
    app.run(host="0.0.0.0", port=5000, debug=True)
