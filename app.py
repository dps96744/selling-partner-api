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
    JSON structure:
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
    OAuth Login URI -> https://auth.cohortanalysis.ai/start
    DRAFT => 'version=beta' => spapi_oauth_code
    """
    spapi_secrets = get_spapi_secrets()
    spapi_solution_id = "amzn1.sp.solution.d9a2df28-9c51-40d1-84b1-89daf7c4d0a4"

    redirect_uri = "https://auth.cohortanalysis.ai/callback"
    state = "randomState123"

    base_url = "https://sellercentral.amazon.com/apps/authorize/consent"
    params = {
        "application_id": spapi_solution_id,
        "redirect_uri": redirect_uri,
        "state": state,
        "version": "beta"  # draft => spapi_oauth_code
    }
    consent_url = f"{base_url}?{urllib.parse.urlencode(params)}"
    return redirect(consent_url)

@app.route("/callback")
def auth_callback():
    """
    Draft flow => spapi_oauth_code => exchange for refresh token
    """
    spapi_secrets = get_spapi_secrets()
    lwa_client_id = spapi_secrets['CLIENT_ID']
    lwa_client_secret = spapi_secrets['CLIENT_SECRET']

    auth_code = request.args.get('spapi_oauth_code')
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

    store_refresh_token(selling_partner_id, refresh_token)

    return f"Authorized seller {selling_partner_id}. You can close this window."

@app.route("/test_sp_api")
def test_sp_api():
    """
    -> https://auth.cohortanalysis.ai/test_sp_api?seller_id=XYZ
    Simple check: Sellers.get_marketplace_participation
    """
    spapi_secrets = get_spapi_secrets()
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id param", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    creds = {
        'lwa_app_id': spapi_secrets['CLIENT_ID'],
        'lwa_client_secret': spapi_secrets['CLIENT_SECRET'],
        'refresh_token': token,
        'aws_access_key': spapi_secrets['AWS_ACCESS_KEY_ID'],
        'aws_secret_key': spapi_secrets['AWS_SECRET_ACCESS_KEY']
    }

    sellers_client = Sellers(credentials=creds, marketplace=Marketplaces.US)
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
    -> https://auth.cohortanalysis.ai/sales?seller_id=XYZ
    Last 7 days from Orders API
    """
    spapi_secrets = get_spapi_secrets()
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    creds = {
        'lwa_app_id': spapi_secrets['CLIENT_ID'],
        'lwa_client_secret': spapi_secrets['CLIENT_SECRET'],
        'refresh_token': token,
        'aws_access_key': spapi_secrets['AWS_ACCESS_KEY_ID'],
        'aws_secret_key': spapi_secrets['AWS_SECRET_ACCESS_KEY']
    }

    orders_client = Orders(credentials=creds, marketplace=Marketplaces.US)
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

@app.route("/orders_2022")
def orders_2022():
    """
    -> https://auth.cohortanalysis.ai/orders_2022?seller_id=XYZ

    Requests GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL for 2022 (1 year).
    This might be more likely to succeed than older years, but no guarantee—Amazon
    can still CANCEL if it doesn't store data or the seller account doesn't qualify.
    """
    spapi_secrets = get_spapi_secrets()
    seller_id = request.args.get('seller_id')
    if not seller_id:
        return "Missing seller_id param", 400

    token = get_refresh_token(seller_id)
    if not token:
        return f"No refresh token found for {seller_id}", 404

    creds = {
        'lwa_app_id': spapi_secrets['CLIENT_ID'],
        'lwa_client_secret': spapi_secrets['CLIENT_SECRET'],
        'refresh_token': token,
        'aws_access_key': spapi_secrets['AWS_ACCESS_KEY_ID'],
        'aws_secret_key': spapi_secrets['AWS_SECRET_ACCESS_KEY']
    }

    reports_client = Reports(credentials=creds, marketplace=Marketplaces.US)

    # 2022 => Jan 1, 2022 -> Jan 1, 2023
    start_2022 = "2022-01-01T00:00:00Z"
    end_2023 = "2023-01-01T00:00:00Z"

    try:
        # 1) Create the All Orders by Date General report
        create_report_resp = reports_client.create_report(
            reportType="GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL",
            dataStartTime=start_2022,
            dataEndTime=end_2023,
            marketplaceIds=["ATVPDKIKX0DER"]
        )
        create_payload = create_report_resp.payload or {}
        report_id = create_payload.get("reportId")
        if not report_id:
            return jsonify({"error": "No reportId returned"}), 400

        # 2) Poll until DONE or CANCELLED/FATAL
        while True:
            status_resp = reports_client.get_report(reportId=report_id)
            status_payload = status_resp.payload or {}
            processing_status = status_payload.get("processingStatus")
            if processing_status == "DONE":
                doc_id = status_payload.get("reportDocumentId")
                if not doc_id:
                    return jsonify({"error": "No reportDocumentId returned"}), 400
                break
            elif processing_status in ("CANCELLED", "FATAL"):
                return jsonify({"error": f"Report {report_id} canceled/fatal: {status_payload}"}), 400

            time.sleep(5)

        # 3) Retrieve the doc
        doc_resp = reports_client.get_report_document(reportDocumentId=doc_id)
        if not hasattr(doc_resp, 'file'):
            return jsonify({"error": "doc_resp has no file attribute"}), 400

        file_bytes = doc_resp.file
        if not file_bytes:
            return jsonify({"error": "No file content returned"}), 400

        content = file_bytes.decode('utf-8', errors='replace')
        return jsonify({
            "seller_id": seller_id,
            "report_id": report_id,
            "report_document_id": doc_id,
            "file_length": len(content),
            "report_file_preview": content[:1000]
        })

    except SellingApiException as exc:
        return jsonify({"error": str(exc)}), 400

if __name__ == "__main__":
    create_sellers_table()
    app.run(host="0.0.0.0", port=5000, debug=True)
