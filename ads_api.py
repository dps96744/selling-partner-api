#!/usr/bin/env python3

import os
import json
import requests
import boto3

############################
# AWS Secrets Manager Part #
############################

def get_ads_secrets():
    """
    Fetch your Amazon Ads client_id and client_secret from AWS Secrets Manager.
    The secret should be named, for example, 'ads-api-credentials' and contain JSON:
    {
       "ADS_CLIENT_ID": "PLACEHOLDER_ADS_CLIENT_ID",
       "ADS_CLIENT_SECRET": "PLACEHOLDER_ADS_CLIENT_SECRET"
    }
    """
    secret_name = "ads-api-credentials"  # or whatever name you used in Secrets Manager
    region_name = "us-east-2"           # adjust if your secrets are in another region

    client = boto3.client('secretsmanager', region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)

    secret_str = response['SecretString']
    secret_dict = json.loads(secret_str)

    # Expecting keys: ADS_CLIENT_ID, ADS_CLIENT_SECRET
    return secret_dict

###########################
# Amazon Ads API Client   #
###########################

AMAZON_ADS_OAUTH_URL = "https://api.amazon.com/auth/o2/token"
AMAZON_ADS_API_BASE  = "https://advertising-api.amazon.com"

class AmazonAdsClient:
    """
    A minimal Amazon Ads API client that:
      - Exchanges a stored refresh_token for a short-lived access_token
      - Performs basic calls (e.g., get_profiles)
    """

    def __init__(self, refresh_token):
        """
        :param refresh_token: The advertiser-specific refresh token
                             retrieved from your DB after OAuth callback.
        """
        # We'll fetch client_id and client_secret from Secrets Manager
        ads_secrets = get_ads_secrets()

        self.client_id = ads_secrets["ADS_CLIENT_ID"]
        self.client_secret = ads_secrets["ADS_CLIENT_SECRET"]
        self.refresh_token = refresh_token
        self.access_token  = None

    def get_access_token(self):
        """
        Exchanges the stored refresh_token for a short-lived access token
        and sets self.access_token for subsequent requests.
        """
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }

        resp = requests.post(AMAZON_ADS_OAUTH_URL, data=data)
        if resp.status_code != 200:
            raise Exception(f"[AmazonAdsClient] OAuth error: {resp.status_code} => {resp.text}")

        tokens = resp.json()
        self.access_token = tokens["access_token"]
        return self.access_token

    def _ensure_access_token(self):
        """
        Helper to make sure we have a valid access_token.
        Call this before any Ads API request.
        """
        if not self.access_token:
            self.get_access_token()

    def get_profiles(self):
        """
        Example Ads API call: retrieve the advertiser 'profiles' 
        that this access_token can manage.
        Docs: https://advertising.amazon.com/API/docs/en-us/concepts/profiles
        """
        self._ensure_access_token()

        url = f"{AMAZON_ADS_API_BASE}/v2/profiles"
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Amazon-Advertising-API-ClientId": self.client_id,
            "Content-Type": "application/json"
        }

        resp = requests.get(url, headers=headers)
        if resp.status_code != 200:
            raise Exception(f"[AmazonAdsClient] get_profiles error: {resp.status_code} => {resp.text}")

        return resp.json()

#############################
# (Optional) OAuth Example  #
#############################

def exchange_auth_code_for_refresh_token(auth_code, redirect_uri):
    """
    If you do the OAuth flow in your 'app.py', you might call this to exchange
    an authorization 'code' for a refresh_token. It's a convenience function.
    You can integrate it directly in your Flask callback route if preferred.
    """
    ads_secrets = get_ads_secrets()
    client_id     = ads_secrets["ADS_CLIENT_ID"]
    client_secret = ads_secrets["ADS_CLIENT_SECRET"]

    data = {
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri,
        "client_id": client_id,
        "client_secret": client_secret
    }

    resp = requests.post(AMAZON_ADS_OAUTH_URL, data=data)
    if resp.status_code != 200:
        raise Exception(f"[AmazonAdsClient] OAuth exchange error: {resp.status_code} => {resp.text}")

    tokens = resp.json()
    refresh_token = tokens.get("refresh_token")
    return refresh_token

###########################
# Example usage (local)   #
###########################
if __name__ == "__main__":
    # Quick test or usage example (pseudo-code).
    # In real usage, you'd handle this in your Flask routes or DB code.
    print("Testing Amazon Ads secrets retrieval...")

    # 1) Retrieve Ads secrets from Secrets Manager
    ads_creds = get_ads_secrets()
    print("ADS_CLIENT_ID:", ads_creds["ADS_CLIENT_ID"])
    # 2) Suppose you already have a stored refresh_token for a brand:
    mock_refresh_token = "ATZ|...someRefreshTokenFromDB..."

    # 3) Initialize client
    client = AmazonAdsClient(refresh_token=mock_refresh_token)
    # 4) Attempt to get profiles
    try:
        profiles = client.get_profiles()
        print("Profiles:", profiles)
    except Exception as e:
        print("Error calling Ads API:", e)
