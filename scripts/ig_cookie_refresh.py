#!/usr/bin/env python3
"""
Instagram Cookie Refresh via Camoufox
Logs in with email/password and extracts fresh session cookies.
Outputs JSON: {"session_id": "...", "csrf_token": "...", "ds_user_id": "..."}
"""

import json
import os
import sys
import time

def refresh_cookies():
    """Log into Instagram via Camoufox and extract cookies."""
    email = os.environ.get('IG_EMAIL', '')
    password = os.environ.get('IG_PASSWORD', '')
    
    if not email or not password:
        print(json.dumps({"error": "IG_EMAIL and IG_PASSWORD required"}))
        sys.exit(1)
    
    try:
        from camoufox.sync_api import Camoufox
    except ImportError:
        print(json.dumps({"error": "camoufox not installed: pip install camoufox"}))
        sys.exit(1)
    
    with Camoufox(headless=True) as browser:
        page = browser.new_page()
        
        # Navigate to Instagram login
        page.goto('https://www.instagram.com/accounts/login/', wait_until='networkidle')
        time.sleep(3)
        
        # Accept cookies dialog if present
        try:
            cookie_btn = page.query_selector('button:has-text("Allow")')
            if cookie_btn:
                cookie_btn.click()
                time.sleep(1)
        except:
            pass
        
        # Fill login form
        username_input = page.wait_for_selector('input[name="username"]', timeout=10000)
        password_input = page.wait_for_selector('input[name="password"]', timeout=10000)
        
        username_input.fill(email)
        time.sleep(0.5)
        password_input.fill(password)
        time.sleep(0.5)
        
        # Click login button
        login_btn = page.query_selector('button[type="submit"]')
        if login_btn:
            login_btn.click()
        
        # Wait for navigation (login success or challenge)
        time.sleep(8)
        
        # Check for 2FA / challenge
        current_url = page.url
        if 'challenge' in current_url or 'two_factor' in current_url:
            print(json.dumps({"error": "2FA or challenge required — manual intervention needed"}))
            sys.exit(1)
        
        # Extract cookies
        cookies = page.context.cookies()
        
        session_id = ''
        csrf_token = ''
        ds_user_id = ''
        
        for cookie in cookies:
            if cookie['name'] == 'sessionid':
                session_id = cookie['value']
            elif cookie['name'] == 'csrftoken':
                csrf_token = cookie['value']
            elif cookie['name'] == 'ds_user_id':
                ds_user_id = cookie['value']
        
        if session_id and csrf_token and ds_user_id:
            print(json.dumps({
                "session_id": session_id,
                "csrf_token": csrf_token,
                "ds_user_id": ds_user_id
            }))
            sys.exit(0)
        else:
            print(json.dumps({
                "error": "Login may have failed — missing cookies",
                "found_cookies": [c['name'] for c in cookies if 'instagram' in c.get('domain', '')]
            }))
            sys.exit(1)


if __name__ == '__main__':
    refresh_cookies()
