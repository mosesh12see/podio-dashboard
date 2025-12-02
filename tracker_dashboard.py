#!/usr/bin/env python3
"""
Tracker Apps Dashboard Generator
Fetches data from all tracker apps and matches with Closer App data
"""

import requests
import json
import re
import os
import time
from datetime import datetime, timedelta

# Podio credentials
CLIENT_ID = "gpt-operator"
CLIENT_SECRET = "yn58tFMJO0HR8JRnUgKOWKph5FEq1Fn3WgWA4NA7oS4pMSSHmAuXTpxcE6hHtwPB"

# Closer App (has KW and close status data)
CLOSER_APP_ID = "29175634"
CLOSER_APP_TOKEN = "117d3fca26a11d72e48dc62e07d2e793"

# Tracker apps configuration
TRACKERS = {
    "elevateYou": {
        "name": "Elevate You",
        "app_id": "30482119",
        "token": "4628221c76dc8a6e894df159b438b54a",
        "rate_per_sit": 350,
        "rate_per_watt": 0.20
    },
    "suntria": {
        "name": "Suntria",
        "app_id": "30481163",
        "token": "2ede612c6fccb7eb1d34ff222bb0b1c2",
        "rate_per_sit": 450,
        "rate_per_watt": 0.10
    },
    "meb": {
        "name": "MEB",
        "app_id": "30480778",
        "token": "4e7113fb6f48e031d32b1ffcc7ff3ba9",
        "rate_per_sit": 450,
        "rate_per_watt": 0.05  # MEB pays $0.05 per watt
    }
}

# Cache file for storing last successful Closer App data
CACHE_FILE = "closer_app_cache.json"


def save_cache(data):
    """Save Closer App data to cache with timestamp"""
    cache = {
        "timestamp": datetime.now().isoformat(),
        "data": data
    }
    with open(CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)
    print(f"   💾 Cached {len(data)} appointments at {cache['timestamp']}")


def load_cache():
    """Load Closer App data from cache"""
    if not os.path.exists(CACHE_FILE):
        return None

    try:
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
        print(f"   📂 Loaded {len(cache['data'])} appointments from cache (saved at {cache['timestamp']})")
        return cache  # Return the whole cache object with timestamp
    except:
        return None


def retry_request(func, max_retries=5, initial_delay=2):
    """
    Retry a function that makes HTTP requests with exponential backoff

    Args:
        func: Function to retry (should return a response object)
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay in seconds before first retry

    Returns:
        Response object if successful, None if all retries failed
    """
    delay = initial_delay

    for attempt in range(max_retries):
        try:
            response = func()
            return response
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:

            if attempt < max_retries - 1:
                print(f"   ⚠️  Connection error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                print(f"   ⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"   ❌ All {max_retries} retry attempts failed")
                raise

    return None


def get_access_token(app_id, app_token):
    """Get Podio access token"""
    data = {
        'grant_type': 'app',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'app_id': app_id,
        'app_token': app_token
    }

    response = retry_request(
        lambda: requests.post('https://podio.com/oauth/token', data=data)
    )
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        print(f"❌ Auth failed for app {app_id}: {response.status_code}")
        return None


def fetch_app_items(app_id, access_token):
    """Fetch all items from a Podio app"""
    all_items = []
    offset = 0
    limit = 100

    while True:
        headers = {
            'Authorization': f'OAuth2 {access_token}',
            'Content-Type': 'application/json'
        }

        body = {
            'limit': limit,
            'offset': offset
        }

        # Wrap the request in retry logic
        response = retry_request(
            lambda: requests.post(
                f"https://api.podio.com/item/app/{app_id}/filter/",
                headers=headers,
                json=body
            )
        )

        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])

            if not items:
                break

            all_items.extend(items)

            if len(items) < limit:
                break

            offset += limit
        else:
            print(f"Error fetching items from app {app_id}: {response.text}")
            break

    return all_items


def get_field_value(item, external_id):
    """Extract field value from Podio item by external_id"""
    for field in item.get('fields', []):
        if field.get('external_id') == external_id:
            values = field.get('values', [])
            if not values:
                return None

            # Handle different field types
            field_type = field.get('type')

            if field_type == 'text':
                return values[0].get('value', '')
            elif field_type == 'number':
                return values[0].get('value', 0)
            elif field_type == 'category':
                return values[0].get('value', {}).get('text', '')
            elif field_type == 'date':
                return values[0].get('start', '')
            elif field_type == 'contact':
                return values[0].get('value', {}).get('name', '')
            elif field_type == 'app':
                return values[0].get('value', {}).get('title', '')
            else:
                return str(values[0].get('value', ''))

    return None


def main():
    print("=" * 60)
    print("TRACKER APPS DASHBOARD GENERATOR")
    print("=" * 60)

    # Step 1: Fetch Closer App data (has KW and close status)
    # ALWAYS fetch fresh data from API
    print("\n📋 Fetching Closer App data from API...")

    closer_items = []
    data_source = "Fresh API"
    data_timestamp = datetime.now().isoformat()

    # Fetch fresh data from API
    closer_token = get_access_token(CLOSER_APP_ID, CLOSER_APP_TOKEN)

    if closer_token:
        closer_items = fetch_app_items(CLOSER_APP_ID, closer_token)
        print(f"   ✅ Found {len(closer_items)} appointments in Closer App")

        # Save to cache as backup
        if len(closer_items) > 0:
            save_cache(closer_items)
        else:
            print("   ❌ No items fetched from API")
            return
    else:
        # API failed - try cache as fallback
        print("   ❌ Auth failed, trying cache as fallback...")
        cached_data = load_cache()
        if cached_data:
            closer_items = cached_data['data']
            data_timestamp = cached_data['timestamp']
            data_source = f"Cache (API failed, from {cached_data['timestamp']})"
            print(f"   ⚠️ Using cached data: {len(closer_items)} appointments")
        else:
            print("   ❌ No cache available")
            return

    # Build lookup by customer name
    closer_lookup = {}
    for item in closer_items:
        customer = get_field_value(item, "customer-name")
        if not customer:
            continue

        kw = get_field_value(item, "kw-size") or 0
        status = get_field_value(item, "status") or "Unknown"
        sit = get_field_value(item, "sit") or "No"
        manager = get_field_value(item, "closer-assigned") or ""

        # Get appointment date
        appt_date = get_field_value(item, "appointment-date")

        try:
            kw_float = float(kw) if kw else 0
        except:
            kw_float = 0

        # More flexible matching for "closed" status
        status_str = str(status).lower()
        is_closed = (
            "closed" in status_str or
            "close" in status_str or
            "sold" in status_str
        )

        closer_lookup[customer.lower().strip()] = {
            "kw": kw_float,
            "status": status,
            "sit": sit,
            "is_closed": is_closed,
            "appt_date": appt_date,
            "manager": manager
        }

    print(f"   ✅ Built lookup for {len(closer_lookup)} customers")

    # Calculate date boundaries for YTD, MTD, WTD
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_end = today_start + timedelta(days=1)  # Midnight tomorrow (exclusive)
    year_start = datetime(now.year, 1, 1)
    month_start = datetime(now.year, now.month, 1)
    week_start = now - timedelta(days=now.weekday())  # Monday of current week
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)

    # Initialize YTD/MTD/WTD counters (will accumulate across all tracker apps)
    ytd_sales = 0
    mtd_sales = 0
    wtd_sales = 0

    # Initialize monthly sits counter (last 6 months)
    monthly_sits = {}
    for i in range(6):
        month_date = now - timedelta(days=30 * i)
        month_key = month_date.strftime('%Y-%m')
        month_name = month_date.strftime('%B %Y')
        monthly_sits[month_key] = {"name": month_name, "count": 0, "elevate": 0, "suntria": 0, "meb": 0}

    # Manager mappings for monthly sits tracking
    manager_to_company = {
        'kyle': 'suntria',
        'koz': 'suntria',
        'walter': 'meb',
        'zueleta': 'meb',
        'zuelta': 'meb',
        'austin': 'elevate',
        'king': 'elevate'
    }

    # Step 2: Fetch all tracker apps data
    tracker_data = {}

    for key, config in TRACKERS.items():
        print(f"\n📊 Fetching {config['name']} tracker...")
        token = get_access_token(config['app_id'], config['token'])
        if not token:
            print(f"   ❌ Failed to authenticate")
            continue

        items = fetch_app_items(config['app_id'], token)
        print(f"   ✅ Found {len(items)} appointments")

        # Process items
        appointments = []
        total_sit_balance = 0
        total_watt_balance = 0
        paid_count = 0
        unpaid_count = 0
        closed_deals_count = 0
        total_kw_closed = 0

        for item in items:
            # Try different field names for customer
            customer = get_field_value(item, "customer-name") or get_field_value(item, "customer-full")
            if not customer:
                continue

            # Get payment status
            paid_status = get_field_value(item, "paid") or get_field_value(item, "paid-status") or "unpaid"
            is_paid = "paid" in str(paid_status).lower()

            # Match with Closer App
            closer_data = closer_lookup.get(customer.lower().strip(), {})
            kw = closer_data.get("kw", 0)
            is_closed = closer_data.get("is_closed", False)
            sit_status = closer_data.get("sit", "")

            # Count sits by month based on appointment date and manager
            appt_date_str = closer_data.get('appt_date')
            manager = closer_data.get('manager', '')
            if appt_date_str and "yes" in str(sit_status).lower():
                try:
                    if 'T' in str(appt_date_str):
                        date_part = appt_date_str.split('T')[0]
                    elif ' ' in str(appt_date_str):
                        date_part = appt_date_str.split(' ')[0]
                    else:
                        date_part = appt_date_str

                    appt_date = datetime.fromisoformat(date_part)
                    month_key = appt_date.strftime('%Y-%m')

                    if month_key in monthly_sits:
                        monthly_sits[month_key]["count"] += 1

                        # Track by company based on manager name
                        manager_lower = str(manager).lower()
                        company_key = None
                        for key, comp in manager_to_company.items():
                            if key in manager_lower:
                                company_key = comp
                                break

                        if company_key:
                            monthly_sits[month_key][company_key] += 1
                except (ValueError, AttributeError, TypeError):
                    pass

            # Calculate balances
            sit_payment = 0
            watt_payment = 0

            if not is_paid:
                unpaid_count += 1
                sit_payment = config['rate_per_sit']
                total_sit_balance += sit_payment
            else:
                paid_count += 1

            # Count all closed deals
            if is_closed:
                closed_deals_count += 1

                # Count YTD/MTD/WTD sales based on appointment date
                appt_date_str = closer_data.get('appt_date')
                if appt_date_str:
                    try:
                        # Parse the date
                        if 'T' in str(appt_date_str):
                            date_part = appt_date_str.split('T')[0]
                        elif ' ' in str(appt_date_str):
                            date_part = appt_date_str.split(' ')[0]
                        else:
                            date_part = appt_date_str

                        appt_date = datetime.fromisoformat(date_part)

                        # Count closed deals by period - exclude future appointments
                        if appt_date >= year_start and appt_date < today_end:
                            ytd_sales += 1
                        if appt_date >= month_start and appt_date < today_end:
                            mtd_sales += 1
                        if appt_date >= week_start and appt_date < today_end:
                            wtd_sales += 1
                    except (ValueError, AttributeError, TypeError):
                        pass

            # Calculate watt-based payment for closed deals with KW (regardless of paid status)
            if is_closed and kw > 0 and config.get('rate_per_watt', 0) > 0:
                watt_payment = kw * 1000 * config['rate_per_watt']
                total_watt_balance += watt_payment
                total_kw_closed += kw

            appointments.append({
                "customer": customer,
                "paid_status": paid_status,
                "is_paid": is_paid,
                "kw": kw,
                "status": closer_data.get("status", "Unknown"),
                "sit": closer_data.get("sit", "Unknown"),
                "is_closed": is_closed,
                "sit_payment": sit_payment,
                "watt_payment": watt_payment
            })

        # Note: If closer_lookup is empty, counts will show 0 (indicating we need fresh API data or cache)

        tracker_data[key] = {
            "name": config['name'],
            "total_appointments": len(appointments),
            "paid_count": paid_count,
            "unpaid_count": unpaid_count,
            "total_sit_balance": total_sit_balance,
            "total_watt_balance": total_watt_balance,
            "total_balance": total_sit_balance + total_watt_balance,
            "rate_per_sit": config['rate_per_sit'],
            "rate_per_watt": config.get('rate_per_watt', 0),
            "closed_deals_count": closed_deals_count,
            "total_kw_closed": total_kw_closed,
            "appointments": appointments
        }

        print(f"   💰 Total: {len(appointments)} appts, {unpaid_count} unpaid sits = ${total_sit_balance:,}, {closed_deals_count} closed deals = ${total_watt_balance:,.0f}, Total Balance = ${total_sit_balance + total_watt_balance:,.0f}")

    # Step 3: Generate HTML
    print("\n📝 Generating HTML dashboard...")
    print(f"   💰 YTD Sales: {ytd_sales} deals | MTD Sales: {mtd_sales} deals | WTD Sales: {wtd_sales} deals")

    # Print monthly sits summary
    sorted_months = sorted(monthly_sits.items(), reverse=True)
    print(f"   📅 Monthly Sits:")
    for month_key, data in sorted_months:
        print(f"      {data['name']}: {data['count']} sits")

    html_content = generate_html(tracker_data, data_source, data_timestamp, ytd_sales, mtd_sales, wtd_sales, monthly_sits)

    with open("tracker_summary.html", "w") as f:
        f.write(html_content)

    print("\n✅ Dashboard generated: tracker_summary.html")
    print("=" * 60)


def generate_html(tracker_data, data_source="Unknown", data_timestamp="Unknown", ytd_sales=0, mtd_sales=0, wtd_sales=0, monthly_sits=None):
    """Generate HTML dashboard"""

    if monthly_sits is None:
        monthly_sits = {}

    # Calculate totals
    grand_total_balance = sum(t['total_balance'] for t in tracker_data.values())
    grand_total_appts = sum(t['total_appointments'] for t in tracker_data.values())
    grand_total_unpaid = sum(t['unpaid_count'] for t in tracker_data.values())
    grand_total_closed = sum(t['closed_deals_count'] for t in tracker_data.values())
    grand_close_rate = (grand_total_closed / grand_total_appts * 100) if grand_total_appts > 0 else 0

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Tracker Apps Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 16px;
            min-height: 100vh;
            font-size: 14px;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 22px;
        }}
        .header h1 {{ font-size: 1.9em; margin-bottom: 9px; text-shadow: 2px 2px 4px rgba(0,0,0,0.3); }}
        .totals-banner {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            padding: 22px;
            border-radius: 13px;
            margin-bottom: 22px;
            color: white;
            text-align: center;
            box-shadow: 0 9px 22px rgba(0,0,0,0.3);
        }}
        .totals-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 13px;
            margin-top: 16px;
        }}
        .sales-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 13px;
            margin-top: 13px;
        }}
        .total-card {{ background: rgba(255,255,255,0.2); padding: 16px; border-radius: 9px; }}
        .total-card h3 {{ font-size: 0.87em; opacity: 0.9; margin-bottom: 9px; }}
        .total-card .number {{ font-size: 1.7em; font-weight: bold; }}

        .trackers-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 20px;
            margin-bottom: 30px;
        }}
        .tracker-card {{
            background: white;
            border-radius: 13px;
            padding: 20px;
            box-shadow: 0 9px 22px rgba(0,0,0,0.2);
        }}
        .tracker-card h2 {{
            color: #667eea;
            margin-bottom: 16px;
            font-size: 1.4em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 9px;
        }}
        .stat {{
            display: flex;
            justify-content: space-between;
            padding: 9px 0;
            border-bottom: 1px solid #f0f0f0;
            font-size: 0.97em;
        }}
        .stat:last-child {{ border-bottom: none; }}
        .stat-label {{ color: #666; font-weight: 500; }}
        .stat-value {{ font-weight: bold; color: #2c3e50; }}
        .stat-value.money {{ color: #27ae60; font-size: 1.07em; }}

        .appointments-section {{
            background: white;
            border-radius: 13px;
            padding: 22px;
            box-shadow: 0 9px 22px rgba(0,0,0,0.2);
        }}
        .appointments-section h2 {{
            color: #667eea;
            margin-bottom: 16px;
            font-size: 1.5em;
        }}
        .appt-card {{
            background: #f8f9fa;
            padding: 13px;
            margin: 9px 0;
            border-radius: 9px;
            border-left: 4px solid #667eea;
            font-size: 0.97em;
        }}
        .appt-card.paid {{
            border-left-color: #27ae60;
            background: #f0fff4;
        }}
        .appt-card.unpaid {{
            border-left-color: #e74c3c;
            background: #fff5f5;
        }}
        .appt-header {{
            display: flex;
            justify-content: space-between;
            margin-bottom: 9px;
        }}
        .appt-customer {{
            font-size: 1.07em;
            font-weight: bold;
            color: #2c3e50;
        }}
        .appt-company {{
            background: #667eea;
            color: white;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.85em;
        }}
        .appt-details {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 10px;
            font-size: 0.9em;
        }}
        .appt-detail {{ color: #666; }}
        .appt-detail strong {{ color: #2c3e50; }}
        .badge {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 10px;
            font-size: 0.8em;
            font-weight: bold;
        }}
        .badge.paid {{ background: #27ae60; color: white; }}
        .badge.unpaid {{ background: #e74c3c; color: white; }}
        .badge.closed {{ background: #3498db; color: white; }}
        .badge.warning {{ background: #f39c12; color: white; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Tracker Apps Dashboard</h1>
            <p>Complete overview of all tracker apps with Closer App data</p>
            <p style="font-size: 0.9em; opacity: 0.9;">Data Source: {data_source} | Last Updated: {data_timestamp}</p>
            <p style="font-size: 0.7em; opacity: 0.7; margin-top: 8px; font-style: italic;">cron job: hourly from 8am to 8pm daily | last ran: {datetime.now().strftime('%B %d, %Y at %I:%M %p').lower()}</p>
        </div>

        <!-- Fixed floating refresh timer -->
        <div id="refreshTimer" style="position: fixed; bottom: 20px; right: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 20px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3); z-index: 9999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; min-width: 200px; border: 2px solid rgba(255, 255, 255, 0.3);">
            <div id="nextRefresh" style="font-size: 13px; font-weight: 600; margin-bottom: 5px; color: #fff;"></div>
            <div id="countdown" style="font-size: 14px; font-weight: 700; color: #ffeb3b;"></div>
        </div>

        <div class="totals-banner">
            <h2>Grand Totals</h2>
            <div class="totals-grid">
                <div class="total-card">
                    <h3>Total Balance Owed</h3>
                    <div class="number">${grand_total_balance:,}</div>
                </div>
                <div class="total-card">
                    <h3>Total Appointments</h3>
                    <div class="number">{grand_total_appts}</div>
                </div>
                <div class="total-card">
                    <h3>Closed Deals</h3>
                    <div class="number">{grand_total_closed}</div>
                </div>
                <div class="total-card">
                    <h3>Close Rate</h3>
                    <div class="number">{grand_close_rate:.1f}%</div>
                </div>
            </div>
            <div class="sales-grid">
                <div class="total-card">
                    <h3>Unpaid Sits</h3>
                    <div class="number">{grand_total_unpaid}</div>
                </div>
                <div class="total-card">
                    <h3>YTD Sales</h3>
                    <div class="number">{ytd_sales}</div>
                </div>
                <div class="total-card">
                    <h3>MTD Sales</h3>
                    <div class="number">{mtd_sales}</div>
                </div>
                <div class="total-card">
                    <h3>WTD Sales</h3>
                    <div class="number">{wtd_sales}</div>
                </div>
            </div>
        </div>

        <div class="trackers-grid">
"""

    # Add tracker cards
    for key, data in tracker_data.items():
        rate_per_watt = data.get('rate_per_watt', 0)
        close_rate = (data['closed_deals_count'] / data['total_appointments'] * 100) if data['total_appointments'] > 0 else 0

        watt_info = ""
        if rate_per_watt > 0:
            watt_info = f"""
                <div class="stat">
                    <span class="stat-label">Total KW Closed</span>
                    <span class="stat-value">{data['total_kw_closed']:.2f} KW</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Watt Rate</span>
                    <span class="stat-value">${rate_per_watt}/watt</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Watt Balance</span>
                    <span class="stat-value money">${data['total_watt_balance']:,.0f}</span>
                </div>"""

        html += f"""
            <div class="tracker-card">
                <h2>{data['name']}</h2>
                <div class="stat">
                    <span class="stat-label">Total Appointments</span>
                    <span class="stat-value">{data['total_appointments']}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Paid / Unpaid</span>
                    <span class="stat-value">{data['paid_count']} / {data['unpaid_count']}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Closed Deals</span>
                    <span class="stat-value" style="color: #3498db; font-weight: bold;">{data['closed_deals_count']}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Close Rate</span>
                    <span class="stat-value" style="color: #3498db; font-weight: bold;">{close_rate:.1f}%</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Rate Per Sit</span>
                    <span class="stat-value">${data['rate_per_sit']}</span>
                </div>
                <div class="stat">
                    <span class="stat-label">Sit Balance (Unpaid)</span>
                    <span class="stat-value money">${data['total_sit_balance']:,}</span>
                </div>{watt_info}
                <div class="stat">
                    <span class="stat-label"><strong>Total Balance Owed</strong></span>
                    <span class="stat-value money"><strong>${data['total_balance']:,.0f}</strong></span>
                </div>
            </div>
"""

    html += """
        </div>

        <div class="appointments-section">
            <h2>Closed Deals Missing KW Data</h2>
            <p style="color: #666; margin-bottom: 20px;">These closed deals need KW information added in the Closer App</p>
"""

    # Add only closed deals missing KW, grouped by company
    for key, data in tracker_data.items():
        missing_kw_deals = [appt for appt in data['appointments'] if appt['is_closed'] and appt['kw'] == 0]

        if missing_kw_deals:
            html += f"""
            <h3 style="color: #667eea; margin: 20px 0 10px 0; border-bottom: 2px solid #667eea; padding-bottom: 5px;">
                {data['name']} ({len(missing_kw_deals)} deals)
            </h3>
"""
            for appt in missing_kw_deals:
                html += f"""
            <div class="appt-card unpaid">
                <div class="appt-header">
                    <span class="appt-customer">{appt['customer']}</span>
                    <span class="badge warning">NO KW DATA</span>
                </div>
                <div class="appt-details">
                    <div class="appt-detail"><strong>Status:</strong> {appt['status']}</div>
                    <div class="appt-detail"><strong>Sit:</strong> {appt['sit']}</div>
                    <div class="appt-detail"><strong>KW:</strong> {appt['kw']}</div>
                </div>
            </div>
"""

    html += """
        </div>

        <div class="appointments-section" style="margin-top: 30px;">
            <h2>Sits Per Month (Last 6 Months)</h2>
            <p style="color: #666; margin-bottom: 20px;">Monthly breakdown of all sits across all trackers (Elevate You + Suntria + MEB)</p>
"""

    # Add monthly sits cards
    sorted_months = sorted(monthly_sits.items(), reverse=True)

    # Split into 2 rows of 4 (showing last 6 months, recent first)
    first_row = sorted_months[:4]
    second_row = sorted_months[4:6] if len(sorted_months) > 4 else []

    # First row
    html += """
            <div class="totals-grid" style="margin-top: 20px;">
"""
    for month_key, data in first_row:
        elevate = data.get('elevate', 0)
        suntria = data.get('suntria', 0)
        meb = data.get('meb', 0)
        html += f"""
                <div class="total-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                    <h3>{data['name']}</h3>
                    <div class="number">{data['count']}</div>
                    <p style="font-size: 0.7em; opacity: 0.8; margin-top: 5px;">Elevate: {elevate} | Suntria: {suntria} | MEB: {meb}</p>
                </div>
"""

    # Fill empty slots in first row
    for _ in range(4 - len(first_row)):
        html += """
                <div class="total-card" style="background: rgba(255,255,255,0.1); color: rgba(255,255,255,0.5);">
                    <h3></h3>
                    <div class="number"></div>
                </div>
"""

    html += """
            </div>
"""

    # Second row
    if second_row:
        html += """
            <div class="totals-grid" style="margin-top: 20px;">
"""
        for month_key, data in second_row:
            elevate = data.get('elevate', 0)
            suntria = data.get('suntria', 0)
            meb = data.get('meb', 0)
            html += f"""
                <div class="total-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">
                    <h3>{data['name']}</h3>
                    <div class="number">{data['count']}</div>
                    <p style="font-size: 0.7em; opacity: 0.8; margin-top: 5px;">Elevate: {elevate} | Suntria: {suntria} | MEB: {meb}</p>
                </div>
"""

        # Fill empty slots in second row (should be 2 empty slots for 6 months total)
        for _ in range(4 - len(second_row)):
            html += """
                <div class="total-card" style="background: rgba(255,255,255,0.1); color: rgba(255,255,255,0.5);">
                    <h3></h3>
                    <div class="number"></div>
                </div>
"""

        html += """
            </div>
"""

    html += """
        </div>
    </div>

    <script>
        // Next refresh calculator and countdown timer
        function updateRefreshInfo() {
            const now = new Date();
            const cronTimes = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]; // Hourly from 8am to 8pm

            // Find next refresh time
            let nextRefreshHour = null;
            const currentHour = now.getHours();

            // Find next cron time today
            for (let hour of cronTimes) {
                if (hour > currentHour) {
                    nextRefreshHour = hour;
                    break;
                }
            }

            // If no more cron times today, use first one tomorrow
            const nextRefresh = new Date(now);
            if (nextRefreshHour === null) {
                nextRefresh.setDate(nextRefresh.getDate() + 1);
                nextRefreshHour = cronTimes[0];
            }
            nextRefresh.setHours(nextRefreshHour, 0, 0, 0);

            // Calculate time difference
            const diff = nextRefresh - now;
            const hours = Math.floor(diff / (1000 * 60 * 60));
            const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            const seconds = Math.floor((diff % (1000 * 60)) / 1000);

            // Format next refresh time
            const options = { hour: 'numeric', minute: '2-digit', hour12: true };
            const timeStr = nextRefresh.toLocaleTimeString('en-US', options);
            const dateStr = nextRefresh.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });

            // Update display
            document.getElementById('nextRefresh').textContent = `Next refresh: ${timeStr}`;
            document.getElementById('countdown').textContent = `Time until: ${hours}h ${minutes}m`;
        }

        // Update immediately and then every second
        updateRefreshInfo();
        setInterval(updateRefreshInfo, 1000);
    </script>
</body>
</html>
"""

    return html


if __name__ == "__main__":
    main()
