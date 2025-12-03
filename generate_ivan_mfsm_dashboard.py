#!/usr/bin/env python3
"""
Generate IVAN MFSM Stats Dashboard
IVAN MANAGER (HARD-CODED):
- Ivan
"""

import requests
import time
from datetime import datetime, timedelta

# State name to abbreviation mapping
STATE_MAP = {
    'alabama': 'AL', 'alaska': 'AK', 'arizona': 'AZ', 'arkansas': 'AR',
    'california': 'CA', 'colorado': 'CO', 'connecticut': 'CT', 'delaware': 'DE',
    'florida': 'FL', 'georgia': 'GA', 'hawaii': 'HI', 'idaho': 'ID',
    'illinois': 'IL', 'indiana': 'IN', 'iowa': 'IA', 'kansas': 'KS',
    'kentucky': 'KY', 'louisiana': 'LA', 'maine': 'ME', 'maryland': 'MD',
    'massachusetts': 'MA', 'michigan': 'MI', 'minnesota': 'MN', 'mississippi': 'MS',
    'missouri': 'MO', 'montana': 'MT', 'nebraska': 'NE', 'nevada': 'NV',
    'new hampshire': 'NH', 'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY',
    'north carolina': 'NC', 'north dakota': 'ND', 'ohio': 'OH', 'oklahoma': 'OK',
    'oregon': 'OR', 'pennsylvania': 'PA', 'rhode island': 'RI', 'south carolina': 'SC',
    'south dakota': 'SD', 'tennessee': 'TN', 'texas': 'TX', 'utah': 'UT',
    'vermont': 'VT', 'virginia': 'VA', 'washington': 'WA', 'west virginia': 'WV',
    'wisconsin': 'WI', 'wyoming': 'WY'
}

CLIENT_ID = "gpt-operator"
CLIENT_SECRET = "yn58tFMJO0HR8JRnUgKOWKph5FEq1Fn3WgWA4NA7oS4pMSSHmAuXTpxcE6hHtwPB"

# Using main CLOSER APP (MFSM is tracked here as a manager)
CLOSER_APP_ID = "29175634"
CLOSER_APP_TOKEN = "117d3fca26a11d72e48dc62e07d2e793"

# HARD-CODED MFSM MANAGER (all variations)
MFSM_MANAGERS = [
    "MFSM",
    "mfsm",
    "Mfsm"
]

# Commission: $0.59 per watt (same as Spartan)
COMMISSION_PER_WATT = 0.59


def retry_request(func, max_retries=5, initial_delay=2):
    """Retry HTTP requests with exponential backoff"""
    delay = initial_delay
    for attempt in range(max_retries):
        try:
            return func()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as e:
            if attempt < max_retries - 1:
                print(f"   ⚠️  Connection error (attempt {attempt + 1}/{max_retries}): {str(e)[:100]}")
                print(f"   ⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                print(f"   ❌ All {max_retries} retry attempts failed")
                raise
    return None


print("=" * 70)
print("IVAN MFSM STATS DASHBOARD GENERATOR")
print("=" * 70)
print(f"\nMFSM Manager (hard-coded):")
for mgr in MFSM_MANAGERS:
    print(f"  • {mgr}")

# Calculate date ranges
today = datetime.now().date()
year_start = datetime(today.year, 1, 1).date()
month_start = today.replace(day=1)

# Week starts on Monday (weekday=0)
# If today is Monday, week_start = today
# If today is Sunday (weekday=6), week_start = today - 6 days (last Monday)
week_start = today - timedelta(days=today.weekday())

print(f"\n📅 Date Ranges:")
print(f"   Today: {today} ({today.strftime('%A')})")
print(f"   YTD: {year_start} to {today}")
print(f"   MTD: {month_start} to {today}")
print(f"   WTD: {week_start} to {today} (Monday to Today)")

# Auth
print("\n🔑 Authenticating...")
data = {
    'grant_type': 'app',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    'app_id': CLOSER_APP_ID,
    'app_token': CLOSER_APP_TOKEN
}
token = retry_request(lambda: requests.post('https://podio.com/oauth/token', data=data)).json()['access_token']
print("✅ Authenticated")

# Fetch YTD data
print(f"\n📋 Fetching YTD data (since {year_start})...")
headers = {'Authorization': f'OAuth2 {token}', 'Content-Type': 'application/json'}

body = {
    'limit': 500,
    'filters': {
        'appointment-date': {
            'from': str(year_start)
        }
    }
}

all_items = []
offset = 0

while True:
    body['offset'] = offset

    # Retry loop with exponential backoff - keep trying until success
    retry_count = 0
    max_backoff = 300  # Max 5 minutes between retries

    while True:
        try:
            resp = requests.post(f"https://api.podio.com/item/app/{CLOSER_APP_ID}/filter/",
                                headers=headers, json=body, timeout=600)

            if resp.status_code != 200:
                print(f"❌ Error: {resp.status_code}, retrying...")
                retry_count += 1
                backoff = min(2 ** retry_count, max_backoff)
                print(f"   Waiting {backoff} seconds before retry...")
                time.sleep(backoff)
                continue

            # Success! Break out of retry loop
            break

        except (requests.exceptions.Timeout, requests.exceptions.RequestException) as e:
            retry_count += 1
            backoff = min(2 ** retry_count, max_backoff)
            print(f"⚠️  Request failed ({type(e).__name__}), retry #{retry_count} in {backoff}s...")
            time.sleep(backoff)
            continue

    items = resp.json().get('items', [])
    if not items:
        break

    all_items.extend(items)
    print(f"   Fetched {len(all_items)} appointments...")

    if len(items) < 500:
        break
    offset += 500

print(f"✅ Total YTD appointments: {len(all_items)}")

# Process items
print("\n🔍 Processing appointments (MFSM manager only)...")

ytd_appts = []
mtd_appts = []
wtd_appts = []

for item in all_items:
    appt_date = None
    sit_status = None
    close_status = None
    manager = None
    kw = 0
    state = None

    for field in item.get('fields', []):
        ext_id = field.get('external_id', '')
        values = field.get('values', [])
        if not values:
            continue

        if ext_id == 'sit':
            val = values[0].get('value', {})
            sit_status = val.get('text', '') if isinstance(val, dict) else str(val)

        elif ext_id == 'status':
            val = values[0].get('value', {})
            close_status = val.get('text', '') if isinstance(val, dict) else str(val)

        elif ext_id == 'appointment-date':
            date_str = values[0].get('start', '')
            if date_str:
                try:
                    appt_date = datetime.strptime(date_str.split()[0], '%Y-%m-%d').date()
                except:
                    pass

        elif ext_id == 'closer-assigned':
            val = values[0].get('value', {})
            if isinstance(val, dict):
                manager = val.get('name', '').strip()

        elif ext_id == 'kw-size' or ext_id == 'kw':
            try:
                kw = float(values[0].get('value', 0))
            except:
                kw = 0

        elif ext_id == 'address':
            val = values[0].get('value', '')
            if val:
                # Extract state from address (could be in 'state' field or parsed from string)
                if isinstance(val, dict):
                    state = val.get('state', '')
                elif isinstance(val, str):
                    # Address format: "Street, ZIP City, STATE" or "Street, City, State, COUNTRY"
                    # State is typically near the end
                    try:
                        parts = val.split(',')
                        # Check the last few parts for state
                        for part in reversed(parts[-3:]):  # Check last 3 parts
                            part_clean = part.strip().lower()

                            # Skip common country names
                            if part_clean in ['usa', 'united states', 'us']:
                                continue

                            # Check if it's a 2-letter state code
                            if len(part_clean) == 2 and part_clean.isalpha():
                                state = part_clean.upper()
                                break

                            # Check if it's a full state name
                            if part_clean in STATE_MAP:
                                state = STATE_MAP[part_clean]
                                break
                    except:
                        pass  # If parsing fails, state remains None

    # ONLY include if manager is MFSM
    if not manager or manager not in MFSM_MANAGERS:
        continue

    if not appt_date:
        continue

    # Count both "Yes" and "Reset by Closer" as sits
    is_sit = sit_status and (sit_status.lower() == 'yes' or 'reset by closer' in sit_status.lower())
    is_closed = close_status and 'closed' in close_status.lower() and '$' in close_status

    appt_data = {
        'manager': manager,
        'date': appt_date,
        'is_sit': is_sit,
        'is_closed': is_closed,
        'kw': kw if is_closed else 0,  # Only count KW for closed deals
        'state': state if state else 'Unknown',
        'sit_status': sit_status  # Store original sit status to check if updated
    }

    # Only count appointments that have already happened or are today
    # YTD: from year start to today
    if year_start <= appt_date <= today:
        ytd_appts.append(appt_data)

    # MTD: from month start to today (not future dates in the month)
    if month_start <= appt_date <= today:
        mtd_appts.append(appt_data)

    # WTD: from week start (Monday) to today (not future dates in the week)
    if week_start <= appt_date <= today:
        wtd_appts.append(appt_data)

print(f"\n✅ Filtered appointments:")
print(f"   YTD: {len(ytd_appts)} appointments")
print(f"   MTD: {len(mtd_appts)} appointments")
print(f"   WTD: {len(wtd_appts)} appointments")

# Debug: Show WTD appointments by date
if wtd_appts:
    print(f"\n🔍 WTD Appointments Breakdown:")
    from collections import Counter
    wtd_dates = Counter([a['date'] for a in wtd_appts])
    for date in sorted(wtd_dates.keys()):
        print(f"   {date}: {wtd_dates[date]} appointments")

# Calculate stats
def calc_stats(appts):
    total = len(appts)
    if total == 0:
        return {
            'total': 0,
            'sits': 0,
            'closed': 0,
            'sit_rate': 0,
            'close_rate': 0,
            'total_kw': 0,
            'states': []
        }

    # Filter out today's appointments if they don't have a sit status (not yet updated)
    # This prevents skewing sit rates with unprocessed appointments
    today_date = datetime.now().date()
    appts_for_rate_calc = [
        a for a in appts
        if not (a['date'] == today_date and not a.get('sit_status'))
    ]

    total_for_sit_rate = len(appts_for_rate_calc)
    sits = sum(1 for a in appts_for_rate_calc if a['is_sit'])
    closed = sum(1 for a in appts if a['is_closed'])
    total_kw = sum(a['kw'] for a in appts)

    # Count states (use all appts, not filtered)
    state_counts = {}
    for a in appts:
        state = a.get('state', 'Unknown')
        state_counts[state] = state_counts.get(state, 0) + 1

    # Format states as "STATE (count)" sorted by count
    states_list = sorted(state_counts.items(), key=lambda x: x[1], reverse=True)
    states_formatted = [f"{state} ({count})" for state, count in states_list]

    # Sit rate: sits / total appointments (excluding today's unupdated appts)
    # Close rate: closed / sits (not total!)
    return {
        'total': total,
        'sits': sits,
        'closed': closed,
        'sit_rate': round((sits / total_for_sit_rate) * 100, 1) if total_for_sit_rate > 0 else 0,
        'close_rate': round((closed / sits) * 100, 1) if sits > 0 else 0,
        'total_kw': round(total_kw, 2),
        'states': states_formatted
    }

ytd_stats = calc_stats(ytd_appts)
mtd_stats = calc_stats(mtd_appts)
wtd_stats = calc_stats(wtd_appts)

print("\n📊 IVAN MFSM STATISTICS:")
print(f"\n   YTD: {ytd_stats['total']} appts | {ytd_stats['sits']} sits ({ytd_stats['sit_rate']}%) | {ytd_stats['closed']} closed ({ytd_stats['close_rate']}%) | {ytd_stats['total_kw']} KW")
print(f"   YTD States: {', '.join(ytd_stats['states'])}")
print(f"\n   MTD: {mtd_stats['total']} appts | {mtd_stats['sits']} sits ({mtd_stats['sit_rate']}%) | {mtd_stats['closed']} closed ({mtd_stats['close_rate']}%) | {mtd_stats['total_kw']} KW")
print(f"   MTD States: {', '.join(mtd_stats['states'])}")
print(f"\n   WTD: {wtd_stats['total']} appts | {wtd_stats['sits']} sits ({wtd_stats['sit_rate']}%) | {wtd_stats['closed']} closed ({wtd_stats['close_rate']}%) | {wtd_stats['total_kw']} KW")
print(f"   WTD States: {', '.join(wtd_stats['states'])}")

# Generate HTML
print("\n📝 Generating HTML dashboard...")

html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ivan MFSM Stats Dashboard</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            min-height: 100vh;
            padding: 20px;
            color: #fff;
            font-size: 13px;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .header {{
            text-align: center;
            margin-bottom: 30px;
        }}

        .header h1 {{
            font-size: 2.2em;
            margin-bottom: 10px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }}

        .header p {{
            font-size: 1em;
            opacity: 0.8;
        }}

        .last-updated {{
            text-align: center;
            opacity: 0.6;
            margin-bottom: 20px;
            font-size: 0.85em;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 30px;
            margin-bottom: 40px;
        }}

        .stat-card {{
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            border-radius: 15px;
            padding: 20px;
            border: 1px solid rgba(255, 255, 255, 0.1);
            transition: transform 0.3s, box-shadow 0.3s;
        }}

        .stat-card:hover {{
            transform: translateY(-3px);
            box-shadow: 0 15px 40px rgba(102, 126, 234, 0.4);
        }}

        .stat-card h2 {{
            font-size: 1.4em;
            margin-bottom: 15px;
            color: #667eea;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}

        .metric {{
            margin: 12px 0;
        }}

        .metric-label {{
            font-size: 0.8em;
            opacity: 0.7;
            margin-bottom: 5px;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }}

        .metric-value {{
            font-size: 1.6em;
            font-weight: bold;
            color: #fff;
        }}

        .metric-value.percentage {{
            color: #4ade80;
        }}

        .metric-value.money {{
            color: #fbbf24;
        }}

        .metric-row {{
            display: flex;
            justify-content: space-between;
            align-items: baseline;
            padding: 8px 0;
            border-bottom: 1px solid rgba(255, 255, 255, 0.1);
            font-size: 0.95em;
        }}

        .metric-row:last-child {{
            border-bottom: none;
        }}

        .metric-row-label {{
            font-size: 1em;
            opacity: 0.8;
        }}

        .metric-row-value {{
            font-size: 1.1em;
            font-weight: bold;
        }}

        .summary-section {{
            background: rgba(102, 126, 234, 0.1);
            border: 2px solid #667eea;
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
        }}

        .summary-section h2 {{
            font-size: 1.5em;
            margin-bottom: 18px;
            color: #667eea;
        }}

        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 15px;
        }}

        .summary-box {{
            background: rgba(255, 255, 255, 0.05);
            padding: 15px;
            border-radius: 10px;
            text-align: center;
        }}

        .summary-box .label {{
            font-size: 0.85em;
            opacity: 0.7;
            margin-bottom: 8px;
        }}

        .summary-box .value {{
            font-size: 1.8em;
            font-weight: bold;
            color: #4ade80;
        }}

        .footer {{
            text-align: center;
            margin-top: 60px;
            opacity: 0.5;
            font-size: 0.9em;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚡ IVAN MFSM STATS</h1>
            <p>Performance Dashboard (Ivan Solar Boss Manager)</p>
        </div>

        <div class="last-updated">
            Last Updated: {datetime.now().strftime('%B %d, %Y at %I:%M %p')}<br>
            <span style="font-size: 0.75em; opacity: 0.7; font-style: italic;">Auto-updates hourly 8am-8pm daily</span>
        </div>

        <!-- Fixed floating refresh timer -->
        <div id="refreshTimer" style="position: fixed; bottom: 20px; right: 20px; background: rgba(0, 0, 0, 0.9); color: white; padding: 15px 20px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.5); z-index: 9999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; min-width: 200px;">
            <div id="nextRefresh" style="font-size: 13px; font-weight: 600; margin-bottom: 5px; color: #fff;"></div>
            <div id="countdown" style="font-size: 14px; font-weight: 700; color: #00ff88;"></div>
        </div>

        <div class="summary-section">
            <h2>📊 Quick Summary</h2>
            <div class="summary-grid">
                <div class="summary-box">
                    <div class="label">YTD Total Appointments</div>
                    <div class="value">{ytd_stats['total']}</div>
                </div>
                <div class="summary-box">
                    <div class="label">MTD Total Appointments</div>
                    <div class="value">{mtd_stats['total']}</div>
                </div>
                <div class="summary-box">
                    <div class="label">WTD Total Appointments</div>
                    <div class="value">{wtd_stats['total']}</div>
                </div>
            </div>
        </div>

        <div class="stats-grid">
            <!-- YTD Card -->
            <div class="stat-card">
                <h2>📅 YTD (Year to Date)</h2>
                <div class="metric">
                    <div class="metric-label">Sit Rate</div>
                    <div class="metric-value percentage">{ytd_stats['sit_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Close Rate</div>
                    <div class="metric-value percentage">{ytd_stats['close_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">States</div>
                    <div class="metric-value" style="font-size: 1.1em; line-height: 1.5;">{', '.join(ytd_stats['states'])}</div>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total Appointments</span>
                    <span class="metric-row-value">{ytd_stats['total']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Sits</span>
                    <span class="metric-row-value">{ytd_stats['sits']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Closed</span>
                    <span class="metric-row-value">{ytd_stats['closed']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total KW</span>
                    <span class="metric-row-value">{ytd_stats['total_kw']} KW</span>
                </div>
            </div>

            <!-- MTD Card -->
            <div class="stat-card">
                <h2>📆 MTD (Month to Date)</h2>
                <div class="metric">
                    <div class="metric-label">Sit Rate</div>
                    <div class="metric-value percentage">{mtd_stats['sit_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Close Rate</div>
                    <div class="metric-value percentage">{mtd_stats['close_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">States</div>
                    <div class="metric-value" style="font-size: 1.1em; line-height: 1.5;">{', '.join(mtd_stats['states'])}</div>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total Appointments</span>
                    <span class="metric-row-value">{mtd_stats['total']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Sits</span>
                    <span class="metric-row-value">{mtd_stats['sits']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Closed</span>
                    <span class="metric-row-value">{mtd_stats['closed']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total KW</span>
                    <span class="metric-row-value">{mtd_stats['total_kw']} KW</span>
                </div>
            </div>

            <!-- WTD Card -->
            <div class="stat-card">
                <h2>📅 WTD (Week to Date)</h2>
                <div class="metric">
                    <div class="metric-label">Sit Rate</div>
                    <div class="metric-value percentage">{wtd_stats['sit_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">Close Rate</div>
                    <div class="metric-value percentage">{wtd_stats['close_rate']}%</div>
                </div>
                <div class="metric">
                    <div class="metric-label">States</div>
                    <div class="metric-value" style="font-size: 1.1em; line-height: 1.5;">{', '.join(wtd_stats['states'])}</div>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total Appointments</span>
                    <span class="metric-row-value">{wtd_stats['total']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Sits</span>
                    <span class="metric-row-value">{wtd_stats['sits']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Closed</span>
                    <span class="metric-row-value">{wtd_stats['closed']}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-row-label">Total KW</span>
                    <span class="metric-row-value">{wtd_stats['total_kw']} KW</span>
                </div>
            </div>
        </div>

        <div class="footer">
            <p>Ivan MFSM Stats Dashboard | Data from Podio Closer App</p>
            <p>YTD: {year_start} to {today} | MTD: {month_start} to {today} | WTD: {week_start} ({week_start.strftime('%A')}) to {today} ({today.strftime('%A')})</p>
            <p style="margin-top: 10px;">Tracking: MFSM Manager (Ivan Solar Boss)</p>
        </div>
    </div>

    <script>
        // Next refresh calculator and countdown timer
        function updateRefreshInfo() {{
            const now = new Date();
            const cronTimes = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]; // Hourly from 8am to 8pm

            // Find next refresh time
            let nextRefreshHour = null;
            const currentHour = now.getHours();

            // Find next cron time today
            for (let hour of cronTimes) {{
                if (hour > currentHour) {{
                    nextRefreshHour = hour;
                    break;
                }}
            }}

            // If no more cron times today, use first one tomorrow
            const nextRefresh = new Date(now);
            if (nextRefreshHour === null) {{
                nextRefresh.setDate(nextRefresh.getDate() + 1);
                nextRefreshHour = cronTimes[0];
            }}
            nextRefresh.setHours(nextRefreshHour, 0, 0, 0);

            // Calculate time difference
            const diff = nextRefresh - now;
            const hours = Math.floor(diff / (1000 * 60 * 60));
            const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            const seconds = Math.floor((diff % (1000 * 60)) / 1000);

            // Format next refresh time
            const options = {{ hour: 'numeric', minute: '2-digit', hour12: true }};
            const timeStr = nextRefresh.toLocaleTimeString('en-US', options);
            const dateStr = nextRefresh.toLocaleDateString('en-US', {{ month: 'short', day: 'numeric' }});

            // Update display
            document.getElementById('nextRefresh').textContent = `Next refresh: ${{timeStr}}`;
            document.getElementById('countdown').textContent = `Time until: ${{hours}}h ${{minutes}}m`;
        }}

        // Update immediately and then every second
        updateRefreshInfo();
        setInterval(updateRefreshInfo, 1000);
    </script>
</body>
</html>
"""

with open("/Users/mosesherrera/Desktop/Podio Api Dashboard/ivan_mfsm_dashboard.html", "w") as f:
    f.write(html)

print("✅ Dashboard saved: ivan_mfsm_dashboard.html")
print("\n" + "=" * 70)
print("DONE!")
print("=" * 70)
