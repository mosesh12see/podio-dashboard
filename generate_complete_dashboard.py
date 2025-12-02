#!/usr/bin/env python3
"""
Complete Podio Dashboard Generator
Shows all sits and assigns by manager with WTD, MTD, YTD, and month-by-month breakdown
Excludes Infinit AI appointments
"""

import json
import requests
from datetime import datetime, timedelta
from collections import defaultdict

# Podio credentials
CLIENT_ID = "gpt-operator"
CLIENT_SECRET = "yn58tFMJO0HR8JRnUgKOWKph5FEq1Fn3WgWA4NA7oS4pMSSHmAuXTpxcE6hHtwPB"

# Closer App (Main - S.E.E. workspace)
CLOSER_APP_ID = "29175634"
CLOSER_APP_TOKEN = "117d3fca26a11d72e48dc62e07d2e793"

# Cache file
CACHE_FILE = "closer_app_cache.json"

# Revenue and Profit per close
# $5.10/watt × 8,150 watts per close = $41,565
# $0.42/watt × 8,150 watts per close = $3,423
WATTS_PER_CLOSE = 8150
REVENUE_PER_WATT = 5.10
PROFIT_PER_WATT = 0.42
REVENUE_PER_CLOSE = int(WATTS_PER_CLOSE * REVENUE_PER_WATT)  # $41,565
PROFIT_PER_CLOSE = int(WATTS_PER_CLOSE * PROFIT_PER_WATT)   # $3,423


def get_field_value(item, field_label, default=""):
    """Extract field value from Podio item by field label"""
    for field in item.get('fields', []):
        if field.get('label') == field_label:
            values = field.get('values', [])
            if not values:
                return default

            if field['type'] == 'text':
                return values[0].get('value', default)
            elif field['type'] == 'calculation':
                val = values[0].get('value', default)
                return val if val else default
            elif field['type'] == 'category':
                options = values[0].get('value', {})
                if isinstance(options, dict):
                    return options.get('text', default)
                return str(options) if options else default
            elif field['type'] == 'date':
                return values[0].get('start', default)
            elif field['type'] == 'app':
                ref_values = []
                for val in values:
                    ref_item = val.get('value', {})
                    if isinstance(ref_item, dict):
                        ref_title = ref_item.get('title', '')
                        if ref_title:
                            ref_values.append(ref_title)
                return ', '.join(ref_values) if ref_values else default
            elif field['type'] == 'contact':
                contacts = []
                for val in values:
                    contact = val.get('value', {})
                    if isinstance(contact, dict):
                        name = contact.get('name', '')
                        if name:
                            contacts.append(name)
                return ', '.join(contacts) if contacts else default
            elif field['type'] == 'number':
                return values[0].get('value', default)
            else:
                return str(values[0].get('value', default))
    return default


def is_infinit_ai_appointment(item):
    """Check if appointment is from Infinit AI (to be excluded)"""
    partner = get_field_value(item, 'Partner assigned from Full org app', '').lower()
    manager = get_field_value(item, '* MANAGER', '').lower()
    customer = get_field_value(item, 'Customer Full', '').lower()

    return ('chase' in partner or 'chase' in manager or
            'infinite' in partner or 'infinite' in manager or
            'infinit' in partner or 'infinit' in manager or
            'chase' in customer or 'infinite' in customer or 'infinit' in customer)


def load_cache():
    """Load Closer App data from cache"""
    try:
        with open(CACHE_FILE, 'r') as f:
            cache = json.load(f)
        print(f"✅ Loaded {len(cache['data'])} appointments from cache (saved at {cache['timestamp']})")
        return cache['data'], cache['timestamp']
    except Exception as e:
        print(f"❌ Error loading cache: {e}")
        return None, None


def analyze_data(items):
    """Analyze appointments and generate statistics"""

    # Calculate date boundaries
    now = datetime.now()
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = today_start - timedelta(seconds=1)
    year_start = datetime(now.year, 1, 1)
    month_start = datetime(now.year, now.month, 1)
    week_start = now - timedelta(days=now.weekday())  # Monday of current week
    week_start = week_start.replace(hour=0, minute=0, second=0, microsecond=0)
    last_14_days_start = today_start - timedelta(days=14)

    # Initialize stats by manager
    manager_stats = defaultdict(lambda: {
        'wtd_sits': 0,
        'wtd_assigns': 0,
        'wtd_closes': 0,
        'mtd_sits': 0,
        'mtd_assigns': 0,
        'mtd_closes': 0,
        'ytd_sits': 0,
        'ytd_assigns': 0,
        'ytd_closes': 0,
        # Stats excluding today (for sit rate calculations)
        'wtd_sits_excl_today': 0,
        'wtd_assigns_excl_today': 0,
        'mtd_sits_excl_today': 0,
        'mtd_assigns_excl_today': 0,
        'ytd_sits_excl_today': 0,
        'ytd_assigns_excl_today': 0,
        # Last 14 days stats (for low performer tracking)
        'last_14_sits': 0,
        'last_14_assigns': 0,
        'monthly': defaultdict(lambda: {'sits': 0, 'assigns': 0, 'closes': 0})
    })

    # Overall totals
    totals = {
        'wtd_sits': 0,
        'wtd_assigns': 0,
        'wtd_closes': 0,
        'mtd_sits': 0,
        'mtd_assigns': 0,
        'mtd_closes': 0,
        'ytd_sits': 0,
        'ytd_assigns': 0,
        'ytd_closes': 0,
        # Stats excluding today (for sit rate calculations)
        'wtd_sits_excl_today': 0,
        'wtd_assigns_excl_today': 0,
        'mtd_sits_excl_today': 0,
        'mtd_assigns_excl_today': 0,
        'ytd_sits_excl_today': 0,
        'ytd_assigns_excl_today': 0,
        'monthly': defaultdict(lambda: {'sits': 0, 'assigns': 0, 'closes': 0}),
        'total_appointments': 0,
        'excluded_infinit_ai': 0
    }

    # Process each appointment
    for item in items:
        # Check if Infinit AI appointment (exclude)
        if is_infinit_ai_appointment(item):
            totals['excluded_infinit_ai'] += 1
            continue

        # Get appointment data
        appt_date_str = get_field_value(item, 'Appointment Date')
        if not appt_date_str:
            continue

        # Parse date
        try:
            if 'T' in str(appt_date_str):
                date_part = appt_date_str.split('T')[0]
            elif ' ' in str(appt_date_str):
                date_part = appt_date_str.split(' ')[0]
            else:
                date_part = appt_date_str

            appt_date = datetime.fromisoformat(date_part)
        except (ValueError, AttributeError, TypeError):
            continue

        # Get manager and sit status
        manager = get_field_value(item, '* MANAGER', 'Unknown')
        if not manager or manager == 'Unknown':
            manager = 'Unassigned'

        sit_status = get_field_value(item, 'Sit', '').strip()
        is_sit = sit_status.lower() in ['yes', 'reset by closer']

        # Check if it's a closed deal
        status = get_field_value(item, 'Status', '').lower()
        is_closed = ('closed' in status or 'close' in status or 'sold' in status)

        # Count as appointment
        totals['total_appointments'] += 1

        # Get month key
        month_key = appt_date.strftime('%Y-%m')
        month_name = appt_date.strftime('%B %Y')

        # Update monthly totals
        if is_sit:
            totals['monthly'][month_key]['sits'] += 1
            manager_stats[manager]['monthly'][month_key]['sits'] += 1
        else:
            totals['monthly'][month_key]['assigns'] += 1
            manager_stats[manager]['monthly'][month_key]['assigns'] += 1

        if is_closed:
            totals['monthly'][month_key]['closes'] += 1
            manager_stats[manager]['monthly'][month_key]['closes'] += 1

        # Check if appointment is from today
        is_from_today = appt_date >= today_start

        # Update last 14 days stats (for low performer tracking)
        if appt_date >= last_14_days_start and appt_date < today_start:
            if is_sit:
                manager_stats[manager]['last_14_sits'] += 1
            else:
                manager_stats[manager]['last_14_assigns'] += 1

        # Update YTD - exclude future appointments
        if appt_date >= year_start and appt_date < today_start + timedelta(days=1):
            if is_sit:
                totals['ytd_sits'] += 1
                manager_stats[manager]['ytd_sits'] += 1
            else:
                totals['ytd_assigns'] += 1
                manager_stats[manager]['ytd_assigns'] += 1

            if is_closed:
                totals['ytd_closes'] += 1
                manager_stats[manager]['ytd_closes'] += 1

            # Track YTD excluding today for sit rate
            if not is_from_today:
                if is_sit:
                    totals['ytd_sits_excl_today'] += 1
                    manager_stats[manager]['ytd_sits_excl_today'] += 1
                else:
                    totals['ytd_assigns_excl_today'] += 1
                    manager_stats[manager]['ytd_assigns_excl_today'] += 1

        # Update MTD - exclude future appointments
        if appt_date >= month_start and appt_date < today_start + timedelta(days=1):
            if is_sit:
                totals['mtd_sits'] += 1
                manager_stats[manager]['mtd_sits'] += 1
            else:
                totals['mtd_assigns'] += 1
                manager_stats[manager]['mtd_assigns'] += 1

            if is_closed:
                totals['mtd_closes'] += 1
                manager_stats[manager]['mtd_closes'] += 1

            # Track MTD excluding today for sit rate
            if not is_from_today:
                if is_sit:
                    totals['mtd_sits_excl_today'] += 1
                    manager_stats[manager]['mtd_sits_excl_today'] += 1
                else:
                    totals['mtd_assigns_excl_today'] += 1
                    manager_stats[manager]['mtd_assigns_excl_today'] += 1

        # Update WTD - exclude future appointments
        if appt_date >= week_start and appt_date < today_start + timedelta(days=1):
            if is_sit:
                totals['wtd_sits'] += 1
                manager_stats[manager]['wtd_sits'] += 1
            else:
                totals['wtd_assigns'] += 1
                manager_stats[manager]['wtd_assigns'] += 1

            if is_closed:
                totals['wtd_closes'] += 1
                manager_stats[manager]['wtd_closes'] += 1

            # Track WTD excluding today for sit rate
            if not is_from_today:
                if is_sit:
                    totals['wtd_sits_excl_today'] += 1
                    manager_stats[manager]['wtd_sits_excl_today'] += 1
                else:
                    totals['wtd_assigns_excl_today'] += 1
                    manager_stats[manager]['wtd_assigns_excl_today'] += 1

    # Calculate MTD pacing/projection
    days_elapsed = now.day  # Current day of month
    days_in_month = (datetime(now.year, now.month + 1, 1) - timedelta(days=1)).day if now.month < 12 else 31
    days_remaining = days_in_month - days_elapsed

    # Calculate average total appointments per day and projected total
    mtd_total_current = totals['mtd_sits'] + totals['mtd_assigns']
    mtd_avg_per_day = mtd_total_current / days_elapsed if days_elapsed > 0 else 0
    mtd_projected_total = mtd_total_current + (mtd_avg_per_day * days_remaining)

    totals['mtd_days_elapsed'] = days_elapsed
    totals['mtd_days_in_month'] = days_in_month
    totals['mtd_days_remaining'] = days_remaining
    totals['mtd_avg_per_day'] = mtd_avg_per_day
    totals['mtd_projected_total'] = mtd_projected_total

    return manager_stats, totals


def identify_low_performers(manager_stats):
    """Identify managers with 3+ sits and <50% sit rate in last 14 days"""
    low_performers = []

    for manager, stats in manager_stats.items():
        last_14_total = stats['last_14_sits'] + stats['last_14_assigns']

        # Only consider managers with 3+ sits
        if stats['last_14_sits'] >= 3 and last_14_total > 0:
            sit_rate = (stats['last_14_sits'] / last_14_total) * 100

            # Check if sit rate is under 50%
            if sit_rate < 50:
                low_performers.append({
                    'manager': manager,
                    'sits': stats['last_14_sits'],
                    'assigns': stats['last_14_assigns'],
                    'total': last_14_total,
                    'sit_rate': sit_rate
                })

    # Sort by sit rate (lowest first)
    low_performers.sort(key=lambda x: x['sit_rate'])

    return low_performers


def generate_html(manager_stats, totals, data_timestamp):
    """Generate HTML dashboard"""

    now = datetime.now()

    # Sort managers by YTD sits (highest to lowest)
    sorted_managers = sorted(
        manager_stats.items(),
        key=lambda x: x[1]['ytd_sits'],
        reverse=True
    )

    # Identify low performers (5+ sits, <50% sit rate in last 14 days)
    low_performers = identify_low_performers(manager_stats)

    # Get last 12 months for month-by-month section
    months = []
    for i in range(11, -1, -1):
        month_date = now - timedelta(days=30 * i)
        month_key = month_date.strftime('%Y-%m')
        month_name = month_date.strftime('%B %Y')
        months.append((month_key, month_name))

    # Generate month by month section HTML first (to be placed at top)
    month_by_month_html = """
        <!-- Month by Month Section -->
        <div class="section">
            <h2>📊 Month by Month Breakdown (Last 12 Months)</h2>
            <div class="monthly-grid">
"""

    # Monthly cards
    for month_key, month_name in months:
        month_data = totals['monthly'].get(month_key, {'sits': 0, 'assigns': 0, 'closes': 0})
        total = month_data['sits'] + month_data['assigns']
        close_rate = (month_data['closes'] / total * 100) if total > 0 else 0
        revenue = month_data['closes'] * REVENUE_PER_CLOSE
        profit = month_data['closes'] * PROFIT_PER_CLOSE

        # Calculate sit revenue and close revenue using watt-based calculation
        sit_revenue = month_data['sits'] * 550
        close_revenue = month_data['closes'] * REVENUE_PER_CLOSE  # closes × $5.10/watt × 8150 watts
        total_revenue = sit_revenue + close_revenue

        # Calculate profit using watt-based calculation
        cost = month_data['sits'] * 350
        close_profit = month_data['closes'] * PROFIT_PER_CLOSE  # closes × $0.42/watt × 8150 watts
        profit = (sit_revenue - cost) + close_profit

        # Check if this is the current month and calculate projection
        projection_html = ""
        weeks_in_month = 4.33

        if month_key == now.strftime('%Y-%m'):  # Current month
            days_elapsed = now.day
            days_in_month = (datetime(now.year, now.month % 12 + 1, 1) - timedelta(days=1)).day if now.month < 12 else 31
            if now.month == 12:
                days_in_month = 31
            days_remaining = days_in_month - days_elapsed

            if days_elapsed > 0 and total > 0:
                # Project total appointments
                avg_per_day = total / days_elapsed
                projected_total = total + (avg_per_day * days_remaining)

                # Project sits based on current ratio
                sits_ratio = month_data['sits'] / total if total > 0 else 0
                projected_sits = projected_total * sits_ratio

                # Calculate projected avg sits per week
                avg_sits_per_week = projected_sits / weeks_in_month

                projection_html = f'<div style="margin-top: 8px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.2);"><strong style="color: #00d4ff;">Projected: {projected_total:.0f}</strong><br><span style="font-size: 11px; opacity: 0.7;">Based on current pace</span></div>'
        else:
            # For past months, use actual sits
            avg_sits_per_week = month_data['sits'] / weeks_in_month if month_data['sits'] > 0 else 0

        month_by_month_html += f"""
                <div class="month-card">
                    <h3>{month_name}</h3>
                    <div class="total">{total}</div>
                    <div class="breakdown">
                        {month_data['sits']} sits • {month_data['assigns']} assigns<br>
                        {month_data['closes']} closes ({close_rate:.1f}%)<br>
                        <span style="color: #00ff88;">Avg: {avg_sits_per_week:.1f} sits/week</span><br>
                        Sit Rev: ${sit_revenue:,.0f} | Close Rev: ${close_revenue:,.0f}<br>
                        <strong>Total: ${total_revenue:,.0f}</strong><br>
                        <strong>Profit: ${profit:,.0f}</strong>
                        {projection_html}
                    </div>
                </div>
"""

    month_by_month_html += """
            </div>
        </div>
"""

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Complete Podio Dashboard - Sits & Assigns</title>
    <meta charset="utf-8">
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        .container {{ max-width: 1600px; margin: 0 auto; }}
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }}
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
            margin: 5px 0;
        }}

        .totals-banner {{
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 20px;
            color: white;
            box-shadow: 0 5px 15px rgba(0,0,0,0.4);
        }}
        .totals-banner h2 {{
            font-size: 1.3em;
            margin-bottom: 12px;
            text-align: center;
        }}

        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 10px;
            margin-top: 12px;
        }}
        .stat-card {{
            background: rgba(255,255,255,0.1);
            padding: 12px;
            border-radius: 8px;
            text-align: center;
        }}
        .stat-card h3 {{
            font-size: 0.75em;
            opacity: 0.85;
            margin-bottom: 6px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-card .number {{
            font-size: 1.8em;
            font-weight: bold;
        }}
        .stat-card .breakdown {{
            font-size: 0.7em;
            margin-top: 5px;
            opacity: 0.8;
        }}

        .section {{
            background: white;
            border-radius: 15px;
            padding: 30px;
            margin-bottom: 30px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }}
        .section h2 {{
            color: #667eea;
            font-size: 2em;
            margin-bottom: 20px;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }}

        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
        }}
        th {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 1px;
        }}
        td {{
            padding: 15px;
            border-bottom: 1px solid #e0e0e0;
        }}
        tr:hover {{
            background: #f5f5f5;
        }}
        .manager-name {{
            font-weight: 600;
            color: #2c3e50;
            font-size: 1.1em;
        }}
        .sits-count {{
            color: #27ae60;
            font-weight: bold;
            font-size: 1.2em;
        }}
        .assigns-count {{
            color: #3498db;
            font-weight: bold;
            font-size: 1.2em;
        }}
        .total-count {{
            color: #8e44ad;
            font-weight: bold;
            font-size: 1.2em;
        }}

        .monthly-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 15px;
            margin-top: 20px;
        }}
        .month-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            text-align: center;
        }}
        .month-card h3 {{
            font-size: 0.9em;
            margin-bottom: 10px;
            opacity: 0.9;
        }}
        .month-card .total {{
            font-size: 2em;
            font-weight: bold;
            margin: 10px 0;
        }}
        .month-card .breakdown {{
            font-size: 0.8em;
            opacity: 0.85;
        }}

        .info-banner {{
            background: #fff3cd;
            border-left: 4px solid #ffc107;
            padding: 15px;
            margin-bottom: 20px;
            border-radius: 5px;
        }}
        .info-banner p {{
            color: #856404;
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Complete Podio Dashboard</h1>
            <p>All Sits & Assigns by Manager (Excluding Infinit AI)</p>
            <p style="font-size: 0.9em; opacity: 0.8;">Last Updated: {data_timestamp}</p>
            <p style="font-size: 0.8em; opacity: 0.7; margin-top: 10px;">Generated: {now.strftime('%B %d, %Y at %I:%M %p')}</p>
            <p style="font-size: 0.75em; opacity: 0.6; margin-top: 5px; font-style: italic;">cron job: hourly from 8am to 8pm</p>
        </div>

        <!-- Fixed floating refresh timer -->
        <div id="refreshTimer" style="position: fixed; bottom: 20px; right: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 20px; border-radius: 10px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3); z-index: 9999; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; min-width: 200px; border: 2px solid rgba(255, 255, 255, 0.3);">
            <div id="nextRefresh" style="font-size: 13px; font-weight: 600; margin-bottom: 5px; color: #fff;"></div>
            <div id="countdown" style="font-size: 14px; font-weight: 700; color: #ffeb3b;"></div>
        </div>

{month_by_month_html}

        <div class="totals-banner">
            <h2>📈 Overall Totals</h2>
            <div class="stats-grid">
                <div class="stat-card">
                    <h3>WTD Total</h3>
                    <div class="number">{totals['wtd_sits'] + totals['wtd_assigns']}</div>
                    <div class="breakdown">{totals['wtd_sits']} sits • {totals['wtd_assigns']} assigns • {totals['wtd_closes']} closes</div>
                </div>
                <div class="stat-card">
                    <h3>WTD Close Rate</h3>
                    <div class="number">{(totals['wtd_closes'] / (totals['wtd_sits'] + totals['wtd_assigns']) * 100) if (totals['wtd_sits'] + totals['wtd_assigns']) > 0 else 0:.1f}%</div>
                    <div class="breakdown">{totals['wtd_closes']} closes this week</div>
                </div>
                <div class="stat-card">
                    <h3>WTD Revenue</h3>
                    <div class="number">${totals['wtd_closes'] * REVENUE_PER_CLOSE:,.0f}</div>
                    <div class="breakdown">Profit: ${totals['wtd_closes'] * PROFIT_PER_CLOSE:,.0f}</div>
                </div>
                <div class="stat-card">
                    <h3>MTD Total</h3>
                    <div class="number">{totals['mtd_sits'] + totals['mtd_assigns']}</div>
                    <div class="breakdown">{totals['mtd_sits']} sits • {totals['mtd_assigns']} assigns • {totals['mtd_closes']} closes</div>
                </div>
                <div class="stat-card">
                    <h3>MTD Close Rate</h3>
                    <div class="number">{(totals['mtd_closes'] / (totals['mtd_sits'] + totals['mtd_assigns']) * 100) if (totals['mtd_sits'] + totals['mtd_assigns']) > 0 else 0:.1f}%</div>
                    <div class="breakdown">{totals['mtd_closes']} closes this month</div>
                </div>
                <div class="stat-card">
                    <h3>MTD Revenue</h3>
                    <div class="number">${totals['mtd_closes'] * REVENUE_PER_CLOSE:,.0f}</div>
                    <div class="breakdown">Profit: ${totals['mtd_closes'] * PROFIT_PER_CLOSE:,.0f}</div>
                </div>
                <div class="stat-card">
                    <h3>MTD Pacing</h3>
                    <div class="number">{totals['mtd_projected_total']:.0f} appts</div>
                    <div class="breakdown">Avg: {totals['mtd_avg_per_day']:.1f}/day • {totals['mtd_days_remaining']} days left</div>
                </div>
                <div class="stat-card">
                    <h3>YTD Total</h3>
                    <div class="number">{totals['ytd_sits'] + totals['ytd_assigns']}</div>
                    <div class="breakdown">{totals['ytd_sits']} sits • {totals['ytd_assigns']} assigns • {totals['ytd_closes']} closes</div>
                </div>
                <div class="stat-card">
                    <h3>YTD Close Rate</h3>
                    <div class="number">{(totals['ytd_closes'] / (totals['ytd_sits'] + totals['ytd_assigns']) * 100) if (totals['ytd_sits'] + totals['ytd_assigns']) > 0 else 0:.1f}%</div>
                    <div class="breakdown">{totals['ytd_closes']} closes this year</div>
                </div>
                <div class="stat-card">
                    <h3>YTD Revenue</h3>
                    <div class="number">${totals['ytd_closes'] * REVENUE_PER_CLOSE:,.0f}</div>
                    <div class="breakdown">Profit: ${totals['ytd_closes'] * PROFIT_PER_CLOSE:,.0f}</div>
                </div>
                <div class="stat-card">
                    <h3>YTD Sit Rate</h3>
                    <div class="number">{(totals['ytd_sits_excl_today'] / (totals['ytd_sits_excl_today'] + totals['ytd_assigns_excl_today']) * 100) if (totals['ytd_sits_excl_today'] + totals['ytd_assigns_excl_today']) > 0 else 0:.1f}%</div>
                    <div class="breakdown">Percentage of sits (excl. today)</div>
                </div>
            </div>
        </div>

        <!-- Low Performers Alert Section (Last 14 Days) -->
"""

    # Add low performers section if there are any
    if low_performers:
        html += """
        <div class="section" style="background: linear-gradient(135deg, #8b0000 0%, #5c0a0a 100%); color: white; border: 2px solid #4a0000; padding: 15px; margin-bottom: 20px;">
            <h2 style="color: white; border-bottom-color: rgba(255,255,255,0.3); font-size: 1.3em; margin-bottom: 12px; padding-bottom: 8px;">⚠️ Low Sit Rate Alert (Last 14 Days)</h2>
            <p style="margin-bottom: 12px; opacity: 0.9; font-size: 0.85em;">Managers with 3+ sits and sit rate under 50% in the last 14 days (excluding today):</p>
            <table>
                <thead>
                    <tr style="background: rgba(0,0,0,0.4);">
                        <th style="padding: 10px; font-size: 0.75em;">Manager</th>
                        <th style="padding: 10px; font-size: 0.75em;">Sits</th>
                        <th style="padding: 10px; font-size: 0.75em;">Assigns</th>
                        <th style="padding: 10px; font-size: 0.75em;">Total</th>
                        <th style="padding: 10px; font-size: 0.75em;">Sit Rate</th>
                    </tr>
                </thead>
                <tbody>
"""

        for performer in low_performers:
            html += f"""
                    <tr style="background: rgba(255,255,255,0.05);">
                        <td class="manager-name" style="color: white; padding: 10px; font-size: 0.95em;">{performer['manager']}</td>
                        <td class="sits-count" style="color: #ffd700; padding: 10px;">{performer['sits']}</td>
                        <td class="assigns-count" style="color: #87ceeb; padding: 10px;">{performer['assigns']}</td>
                        <td class="total-count" style="color: white; padding: 10px;">{performer['total']}</td>
                        <td style="color: #ff6b6b; font-weight: bold; font-size: 1em; padding: 10px;">{performer['sit_rate']:.1f}%</td>
                    </tr>
"""

        html += """
                </tbody>
            </table>
        </div>
"""

    # Calculate WTD totals for header (excluding today)
    wtd_total_sits = totals['wtd_sits_excl_today']
    wtd_total_assigns = totals['wtd_assigns_excl_today']
    wtd_total = wtd_total_sits + wtd_total_assigns
    wtd_avg_sit_rate = (wtd_total_sits / wtd_total * 100) if wtd_total > 0 else 0

    html += f"""
        <!-- WTD Section -->
        <div class="section">
            <h2 style="display: flex; justify-content: space-between; align-items: center;">
                <span>📅 Week to Date (WTD)</span>
                <span style="font-size: 0.6em; font-weight: normal; color: #888;">
                    Total: {wtd_total_sits} sits • {wtd_total_assigns} assigns •
                    <strong style="color: #667eea;">Avg Sit Rate: {wtd_avg_sit_rate:.1f}%</strong>
                </span>
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>Manager</th>
                        <th>Sits</th>
                        <th>Assigns</th>
                        <th>Total</th>
                        <th>Closes</th>
                        <th>Sit Rate</th>
                        <th>Revenue</th>
                        <th>Profit</th>
                    </tr>
                </thead>
                <tbody>
"""

    # WTD rows - Sort by WTD sits (highest to lowest)
    wtd_sorted = sorted(
        manager_stats.items(),
        key=lambda x: x[1]['wtd_sits'],
        reverse=True
    )
    for manager, stats in wtd_sorted:
        total = stats['wtd_sits'] + stats['wtd_assigns']
        if total > 0:
            # Calculate sit rate excluding today's appointments
            total_excl_today = stats['wtd_sits_excl_today'] + stats['wtd_assigns_excl_today']
            sit_rate = (stats['wtd_sits_excl_today'] / total_excl_today * 100) if total_excl_today > 0 else 0
            revenue = stats['wtd_closes'] * REVENUE_PER_CLOSE
            profit = stats['wtd_closes'] * PROFIT_PER_CLOSE
            html += f"""
                    <tr>
                        <td class="manager-name">{manager}</td>
                        <td class="sits-count">{stats['wtd_sits']}</td>
                        <td class="assigns-count">{stats['wtd_assigns']}</td>
                        <td class="total-count">{total}</td>
                        <td class="total-count">{stats['wtd_closes']}</td>
                        <td><strong style="color: #667eea;">{sit_rate:.1f}%</strong></td>
                        <td class="stat-value money">${revenue:,.0f}</td>
                        <td class="stat-value money">${profit:,.0f}</td>
                    </tr>
"""

    html += """
                </tbody>
            </table>
        </div>
"""

    # Calculate MTD totals for header (excluding today)
    mtd_total_sits = totals['mtd_sits_excl_today']
    mtd_total_assigns = totals['mtd_assigns_excl_today']
    mtd_total = mtd_total_sits + mtd_total_assigns
    mtd_avg_sit_rate = (mtd_total_sits / mtd_total * 100) if mtd_total > 0 else 0

    html += f"""
        <!-- MTD Section -->
        <div class="section">
            <h2 style="display: flex; justify-content: space-between; align-items: center;">
                <span>📅 Month to Date (MTD)</span>
                <span style="font-size: 0.6em; font-weight: normal; color: #888;">
                    Total: {mtd_total_sits} sits • {mtd_total_assigns} assigns •
                    <strong style="color: #667eea;">Avg Sit Rate: {mtd_avg_sit_rate:.1f}%</strong>
                </span>
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>Manager</th>
                        <th>Sits</th>
                        <th>Assigns</th>
                        <th>Total</th>
                        <th>Closes</th>
                        <th>Sit Rate</th>
                        <th>Revenue</th>
                        <th>Profit</th>
                    </tr>
                </thead>
                <tbody>
"""

    # MTD rows - Sort by MTD sits (highest to lowest)
    mtd_sorted = sorted(
        manager_stats.items(),
        key=lambda x: x[1]['mtd_sits'],
        reverse=True
    )
    for manager, stats in mtd_sorted:
        total = stats['mtd_sits'] + stats['mtd_assigns']
        if total > 0:
            # Calculate sit rate excluding today's appointments
            total_excl_today = stats['mtd_sits_excl_today'] + stats['mtd_assigns_excl_today']
            sit_rate = (stats['mtd_sits_excl_today'] / total_excl_today * 100) if total_excl_today > 0 else 0
            revenue = stats['mtd_closes'] * REVENUE_PER_CLOSE
            profit = stats['mtd_closes'] * PROFIT_PER_CLOSE
            html += f"""
                    <tr>
                        <td class="manager-name">{manager}</td>
                        <td class="sits-count">{stats['mtd_sits']}</td>
                        <td class="assigns-count">{stats['mtd_assigns']}</td>
                        <td class="total-count">{total}</td>
                        <td class="total-count">{stats['mtd_closes']}</td>
                        <td><strong style="color: #667eea;">{sit_rate:.1f}%</strong></td>
                        <td class="stat-value money">${revenue:,.0f}</td>
                        <td class="stat-value money">${profit:,.0f}</td>
                    </tr>
"""

    html += """
                </tbody>
            </table>
        </div>
"""

    # Calculate YTD totals for header (excluding today)
    ytd_total_sits = totals['ytd_sits_excl_today']
    ytd_total_assigns = totals['ytd_assigns_excl_today']
    ytd_total = ytd_total_sits + ytd_total_assigns
    ytd_avg_sit_rate = (ytd_total_sits / ytd_total * 100) if ytd_total > 0 else 0

    html += f"""
        <!-- YTD Section -->
        <div class="section">
            <h2 style="display: flex; justify-content: space-between; align-items: center;">
                <span>📅 Year to Date (YTD)</span>
                <span style="font-size: 0.6em; font-weight: normal; color: #888;">
                    Total: {ytd_total_sits} sits • {ytd_total_assigns} assigns •
                    <strong style="color: #667eea;">Avg Sit Rate: {ytd_avg_sit_rate:.1f}%</strong>
                </span>
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>Manager</th>
                        <th>Sits</th>
                        <th>Assigns</th>
                        <th>Total</th>
                        <th>Closes</th>
                        <th>Sit Rate</th>
                        <th>Revenue</th>
                        <th>Profit</th>
                    </tr>
                </thead>
                <tbody>
"""

    # YTD rows - Using sorted_managers (already sorted by YTD sits, highest to lowest)
    for manager, stats in sorted_managers:
        total = stats['ytd_sits'] + stats['ytd_assigns']
        if total > 0:
            # Calculate sit rate excluding today's appointments
            total_excl_today = stats['ytd_sits_excl_today'] + stats['ytd_assigns_excl_today']
            sit_rate = (stats['ytd_sits_excl_today'] / total_excl_today * 100) if total_excl_today > 0 else 0
            revenue = stats['ytd_closes'] * REVENUE_PER_CLOSE
            profit = stats['ytd_closes'] * PROFIT_PER_CLOSE
            html += f"""
                    <tr>
                        <td class="manager-name">{manager}</td>
                        <td class="sits-count">{stats['ytd_sits']}</td>
                        <td class="assigns-count">{stats['ytd_assigns']}</td>
                        <td class="total-count">{total}</td>
                        <td class="total-count">{stats['ytd_closes']}</td>
                        <td><strong style="color: #667eea;">{sit_rate:.1f}%</strong></td>
                        <td class="stat-value money">${revenue:,.0f}</td>
                        <td class="stat-value money">${profit:,.0f}</td>
                    </tr>
"""

    html += """
                </tbody>
            </table>
        </div>

        <div class="info-banner" style="margin-top: 30px;">
            <p><strong>ℹ️ Dashboard Information:</strong></p>
            <p>• <strong>Sits:</strong> Appointments marked as "Yes" or "Reset by Closer" in the Sit field</p>
            <p>• <strong>Assigns:</strong> All other appointments (appointments that did not result in a sit)</p>
            <p>• <strong>Closes:</strong> Appointments with "Closed" or "Sold" status</p>
            <p>• <strong>Revenue per Close:</strong> ${REVENUE_PER_CLOSE:,}</p>
            <p>• <strong>Profit per Close:</strong> ${PROFIT_PER_CLOSE:,}</p>
            <p>• <strong>Excluded:</strong> {totals['excluded_infinit_ai']} Infinit AI appointments (not counted)</p>
            <p>• <strong>Total Appointments:</strong> {totals['total_appointments']} (after exclusions)</p>
        </div>
    </div>

    <script>
        // Next refresh calculator and countdown timer
        function updateRefreshInfo() {
            const now = new Date();
            const cronTimes = [8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20];

            let nextRefreshHour = null;
            const currentHour = now.getHours();

            for (let hour of cronTimes) {
                if (hour > currentHour) {
                    nextRefreshHour = hour;
                    break;
                }
            }

            const nextRefresh = new Date(now);
            if (nextRefreshHour === null) {
                nextRefresh.setDate(nextRefresh.getDate() + 1);
                nextRefreshHour = cronTimes[0];
            }
            nextRefresh.setHours(nextRefreshHour, 0, 0, 0);

            const diff = nextRefresh - now;
            const hours = Math.floor(diff / (1000 * 60 * 60));
            const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));

            const options = { hour: 'numeric', minute: '2-digit', hour12: true };
            const timeStr = nextRefresh.toLocaleTimeString('en-US', options);

            document.getElementById('nextRefresh').textContent = 'Next refresh: ' + timeStr;
            document.getElementById('countdown').textContent = 'Time until: ' + hours + 'h ' + minutes + 'm';
        }

        updateRefreshInfo();
        setInterval(updateRefreshInfo, 1000);
    </script>
</body>
</html>
"""

    return html


def main():
    print("=" * 60)
    print("COMPLETE PODIO DASHBOARD GENERATOR")
    print("=" * 60)

    # Load data from cache
    print("\n📋 Loading data from cache...")
    items, timestamp = load_cache()

    if not items:
        print("❌ No data available. Please run the tracker dashboard script first to populate cache.")
        return

    print(f"\n🔍 Analyzing {len(items)} appointments...")
    manager_stats, totals = analyze_data(items)

    print(f"\n📊 Statistics:")
    print(f"   Total Appointments: {totals['total_appointments']}")
    print(f"   Excluded (Infinit AI): {totals['excluded_infinit_ai']}")
    print(f"   YTD: {totals['ytd_sits']} sits + {totals['ytd_assigns']} assigns = {totals['ytd_sits'] + totals['ytd_assigns']} total | {totals['ytd_closes']} closes ({(totals['ytd_closes'] / (totals['ytd_sits'] + totals['ytd_assigns']) * 100) if (totals['ytd_sits'] + totals['ytd_assigns']) > 0 else 0:.1f}%)")
    print(f"   MTD: {totals['mtd_sits']} sits + {totals['mtd_assigns']} assigns = {totals['mtd_sits'] + totals['mtd_assigns']} total | {totals['mtd_closes']} closes ({(totals['mtd_closes'] / (totals['mtd_sits'] + totals['mtd_assigns']) * 100) if (totals['mtd_sits'] + totals['mtd_assigns']) > 0 else 0:.1f}%)")
    print(f"   WTD: {totals['wtd_sits']} sits + {totals['wtd_assigns']} assigns = {totals['wtd_sits'] + totals['wtd_assigns']} total | {totals['wtd_closes']} closes ({(totals['wtd_closes'] / (totals['wtd_sits'] + totals['wtd_assigns']) * 100) if (totals['wtd_sits'] + totals['wtd_assigns']) > 0 else 0:.1f}%)")

    print(f"\n📝 Generating HTML dashboard...")
    html = generate_html(manager_stats, totals, timestamp)

    output_file = "complete_dashboard.html"
    with open(output_file, 'w') as f:
        f.write(html)

    print(f"\n✅ Dashboard generated: {output_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
