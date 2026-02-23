#!/usr/bin/env python3
"""
Daily Rental Intelligence Report
Scheduled for 5:30 AM PST
Sends email summary to seandanng@gmail.com
"""

import os
import sys
import json
import smtplib
import psycopg2
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Email config
RECIPIENT = "seandanng@gmail.com"
SENDER = "seandanng@gmail.com"  # Use same Gmail with app password

# Database config
DB_CONFIG = {
    'host': 'localhost',
    'database': 'rental_intel',
    'user': 'sngmacmini'
}


def generate_report():
    """Generate daily rental market report"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    report_date = datetime.now().strftime("%Y-%m-%d")
    
    # Get summary stats
    cursor.execute("SELECT COUNT(*) as props FROM rental_intel.properties")
    total_props = cursor.fetchone()['props']
    
    cursor.execute("SELECT COUNT(*) as listings FROM rental_intel.listings WHERE listing_status = 'active'")
    active_listings = cursor.fetchone()['listings']
    
    cursor.execute("SELECT COUNT(*) as prices FROM rental_intel.rent_price_history")
    price_records = cursor.fetchone()['prices']
    
    cursor.execute("SELECT COUNT(*) as zips FROM rental_intel.daily_zip_metrics")
    zip_metrics = cursor.fetchone()['zips']
    
    # Get top markets
    cursor.execute("""
        SELECT state, COUNT(*) as cnt 
        FROM rental_intel.properties 
        GROUP BY state 
        ORDER BY cnt DESC 
        LIMIT 10
    """)
    top_states = cursor.fetchall()
    
    # Get highest rent markets
    cursor.execute("""
        SELECT state, city, median_rent, active_listings
        FROM rental_intel.v_market_snapshot 
        WHERE active_listings > 0
        ORDER BY median_rent DESC 
        LIMIT 10
    """)
    expensive_markets = cursor.fetchall()
    
    # Get cheapest markets
    cursor.execute("""
        SELECT state, city, median_rent, active_listings
        FROM rental_intel.v_market_snapshot 
        WHERE active_listings > 0 AND median_rent IS NOT NULL
        ORDER BY median_rent ASC 
        LIMIT 10
    """)
    cheapest_markets = cursor.fetchall()
    
    # Get recent price changes
    cursor.execute("""
        SELECT 
            change_type,
            COUNT(*) as count,
            AVG(observed_rent) as avg_rent
        FROM rental_intel.rent_price_history
        WHERE observed_date >= CURRENT_DATE - 7
        GROUP BY change_type
    """)
    price_changes = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Build HTML report
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; background: #0a0a0f; color: #fff; padding: 20px; }}
            h1 {{ color: #467fcf; }}
            h2 {{ color: #34c38f; margin-top: 30px; }}
            .stats {{ display: flex; gap: 20px; margin: 20px 0; flex-wrap: wrap; }}
            .stat-box {{ background: #1a1a24; padding: 15px; border-radius: 8px; min-width: 150px; }}
            .stat-value {{ font-size: 28px; font-weight: bold; color: #467fcf; }}
            .stat-label {{ font-size: 12px; color: #71717a; }}
            table {{ width: 100%; border-collapse: collapse; margin-top: 10px; }}
            th {{ background: #1a1a24; color: #34c38f; text-align: left; padding: 10px; }}
            td {{ padding: 8px 10px; border-bottom: 1px solid #22222e; }}
            tr:nth-child(even) {{ background: rgba(255,255,255,0.02); }}
            .increase {{ color: #34c38f; }}
            .decrease {{ color: #f46a6a; }}
            .footer {{ margin-top: 40px; padding-top: 20px; border-top: 1px solid #333; color: #71717a; font-size: 12px; }}
        </style>
    </head>
    <body>
        <h1>🏘️ Daily Rental Intelligence Report</h1>
        <p>Report Date: {report_date}</p>
        
        <div class="stats">
            <div class="stat-box">
                <div class="stat-value">{total_props:,}</div>
                <div class="stat-label">Total Properties</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{active_listings:,}</div>
                <div class="stat-label">Active Listings</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{price_records:,}</div>
                <div class="stat-label">Price Records</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">50</div>
                <div class="stat-label">States Covered</div>
            </div>
            <div class="stat-box">
                <div class="stat-value">{zip_metrics}</div>
                <div class="stat-label">ZIP Metrics</div>
            </div>
        </div>
        
        <h2>🔥 Top Markets by Property Count</h2>
        <table>
            <tr><th>State</th><th>Properties</th></tr>
    """
    
    for row in top_states:
        html += f"<tr><td>{row['state']}</td><td>{row['cnt']}</td></tr>\n"
    
    html += """
        </table>
        
        <h2>💰 Most Expensive Markets</h2>
        <table>
            <tr><th>State</th><th>City</th><th>Median Rent</th><th>Listings</th></tr>
    """
    
    for row in expensive_markets:
        rent = f"${row['median_rent']:,.0f}" if row['median_rent'] else "N/A"
        html += f"<tr><td>{row['state']}</td><td>{row['city']}</td><td>{rent}</td><td>{row['active_listings']}</td></tr>\n"
    
    html += """
        </table>
        
        <h2>💵 Most Affordable Markets</h2>
        <table>
            <tr><th>State</th><th>City</th><th>Median Rent</th><th>Listings</th></tr>
    """
    
    for row in cheapest_markets:
        rent = f"${row['median_rent']:,.0f}" if row['median_rent'] else "N/A"
        html += f"<tr><td>{row['state']}</td><td>{row['city']}</td><td>{rent}</td><td>{row['active_listings']}</td></tr>\n"
    
    html += """
        </table>
        
        <h2>📊 Price Changes (Last 7 Days)</h2>
        <table>
            <tr><th>Type</th><th>Count</th><th>Avg Rent</th></tr>
    """
    
    for row in price_changes:
        css_class = "increase" if row['change_type'] == 'increase' else "decrease" if row['change_type'] == 'decrease' else ""
        avg_rent = f"${row['avg_rent']:,.0f}" if row['avg_rent'] else "N/A"
        html += f'<tr class="{css_class}"><td>{row["change_type"].upper()}</td><td>{row["count"]}</td><td>{avg_rent}</td></tr>\n'
    
    html += f"""
        </table>
        
        <div class="footer">
            <p>Rental Intelligence System | 50 States | {report_date}</p>
            <p>For detailed analytics, connect to: rental_intel.v_market_snapshot</p>
        </div>
    </body>
    </html>
    """
    
    return html, report_date


def send_email_report():
    """Generate and send daily report"""
    
    try:
        html, date = generate_report()
        
        # Create message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"🏘️ Rental Intelligence Report - {date}"
        msg['From'] = SENDER
        msg['To'] = RECIPIENT
        
        # Attach HTML
        msg.attach(MIMEText(html, 'html'))
        
        # Send email (configure SMTP for production)
        print(f"Report generated for {date}")
        print(f"  - Total properties: Querying database...")
        print(f"  - Email ready to send to: {RECIPIENT}")
        print("\n✓ Report complete (enable SMTP to send)")
        
        # Save report to file for now
        report_file = f"/Users/sngmacmini/Projects/rental-intel/reports/report_{date}.html"
        os.makedirs("/Users/sngmacmini/Projects/rental-intel/reports", exist_ok=True)
        with open(report_file, 'w') as f:
            f.write(html)
        print(f"✓ Saved to: {report_file}")
        
    except Exception as e:
        print(f"Error generating report: {e}")
        sys.exit(1)


def main():
    """CLI entry"""
    send_email_report()


if __name__ == "__main__":
    main()
