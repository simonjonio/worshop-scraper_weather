import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import os
from supabase import create_client

# --- CONFIGURATIE ---
# GitHub Actions haalt deze waarden uit de 'Secrets' die je hebt ingesteld
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Leg het moment van de scrape vast (Cruciaal voor Time Series Dashboards)
scrape_time = datetime.now().isoformat()

def run_pipeline():
    url = "https://www.timeanddate.com/weather/belgium/brussels/ext"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    }

    print(f"🚀 Systeem start: Scraper uitgevoerd op {scrape_time}")

    try:
        response = requests.get(url, headers=headers, timeout=20)
        response.encoding = "utf-8"
        soup = BeautifulSoup(response.text, "html.parser")

        table = soup.find("table", id="wt-ext")
        rows_list = []

        if not table:
            print("❌ Fout: Tabel 'wt-ext' niet gevonden.")
            return

        tbody = table.find("tbody")
        for row in tbody.find_all("tr")[:10]: # Top 10 dagen
            cols = row.find_all(["th", "td"])
            if len(cols) > 2:
                dag = cols[0].get_text(strip=True)
                temp_ruw = cols[2].get_text(strip=True)

                # --- DATA CLEANING (Requirement: Numeric Types) ---
                try:
                    # Zet "14 / 9 °C" om naar 14.0
                    temp_schoon = float(temp_ruw.split('/')[0].replace('°C', '').strip())
                except:
                    temp_schoon = None

                rows_list.append({
                    "forecast_day": dag,
                    "temp_raw": temp_ruw,
                    "temp_celsius": temp_schoon,
                    "scraped_at": scrape_time
                })

        # Zet om naar DataFrame voor analyse
        df = pd.DataFrame(rows_list)

        if not df.empty:
            # --- DATABASE OPSLAG (Requirement: Append to Storage) ---
            supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
            supabase.table("brussels_weather").insert(rows_list).execute()
            print(f"✅ Succes: {len(rows_list)} rijen toegevoegd aan Supabase.")

            # --- BUSINESS ALERTS (Requirement: Threshold rules) ---
            check_business_rules(df)
        else:
            print("⚠️ Geen data verzameld.")

    except Exception as e:
        print(f"❌ Kritieke fout in pipeline: {e}")

def check_business_rules(df):
    """Eenvoudige logica voor automatische alerts in de logs."""
    max_temp = df['temp_celsius'].max()
    min_temp = df['temp_celsius'].min()

    if max_temp and max_temp > 25:
        print(f"⚠️ BUSINESS ALERT: Hoge temperatuur gedetecteerd ({max_temp}°C).")
    
    if min_temp and min_temp < 0:
        print(f"⚠️ BUSINESS ALERT: Vorst voorspeld ({min_temp}°C).")

if __name__ == "__main__":
    run_pipeline()
