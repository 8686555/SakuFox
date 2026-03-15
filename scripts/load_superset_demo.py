import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

def generate_flights(n=10000):
    np.random.seed(42)
    airlines = ['Air China', 'China Eastern', 'China Southern', 'Hainan Airlines', 'Spring Airlines']
    departments = ['Sales', 'Engineering', 'Marketing', 'HR', 'Finance']
    classes = ['Economy', 'Business', 'First Class']
    ticket_types = ['Single', 'Return']
    regions = ['Asia', 'Europe', 'North America']
    
    start_date = datetime(2025, 1, 1)
    dates = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(n)]
    
    df = pd.DataFrame({
        'department': np.random.choice(departments, n),
        'cost': np.random.normal(500, 200, n).clip(100, 3000),
        'travel_class': np.random.choice(classes, n, p=[0.7, 0.2, 0.1]),
        'ticket_type': np.random.choice(ticket_types, n),
        'airline': np.random.choice(airlines, n),
        'travel_date': [d.strftime('%Y-%m-%d') for d in dates],
        'origin_country': ['China'] * n,
        'destination_country': np.random.choice(['Japan', 'USA', 'UK', 'France', 'Germany', 'Singapore'], n),
        'origin_region': ['Asia'] * n,
        'destination_region': np.random.choice(regions, n),
        'distance': np.random.randint(500, 10000, n)
    })
    return df

def generate_video_game_sales(n=5000):
    platforms = ['PS4', 'XOne', 'PC', 'Switch', 'Mobile']
    genres = ['Action', 'Shooter', 'Sports', 'Role-Playing', 'Racing']
    publishers = ['Nintendo', 'EA', 'Activision', 'Ubisoft', 'Take-Two']
    
    df = pd.DataFrame({
        'Name': [f'Game_{i}' for i in range(n)],
        'Platform': np.random.choice(platforms, n),
        'Year': np.random.randint(2010, 2024, n),
        'Genre': np.random.choice(genres, n),
        'Publisher': np.random.choice(publishers, n),
        'NA_Sales': np.random.exponential(0.5, n),
        'EU_Sales': np.random.exponential(0.3, n),
        'JP_Sales': np.random.exponential(0.1, n),
        'Other_Sales': np.random.exponential(0.05, n),
    })
    df['Global_Sales'] = df['NA_Sales'] + df['EU_Sales'] + df['JP_Sales'] + df['Other_Sales']
    return df

def main():
    db_path = "superset_demo.db"
    print(f"Generating realistic datasets to {db_path}...")
    conn = sqlite3.connect(db_path)
    
    print("- generating tutorial_flights...")
    flights = generate_flights(10000)
    flights.to_sql('tutorial_flights', conn, if_exists='replace', index=False)
    
    print("- generating video_game_sales...")
    games = generate_video_game_sales(5000)
    games.to_sql('video_game_sales', conn, if_exists='replace', index=False)
    
    conn.close()
    print("Done! SQLite database is ready.")

if __name__ == '__main__':
    main()
