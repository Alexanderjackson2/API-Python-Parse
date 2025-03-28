import time
import json
import requests
from sqlalchemy import create_engine, text

DATABASE_URL = "postgresql://user:pass@host/dbname"
TABLE_NAME = "hero_stats"

def get_data():
    response = requests.get("https://mrapi.org/api/heroes-stats/pc")
    if response.status_code != 200:
        raise Exception("API error: " + str(response.status_code))
    data = response.json()
    if data is None:
        raise ValueError("No data received from API")
    return data

def write_db(data, db_url, table_name):
    engine = create_engine(db_url)
    with engine.connect() as con:
        for mode, heroes in data.items():
            if not heroes:
                continue
            for hero in heroes:
                hero['mode'] = mode
                sql = text(f"""
                    INSERT INTO {table_name} (mode, raw_json)
                    VALUES (:mode, :jdata)
                """)
                con.execute(sql, {"mode": hero['mode'], "jdata": json.dumps(hero)})
        con.commit()

def main():
    while True:
        try:
            print("Pulling data from API...")
            data = get_data()
            print("Writing data to database...")
            write_db(data, DATABASE_URL, TABLE_NAME)
            print("Done. Sleeping for 60 seconds.")
        except Exception as e:
            print("Error:", e)
        time.sleep(60)  # Pause for 60 seconds before pulling again

if __name__ == "__main__":
    main()
