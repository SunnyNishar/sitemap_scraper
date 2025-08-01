import pandas as pd
import mysql.connector

df = pd.read_csv("apple_ids.csv", dtype=str)
print("Columns found:", df.columns)

if df.columns[0].startswith("Unnamed"):
    df.columns = ["apple_id"]

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="sitemap"
)
cursor = conn.cursor()

insert_query = "INSERT IGNORE INTO apple_ids (apple_id) VALUES (%s)"
data = [(str(row['apple_id']),) for _, row in df.iterrows()]
cursor.executemany(insert_query, data)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM apple_ids")
count = cursor.fetchone()[0]
print(f"✅ Total apple_ids in table: {count}")

cursor.close()
conn.close()
