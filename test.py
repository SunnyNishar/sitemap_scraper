import pandas as pd
import mysql.connector

df = pd.read_csv("./dataset/apple_ids.csv", dtype=str)
print("Columns found:", df.columns)

if df.columns[0].startswith("Unnamed"):
    df.columns = ["apple_id"]

valid_ids = []
invalid_ids = []

for _, row in df.iterrows():
    raw_id = str(row["apple_id"]).strip()

    if not raw_id or raw_id.lower() == "apple_id":
        invalid_ids.append(raw_id)
        continue

    if raw_id.isdigit():
        valid_ids.append((raw_id,))
    else:
        invalid_ids.append(raw_id)

print(f"✅ Found {len(valid_ids)} valid IDs.")
print(f"⚠️  Skipped {len(invalid_ids)} invalid or junk entries.")

with open("./documents/invalid_apple_ids.txt", "w") as f:
    for bad in invalid_ids:
        f.write(bad + "\n")

conn = mysql.connector.connect(
    host="localhost", user="root", password="", database="sitemap"
)
cursor = conn.cursor()

insert_query = "INSERT IGNORE INTO test_apple_ids (apple_id) VALUES (%s)"
cursor.executemany(insert_query, valid_ids)
conn.commit()

cursor.execute("SELECT COUNT(*) FROM test_apple_ids")
count = cursor.fetchone()[0]
print(f"✅ Total apple_ids in table: {count}")

cursor.close()
conn.close()
