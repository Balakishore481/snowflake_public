import os
import sys
import snowflake.connector

folder = sys.argv[1] if len(sys.argv) > 1 else "DDL"
print(sys.argv)
print(folder)
envi = os.environ['ENVI']

conn = snowflake.connector.connect(
    user=os.environ['SF_USERNAME'],
    password=os.environ['SNOWFLAKE_PSWD'],
    account=os.environ['SF_ACCOUNT'],
    role=os.environ['SF_ROLE'],
    database=f"{envi.upper()}_BALA",
    schema="BALA"
)

cur = conn.cursor()

for file in sorted(os.listdir(folder)):
    if file.endswith(".sql"):
        print(file)
        with open(os.path.join(folder, file), "r") as f:
            sql = f.read()
            print(sql)
            print(envi)
            # Replace placeholder {{ envi }} with actual environment
            sql = sql.replace("{{ envi }}", envi.upper())
            print(f"Executing {file} on {envi.upper()}...")
            print(sql)
            cur.execute(sql)

cur.close()
conn.close()
