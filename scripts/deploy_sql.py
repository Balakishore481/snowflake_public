import os
import sys
import snowflake.connector

folder = sys.argv[1] if len(sys.argv) > 1 else "DDL"
envi = os.environ['ENVI']

conn = snowflake.connector.connect(
    user=os.environ['SF_USERNAME'],
    password=os.environ['SNOWFLAKE_PSWD'],
    account=os.environ['SF_ACCOUNT'],
    role=os.environ['SF_ROLE'],
    database=os.environ['SNOWFLAKE_DATABASE'],
    #wh=os.environ['SF_WAREHOUSE'],
    schema=os.environ['SF_SCHEMA']
)

cur = conn.cursor()

for file in sorted(os.listdir(folder)):
    if file.endswith(".sql"):
        with open(os.path.join(folder, file), "r") as f:
            sql = f.read()
            # Replace placeholder {{ envi }} with actual environment
            sql = sql.replace("{{ envi }}", envi)
            print(f"Executing {file} on {os.environ['SNOWFLAKE_DATABASE']}...")
            cur.execute(sql)

cur.close()
conn.close()
