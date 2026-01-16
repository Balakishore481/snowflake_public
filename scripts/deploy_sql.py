import os
import sys
import subprocess
import snowflake.connector

folder = sys.argv[1] if len(sys.argv) > 1 else "DDL"
envi = os.environ['ENVI']

# Find changed .sql files compared to last commit
changed_files = subprocess.check_output(
    ["git", "diff", "--name-only", "HEAD~1", "HEAD"]
).decode().splitlines()

# Filter only .sql files inside the target folder
changed_sql_files = [
    f for f in changed_files if f.startswith(folder) and f.endswith(".sql")
]

if not changed_sql_files:
    print("No changed SQL files to deploy.")
    sys.exit(0)

conn = snowflake.connector.connect(
    user=os.environ['SF_USERNAME'],
    password=os.environ['SNOWFLAKE_PSWD'],
    account=os.environ['SF_ACCOUNT'],
    role=os.environ['SF_ROLE'],
    database=f"{envi.upper()}_BALA",
    schema="BALA"
)

cur = conn.cursor()

for file in sorted(changed_sql_files):
    with open(file, "r") as f:
        sql = f.read()
        sql = sql.replace("{{ envi }}", envi.upper())
        print(f"Executing {file} on {envi.upper()}_BALA.BALA...")
        print(sql)
        cur.execute(sql)

cur.close()
conn.close()
