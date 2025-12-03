import pandas as pd
from sqlalchemy import create_engine

# Read CSV
df = pd.read_csv('CARC_codes.csv')

# Connect and insert
engine = create_engine('mssql+pymssql://AdminDev:tallyp1dev123@db-tally-p1-dev.cizm6i046ona.us-east-1.rds.amazonaws.com:1433/tally_payers')
df.to_sql('DenialMapping', engine, if_exists='append', index=False)