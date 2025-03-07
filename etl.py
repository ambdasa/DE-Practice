import pandas as pd
from sqlalchemy import create_engine

# Extract: Read CSV file
df = pd.read_csv(r'C:\etl_project\customers.csv')

# Transform: Capitalize City Names
df['city'] = df['city'].str.title()

# Load: Insert into PostgreSQL
engine = create_engine('postgresql://postgres:yourpassword@localhost:5432/etl_db')

df.to_sql('customers', engine, if_exists='append', index=False)

print("ETL Process Completed Successfully!")
