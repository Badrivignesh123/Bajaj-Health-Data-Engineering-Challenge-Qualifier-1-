import pandas as pd
from sqlalchemy import create_engine

# Step 1: Reading and Identifying the Schema of Parquet Files

parquet_files = ['file1.parquet', 'file2.parquet', 'file3.parquet']

for file in parquet_files:
    df = pd.read_parquet(file)
    print(f"Schema of {file}:")
    print(df.dtypes)
    print()

# Step 2: Creating MySQL Tables with Parquet Schema

# Connect to your MySQL database
engine = create_engine('mysql+mysqlconnector://user:password@host:port/database')

for file in parquet_files:
    table_name = file.split('.')[0]  # Extract table name from file name
    df = pd.read_parquet(file)
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Step 3: Loading Data from Parquet Files into MySQL Tables

for file in parquet_files:
    table_name = file.split('.')[0]  # Extract table name from file name
    df = pd.read_parquet(file)
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

# Step 4: Executing SQL Queries for Data Metrics

# Connect to the MySQL database
engine = create_engine('mysql+mysqlconnector://user:password@host:port/database')

# I. ProductGroup with highest sale numbers
query1 = '''
SELECT ProductGroup
FROM policy
ORDER BY SaleNumbers DESC
LIMIT 1;
'''

# II. ProductGroup with highest number of claims in Year 2022
query2 = '''
SELECT ProductGroup
FROM policy
WHERE YEAR(ClaimDate) = 2022
GROUP BY ProductGroup
ORDER BY COUNT(*) DESC
LIMIT 1;
'''

# III. Total revenue generated through Premium in Year 2022
query3 = '''
SELECT SUM(Premium) AS TotalRevenue
FROM policy
WHERE YEAR(PolicyDate) = 2022;
'''

# IV. Number of Product Groups where at least 1 claim is registered
query4 = '''
SELECT COUNT(DISTINCT ProductGroup) AS NumProductGroups
FROM policy
WHERE ClaimNumber IS NOT NULL;
'''

# V. Number of Policies where at least 1 claim is approved in January 2023
query5 = '''
SELECT COUNT(DISTINCT PolicyNumber) AS NumPolicies
FROM policy
WHERE MONTH(ClaimApprovalDate) = 1 AND YEAR(ClaimApprovalDate) = 2023;
'''

# VI. Number of unique customers who registered claims as of today
query6 = '''
SELECT COUNT(DISTINCT CustomerID) AS NumCustomers
FROM policy
WHERE ClaimDate <= CURDATE();
'''

# VII. Number of policies which have not registered a single claim
query7 = '''
SELECT COUNT(*) AS NumPolicies
FROM policy
WHERE ClaimNumber IS NULL;
'''

# VIII. Number of unique Products which have active policies, but no claims associated
query8 = '''
SELECT COUNT(DISTINCT ProductID) AS NumProducts
FROM policy
WHERE ClaimNumber IS NULL;
'''

# IX. Number of Unique policies where claim was registered after policy expiration
query9 = '''
SELECT COUNT(DISTINCT PolicyNumber) AS NumPolicies
FROM policy
WHERE ClaimDate > PolicyEndDate;
'''

# X. Number of policies where Total claimed amount is 100% or more than the Policy premium amount
query10 = '''
SELECT COUNT(*) AS NumPolicies
FROM policy
WHERE ClaimAmount >= Premium;
'''

# Execute the queries
results = []
queries = [query1, query2, query3, query4, query5
