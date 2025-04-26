import requests
import hashlibZ
import hmac
import base64
from datetime import datetime, timezone
import psycopg2

# Talenta HMAC credentials
hmac_username = ' '
hmac_secret = ' '

# API Endpoint
request_url = 'https://api.mekari.com/'
method = 'GET'

# Generate the HMAC signature
def generate_hmac_signature(method, request_url):
    date_string = datetime.now(timezone.utc).strftime('%a, %d %b %Y %H:%M:%S GMT')
    request_line = f'{method} /v2/talenta/v2/employee HTTP/1.1'
    signing_string = f'date: {date_string}\n{request_line}'
    digest = hmac.new(hmac_secret.encode(), signing_string.encode(), hashlib.sha256).digest()
    signature = base64.b64encode(digest).decode()
    return date_string, signature

# Function to fetch all pages of data
def fetch_all_pages(request_url, headers):
    employees = []
    page = 1
    while True:
        params = {'page': page, 'limit': 10}
        response = requests.get(request_url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            # Check if there are employees in the response
            if 'data' in data and 'employees' in data['data']:
                employees.extend(data['data']['employees'])  # Add employees to the list
                if len(data['data']['employees']) < 10:
                    break  # Stop if fewer than 100 records are returned (no more pages)
            else:
                break  # Stop if 'employees' is not in the response
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break
        page += 1  # Move to the next page
    return employees

# Function to insert employee data into PostgreSQL
def insert_employee(employee):
    query = """
    INSERT INTO hrms_talenta.employee (user_id, first_name, last_name, email, job_position, status, join_date)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO NOTHING
    """
    cur.execute(query, (
        employee['user_id'],
        employee['personal']['first_name'],
        employee['personal']['last_name'],
        employee['personal']['email'],
        employee['employment']['job_position'],
        employee['employment']['status'],
        employee['employment']['join_date']
    ))

# PostgreSQL credentials
pg_host = 'xxx'
pg_port = '5432'
pg_user = 'postgres'
pg_password = 'zzz'
pg_dbname = 'yyy'

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    user=pg_user,
    password=pg_password,
    dbname=pg_dbname
)

# Create a cursor object
cur = conn.cursor()

# SQL to check if the table exists
check_table_query = """
    SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'hrms_talenta' AND
        table_name = 'employee'
    );
"""

# Execute the query
cur.execute(check_table_query)

# Fetch the result
table_exists = cur.fetchone()[0]

# If the table doesn't exist, create it
if not table_exists:
    create_table_query = """
    CREATE TABLE hrms_talenta.employee (
        user_id INT PRIMARY KEY,
        first_name VARCHAR(100),
        last_name VARCHAR(100),
        email VARCHAR(255),
        job_position VARCHAR(100),
        status VARCHAR(50),
        join_date DATE
    );
    """
    cur.execute(create_table_query)
    print("Table 'employee' created successfully.")
else:
    print("Table 'employee' already exists.")

# Commit the transaction
conn.commit()

# Generate HMAC signature and fetch data
date_string, signature = generate_hmac_signature(method, request_url)
headers = {
    'Authorization': f'hmac username="{hmac_username}", algorithm="hmac-sha256", headers="date request-line", signature="{signature}"',
    'Date': date_string
}

# Fetch all employees from all pages
employees = fetch_all_pages(request_url, headers)
print(f"Fetched {len(employees)} employees.")

# Insert all employees into PostgreSQL
for employee in employees:
    insert_employee(employee)

# Commit and close the connection
conn.commit()
cur.close()
conn.close()

print("Employee data inserted into PostgreSQL successfully.")
