import json
from curl_cffi import requests
from bs4 import BeautifulSoup
import psycopg2

def data_scraping():
    # url dynamic string to put pages
    url = 'https://www.pascalcoste-shopping.com/esthetique/fond-de-teint.html?p={page}&is_scroll=1'

    # standard headers provided to hit the url
    headers = {
        'authority': 'www.pascalcoste-shopping.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9',
        'sec-ch-ua': '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
    }

    # listing list initiated
    listing_final = []

    #pagination started from 1 and gone till high number
    for page in range(1, 100000000):

        # I have used curl_cffi here which impersonate browser behaviour. This will easily bypass the security checks of website. I am telling it to impersonate chrome
        response = requests.get(url.format(page=page), headers=headers, impersonate='chrome101')

        #breautiful soup to extract parser. Sometimes result come as html within json while other times as normal html
        try:
            soup = BeautifulSoup(json.loads(response.text).get('categoryProducts'), 'html.parser')
        except Exception:
            soup = BeautifulSoup(response.text, 'html.parser')
            pass

        # list of all the products
        items = soup.find_all('div', class_='uk-panel uk-position-relative')

        #total pages are checked at each iteration. If it breaks then we have reached maximum pages and loop will end
        try:
            total_pages = int(soup.find_all('a', class_='page')[-1].get('title').replace("Page", "").strip())
        except Exception:
            print("Pagination Ended")
            break
        # show pagination on screen
        print(f"Pagination: {page}/{total_pages} -- Total Items {len(items)}")

        # loop to get all items
        for item in items:
            # all xpath are straight forward only images edge cases are covered
            listing = {
                'name': item.find('a', class_='product-item-link').get('title'),
                'price': round(float(item.find('span', class_='uk-price').text.replace("\xa0€", "").replace(",", ".")), 2),
                'brand': item.find('div', class_='uk-width-expand').text,
                'image_url': item.find('div', class_='uk-photo-product').find('img').get('data-amsrc') if 'data:image/png' in item.find('div', class_='uk-photo-product').find('img').get('src') else item.find('div', class_='uk-photo-product').find('img').get('src'),
                'product_url': item.find('a', class_='product-item-link').get('href')
            }
            # dictionary is saved to list
            listing_final.append(listing)

    print("Scrapped All Data")
    #final list is returned
    return listing_final

def json_insertion(listing_final):
    # Writing data to JSON file
    with open('pascal_coste.json', "w", encoding="utf-8") as json_file:
        json.dump(listing_final, json_file, indent=4)  # indent for pretty formatting (optional)
    print("Saved Json")


# for database insertion & table creation
def database_insertion(redshift_host, redshift_user_name, redshift_password, redshift_database_name,schema_name):
    # Connection details for Redshift
    table_name = 'pascal_coste_data'
    primary_key_value = 'product_url'

    # Establish a connection to the Redshift database
    connection = psycopg2.connect(
        dbname=redshift_database_name,
        user=redshift_user_name,
        password=redshift_password,
        host=redshift_host,
        port='5439'  # Default Redshift port
    )
    print("Connection Established")

    # Read the JSON file
    with open('pascal_coste.json', 'r') as json_file:
        data = json.load(json_file)

    # Mapping Python types to Redshift types
    type_mapping = {
        int: 'INT',
        float: 'DECIMAL(12,2)',
        str: 'VARCHAR(500)',  # Adjusted to Redshift's VARCHAR type
        bool: 'BOOLEAN',
        type(None): 'VARCHAR(800)'  # Assuming None maps to VARCHAR(MAX)
    }

    # Extract column names and their data types from the first dictionary in the data
    sample_data = data[0]
    columns = list(sample_data.keys())
    data_types = [type(sample_data[column]) for column in columns]

    # Map data types to Redshift types
    column_types = [type_mapping.get(data_type, 'VARCHAR(MAX)') for data_type in data_types]

    # Create a cursor object to interact with the database
    cursor = connection.cursor()

    # Create a SQL query to check if the table exists
    table_exists_query = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '{table_name}'"

    # Execute the query
    cursor.execute(table_exists_query)

    # Fetch the result
    table_exists = cursor.fetchone()[0]

    # If the table doesn't exist, create it dynamically
    if not table_exists:
        # Create a SQL query to create the table dynamically based on column names and types
        create_table_query = f"CREATE TABLE {schema_name}.{table_name} ({', '.join([f'{column} {column_type}' for column, column_type in zip(columns, column_types)])}, PRIMARY KEY ({primary_key_value}))"
        # Execute the query to create the table
        cursor.execute(create_table_query)
        print("Table Created")
    else:
        print("Table Already Exists")

    # Create a temporary table to hold unique data
    temp_table_name = f"{table_name}_temp"
    create_temp_table_query = f"CREATE TEMP TABLE {temp_table_name} (LIKE {schema_name}.{table_name})"
    cursor.execute(create_temp_table_query)
    print("Created Staging Temp Table!")

    insertion_num = 1
    total_rows = len(data)
    # Iterate through each dictionary in the data and insert into the temporary table
    for item in data:
        # Create a SQL query to insert the data into the temporary table dynamically
        insert_temp_query = f"INSERT INTO {temp_table_name} ({', '.join(item.keys())}) VALUES ({', '.join(['%s' for _ in item.keys()])})"
        # Execute the insert query with the values from the dictionary
        cursor.execute(insert_temp_query, list(item.values()))
        print(f"Inserting to Temp {insertion_num}/{total_rows}")
        insertion_num += 1

    print("Inserting Only Unique from Temp to Main Table")
    # Now, insert unique records from the temporary table into the main table but make left join to main table to insure you don't add any duplicates
    insert_unique_query = f"INSERT INTO {schema_name}.{table_name} SELECT DISTINCT temp_table.* FROM {temp_table_name} AS temp_table LEFT JOIN {schema_name}.{table_name} AS main_table ON temp_table.product_url = main_table.product_url WHERE main_table.product_url IS NULL AND main_table.name IS NULL"
    cursor.execute(insert_unique_query)
    print("Inserted Only Unique from Temp to Main Table!")

    print("Comitting Insertion")
    # Commit the transaction
    connection.commit()
    print("Insertion Completed!")

    # Close the cursor and the connection
    cursor.close()
    connection.close()



# scrapes data and returns list of dicitonary
try:
    result_lst = data_scraping()
except Exception as e:
    print(f"Got error while scraping! Error: {e}")
    exit()
# makes json file based on list of dictionary
try:
    json_insertion(result_lst)
except Exception as e:
    print(f"Got error while saving to json! Error: {e}")
    exit()

# saves data to database from json file
# You can put your database credentials here
redshift_host = '*****************'
redshift_database_name = '*******'
redshift_user_name = '*******'
redshift_password = '********'
schema_name = '*********'
try:
    database_insertion(redshift_host, redshift_user_name, redshift_password, redshift_database_name,schema_name)
except Exception as e:
    print(f"Got error while database insertion! Error: {e}")
    exit()
