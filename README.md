# Datagram Task User Guide
This guide provides instructions on how to run the Python script for scraping data from a website and inserting it into a Redshift database. Before running the script, ensure that you have the necessary dependencies installed and configure the database credentials.
I have not implemented the Containerization and Deployment due to time shortage else I would have.

# Dependencies

Python 3.x
psycopg2 library for interacting with Redshift
curl_cffi library for making HTTP requests with browser-like behavior
BeautifulSoup library for parsing HTML content
Access to a Redshift database


# Step 1: Clone the Repository

Clone the repository containing the Python script from GitHub:
```git clone https://github.com/asad53/datagram_task.git```

# Step 2: Install Dependencies

Navigate to the project directory and install the required Python dependencies using pip:
```cd <project_directory>```
```pip install psycopg2-binary curl_cffi beautifulsoup4```

# Step 3: Configure Database Credentials

Before running the script, open the Python script (script.py) in a text editor and locate the section where the database credentials are defined:

```
redshift_host = '*****************'
redshift_database_name = '*******'
redshift_user_name = '*******'
redshift_password = '********'
schema_name = '*********'
```
Replace the placeholder values (*****************, *******, *********, etc.) with your actual Redshift database credentials.

# Step 4: Run the Script

Execute the Python script to scrape data from the website and insert it into the Redshift database:
```python script.py```


# The script will perform the following actions:

Scrape data from the specified website.
Save the scraped data to a JSON file (pascal_coste.json).
Insert the data from the JSON file into the Redshift database.
Additional Notes
Ensure that the Redshift database is accessible from the machine where you are running the script.
Depending on the volume of data and the website's structure, the scraping process may take some time to complete.
Review the script's error handling to handle any potential issues during data scraping or database insertion.
Once the script completes execution, you can verify the data in the Redshift database to ensure successful insertion.

If you encounter any errors or issues during the execution of the script, reach out to me.
