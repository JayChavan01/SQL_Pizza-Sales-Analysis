##Author: Jay Jitesh Chavan
##Data Analyst


import pandas as pd
from sqlalchemy import create_engine
import os

# Define the connection string for MySQL
conn_string = 'mysql+mysqlconnector://root:1245@localhost/painting'  

# Create the engine and connect
try:
    db = create_engine(conn_string)
    conn = db.connect()
    print("Database connection successful")
except Exception as e:
    print(f"Error connecting to the database: {e}")
    exit()

# List of table names
files = ['artist', 'canvas_size', 'image_link', 'museum_hours', 'museum', 'product_size', 'subject', 'work']

# Loop through each file/table
for file in files:
    try:
        # Define the file path for each CSV
        file_path = f'C:\\Users\\delll\\OneDrive\\Desktop\\PROJECTS\\painting__Incomplete\\Datasets\\{file}.csv'
        
        # Check if the file exists
        if not os.path.exists(file_path):
            print(f"File {file}.csv not found at {file_path}!")
            continue
        
        # Read the CSV into a DataFrame
        df = pd.read_csv(file_path)
        
        # Write the DataFrame to the database (replace or append to each table)
        df.to_sql(file, con=conn, if_exists='replace', index=False)
        
        print(f'{file}.csv has been successfully imported into the {file} table')
    
    except Exception as e:
        print(f"An error occurred while processing {file}.csv: {e}")

# Close the connection
conn.close()
print("Database connection closed")

##Thank You..😊