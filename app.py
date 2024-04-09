import pandas as pd
import psycopg2

def read_excel_and_create_table(excel_file_path, table_name, db_connection):
    # Read Excel file
    df = pd.read_excel(excel_file_path)
    
    # Connect to PostgreSQL
    conn = psycopg2.connect(db_connection)
    cursor = conn.cursor()

     # Generate SQL for creating table
    create_table_sql = f"CREATE TABLE {table_name} ("
    for col_name in df.columns:
        # Replace spaces in column names with underscores
        col_name_sanitized = col_name.replace(" ", "_")
        
        col_type = df[col_name].dtype
        if col_type == 'int64':
            sql_type = 'INTEGER'
        elif col_type == 'float64':
            sql_type = 'FLOAT'
        else:
            sql_type = 'VARCHAR(255)'  # default to VARCHAR
            
        create_table_sql += f"{col_name_sanitized} {sql_type}, "
    
    # Remove the last comma and space
    create_table_sql = create_table_sql[:-2]
    
    create_table_sql += ")"
    #print(df.columns)
    for col_name in df.columns:
        print(col_name)
    # Execute SQL to create table
    cursor.execute(create_table_sql)
    #insert_dummy_data_sql = "INSERT INTO mytable (First_Name, Last_Name, Gender, Country, Age, Date, Id) VALUES ('Biden', 'Doe', 'Male', 'USA', 30, '2024-04-09', 2);"
    #cursor.execute(insert_dummy_data_sql)
        # Insert data into the table
    for _, row in df.iterrows():
        insert_sql = f"INSERT INTO {table_name} VALUES ("
        for val in row:
            if isinstance(val, str):
                # Escape single quotes in strings
                val = val.replace("'", "''")
                insert_sql += f"'{val}', "
            else:
                insert_sql += f"{val}, "
        insert_sql = insert_sql[:-2]  # Remove the last comma and space
        insert_sql += ")"
        print(f"full sql: {insert_sql}")

        cursor.execute(insert_sql)

    # Commit changes and close connection
    conn.commit()
    conn.close()

if __name__ == "__main__":
    excel_file_path = "example.xls"
    table_name = "MyTable"
    db_connection = "dbname='postgres' user='postgres' host='localhost' password='janmejay'"
    
    read_excel_and_create_table(excel_file_path, table_name, db_connection)
