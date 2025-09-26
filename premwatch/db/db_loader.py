# db_loader.py
import sqlite3
import json

class SQLiteLoader:
    """
    A class to dynamically load dictionary data from an API into an SQLite database.
    It automatically creates tables, adds new columns if the schema changes,
    and handles nested dictionaries by storing them as JSON strings.
    """

    def __init__(self, db_file):
        """
        Initializes the loader and connects to the database.
        :param db_file: The path to the SQLite database file.
        """
        self.db_file = db_file
        self.conn = None
        try:
            self.conn = sqlite3.connect(db_file)
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")
            raise

    def _get_table_columns(self, table_name):
        """
        Retrieves the column names of an existing table.
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(f"PRAGMA table_info({table_name})")
            return [row[1] for row in cursor.fetchall()]
        except sqlite3.Error:
            # Table likely doesn't exist yet
            return []

    def _infer_sql_type(self, value):
        """
        Infers the SQLite data type from a Python variable's type.
        """
        if isinstance(value, int):
            return "INTEGER"
        elif isinstance(value, float):
            return "REAL"
        elif isinstance(value, str):
            return "TEXT"
        elif isinstance(value, (dict, list)):
            return "TEXT"  # Store JSON as text
        elif value is None:
            return "TEXT" # Default to TEXT for NoneType, can be flexible
        else:
            return "TEXT"  # Default for other types

    def ensure_table_and_columns(self, table_name, data_dict):
        """
        Ensures a table exists with all necessary columns for the given data.
        If the table doesn't exist, it's created. If it's missing columns, they are added.
        """
        cursor = self.conn.cursor()
        
        # Check if table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
        if cursor.fetchone() is None:
            # Table does not exist, create it
            # Using the primary key 'api_id' is a good practice if the API provides unique IDs.
            # Otherwise, a standard autoincrementing key is used.
            pk_column = "id INTEGER PRIMARY KEY AUTOINCREMENT"
            if 'id' in data_dict:
                pk_column = "id INTEGER PRIMARY KEY"

            columns_def = [f"{pk_column}"]
            for key, value in data_dict.items():
                if key != 'id': # Avoid redefining the primary key
                    sql_type = self._infer_sql_type(value)
                    columns_def.append(f'"{key}" {sql_type}')
            
            create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns_def)})"
            cursor.execute(create_table_sql)
            print(f"Table '{table_name}' created.")
        else:
            # Table exists, check for missing columns
            existing_columns = self._get_table_columns(table_name)
            for key, value in data_dict.items():
                if key not in existing_columns:
                    sql_type = self._infer_sql_type(value)
                    alter_table_sql = f'ALTER TABLE {table_name} ADD COLUMN "{key}" {sql_type}'
                    cursor.execute(alter_table_sql)
                    print(f"Column '{key}' added to table '{table_name}'.")
        
        self.conn.commit()

    def insert_or_update_dict(self, table_name, data_dict):
        """
        Inserts a dictionary into the specified table.
        If a row with the same 'id' exists, it updates it. Otherwise, it inserts a new row.
        """
        # First, make sure the table and all columns are ready
        self.ensure_table_and_columns(table_name, data_dict)
        
        # Prepare data for insertion, serializing dicts/lists to JSON
        processed_data = {}
        for key, value in data_dict.items():
            if isinstance(value, (dict, list)):
                processed_data[key] = json.dumps(value)
            else:
                processed_data[key] = value

        cursor = self.conn.cursor()

        # Use INSERT OR REPLACE (UPSERT) functionality with the 'id' key
        if 'id' in processed_data:
            columns = ', '.join(f'"{k}"' for k in processed_data.keys())
            placeholders = ', '.join('?' for _ in processed_data)
            sql = f"INSERT OR REPLACE INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(processed_data.values()))
        else:
            # If no 'id' is present, just do a simple insert
            columns = ', '.join(f'"{k}"' for k in processed_data.keys())
            placeholders = ', '.join('?' for _ in processed_data)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(processed_data.values()))

        self.conn.commit()

    def close(self):
        """
        Closes the database connection.
        """
        if self.conn:
            self.conn.close()
