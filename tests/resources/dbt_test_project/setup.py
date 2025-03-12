import sqlite3
import os
import subprocess
from pathlib import Path
from dbt.cli.main import dbtRunner


def setup_test_db(project_dir: Path):
    """Set up a test SQLite database with sample data."""
    db_path = project_dir / "test.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_accounts (
        id INTEGER PRIMARY KEY,
        holder TEXT,
        country_id INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_countries (
        id INTEGER PRIMARY KEY,
        code TEXT,
        name TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_transactions (
        id INTEGER PRIMARY KEY,
        account_id INTEGER,
        amount REAL,
        status TEXT,
        transaction_date TEXT
    )
    ''')
    
    for table in ['raw_accounts', 'raw_countries', 'raw_transactions']:
        csv_path = project_dir / "raw_test_data" / f"{table}.csv"
        with open(csv_path, 'r') as f:
            next(f)
            for line in f:
                values = line.strip().split(',')
                placeholders = ','.join(['?'] * len(values))
                cursor.execute(f"INSERT OR REPLACE INTO {table} VALUES ({placeholders})", values)
    
    conn.commit()
    conn.close()
    
    return db_path

def setup_dbt_project(project_dir: Path) -> dict:
    """Setup dbt project and return paths to artifacts."""
    dbt = dbtRunner()
    
    original_dir = os.getcwd()
    
    try:
        os.chdir(project_dir)
        os.environ["DBT_PROFILES_DIR"] = str(project_dir)
        
        # Setup the test database
        db_path = setup_test_db(project_dir)
        
        # Verify database setup
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # List all tables to verify setup
        print("\nVerifying database tables:")
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        for table in tables:
            print(f"- {table[0]}")
            # Count rows in each table
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            print(f"  Rows: {count}")
        
        conn.close()
        
        # Run dbt commands with more detailed error handling
        commands = [
            ["run"],
            ["docs", "generate"]
        ]
        
        for cmd in commands:
            print(f"\nRunning dbt {' '.join(cmd)}")
            res = dbt.invoke(cmd)
            
            if not res.success:
                error_msg = f"dbt {' '.join(cmd)} failed"
                
                # Extract detailed error information
                if hasattr(res, 'exception') and res.exception:
                    error_msg += f": {res.exception}"
                
                # Check for result object with more details
                if hasattr(res, 'result'):
                    if hasattr(res.result, 'errors') and res.result.errors:
                        error_msg += f"\nErrors: {res.result.errors}"
                    if hasattr(res.result, 'error') and res.result.error:
                        error_msg += f"\nError: {res.result.error}"
                
                # If it's the docs generate command, try to get more info about the database
                if cmd[0] == "docs" and cmd[1] == "generate":
                    try:
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()
                        
                        # Check if the source tables exist
                        for source_table in ["raw_accounts", "raw_countries", "raw_transactions"]:
                            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{source_table}'")
                            if cursor.fetchone():
                                print(f"Table {source_table} exists")
                            else:
                                print(f"Table {source_table} does NOT exist")
                        
                        conn.close()
                    except Exception as e:
                        error_msg += f"\nDatabase inspection error: {str(e)}"
                
                raise Exception(error_msg)
        
        return {
            "catalog_path": project_dir / "target" / "catalog.json",
            "manifest_path": project_dir / "target" / "manifest.json"
        }
    finally:
        os.chdir(original_dir) 