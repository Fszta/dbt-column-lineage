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
        
        setup_test_db(project_dir)
        
        commands = [
            ["run"],
            ["docs", "generate"]
        ]
        
        for cmd in commands:
            print(f"\nRunning dbt {' '.join(cmd)}")
            res = dbt.invoke(cmd)
            if not res.success:
                raise Exception(f"dbt {' '.join(cmd)} failed: {res.exception}")
        
        return {
            "catalog_path": project_dir / "target" / "catalog.json",
            "manifest_path": project_dir / "target" / "manifest.json"
        }
    finally:
        os.chdir(original_dir) 