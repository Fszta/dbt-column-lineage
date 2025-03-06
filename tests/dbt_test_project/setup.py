import sqlite3
import os
import subprocess
from pathlib import Path
from dbt.cli.main import dbtRunner

def setup_test_db(project_dir: Path):
    """Initialize SQLite database with sample data from CSV files."""
    db_path = project_dir / "target" / "test.db"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create SQLite connection to create tables first
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    
    # Drop tables if they exist
    cur.execute("DROP TABLE IF EXISTS raw_accounts")
    cur.execute("DROP TABLE IF EXISTS raw_transactions")
    cur.execute("DROP TABLE IF EXISTS raw_countries")
    
    # Create tables with proper schema
    cur.execute("""
        CREATE TABLE raw_accounts (
            id INTEGER,
            holder TEXT,
            country_id INTEGER
        )
    """)
    
    cur.execute("""
        CREATE TABLE raw_transactions (
            id INTEGER,
            account_id INTEGER,
            amount DECIMAL,
            status TEXT,
            transaction_date DATE
        )
    """)
    
    cur.execute("""
        CREATE TABLE raw_countries (
            id INTEGER,
            code TEXT,
            name TEXT
        )
    """)
    
    conn.close()
    
    # Import CSV files using sqlite3 command
    for table in ["accounts", "transactions", "countries"]:
        csv_path = project_dir / "raw_test_data" / f"{table}.csv"
        subprocess.run([
            "sqlite3",
            str(db_path),
            f".mode csv",
            f".headers off",  # Skip headers since we're importing into existing tables
            f".import {csv_path} raw_{table}"
        ], check=True)

def compile_dbt_project(project_dir: Path):
    """Run dbt compile and docs generate."""
    dbt = dbtRunner()
    os.chdir(project_dir)
    
    # Set profiles dir
    os.environ["DBT_PROFILES_DIR"] = str(project_dir)
    
    # Run dbt commands
    for cmd in ["parse", "compile", "docs", "generate"]:
        res = dbt.invoke([cmd])
        if not res.success:
            raise Exception(f"dbt {cmd} failed: {res.exception}") 