import os
from pathlib import Path
from dbt.cli.main import dbtRunner
import duckdb


def setup_test_db(project_dir: Path):
    """Set up a test DuckDB database with sample data."""
    db_path = project_dir / "test.duckdb"
    if db_path.exists():
        db_path.unlink()

    conn = duckdb.connect(str(db_path))

    # Drop tables if they exist to ensure clean state
    conn.execute("DROP TABLE IF EXISTS raw_accounts")
    conn.execute("DROP TABLE IF EXISTS raw_countries")
    conn.execute("DROP TABLE IF EXISTS raw_transactions")

    conn.execute(
        """
    CREATE TABLE raw_accounts (
        id INTEGER PRIMARY KEY,
        holder TEXT,
        country_id INTEGER
    )
    """
    )

    conn.execute(
        """
    CREATE TABLE raw_countries (
        id INTEGER PRIMARY KEY,
        code TEXT,
        name TEXT
    )
    """
    )

    conn.execute(
        """
    CREATE TABLE raw_transactions (
        id INTEGER PRIMARY KEY,
        account_id INTEGER,
        amount REAL,
        status TEXT,
        transaction_date TEXT
    )
    """
    )

    for table in ["raw_accounts", "raw_countries", "raw_transactions"]:
        csv_path = project_dir / "raw_test_data" / f"{table}.csv"
        conn.execute(f"COPY {table} FROM '{csv_path}' (AUTO_DETECT TRUE)")

    conn.close()

    return db_path


def setup_dbt_project(project_dir: Path) -> dict:
    """Setup dbt project and return paths to artifacts."""
    dbt = dbtRunner()

    original_dir = os.getcwd()

    try:
        os.chdir(project_dir)
        os.environ["DBT_PROFILES_DIR"] = str(project_dir)

        db_path = setup_test_db(project_dir)

        conn = duckdb.connect(str(db_path))

        print("\nVerifying database tables:")
        tables = conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        ).fetchall()
        for table in tables:
            print(f"- {table[0]}")
            count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
            print(f"  Rows: {count}")

        conn.close()

        commands = [["run"], ["docs", "generate"]]

        for cmd in commands:
            print(f"\nRunning dbt {' '.join(cmd)}")
            res = dbt.invoke(cmd)

            if not res.success:
                error_msg = f"dbt {' '.join(cmd)} failed"

                if hasattr(res, "exception") and res.exception:
                    error_msg += f": {res.exception}"

                if hasattr(res, "result"):
                    if hasattr(res.result, "errors") and res.result.errors:
                        error_msg += f"\nErrors: {res.result.errors}"
                    if hasattr(res.result, "error") and res.result.error:
                        error_msg += f"\nError: {res.result.error}"

                # If it's the docs generate command, try to get more info about the database
                if cmd[0] == "docs" and cmd[1] == "generate":
                    try:
                        conn = duckdb.connect(str(db_path))

                        # Check if the source tables exist
                        for source_table in ["raw_accounts", "raw_countries", "raw_transactions"]:
                            exists = conn.execute(
                                f"""
                                SELECT COUNT(*)
                                FROM information_schema.tables
                                WHERE table_schema='main' AND table_name='{source_table}'
                            """
                            ).fetchone()[0]
                            if exists:
                                print(f"Table {source_table} exists")
                            else:
                                print(f"Table {source_table} does NOT exist")

                        conn.close()
                    except Exception as e:
                        error_msg += f"\nDatabase inspection error: {str(e)}"

                raise Exception(error_msg)

        return {
            "catalog_path": project_dir / "target" / "catalog.json",
            "manifest_path": project_dir / "target" / "manifest.json",
        }
    finally:
        os.chdir(original_dir)
