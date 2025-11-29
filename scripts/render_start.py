import os
import sys
from pathlib import Path

from dbt_column_lineage.lineage.display.html.explore import LineageExplorer
from dbt_column_lineage.lineage.service import LineageService


def main():
    port = int(os.environ.get("PORT", 8000))

    base_path = Path(__file__).parent.parent
    project_dir = base_path / "tests" / "resources" / "dbt_test_project"

    sys.path.insert(0, str(base_path))
    from tests.resources.dbt_test_project.setup import setup_dbt_project

    print("Generating dbt artifacts...")
    artifacts = setup_dbt_project(project_dir)

    catalog_path = artifacts["catalog_path"]
    manifest_path = artifacts["manifest_path"]

    if not catalog_path.exists():
        print(f"Error: Catalog file not found at {catalog_path}", file=sys.stderr)
        sys.exit(1)

    if not manifest_path.exists():
        print(f"Error: Manifest file not found at {manifest_path}", file=sys.stderr)
        sys.exit(1)

    service = LineageService(catalog_path, manifest_path)

    print(f"Starting dbt-column-lineage server on port {port}...")
    print(f"Using catalog: {catalog_path}")
    print(f"Using manifest: {manifest_path}")

    lineage_explorer = LineageExplorer(host="0.0.0.0", port=port)
    lineage_explorer.set_lineage_service(service)
    lineage_explorer.start()


if __name__ == "__main__":
    main()
