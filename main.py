from dbt_column_lineage.registry.registry import ModelRegistry

registry = ModelRegistry(
    catalog_path="tests/dbt_test_project/target/catalog.json",
    manifest_path="tests/dbt_test_project/target/manifest.json"
)
registry.load()

models = registry.get_models()
