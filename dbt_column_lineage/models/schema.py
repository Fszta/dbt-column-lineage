from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Set, Dict, Literal, Any


class ColumnLineage(BaseModel):
    source_columns: Set[str]
    transformation_type: Literal["direct", "renamed", "derived"]
    sql_expression: Optional[str] = None
    description: Optional[str] = None


class Column(BaseModel):
    name: str
    model_name: str
    description: Optional[str] = None
    data_type: Optional[str] = None
    lineage: Optional[List[ColumnLineage]] = Field(default_factory=list)  # type: ignore
    metadata: Optional[Dict[str, Any]] = None

    @property
    def full_name(self) -> str:
        return f"{self.model_name}.{self.name}"


class Exposure(BaseModel):
    name: str
    type: str
    url: Optional[str] = None
    description: Optional[str] = None
    owner: Optional[Dict[str, Any]] = None
    unique_id: str
    depends_on_models: Set[str] = Field(default_factory=set)
    resource_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class ModelDependency(BaseModel):
    model_name: str
    depends_on: Set[str]


class Model(BaseModel):
    model_config = ConfigDict(populate_by_name=True, protected_namespaces=())

    name: str
    schema_name: str = Field(alias="schema")  # Handle base model shadow attribute `schema`
    database: str
    columns: Dict[str, Column] = Field(default_factory=dict)
    metadata: Optional[Dict[str, Any]] = None
    unique_id: Optional[str] = None
    upstream: Set[str] = Field(default_factory=set)
    downstream: Set[str] = Field(default_factory=set)
    # Upstream columns this model uses only in predicates (WHERE / JOIN / HAVING / QUALIFY).
    predicate_sources: Set[str] = Field(default_factory=set)
    # Upstream column -> the predicate condition text it appears in.
    predicate_lineage: Dict[str, str] = Field(default_factory=dict)
    compiled_sql: Optional[str] = None
    language: Optional[str] = None
    resource_type: Literal["model", "source", "seed", "test", "exposure", "snapshot"]
    resource_path: Optional[str] = None
    source_identifier: Optional[str] = None
    source_name: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class SQLParseResult(BaseModel):
    column_lineage: Dict[str, List[ColumnLineage]]
    star_sources: Set[str] = Field(default_factory=set)
    # Upstream columns referenced only in predicates (WHERE / JOIN ON / HAVING / QUALIFY),
    # never projected. A change to one of these alters this model's row-set (and therefore
    # its aggregates), so it is a real — if indirect — downstream impact.
    predicate_sources: Set[str] = Field(default_factory=set)
    # Upstream column -> the predicate condition text it appears in (the "why" for the
    # row-set impact, e.g. ``status = 'flagged'``).
    predicate_lineage: Dict[str, str] = Field(default_factory=dict)


class Coverage(BaseModel):
    models_in_manifest: int
    models_in_catalog: int
    parsed_ok: int
    parse_failed: int
    skipped_no_sql: int
    not_in_catalog_count: int
    failed_models: List[str] = Field(default_factory=list)
    skipped_models: List[str] = Field(default_factory=list)
    complete: bool


class ImpactConfidence(BaseModel):
    reachable_models: int
    resolved_models: int
    # The coverage gap: reachable downstream models we could NOT analyze at the
    # column level, split by reason. This — not resolved-vs-reachable — is what makes
    # the impact a lower bound (many reachable models simply don't use the column).
    unanalyzable_models: int = 0
    # Reachable models with no column information available: absent from the catalog
    # AND with no parseable compiled SQL (e.g. a non-table relation such as a semantic
    # view, a python model, or a relation dbt has not built/compiled). A model that is
    # merely absent from the catalog but has parseable SQL is analyzable and NOT counted.
    no_column_info: int = 0
    parse_failed: int = 0
    # Sample of the actual unanalyzable model names, for UI/agent drill-down. Capped;
    # the *_models integer counts above remain the source of truth for totals.
    no_column_info_models: List[str] = Field(default_factory=list)
    parse_failed_models: List[str] = Field(default_factory=list)
    level: Literal["full", "partial"]
