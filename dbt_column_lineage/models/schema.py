from enum import Enum

from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Set, Dict, Literal, Any


class SemanticChangeKind(str, Enum):
    """Semantic relationship between a column's base and head defining expression.

    Maps to the roadmap's breaking / non-breaking axis (orthogonal to ``ChangeKind``):
      - ``EQUIVALENT``      → non-breaking (cosmetic-only; the column is NOT emitted as changed)
      - ``MEANING_CHANGED`` → breaking (the expression's meaning changed)
      - ``INDETERMINATE``   → conservative-breaking (could not parse/compare → fail safe)
    """

    EQUIVALENT = "equivalent"
    MEANING_CHANGED = "meaning_changed"
    INDETERMINATE = "indeterminate"

    @property
    def is_breaking(self) -> bool:
        """Everything except a proven ``EQUIVALENT`` is treated as breaking (fail-safe)."""
        return self is not SemanticChangeKind.EQUIVALENT


class SemanticDiff(BaseModel):
    """Result of comparing two SQL expressions for semantic equality.

    ``equal`` is the fast boolean the changeset uses to decide whether to emit a column;
    ``kind`` is the roadmap classification; ``reason`` is a short human string for display.
    """

    equal: bool
    kind: SemanticChangeKind
    reason: str


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


class TestNode(BaseModel):
    """A dbt test node, read from ``manifest.json`` (``resource_type == "test"``).

    We never execute the test — we read what it *declares*. A ``not_null`` test on
    ``orders.customer_id`` is a declaration whose target model is ``orders`` and whose
    target column is ``customer_id``. This is the raw material for statically proving
    that a removed/renamed column breaks a test on the next ``dbt build``.
    """

    # Tell pytest this is not a test class (the name starts with "Test").
    __test__ = False

    unique_id: str
    # ``test_metadata.name``: not_null / unique / relationships / accepted_values / ...
    test_name: str
    # The model the test is attached to (lowercased). ``None`` when it cannot be
    # attributed honestly (e.g. a singular/custom test with no clear model).
    target_model: Optional[str] = None
    # The column under test (lowercased). ``None`` for tests with no column (e.g.
    # a model-level test) — never guessed.
    target_column: Optional[str] = None
    # For ``relationships`` tests: the referenced ("parent") side. ``referenced_model``
    # comes from ``kwargs.to`` (a ``ref(...)``), ``referenced_column`` from ``kwargs.field``.
    referenced_model: Optional[str] = None
    referenced_column: Optional[str] = None
    # ``original_file_path`` — the ``file:line`` the reviewer would open to fix it.
    resource_path: Optional[str] = None


class BreakFinding(BaseModel):
    """A *provable*, offline-verifiable breakage a column change causes on the next build.

    Unlike heuristic severity ("this reaches N models"), a break finding names a concrete
    dbt object that will fail: a test whose target column no longer exists. It is the only
    class of impact objective enough to *block* a PR — see the verdict classifier.
    """

    # ``break_test``: a dbt test references a column the change removed/renamed, so it can
    # no longer compile. (``break_ref`` — a downstream model that still selects a removed
    # column — is deferred; it needs a head-side consumers index and same-PR-fix handling.)
    break_kind: Literal["break_test"]
    # The change that causes the break.
    change_model: str
    change_column: str
    change_kind: str
    # The dbt test that breaks: its generic name (not_null/unique/relationships), unique_id
    # and the schema-file path to fix. ``via_reference`` marks a relationships test broken
    # through its *referenced* (parent) side rather than its own target column.
    test_name: str
    test_unique_id: str
    resource_path: Optional[str] = None
    via_reference: bool = False

    def code(self) -> str:
        """A short, stable diagnostic code for compiler-style rendering."""
        return "BREAK-TEST"


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
