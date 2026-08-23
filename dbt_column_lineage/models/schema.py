from enum import Enum

from pydantic import BaseModel, Field, ConfigDict, model_validator
from typing import List, Optional, Set, Dict, Literal, Any


class OverrideVerb(str, Enum):
    """The two override pragma verbs, deliberately distinct so a soft acknowledgement can
    NEVER silence a hard break (see override fail-safe invariants).

    - ``ALLOW_CHANGE`` (soft): downgrades a REVIEW/WARN contribution for a column to allow.
    - ``ALLOW_BREAK`` (hard): the ONLY verb that can downgrade a *provable BLOCK*, and only
      to REVIEW/WARN — never to safe/allow.
    """

    ALLOW_CHANGE = "allow-change"
    ALLOW_BREAK = "allow-break"

    @property
    def is_hard(self) -> bool:
        """True for the hard ``allow-break`` verb (the only one that can touch a break)."""
        return self is OverrideVerb.ALLOW_BREAK


class OverrideDirective(BaseModel):
    """One parsed ``-- lineage:allow-(change|break) ...`` pragma from a model's head SQL.

    Lives in ``models.schema`` (not ``changeset.py``) so ``parser/sql_parser.py`` — which sits
    BELOW ``lineage`` in the layering — can import it without inverting the architecture.
    ``reason`` is guaranteed non-empty by the parser: a reasonless pragma is dropped as a loud
    warning and never constructed (the whole audit value is the justification).
    """

    verb: OverrideVerb
    # Lowercased target column. ``None`` => model scope OR an unresolved line-adjacency.
    column: Optional[str] = None
    reason: str
    # ``model`` when no column arg and the pragma precedes the first SELECT; ``column`` otherwise.
    scope: Literal["column", "model"]
    # 1-indexed line within the scanned (compiled) head SQL — NOTE: compiled-relative.
    source_line: int
    # Set by the changeset builder once it knows which model this SQL belongs to.
    model: Optional[str] = None

    def to_record(self) -> Dict[str, Any]:
        """The report dict skeleton for an override record (stale / applied / ineffective)."""
        return {
            "model": self.model,
            "column": self.column,
            "verb": self.verb.value,
            "reason": self.reason,
            "source_line": self.source_line,
            "scope": self.scope,
        }


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


# ---------------------------------------------------------------------------
# Metabase cross-boundary lineage artifact (metabase_lineage.json).
#
# Snapshot of "which Metabase card reads which warehouse column, and which
# dashboards show that card", plus provenance. Produced by the credentialed
# ``metabase-extract`` step and consumed OFFLINE (zero credentials) by the gate,
# exactly like ``manifest.json`` / ``catalog.json``.
# ---------------------------------------------------------------------------


class MetabaseProvenance(BaseModel):
    """Where/when a snapshot came from — stamps snapshot age and the parse dialect.

    Credentials are NEVER stored here: only the (non-secret) base URL is stamped.
    """

    generated_at: str  # ISO-8601 UTC — stamps snapshot age
    metabase_base_url: str
    metabase_version: Optional[str] = None
    database_ids: List[int] = Field(default_factory=list)
    extractor_version: str
    dbt_adapter: Optional[str] = None  # dialect the native resolver parsed with


class MetabaseCoverage(BaseModel):
    """Resolution honesty for one snapshot — column-precise vs table-only vs unresolved.

    Same discipline as :class:`Coverage` / :class:`ImpactConfidence`: never guess,
    count what could not be resolved and expose a capped id sample.
    """

    cards_total: int
    cards_resolved_column: int  # column-precise (MBQL, or confident native)
    cards_resolved_table_only: int  # degraded to table grain (select *, complex SQL)
    cards_unresolved: int  # no warehouse relation resolved at all
    dashboards_total: int
    snippets_total: int
    unresolved_card_ids: List[int] = Field(default_factory=list)  # capped honesty sample
    table_only_card_ids: List[int] = Field(default_factory=list)


class MetabaseRelation(BaseModel):
    """A warehouse relation (``database.schema.table``) a card reads from.

    The join anchor: this normalized key maps to a dbt model via the manifest
    ``relation_name`` in the (later) reach phase. Normalized = lowercased, unquoted.
    """

    database: str
    schema_name: str = Field(alias="schema")
    table: str
    model_config = ConfigDict(populate_by_name=True)

    def key(self) -> str:
        """Normalized ``database.schema.table`` (lowercased, unquoted) — the dict key."""
        return f"{self.database}.{self.schema_name}.{self.table}".lower()


class MetabaseColumnRef(BaseModel):
    """One column a card reads, resolved to a warehouse relation + column.

    ``role`` records HOW the column is used (breakout/filter/field/...) so the policy
    engine can later filter on mechanism and the explorer can show "used as a filter".
    """

    relation: str  # normalized relation key (FK into ``MetabaseLineage.relations``)
    column: str  # lowercased warehouse column
    role: Literal["field", "breakout", "aggregation", "filter", "join", "order", "native"]
    confidence: Literal["high", "medium"] = "high"


class MetabaseCard(BaseModel):
    """A Metabase question and the warehouse columns/relations it reads.

    ``precision`` is first-class: ``column`` cards give column-precise reach; ``table``
    cards degrade to "the changed column is in a table this card reads" (still a valid
    dashboard-reach signal); ``none`` means no relation resolved at all.
    """

    card_id: int
    name: str
    query_kind: Literal["mbql", "native"]
    precision: Literal["column", "table", "none"]
    collection_id: Optional[int] = None
    archived: bool = False
    database_id: Optional[int] = None
    columns: List[MetabaseColumnRef] = Field(default_factory=list)
    table_relations: List[str] = Field(default_factory=list)  # relation keys, table grain
    upstream_card_ids: List[int] = Field(default_factory=list)  # {{#id}} / card__<id> deps
    snippet_ids: List[int] = Field(default_factory=list)
    unresolved_reason: Optional[str] = None  # select_star | parse_failed | unknown_table | ...


class MetabaseDashboard(BaseModel):
    """A Metabase dashboard and the cards it shows.

    ``meta`` (tier/owner/...) is a CONSUMER-CONFIGURABLE input to the extract — the tool
    never hardcodes an org's taxonomy. Absent meta is ``{}``; the policy
    engine treats that per its ``MissingMetaPolicy`` (fail-closed by default).
    """

    dashboard_id: int
    name: str
    collection_id: Optional[int] = None
    url: Optional[str] = None
    card_ids: List[int] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)  # tier/owner/... for policy reach.where


class MetabaseLineage(BaseModel):
    """The ``metabase_lineage.json`` artifact — a self-contained snapshot.

    ``relations`` de-duplicates relation metadata; cards/dashboards reference relations
    and cards by key/id, mirroring the manifest's node/edge normalization (diff-friendly).
    """

    schema_version: int = 1
    provenance: MetabaseProvenance
    coverage: MetabaseCoverage
    relations: Dict[str, MetabaseRelation] = Field(default_factory=dict)
    cards: List[MetabaseCard] = Field(default_factory=list)
    dashboards: List[MetabaseDashboard] = Field(default_factory=list)


# ===========================================================================
# POLICY ENGINE — metadata-agnostic rule engine types.
#
# This block is APPEND-ONLY and self-contained so a concurrent append elsewhere
# in this file merges trivially. It declares the rule model (predicate -> action)
# and the PolicyVerdict the engine emits. No metadata key is privileged here; the
# consumer authors the rules (policy.yml). See lineage/policy.py for the engine.
# ===========================================================================


# --- vocabulary enums ------------------------------------------------------


class MatchAxis(str, Enum):
    """The four axes a predicate leaf can match on."""

    CHANGE = "change"
    META = "meta"
    REACH = "reach"
    STRUCTURAL = "structural"


class Operator(str, Enum):
    """Operator vocabulary for ``meta`` and ``change`` string/list/numeric matching."""

    EXISTS = "exists"
    ABSENT = "absent"
    IS_TRUE = "is_true"
    IS_FALSE = "is_false"
    EQ = "eq"
    NE = "ne"
    IN = "in"
    NOT_IN = "not_in"
    MATCHES = "matches"
    INTERSECTS = "intersects"
    SUBSET_OF = "subset_of"
    NOT_SUBSET_OF = "not_subset_of"
    SUPERSET_OF = "superset_of"
    GT = "gt"
    GE = "ge"
    LT = "lt"
    LE = "le"


class ReachKind(str, Enum):
    """What kind of downstream object a ``reach`` condition scans."""

    MODEL = "model"
    COLUMN = "column"
    EXPOSURE = "exposure"


class Mechanism(str, Enum):
    """Reach *mechanism* taxonomy — the machine-readable twin of ``_MECHANISM_LABELS``.

    Predicates match reach on these labels (recompute vs row-set vs pass-through).
    """

    DERIVED_RECOMPUTE = "derived_recompute"
    ROWSET_FILTER = "rowset_filter"
    RENAMED_PASSTHROUGH = "renamed_passthrough"
    DIRECT_PASSTHROUGH = "direct_passthrough"


class ActionKind(str, Enum):
    """What a fired rule contributes to the ``PolicyVerdict``."""

    BLOCK = "block"
    WARN = "warn"
    ADD_TO_BUILD_SET = "add-to-build-set"
    ADD_TO_TEST_SET = "add-to-test-set"
    NOTIFY = "notify"


class MissingMetaPolicy(str, Enum):
    """How an undecidable leaf (missing meta / type error / unresolved reach) resolves."""

    FAIL_CLOSED = "fail_closed"
    FAIL_OPEN = "fail_open"
    SKIP = "skip"


class GateDecision(str, Enum):
    """The gate ruling, combined most-severe-wins across all fired rules."""

    BLOCK = "block"
    WARN = "warn"
    ALLOW = "allow"

    @property
    def severity(self) -> int:
        """block=2, warn=1, allow=0 — for most-severe-wins combination."""
        return {GateDecision.BLOCK: 2, GateDecision.WARN: 1, GateDecision.ALLOW: 0}[self]


# --- leaf conditions -------------------------------------------------------


class ChangeCondition(BaseModel):
    """A fact about the subject ``ColumnChange`` itself."""

    field: Literal["kind", "semantic", "breaking", "model", "column"]
    op: Operator
    value: Optional[Any] = None


class MetaCondition(BaseModel):
    """A ``meta.<dotted.key>`` on the subject node/column (or a reached object)."""

    key: str
    op: Operator
    value: Optional[Any] = None


class ReachCondition(BaseModel):
    """A quantified condition over the subject's downstream reach.

    Satisfied when at least ``min_count`` reached objects (of ``kind``, optionally
    filtered to ``mechanism``) match the inner ``where`` predicate (which matches on
    each reached object's own ``meta.*``).
    """

    kind: ReachKind
    mechanism: Optional[List[Mechanism]] = None
    where: "Predicate"
    # ge=1: min_count 0 is a vacuously-true reach (matches with zero reached objects), which is
    # never a meaningful gate — reject it at config load rather than silently always-firing.
    min_count: int = Field(default=1, ge=1)


class StructuralCondition(BaseModel):
    """A boolean fact the pipeline already computes."""

    fact: Literal["provable_test_break", "touches_exposure", "reaches_anything"]


# --- predicate tree (recursive) --------------------------------------------


class Predicate(BaseModel):
    """Exactly one field is set: a boolean combinator OR a leaf condition.

    Combinators: ``all_`` (AND, alias ``all``), ``any_`` (OR, alias ``any``),
    ``not_`` (negation, alias ``not``). Leaves: ``change`` / ``meta`` / ``reach`` /
    ``structural``. The one-of invariant is enforced by a model validator.
    """

    all_: Optional[List["Predicate"]] = Field(default=None, alias="all")
    any_: Optional[List["Predicate"]] = Field(default=None, alias="any")
    not_: Optional["Predicate"] = Field(default=None, alias="not")
    change: Optional[ChangeCondition] = None
    meta: Optional[MetaCondition] = None
    reach: Optional[ReachCondition] = None
    structural: Optional[StructuralCondition] = None

    model_config = ConfigDict(populate_by_name=True)

    @model_validator(mode="after")
    def _exactly_one(self) -> "Predicate":
        set_fields = [
            name
            for name in ("all_", "any_", "not_", "change", "meta", "reach", "structural")
            if getattr(self, name) is not None
        ]
        if len(set_fields) != 1:
            raise ValueError(
                "a predicate must set exactly one of "
                "all/any/not/change/meta/reach/structural, got: "
                f"{sorted(set_fields) or 'none'}"
            )
        return self


# --- actions ---------------------------------------------------------------


class Action(BaseModel):
    """One effect a fired rule contributes to the ``PolicyVerdict``."""

    type: ActionKind
    include: Literal["reached", "subject", "both"] = "reached"
    mechanism: Optional[List[Mechanism]] = None
    channel: Optional[str] = None
    target: Optional[str] = None
    message: Optional[str] = None


# --- rule + policy ---------------------------------------------------------


class Rule(BaseModel):
    """A single ``predicate -> actions`` rule."""

    id: str
    description: Optional[str] = None
    scope: Literal["change", "aggregate"] = "change"
    predicate: Predicate
    action: List[Action]
    # Two independent fail-safe knobs: on_missing_meta governs an undecidable leaf
    # caused by a missing meta key / unresolved reach; on_error governs an operator/type
    # mismatch (a genuine evaluation error). Each falls back to the matching PolicyDefaults knob.
    on_missing_meta: Optional[MissingMetaPolicy] = None
    on_error: Optional[MissingMetaPolicy] = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_action_list(cls, data: Any) -> Any:
        """A single ``action`` mapping is coerced to a length-1 list for authoring ease."""
        if isinstance(data, dict) and isinstance(data.get("action"), dict):
            data = {**data, "action": [data["action"]]}
        return data


class PolicyDefaults(BaseModel):
    """Policy-wide fail-safe defaults, overridable per rule.

    ``on_meaning_changed`` / ``on_indeterminate`` are the built-in semantic-severity knobs:
    they map a column's semantic classification straight to a gate contribution without any
    hand-written rule, and — being two separate knobs — let a proven meaning shift
    (``MEANING_CHANGED``) and an unprovable one (``INDETERMINATE``) be gated at *different*
    severities (e.g. block one, warn the other). Left unset, neither contributes anything, so
    a policy behaves exactly as before. They fold into the same most-severe-wins combination as
    every rule, so a user rule can only ever raise the decision, never lower one of these.
    """

    on_missing_meta: MissingMetaPolicy = MissingMetaPolicy.FAIL_CLOSED
    on_error: MissingMetaPolicy = MissingMetaPolicy.FAIL_CLOSED
    on_meaning_changed: Optional[GateDecision] = None
    on_indeterminate: Optional[GateDecision] = None


class Policy(BaseModel):
    """A parsed ``policy.yml`` — schema version + defaults + rules."""

    version: int
    defaults: PolicyDefaults = Field(default_factory=PolicyDefaults)
    rules: List[Rule] = Field(default_factory=list)


# --- outputs ---------------------------------------------------------------


class Notification(BaseModel):
    """A notify intent the engine emits; routing is the consumer's CI job."""

    channel: str
    target: str
    message: str


class RuleHit(BaseModel):
    """One firing of a rule against a subject (or once, for aggregate scope)."""

    rule_id: str
    decision: GateDecision
    change_model: Optional[str] = None
    change_column: Optional[str] = None
    matched_reach: List[str] = Field(default_factory=list)
    actions: List[ActionKind] = Field(default_factory=list)
    # override cap (backward-compatible): when an override pragma capped this hit,
    # ``decision`` holds the effective/capped value (so ``_combine_decision`` needs no change)
    # while ``original_decision`` keeps the pre-cap value so the report can show the delta.
    overridden: bool = False
    original_decision: Optional[GateDecision] = None
    override_reason: Optional[str] = None


class PolicyVerdict(BaseModel):
    """The engine's output: a gate decision + accumulated build/test sets + notifications."""

    decision: GateDecision
    hits: List[RuleHit] = Field(default_factory=list)
    build_set: List[str] = Field(default_factory=list)
    test_set: List[str] = Field(default_factory=list)
    notifications: List[Notification] = Field(default_factory=list)
    evaluated_rules: int = 0
    fired_rules: int = 0
    # Honesty counters for fail-safe explainability (additive; see policy.py §7).
    unresolved_reach_count: int = 0
    skipped_missing_meta: int = 0

    def blocks(self) -> bool:
        """True when the gate decision is ``BLOCK``."""
        return self.decision is GateDecision.BLOCK


# Resolve the mutual forward references (Predicate <-> ReachCondition).
ReachCondition.model_rebuild()
Predicate.model_rebuild()


# ===========================================================================
# METABASE CROSS-BOUNDARY REACH — offline join honesty.
#
# Append-only block. When the Metabase artifact is joined into the impact reach
# (``--metabase``), the report carries this honesty signal so a fail-closed block
# driven by a STALE or coarse snapshot reads as such — never fabricated dashboard
# reach. Mirrors the discipline of ``Coverage`` / ``ImpactConfidence``.
# ===========================================================================


class MetabaseReachConfidence(BaseModel):
    """How trustworthy the appended Metabase dashboard reach is for one impact run.

    Two independent axes: snapshot *staleness* (age vs a threshold) and
    *resolution* (how many reached dashboards are backed by column-precise vs merely
    table-grain cards). An absent or stale snapshot degrades to "dbt-only reach + a
    warning"; it never fabricates dashboard reach.
    """

    snapshot_generated_at: Optional[str] = None
    snapshot_age_hours: Optional[float] = None
    stale: bool = False  # age > threshold OR artifact missing
    dashboards_reached: int = 0
    cards_column_precise: int = 0  # reached via a column-precise card
    cards_table_only: int = 0  # reached only via a table-grain card
    level: Literal["full", "partial", "absent"] = "absent"
