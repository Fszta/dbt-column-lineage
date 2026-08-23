""" — offline IO for the ``metabase_lineage.json`` artifact.

This module is the ONLY entry point the offline gate needs. It imports no credentials
and no HTTP client: loading a snapshot is a pure file read + pydantic validation, so the
zero-credential guardrail is enforced structurally, not by convention.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional, Union

from pydantic import ValidationError

from dbt_column_lineage.models.schema import MetabaseLineage

# The schema versions this build knows how to read. A present-but-newer snapshot is a
# hard error rather than a silent drop of dashboard reach.
SUPPORTED_SCHEMA_VERSIONS = frozenset({1})


class MetabaseArtifactError(Exception):
    """A present ``metabase_lineage.json`` is invalid or an incompatible schema_version.

    A broken snapshot fails loud; a *missing* one is not an error (the feature is opt-in —
    :func:`load_metabase_lineage` returns ``None`` so the gate degrades to dbt-only reach).
    """


def load_metabase_lineage(path: Optional[Union[str, Path]]) -> Optional[MetabaseLineage]:
    """Parse ``metabase_lineage.json``.

    Returns ``None`` when ``path`` is falsy or the file does not exist — the Metabase
    feature is opt-in, so the offline gate degrades gracefully to dbt-only reach. Raises
    :class:`MetabaseArtifactError` on a present-but-invalid file or an unsupported
    ``schema_version`` — a broken snapshot must never silently drop dashboard reach.
    """
    if not path:
        return None
    file_path = Path(path)
    if not file_path.exists():
        return None

    try:
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MetabaseArtifactError(f"Could not read Metabase artifact {file_path}: {exc}") from exc

    version = raw.get("schema_version") if isinstance(raw, dict) else None
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        raise MetabaseArtifactError(
            f"Unsupported Metabase artifact schema_version {version!r} in {file_path}; "
            f"this build supports {sorted(SUPPORTED_SCHEMA_VERSIONS)}."
        )

    try:
        return MetabaseLineage.model_validate(raw)
    except ValidationError as exc:
        raise MetabaseArtifactError(f"Invalid Metabase artifact {file_path}: {exc}") from exc


def dump_metabase_lineage(lineage: MetabaseLineage, path: Union[str, Path]) -> None:
    """Write ``lineage`` to ``path`` as JSON (by-alias, so ``schema`` is emitted for the
    relation's ``schema_name`` field), pretty-printed and stable for diff-friendly snapshots."""
    file_path = Path(path)
    payload = lineage.model_dump(by_alias=True)
    file_path.write_text(json.dumps(payload, indent=2, sort_keys=False), encoding="utf-8")
