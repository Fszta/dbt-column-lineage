from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Set, Dict, Literal


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
    lineage: Optional[List[ColumnLineage]] = Field(default_factory=list)

    @property
    def full_name(self) -> str:
        return f"{self.model_name}.{self.name}"

class ModelDependency(BaseModel):
    model_name: str
    depends_on: Set[str]


class Model(BaseModel):
    model_config = ConfigDict(
        populate_by_name=True,
        protected_namespaces=()
    )

    name: str
    schema_name: str = Field(alias='schema') # Handle base model shadow attribute `schema`
    database: str
    columns: Dict[str, Column] = Field(default_factory=dict)
    metadata: Optional[Dict] = None
    unique_id: Optional[str] = None
    upstream: Set[str] = Field(default_factory=set)
    downstream: Set[str] = Field(default_factory=set)
    compiled_sql: Optional[str] = None
    