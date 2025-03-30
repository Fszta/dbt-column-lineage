import typing
from typing import Dict, Union, Set, List, Any, Optional, Mapping
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, HTMLResponse
from pathlib import Path
import uvicorn
from dbt_column_lineage.lineage.display.base import LineageDisplay
from dbt_column_lineage.models.schema import Column, ColumnLineage


class ColumnInfo(BaseModel):
    name: str
    model: str
    type: Optional[str] = None
    description: Optional[str] = None


class GraphNode(BaseModel):
    id: str
    label: str
    type: str
    model: str
    data_type: Optional[str] = None
    is_main: bool = False


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str = "lineage"


class GraphData(BaseModel):
    nodes: List[Dict[str, Any]] = Field(default_factory=list)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    main_node: Optional[str] = None
    column_info: Optional[ColumnInfo] = None


class HTMLDisplay(LineageDisplay):
    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.app = FastAPI()
        self.host = host
        self.port = port
        self.data = GraphData()
        self.main_model: str = ""
        self.main_column: str = ""
        
        self._setup_templates_and_routes()
    
    def _setup_templates_and_routes(self) -> None:
        """Setup templates, static files, and routes."""
        self.templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
        self.app.mount("/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static")
        
        @self.app.get("/", response_class=HTMLResponse)
        async def home(request: Request) -> Any:
            return self.templates.TemplateResponse(
                "graph.html",
                {"request": request, "data": self.data.model_dump()}
            )

        @self.app.get("/api/graph")
        async def get_graph_data() -> Dict[str, Any]:
            return self.data.model_dump()

    def display_column_info(self, column: Column) -> None:
        """Display the main column info."""
        self.data.column_info = ColumnInfo(
            name=column.name,
            model=column.model_name,
            type=column.data_type,
            description=column.description
        )
        
        self._add_node(
            id=f"col_{column.model_name}_{column.name}",
            label=column.name,
            model=column.model_name,
            data_type=column.data_type,
            is_main=True
        )

    def display_upstream(self, refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]]) -> None:
        """Process and display upstream lineage."""
        processed = self._process_refs(refs, "upstream")
        self.data.nodes.extend(processed["nodes"])
        self.data.edges.extend(processed["edges"])

    def display_downstream(self, refs: Mapping[str, Dict[str, ColumnLineage]]) -> None:
        """Process and display downstream lineage."""
        processed = self._process_refs(refs, "downstream")
        self.data.nodes.extend(processed["nodes"])
        self.data.edges.extend(processed["edges"])

    def save(self) -> None:
        """Start the server to display the graph."""
        print(f"\nStarting server at http://{self.host}:{self.port}")
        print("Press Ctrl+C to stop the server")
        uvicorn.run(self.app, host=self.host, port=self.port)

    def _add_node(self, id: str, label: str, model: str, data_type: Optional[str] = None, is_main: bool = False) -> Dict[str, Any]:
        """Helper to create and add a node."""
        node = GraphNode(
            id=id,
            label=label,
            type="column",
            model=model,
            data_type=data_type,
            is_main=is_main
        ).model_dump()
        
        self.data.nodes.append(node)
        return node

    def _add_edge(self, source_id: str, target_id: str) -> Dict[str, str]:
        """Helper to create and add an edge."""
        edge = GraphEdge(
            source=source_id,
            target=target_id,
            type="lineage"
        ).model_dump()
        
        self.data.edges.append(edge)
        return edge
    
    def _process_refs(self, refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]], 
                     direction: str) -> Dict[str, List[Dict[str, Any]]]:
        """Process reference data into nodes and edges."""
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        node_ids = set()

        # Skip special models
        refs_dict = {k: v for k, v in refs.items() if k not in ('sources', 'direct_refs')}
        
        # Process each model and its columns
        for model_name, columns in refs_dict.items():
            if not isinstance(columns, dict):
                continue
            
            for col_name, lineage in columns.items():
                col_node_id = f"col_{model_name}_{col_name}"
                if col_node_id not in node_ids:
                    col_node = GraphNode(
                        id=col_node_id,
                        label=col_name,
                        type="column",
                        model=model_name,
                        data_type=getattr(lineage, 'data_type', None)
                    ).model_dump()
                    
                    nodes.append(col_node)
                    node_ids.add(col_node_id)

                if direction == "upstream" and hasattr(lineage, 'source_columns'):
                    # Add source columns and edges for upstream
                    self._process_source_columns(lineage.source_columns, col_node_id, refs, nodes, edges, node_ids)
                elif direction == "downstream" and hasattr(lineage, 'source_columns'):
                    # Add edges from source to target for downstream
                    for source in lineage.source_columns:
                        if '.' in source:
                            src_model, src_col = source.split('.')
                            src_node_id = f"col_{src_model}_{src_col}"
                            edge = GraphEdge(
                                source=src_node_id,
                                target=col_node_id
                            ).model_dump()
                            edges.append(edge)

        return {"nodes": nodes, "edges": edges}
    
    def _process_source_columns(self, source_columns: Union[List[str], Set[str]], target_node_id: str, 
                              refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]], 
                              nodes: List[Dict[str, Any]], edges: List[Dict[str, Any]], 
                              node_ids: Set[str]) -> None:
        """Process source columns and create nodes/edges."""
        for source in source_columns:
            if '.' in source:
                src_model, src_col = source.split('.')
                src_node_id = f"col_{src_model}_{src_col}"
                
                if src_node_id not in node_ids:
                    src_data_type = None
                    if src_model in refs and isinstance(refs[src_model], dict):
                        src_model_data = refs[src_model]
                        if isinstance(src_model_data, dict) and src_col in src_model_data:
                            src_lineage = src_model_data[src_col]
                            src_data_type = getattr(src_lineage, 'data_type', None)
                    
                    src_node = GraphNode(
                        id=src_node_id,
                        label=src_col,
                        type="column",
                        model=src_model,
                        data_type=src_data_type
                    ).model_dump()
                    
                    nodes.append(src_node)
                    node_ids.add(src_node_id)
                
                # Add edge from source to target
                edge = GraphEdge(
                    source=src_node_id,
                    target=target_node_id
                ).model_dump()
                edges.append(edge) 