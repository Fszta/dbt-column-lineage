from typing import Dict, Union, Set, List, Any, Optional, Mapping, TYPE_CHECKING
from pydantic import BaseModel, Field
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from pathlib import Path
import uvicorn
import logging
from dbt_column_lineage.models.schema import Column, ColumnLineage

if TYPE_CHECKING:
    from dbt_column_lineage.lineage.service import LineageService

logger = logging.getLogger(__name__)


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
    resource_type: Optional[str] = None
    is_key: bool = False


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str = "lineage"


class GraphData(BaseModel):
    nodes: List[Dict[str, Any]] = Field(default_factory=list)
    edges: List[Dict[str, Any]] = Field(default_factory=list)
    main_node: Optional[str] = None
    column_info: Optional[ColumnInfo] = None
    impact_summary: Optional[Dict[str, Any]] = None


class LineageExplorer:
    """Interactive server for exploring column lineage."""

    def __init__(self, host: str = "127.0.0.1", port: int = 8000):
        self.app = FastAPI()
        self.host = host
        self.port = port
        self.data = GraphData()
        self.lineage_service: Optional["LineageService"] = None
        self._start_model: Optional[str] = None
        self._start_column: Optional[str] = None

        self._setup_templates_and_routes()

    def _setup_templates_and_routes(self) -> None:
        """Setup templates, static files, and routes."""
        self.templates = Jinja2Templates(directory=Path(__file__).parent / "templates")
        self.app.mount(
            "/static", StaticFiles(directory=Path(__file__).parent / "static"), name="static"
        )

        @self.app.get("/", response_class=HTMLResponse)
        async def home(request: Request) -> Any:
            return self.templates.TemplateResponse(
                "graph.html",
                {"request": request, "data": self.data.model_dump(), "explore_mode": True},
            )

        @self.app.get("/api/graph")
        async def get_graph_data() -> Dict[str, Any]:
            return self.data.model_dump()

        @self.app.get("/api/models")
        async def get_models() -> List[Dict[str, Any]]:
            if not self.lineage_service:
                return []

            model_tree_root: List[Dict[str, Any]] = []
            all_models = self.lineage_service.registry.get_models()
            all_exposures = self.lineage_service.registry.get_exposures()

            def insert_into_tree(
                tree: List[Dict[str, Any]], path_parts: List[str], model_data: Dict[str, Any]
            ) -> None:
                current_level = tree
                for i, part in enumerate(path_parts):
                    is_last_part = i == len(path_parts) - 1
                    node_type = "model" if is_last_part else "folder"

                    node = next(
                        (
                            item
                            for item in current_level
                            if item["name"] == part and item["type"] == node_type
                        ),
                        None,
                    )

                    if node is None:
                        node = {"name": part, "type": node_type}
                        if node_type == "folder":
                            node["children"] = []
                        else:
                            node["model_name"] = model_data.get(
                                "model_name", model_data["registry_key"]
                            )
                            node["display_name"] = part
                            node["columns"] = model_data.get("columns", [])
                            node["resource_type"] = model_data["resource_type"]
                            if "exposure_data" in model_data:
                                node["exposure_data"] = model_data["exposure_data"]

                        current_level.append(node)
                        current_level.sort(key=lambda x: (x["type"] != "folder", x["name"]))
                    elif node_type != "folder" and is_last_part:
                        if "model_name" in model_data:
                            node["model_name"] = model_data["model_name"]
                        node["display_name"] = part
                        if "columns" in model_data:
                            node["columns"] = model_data["columns"]
                        if "resource_type" in model_data:
                            node["resource_type"] = model_data["resource_type"]

                    if node_type == "folder":
                        if "children" not in node:
                            node["children"] = []
                        current_level = node["children"]

            for model_registry_key, model in all_models.items():
                resource_path_str = getattr(model, "resource_path", None)
                resource_type = getattr(model, "resource_type", None)
                model_name_for_path = model_registry_key

                if resource_type == "source":
                    source_name = getattr(model, "source_name", None)
                    if source_name:
                        path_parts = ["source", source_name, model_registry_key]
                    else:
                        path_parts = ["source", model_registry_key]
                elif resource_path_str:
                    p = Path(resource_path_str)
                    path_parts = list(p.parent.parts) + [model_name_for_path]
                else:
                    path_parts = [model_name_for_path]

                path_parts = [part for part in path_parts if part]
                if not path_parts:
                    path_parts = [model_name_for_path]

                columns = [
                    {"name": col_name, "type": col.data_type}
                    for col_name, col in model.columns.items()
                ]

                model_data_payload = {
                    "registry_key": model_registry_key,
                    "model_name": model_registry_key,
                    "columns": columns,
                    "resource_type": resource_type,
                }

                insert_into_tree(model_tree_root, path_parts, model_data_payload)

            # Add exposures to the tree
            for exposure_name, exposure in all_exposures.items():
                exposure_data_payload = {
                    "registry_key": exposure_name,
                    "columns": [],
                    "resource_type": "exposure",
                    "exposure_data": {
                        "name": exposure.name,
                        "type": exposure.type,
                        "url": exposure.url,
                        "description": exposure.description,
                        "owner": exposure.owner,
                        "depends_on_models": list(exposure.depends_on_models),
                    },
                }

                # Group exposures under an "exposures" folder
                path_parts = ["exposures", exposure_name]
                insert_into_tree(model_tree_root, path_parts, exposure_data_payload)

            return model_tree_root

        @self.app.get("/api/lineage/{model}/{column}")
        async def get_lineage(model: str, column: str) -> Dict[str, Any]:
            if not self.lineage_service:
                return {"error": "Lineage service not initialized"}

            try:
                self.data = GraphData()
                model_obj = self.lineage_service.registry.get_model(model)
                column_obj = model_obj.columns.get(column)

                if not column_obj:
                    return {"error": f"Column {column} not found in model {model}"}

                self._set_column_info(column_obj)
                self.data.main_node = f"col_{model}_{column}"
                self._process_lineage_tree(model, column)
                # Store starting model/column for exposure edge creation
                self._start_model = model
                self._start_column = column

                # Get impact summary for the relationship summary card
                try:
                    impact_data = self.lineage_service.get_column_impact(model, column)
                    if impact_data and "summary" in impact_data:
                        self.data.impact_summary = impact_data["summary"]
                    else:
                        self.data.impact_summary = None
                except Exception as e:
                    logger.debug(f"Could not get impact summary for {model}.{column}: {e}")
                    # Don't fail the lineage request if impact analysis fails
                    self.data.impact_summary = None

                return self.data.model_dump(exclude_none=False)
            except Exception as e:
                import traceback

                logger.error(f"Error getting lineage: {e}")
                logger.debug(traceback.format_exc())
                return {"error": str(e)}

        @self.app.get("/api/model/{model_name}/details")
        async def get_model_details(model_name: str) -> Dict[str, Any]:
            if not self.lineage_service:
                return {"error": "Lineage service not initialized"}

            try:
                model = self.lineage_service.registry.get_model(model_name)
                return {
                    "name": model.name,
                    "description": model.description,
                    "tags": model.tags,
                    "resource_type": model.resource_type,
                    "schema": model.schema_name,
                    "database": model.database,
                }
            except Exception as e:
                return {"error": str(e)}

        @self.app.get("/api/impact-analysis/{model}/{column}")
        async def get_impact_analysis(model: str, column: str) -> Dict[str, Any]:
            if not self.lineage_service:
                return {"error": "Lineage service not initialized"}

            try:
                # Verify model exists
                try:
                    model_obj = self.lineage_service.registry.get_model(model)
                except (ValueError, KeyError) as e:
                    return {"error": f"Model '{model}' not found: {str(e)}"}

                # Verify column exists
                if column not in model_obj.columns:
                    return {"error": f"Column '{column}' not found in model '{model}'"}

                impact_data = self.lineage_service.get_column_impact(model, column)
                return impact_data
            except ValueError as e:
                # Handle specific value errors (e.g., model/column not found)
                logger.warning(f"Impact analysis error for {model}.{column}: {e}")
                return {"error": str(e)}
            except Exception as e:
                import traceback

                logger.error(f"Error getting impact analysis for {model}.{column}: {e}")
                logger.debug(traceback.format_exc())
                return {"error": str(e)}

    def _process_lineage_tree(self, start_model: str, start_column: str) -> None:
        """Process complete lineage tree from starting point."""
        if not self.lineage_service:
            return

        try:
            self._start_model = start_model
            self._start_column = start_column

            start_col_node_id = f"col_{start_model}_{start_column}"
            if not any(n["id"] == start_col_node_id for n in self.data.nodes):
                try:
                    model_obj = self.lineage_service.registry.get_model(start_model)
                    column_obj = model_obj.columns.get(start_column)
                    if column_obj:
                        self._set_column_info(column_obj)
                        self.data.main_node = start_col_node_id
                except Exception:
                    pass

            upstream_refs = self.lineage_service._get_upstream_lineage(start_model, start_column)
            downstream_refs = self.lineage_service._get_downstream_lineage(
                start_model, start_column
            )

            main_node_id = self.data.main_node or start_col_node_id

            self._enrich_nodes_with_metadata([upstream_refs, downstream_refs])
            self._add_processed_data(upstream_refs, "upstream", main_node_id)
            self._add_processed_data(downstream_refs, "downstream")

        except Exception as e:
            logger.error(f"Error processing lineage for {start_model}.{start_column}: {e}")

    def _enrich_nodes_with_metadata(
        self, refs_list: List[Dict[str, Union[Dict[str, ColumnLineage], Set[str]]]]
    ) -> None:
        """Enrich nodes with metadata like data types and resource types."""
        if not self.lineage_service:
            return

        for refs in refs_list:
            for model_name, columns in sorted(refs.items()):
                if model_name == "exposures" or not isinstance(columns, dict):
                    continue

                try:
                    model_obj = self.lineage_service.registry.get_model(model_name)
                    if not model_obj:
                        continue

                    resource_type = getattr(model_obj, "resource_type", None)

                    for col_name in sorted(columns.keys()):
                        col_obj = model_obj.columns.get(col_name)
                        if not col_obj:
                            continue

                        node_id = f"col_{model_name}_{col_name}"

                        found = False
                        for node in self.data.nodes:
                            if node["id"] == node_id:
                                node["data_type"] = col_obj.data_type
                                node["resource_type"] = resource_type
                                found = True
                                break

                        if not found:
                            self._add_node(
                                id=node_id,
                                label=col_name,
                                model=model_name,
                                data_type=col_obj.data_type,
                                resource_type=resource_type,
                            )
                except Exception as e:
                    logger.error(f"Error enriching node metadata for {model_name}: {e}")

    def _queue_additional_nodes(
        self,
        upstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]],
        downstream_refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]],
        processed: Set[tuple[str, str]],
        to_process: List[tuple[str, str]],
    ) -> None:
        """Queue additional nodes for processing in sorted order for deterministic BFS."""
        new_nodes: List[tuple[str, str]] = []
        for refs in [upstream_refs, downstream_refs]:
            for model_name, columns in sorted(refs.items()):
                if model_name == "exposures" or not isinstance(columns, dict):
                    continue

                for col_name in sorted(columns.keys()):
                    if (model_name, col_name) not in processed and (
                        model_name,
                        col_name,
                    ) not in to_process:
                        new_nodes.append((model_name, col_name))

        new_nodes.sort()
        to_process.extend(new_nodes)

    def _add_processed_data(
        self,
        refs: Dict[str, Union[Dict[str, ColumnLineage], Set[str]]],
        direction: str,
        main_node_id: Optional[str] = None,
    ) -> None:
        """Process refs and add to graph."""
        processed = self._process_refs(refs, direction, main_node_id)

        for node in processed["nodes"]:
            existing_node = next((n for n in self.data.nodes if n["id"] == node["id"]), None)
            if existing_node:
                if "direction" in node and "direction" not in existing_node:
                    existing_node["direction"] = node["direction"]
                for key in ["data_type", "resource_type"]:
                    if key in node and (key not in existing_node or existing_node[key] is None):
                        existing_node[key] = node[key]
            else:
                self.data.nodes.append(node)

        for edge in processed["edges"]:
            if not any(
                e["source"] == edge["source"] and e["target"] == edge["target"]
                for e in self.data.edges
            ):
                self.data.edges.append(edge)

        if direction == "upstream" and main_node_id and self.lineage_service:
            try:
                if self._start_model and self._start_column:
                    model_obj = self.lineage_service.registry.get_model(self._start_model)
                    col_obj = model_obj.columns.get(self._start_column)
                    if col_obj and col_obj.lineage:
                        for lin in col_obj.lineage:
                            for source in lin.source_columns:
                                split_result = self._split_qualified_name(source)
                                if split_result:
                                    src_model, src_col = split_result
                                    src_node_id = f"col_{src_model}_{src_col}"
                                    edge = GraphEdge(
                                        source=src_node_id, target=main_node_id
                                    ).model_dump()
                                    if not any(
                                        e["source"] == edge["source"]
                                        and e["target"] == edge["target"]
                                        for e in self.data.edges
                                    ):
                                        self.data.edges.append(edge)
            except Exception as e:
                logger.debug(f"Could not create main_node upstream edges: {e}")

        if direction == "downstream" and self.lineage_service:
            try:
                if "exposures" not in refs or not isinstance(refs["exposures"], set):
                    return

                exposure_names = refs["exposures"]
                if not exposure_names:
                    return

                for exposure_name in sorted(exposure_names):
                    try:
                        if not self.lineage_service:
                            continue
                        exposure = self.lineage_service.registry.get_exposure(exposure_name)
                        if not exposure or not hasattr(exposure, "depends_on_models"):
                            continue

                        exposure_node_id = f"exposure_{exposure_name}"

                        for model_name in sorted(exposure.depends_on_models):
                            model_in_refs = model_name in refs and isinstance(
                                refs[model_name], dict
                            )
                            is_starting_model = (
                                hasattr(self, "_start_model") and model_name == self._start_model
                            )

                            if model_in_refs or is_starting_model:
                                try:
                                    model = self.lineage_service.registry.get_model(model_name)
                                    if not model or not hasattr(model, "columns"):
                                        continue

                                    for col_name in sorted(model.columns.keys()):
                                        if is_starting_model and col_name != self._start_column:
                                            continue

                                        col_node_id = f"col_{model_name}_{col_name}"
                                        if any(
                                            n["id"] == col_node_id for n in self.data.nodes
                                        ) and any(
                                            n["id"] == exposure_node_id for n in self.data.nodes
                                        ):
                                            edge = GraphEdge(
                                                source=col_node_id,
                                                target=exposure_node_id,
                                                type="exposure",
                                            ).model_dump()
                                            if not any(
                                                e["source"] == edge["source"]
                                                and e["target"] == edge["target"]
                                                for e in self.data.edges
                                            ):
                                                self.data.edges.append(edge)
                                except (ValueError, KeyError, AttributeError) as e:
                                    logger.debug(
                                        f"Failed to process exposure edge for {model_name} -> {exposure_name}: {e}"
                                    )
                                    continue
                                except Exception as e:
                                    logger.warning(
                                        f"Unexpected error processing exposure edge: {e}"
                                    )
                                    continue
                    except (ValueError, KeyError) as e:
                        logger.debug(f"Exposure {exposure_name} not found: {e}")
                        continue
                    except Exception as e:
                        logger.warning(f"Failed to process exposure {exposure_name}: {e}")
                        continue
            except Exception as e:
                logger.warning(f"Failed to add exposure edges: {e}", exc_info=True)

    def set_lineage_service(self, lineage_service: "LineageService") -> None:
        """Set the lineage service for the explore server."""
        self.lineage_service = lineage_service

    def _set_column_info(self, column: Column) -> None:
        """Set the main column info for display."""
        model_name = column.model_name

        self.data.column_info = ColumnInfo(
            name=column.name,
            model=model_name,
            type=column.data_type,
            description=column.description,
        )

        resource_type = self._get_model_resource_type(model_name)

        self._add_node(
            id=f"col_{model_name}_{column.name}",
            label=column.name,
            model=model_name,
            data_type=column.data_type,
            is_main=True,
            resource_type=resource_type,
        )

    def _get_model_resource_type(self, model_name: str) -> Optional[str]:
        """Get resource type for a model."""
        try:
            if self.lineage_service:
                model_obj = self.lineage_service.registry.get_model(model_name)
                if model_obj:
                    return getattr(model_obj, "resource_type", None)
        except Exception:
            pass
        return None

    def start(self) -> None:
        """Start the server to display the graph."""
        uvicorn.run(self.app, host=self.host, port=self.port)

    def _add_node(
        self,
        id: str,
        label: str,
        model: str,
        data_type: Optional[str] = None,
        is_main: bool = False,
        resource_type: Optional[str] = None,
        is_key: bool = False,
    ) -> Dict[str, Any]:
        """Helper to create and add a node."""
        node = GraphNode(
            id=id,
            label=label,
            type="column",
            model=model,
            data_type=data_type,
            is_main=is_main,
            resource_type=resource_type,
            is_key=is_key,
        ).model_dump()

        self.data.nodes.append(node)
        return node

    def _add_edge(self, source_id: str, target_id: str) -> Dict[str, str]:
        """Helper to create and add an edge."""
        edge = GraphEdge(source=source_id, target=target_id, type="lineage").model_dump()

        self.data.edges.append(edge)
        return edge

    def _process_refs(
        self,
        refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]],
        direction: str,
        main_node_id: Optional[str] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Process reference data into nodes and edges."""
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        node_ids = set()

        if "exposures" in refs and isinstance(refs["exposures"], set):
            exposure_names = refs["exposures"]
            for exposure_name in sorted(exposure_names):
                try:
                    if not self.lineage_service:
                        continue
                    exposure = self.lineage_service.registry.get_exposure(exposure_name)
                    exposure_node_id = f"exposure_{exposure_name}"
                    if exposure_node_id not in node_ids:
                        exposure_node = GraphNode(
                            id=exposure_node_id,
                            label=exposure_name,
                            type="exposure",
                            model=exposure_name,
                            resource_type="exposure",
                        ).model_dump()
                        exposure_node["exposure_data"] = {
                            "name": exposure.name,
                            "type": exposure.type,
                            "url": exposure.url,
                            "description": exposure.description,
                        }
                        nodes.append(exposure_node)
                        node_ids.add(exposure_node_id)
                except Exception as e:
                    logger.warning(f"Failed to process exposure {exposure_name}: {e}")

        for model_name, columns in sorted(refs.items()):
            if model_name == "exposures" or not isinstance(columns, dict):
                continue

            model_resource_type = self._get_model_resource_type(model_name)

            for col_name, lineage in sorted(columns.items()):
                col_node_id = f"col_{model_name}_{col_name}"
                if col_node_id not in node_ids:
                    col_node = GraphNode(
                        id=col_node_id,
                        label=col_name,
                        type="column",
                        model=model_name,
                        data_type=getattr(lineage, "data_type", None),
                        resource_type=model_resource_type,
                    ).model_dump()
                    col_node["direction"] = direction

                    nodes.append(col_node)
                    node_ids.add(col_node_id)

                if direction == "upstream" and hasattr(lineage, "source_columns"):
                    filtered_sources = [
                        src
                        for src in lineage.source_columns
                        if not src.lower().endswith(f"{model_name}.{col_name}".lower())
                    ]
                    if filtered_sources:
                        self._process_source_columns(
                            filtered_sources, col_node_id, refs, nodes, edges, node_ids
                        )
                elif direction == "downstream" and hasattr(lineage, "source_columns"):
                    self._add_downstream_edges(lineage.source_columns, col_node_id, edges)

                    if "exposures" in refs and isinstance(refs["exposures"], set):
                        for exposure_name in sorted(refs["exposures"]):
                            try:
                                if not self.lineage_service:
                                    continue
                                exposure = self.lineage_service.registry.get_exposure(exposure_name)
                                if model_name in exposure.depends_on_models:
                                    exposure_node_id = f"exposure_{exposure_name}"
                                    if exposure_node_id in node_ids:
                                        edge = GraphEdge(
                                            source=col_node_id,
                                            target=exposure_node_id,
                                            type="exposure",
                                        ).model_dump()
                                        if not any(
                                            e["source"] == edge["source"]
                                            and e["target"] == edge["target"]
                                            for e in edges
                                        ):
                                            edges.append(edge)
                            except Exception:
                                pass

        return {"nodes": nodes, "edges": edges}

    def _split_qualified_name(self, qualified_name: str) -> Optional[tuple[str, str]]:
        """Split a fully qualified name into model and column parts. Returns None if invalid."""
        if "." not in qualified_name:
            return None
        parts = qualified_name.split(".")
        if len(parts) < 2:
            return None
        model_part = ".".join(parts[:-1])
        column_part = parts[-1]
        return (model_part, column_part)

    def _add_downstream_edges(
        self,
        source_columns: Union[List[str], Set[str]],
        target_node_id: str,
        edges: List[Dict[str, Any]],
    ) -> None:
        """Add edges for downstream lineage."""
        for source in source_columns:
            split_result = self._split_qualified_name(source)
            if split_result is None:
                continue
            src_model, src_col = split_result
            src_node_id = f"col_{src_model}_{src_col}"
            edge = GraphEdge(source=src_node_id, target=target_node_id).model_dump()
            edges.append(edge)

    def _process_source_columns(
        self,
        source_columns: Union[List[str], Set[str]],
        target_node_id: str,
        refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]],
        nodes: List[Dict[str, Any]],
        edges: List[Dict[str, Any]],
        node_ids: Set[str],
    ) -> None:
        """Process source columns and create nodes/edges."""
        for source in source_columns:
            split_result = self._split_qualified_name(source)
            if split_result is None:
                continue
            src_model, src_col = split_result
            src_node_id = f"col_{src_model}_{src_col}"

            if src_node_id not in node_ids:
                self._add_source_node(src_model, src_col, refs, nodes, node_ids)

            edge = GraphEdge(source=src_node_id, target=target_node_id).model_dump()
            edges.append(edge)

    def _add_source_node(
        self,
        src_model: str,
        src_col: str,
        refs: Mapping[str, Union[Dict[str, ColumnLineage], Set[str]]],
        nodes: List[Dict[str, Any]],
        node_ids: Set[str],
    ) -> None:
        """Add a source node to the graph."""
        src_node_id = f"col_{src_model}_{src_col}"
        src_data_type = None
        model_resource_type = self._get_model_resource_type(src_model)

        if src_model in refs and isinstance(refs[src_model], dict):
            src_model_data = refs[src_model]
            if isinstance(src_model_data, dict) and src_col in src_model_data:
                src_lineage = src_model_data[src_col]
                src_data_type = getattr(src_lineage, "data_type", None)

        src_node = GraphNode(
            id=src_node_id,
            label=src_col,
            type="column",
            model=src_model,
            data_type=src_data_type,
            resource_type=model_resource_type,
        ).model_dump()
        src_node["direction"] = "upstream"

        nodes.append(src_node)
        node_ids.add(src_node_id)
