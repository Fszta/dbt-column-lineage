/**
 * Process input data, compute layouts, and build relationships
 */

// Create initial state object to store graph data
function createState() {
    return {
        models: [],
        exposures: [],
        nodeIndex: new Map(),
        columnPositions: new Map(),
        exposurePositions: new Map(),
        columnElements: new Map(),
        modelEdges: new Map(),
        levelGroups: new Map(),
        lineage: {
            upstream: new Map(),
            downstream: new Map()
        },
        visibleModels: new Set(),
        modelDownstream: new Map(),
        modelUpstream: new Map(),
        // Synthetic "+N more" placeholder nodes for capped fan-out (progressive disclosure).
        moreNodes: []
    };
}

// Process input data to build models and indexes
function processData(data, state) {
    // Normalize row-set (filter/JOIN/QUALIFY) dependents into the exposure family: they are
    // leaf consumer nodes hanging off a column, exactly like exposures, so they reuse the same
    // positioning/edge/visibility pipeline. A flag on exposure_data lets the renderer style them
    // distinctly (dashed "ROW-SET" box) and surface the predicate as a note.
    data.nodes.forEach(node => {
        if (node.type === 'rowset') {
            node.type = 'exposure';
            node.exposure_data = { type: 'row-set', rowset: true, note: node.note || '' };
        }
    });
    data.edges.forEach(edge => {
        if (edge.type === 'rowset') edge.type = 'exposure';
    });

    // Index nodes for quick lookup
    data.nodes.forEach(node => {
        state.nodeIndex.set(node.id, node);
    });

    const modelGroups = {};
    const modelTypes = {};
    const exposureGroups = {};

    // First pass: gather all resource_types by model to handle cases
    // where only some columns in a model have the type defined.
    data.nodes.forEach(node => {
        if (node.type === 'column' && node.resource_type) {
            if (!modelTypes[node.model]) {
                modelTypes[node.model] = node.resource_type;
            }
        }
    });

    // Second pass: create model groups using the determined type.
    data.nodes.forEach(node => {
        if (node.type === 'column') {
            const resourceType = modelTypes[node.model] || node.resource_type || 'model';

            if (!modelGroups[node.model]) {
                modelGroups[node.model] = {
                    name: node.model,
                    columns: [],
                    isMain: node.is_main || false,
                    type: resourceType
                };
            }

            modelGroups[node.model].columns.push({
                name: node.label,
                id: node.id,
                dataType: node.data_type,
                isKey: node.is_key || false,
                tests: node.tests || []
            });
        } else if (node.type === 'exposure') {
            if (!exposureGroups[node.model]) {
                exposureGroups[node.model] = {
                    name: node.model,
                    columns: [],
                    isMain: false,
                    type: 'exposure',
                    exposureData: node.exposure_data || {}
                };
            }
        }
    });

    state.models = Object.values(modelGroups);
    state.exposures = Object.values(exposureGroups);

    buildLineageMaps(data, state);
    buildModelDownstreamMap(data, state);
    buildModelUpstreamMap(data, state);
    initializeVisibility(data, state);
    layoutModels(data, state);
}

// Build map of downstream models for each model (including exposures)
function buildModelDownstreamMap(data, state) {
    state.models.forEach(model => {
        const downstreamModels = new Set();

        // Find all columns in this model
        const modelColumns = model.columns.map(col => col.id);

        // Find all edges where source is a column in this model
        data.edges.filter(e => e.type === 'lineage').forEach(edge => {
            const sourceNode = state.nodeIndex.get(edge.source);
            if (sourceNode && modelColumns.includes(edge.source)) {
                const targetNode = state.nodeIndex.get(edge.target);
                if (targetNode && targetNode.model !== model.name) {
                    downstreamModels.add(targetNode.model);
                }
            }
        });

        // Also check for exposure edges - exposures are downstream
        data.edges.filter(e => e.type === 'exposure').forEach(edge => {
            const sourceNode = state.nodeIndex.get(edge.source);
            if (sourceNode && modelColumns.includes(edge.source)) {
                const targetNode = state.nodeIndex.get(edge.target);
                if (targetNode && targetNode.type === 'exposure') {
                    downstreamModels.add(targetNode.model);
                }
            }
        });

        state.modelDownstream.set(model.name, downstreamModels);
    });
}

function buildModelUpstreamMap(data, state) {
    state.models.forEach(model => {
        const upstreamModels = new Set();
        const modelColumns = model.columns.map(col => col.id);

        data.edges.filter(e => e.type === 'lineage').forEach(edge => {
            const targetNode = state.nodeIndex.get(edge.target);
            if (targetNode && modelColumns.includes(edge.target)) {
                const sourceNode = state.nodeIndex.get(edge.source);
                if (sourceNode && sourceNode.model !== model.name) {
                    upstreamModels.add(sourceNode.model);
                }
            }
        });

        state.modelUpstream.set(model.name, upstreamModels);
    });
}

// Initialize visibility: show main model and its direct neighbors, but CAP the
// fan-out so a high-degree column stays legible. Anything over the caps in
// config's GRAPH_NODE_LIMITS is collapsed behind a synthetic "+N more" node
// (state.moreNodes), which the user can click to reveal the rest.
function initializeVisibility(data, state) {
    state.moreNodes = [];

    // Find the main model
    const mainModel = state.models.find(m => m.isMain);
    if (!mainModel) {
        // If no main model, show all models and exposures
        state.models.forEach(m => state.visibleModels.add(m.name));
        state.exposures.forEach(e => state.visibleModels.add(e.name));
        return;
    }

    // Caps (fall back to generous defaults if config const is unavailable).
    const limits = (typeof GRAPH_NODE_LIMITS !== 'undefined')
        ? GRAPH_NODE_LIMITS
        : { maxDownstreamModels: 8, maxExposures: 8 };

    // Add main model
    state.visibleModels.add(mainModel.name);

    // Find direct upstream and downstream neighbors
    const mainModelColumns = new Set(mainModel.columns.map(col => col.id));
    const upstreamModels = new Set();
    const downstreamModels = new Set();

    data.edges.filter(e => e.type === 'lineage').forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);
        if (!sourceNode || !targetNode) return;

        // If source is in main model, target is downstream
        if (mainModelColumns.has(edge.source) && targetNode.model !== mainModel.name) {
            downstreamModels.add(targetNode.model);
        }
        // If target is in main model, source is upstream
        if (mainModelColumns.has(edge.target) && sourceNode.model !== mainModel.name) {
            upstreamModels.add(sourceNode.model);
        }
    });

    // Upstream is shown in full — the readability problem is downstream fan-out.
    upstreamModels.forEach(modelName => state.visibleModels.add(modelName));

    // Downstream MODELS: show the first N (sorted for deterministic order), collapse the rest.
    const downstreamList = Array.from(downstreamModels).sort();
    const shownDownstream = downstreamList.slice(0, limits.maxDownstreamModels);
    const hiddenDownstream = downstreamList.slice(limits.maxDownstreamModels);
    shownDownstream.forEach(modelName => state.visibleModels.add(modelName));
    if (hiddenDownstream.length > 0) {
        state.moreNodes.push({
            id: '__more_downstream__',
            kind: 'model',
            anchor: mainModel.name,
            siblings: shownDownstream,   // used to position the badge in the same column
            hidden: hiddenDownstream
        });
    }

    // EXPOSURES (incl. row-set dependents): an exposure is a candidate if any of
    // its source models is currently visible. Cap the candidate set too.
    const candidateExposures = new Set();
    data.edges.filter(e => e.type === 'exposure').forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);
        if (sourceNode && targetNode && targetNode.type === 'exposure') {
            if (state.visibleModels.has(sourceNode.model)) {
                candidateExposures.add(targetNode.model);
            }
        }
    });

    const exposureList = Array.from(candidateExposures).sort();
    const shownExposures = exposureList.slice(0, limits.maxExposures);
    const hiddenExposures = exposureList.slice(limits.maxExposures);
    shownExposures.forEach(name => state.visibleModels.add(name));
    if (hiddenExposures.length > 0) {
        state.moreNodes.push({
            id: '__more_exposures__',
            kind: 'exposure',
            anchor: mainModel.name,
            siblings: shownExposures,
            hidden: hiddenExposures
        });
    }
}

// Position the synthetic "+N more" nodes just beneath the last shown sibling in
// their column, so a badge reads clearly as the tail of the group it summarizes.
// Runs AFTER positionModels (which sets sibling x/y). Kept separate from the
// level-based layout: these are overlays, not real graph nodes.
function positionMoreNodes(state, config) {
    if (!state.moreNodes || state.moreNodes.length === 0) return;

    const moreHeight = config.box.titleHeight + 20; // compact, clearly smaller than a model

    state.moreNodes.forEach(mn => {
        mn.height = moreHeight;

        const lookup = mn.kind === 'exposure'
            ? name => state.exposures.find(e => e && e.name === name)
            : name => state.models.find(m => m && m.name === name);

        let columnX = null;
        let maxBottom = -Infinity;
        (mn.siblings || []).forEach(name => {
            const obj = lookup(name);
            if (obj && typeof obj.x === 'number' && !isNaN(obj.x) &&
                typeof obj.y === 'number' && !isNaN(obj.y)) {
                columnX = obj.x;
                const bottom = obj.y + (obj.height || 0) / 2;
                if (bottom > maxBottom) maxBottom = bottom;
            }
        });

        if (columnX === null) {
            // No visible siblings to anchor to — fall back to one level right of the anchor.
            const anchor = state.models.find(m => m && m.name === mn.anchor);
            if (anchor && typeof anchor.x === 'number' && !isNaN(anchor.x)) {
                columnX = anchor.x + config.box.width + config.layout.xSpacing;
                maxBottom = (typeof anchor.y === 'number' && !isNaN(anchor.y))
                    ? anchor.y : config.box.padding;
            } else {
                columnX = config.box.padding;
                maxBottom = config.box.padding;
            }
        }

        mn.x = columnX;
        mn.y = maxBottom + config.layout.ySpacing + moreHeight / 2;
    });
}

// Build maps of upstream and downstream relationships for columns
function buildLineageMaps(data, state) {
    const upstreamMap = new Map();
    const downstreamMap = new Map();

    data.edges.filter(e => e.type === 'lineage').forEach(edge => {
        const sourceId = edge.source;
        const targetId = edge.target;

        if (!upstreamMap.has(targetId)) {
            upstreamMap.set(targetId, new Set());
        }
        upstreamMap.get(targetId).add(sourceId);
        upstreamMap.get(targetId).add(targetId); // Include self

        if (!downstreamMap.has(sourceId)) {
            downstreamMap.set(sourceId, new Set());
        }
        downstreamMap.get(sourceId).add(targetId);
        downstreamMap.get(sourceId).add(sourceId); // Include self
    });

    // Recursively find all connected columns (full upstream/downstream)
    function getAllConnected(columnId, map, visited = new Set()) {
        if (visited.has(columnId)) return visited;

        visited.add(columnId);
        const directConnections = map.get(columnId);

        if (directConnections) {
            directConnections.forEach(connectedId => {
                getAllConnected(connectedId, map, visited);
            });
        }

        return visited;
    }

    upstreamMap.forEach((_, columnId) => {
        state.lineage.upstream.set(columnId, getAllConnected(columnId, upstreamMap));
    });

    downstreamMap.forEach((_, columnId) => {
        state.lineage.downstream.set(columnId, getAllConnected(columnId, downstreamMap));
    });
}

// Calculate model positions based on their dependencies
function layoutModels(data, state) {
    const dependencies = new Map();
    const modelIncomingEdges = new Map();
    const modelOutgoingEdges = new Map();

    state.models.forEach(model => {
        dependencies.set(model.name, { model, inDegree: 0, outDegree: 0, level: -1 });
        modelIncomingEdges.set(model.name, new Set());
        modelOutgoingEdges.set(model.name, new Set());
    });

    state.exposures.forEach(exposure => {
        dependencies.set(exposure.name, { model: exposure, inDegree: 0, outDegree: 0, level: -1 });
        modelIncomingEdges.set(exposure.name, new Set());
        modelOutgoingEdges.set(exposure.name, new Set());
    });

    data.edges.forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);

        if (sourceNode && targetNode && sourceNode.model !== targetNode.model) {
            const sourceModel = sourceNode.model;
            const targetModel = targetNode.model;

            const sourceEdges = modelOutgoingEdges.get(sourceModel);
            const targetEdges = modelIncomingEdges.get(targetModel);

            if (sourceEdges && targetEdges) {
                sourceEdges.add(targetModel);
                targetEdges.add(sourceModel);

                const sourceInfo = dependencies.get(sourceModel);
                const targetInfo = dependencies.get(targetModel);

                if (sourceInfo && targetInfo) {
                    sourceInfo.outDegree++;
                    targetInfo.inDegree++;
                }
            }
        }
    });

    const visited = new Set();
    let currentLevel = 0;

    let modelsInCurrentLevel = [...dependencies.values()]
        .filter(info => info.inDegree === 0)
        .map(info => info.model.name);

    if (modelsInCurrentLevel.length === 0 && state.models.length > 0) {
        let minInDegree = Infinity;
        state.models.forEach(model => {
            const info = dependencies.get(model.name);
            if (info && info.inDegree < minInDegree) {
                minInDegree = info.inDegree;
            }
        });
        modelsInCurrentLevel = [...dependencies.values()]
            .filter(info => info.inDegree === minInDegree)
            .map(info => info.model.name);
    }

    while (modelsInCurrentLevel.length > 0) {
        const nextLevelModels = new Set();

        modelsInCurrentLevel.forEach(modelName => {
            if (visited.has(modelName)) return;

            const info = dependencies.get(modelName);
            if (info) {
                info.level = currentLevel;
                visited.add(modelName);
            }

            modelOutgoingEdges.get(modelName).forEach(targetModel => {
                if (!visited.has(targetModel)) {
                    const allDepsProcessed = [...modelIncomingEdges.get(targetModel)]
                        .every(depModel => visited.has(depModel));

                    if (allDepsProcessed) {
                        nextLevelModels.add(targetModel);
                    }
                }
            });
        });

        modelsInCurrentLevel = [...nextLevelModels];
        currentLevel++;
    }

    dependencies.forEach((info, modelName) => {
        if (info.level === -1) {
            info.level = currentLevel;
            currentLevel++;
        }
    });

    const levelGroups = new Map();
    dependencies.forEach((info) => {
        if (!levelGroups.has(info.level)) {
            levelGroups.set(info.level, []);
        }
        levelGroups.get(info.level).push(info.model);
    });

    state.levelGroups = levelGroups;
}

// Position models in the grid layout (only visible models)
function positionModels(state, config) {
    // Early return if no visible models
    if (!state.visibleModels || state.visibleModels.size === 0) {
        return;
    }

    let currentXOffset = config.box.padding;
    const levelWidths = new Map();

    // Calculate heights for all models (needed for layout calculation)
    state.models.forEach(model => {
        if (!model) return;
        model.columnsCollapsed = model.columnsCollapsed || false;
        model.height = config.box.titleHeight + 28 +
                      ((model.columns && model.columns.length) ? (model.columns.length * config.box.columnHeight) : 0) +
                      config.box.padding;
        if (model.columnsCollapsed) {
            model.height = config.box.titleHeight + 28;
        }
    });

    // Calculate heights for ALL exposures (even if not visible yet)
    state.exposures.forEach(exposure => {
        if (!exposure) return;
        const exposureData = exposure.exposureData || {};
        let detailRows = 0;
        if (exposureData.type) detailRows++;
        if (exposureData.url) detailRows++;
        if (exposureData.rowset && exposureData.note) detailRows++;

        exposure.height = config.box.titleHeight +
                          (detailRows * config.box.columnHeight) +
                          config.box.padding;
    });

    // Filter level groups to only include visible models
    const visibleLevelGroups = new Map();
    if (state.levelGroups) {
        state.levelGroups.forEach((models, level) => {
            if (!models || !Array.isArray(models)) return;
            const visibleModels = models.filter(m => m && m.name && state.visibleModels.has(m.name));
            if (visibleModels.length > 0) {
                visibleLevelGroups.set(level, visibleModels);
            }
        });
    }

    // Also add visible exposures that aren't in level groups
    // Position exposures in the same level as the models they depend on, or right after the last level
    const visibleExposures = state.exposures.filter(e => e && e.name && state.visibleModels.has(e.name));
    if (visibleExposures.length > 0) {
        // Find the maximum level
        const maxLevel = visibleLevelGroups.size > 0
            ? Math.max(...Array.from(visibleLevelGroups.keys()))
            : -1;

        // Instead of creating a new level far away, add exposures to the last level
        // This keeps them closer to the models they depend on
        if (maxLevel >= 0 && visibleLevelGroups.has(maxLevel)) {
            // Add exposures to the last level
            const lastLevelModels = visibleLevelGroups.get(maxLevel);
            visibleLevelGroups.set(maxLevel, [...lastLevelModels, ...visibleExposures]);
        } else {
            // If no levels exist, create a new level for exposures
            visibleLevelGroups.set(0, visibleExposures);
        }
    }

    const sortedLevels = Array.from(visibleLevelGroups.keys()).sort((a, b) => a - b);

    // Early return if no levels to position
    if (sortedLevels.length === 0) {
        return;
    }

    sortedLevels.forEach(level => {
        const modelsInLevel = visibleLevelGroups.get(level);
        let currentYOffset = config.box.padding;
        let maxModelWidthInLevel = 0;

        modelsInLevel.forEach((item, idx) => {
            if (!item) return;

            if (item.type === 'exposure') {
                if (!item.height || isNaN(item.height)) {
                    const exposureData = item.exposureData || {};
                    let detailRows = 0;
                    if (exposureData.type) detailRows++;
                    if (exposureData.url) detailRows++;
                    if (exposureData.rowset && exposureData.note) detailRows++;

                    item.height = config.box.titleHeight +
                                  (detailRows * config.box.columnHeight) +
                                  config.box.padding;
                }
            } else {
                if (!item.height || isNaN(item.height)) {
                    item.height = config.box.titleHeight + 28 +
                                  ((item.columns && item.columns.length) ? (item.columns.length * config.box.columnHeight) : 0) +
                                  config.box.padding;
                    if (item.columnsCollapsed) {
                        item.height = config.box.titleHeight + 28;
                    }
                }
            }

            // Ensure height is valid
            if (!item.height || isNaN(item.height) || item.height <= 0) {
                item.height = config.box.titleHeight + 28 + config.box.padding;
            }

            // Validate height before positioning
            if (!item.height || isNaN(item.height) || item.height <= 0) {
                console.warn(`Invalid height for ${item.name || 'unknown'}, skipping`);
                return; // Skip this item
            }

            item.x = currentXOffset;
            item.y = currentYOffset + item.height / 2;

            // Validate positions are valid numbers
            if (isNaN(item.x) || isNaN(item.y)) {
                console.warn(`Invalid position calculated for ${item.name || 'unknown'}, skipping`);
                return;
            }

            currentYOffset += item.height + config.layout.ySpacing;

            maxModelWidthInLevel = Math.max(maxModelWidthInLevel, config.box.width);
        });

        if (modelsInLevel.length > 0) {
            levelWidths.set(level, maxModelWidthInLevel);
            currentXOffset += maxModelWidthInLevel + config.layout.xSpacing;
        } else {
            levelWidths.set(level, 0);
        }
    });

    let maxYOffset = 0;
    sortedLevels.forEach(level => {
        const modelsInLevel = visibleLevelGroups.get(level);
        let levelHeight = 0;
        if (modelsInLevel && modelsInLevel.length > 0) {
            const lastModel = modelsInLevel[modelsInLevel.length - 1];
            if (lastModel && typeof lastModel.y === 'number' && !isNaN(lastModel.y) &&
                typeof lastModel.height === 'number' && !isNaN(lastModel.height)) {
                levelHeight = (lastModel.y + lastModel.height / 2);
            }
        }
        maxYOffset = Math.max(maxYOffset, levelHeight);
    });

    sortedLevels.forEach(level => {
        const modelsInLevel = visibleLevelGroups.get(level);
         let currentLevelHeight = 0;
         if (modelsInLevel && modelsInLevel.length > 0) {
             const lastModel = modelsInLevel[modelsInLevel.length - 1];
             if (lastModel && typeof lastModel.y === 'number' && !isNaN(lastModel.y) &&
                 typeof lastModel.height === 'number' && !isNaN(lastModel.height)) {
                 currentLevelHeight = (lastModel.y + lastModel.height / 2);
             }
         }
         const verticalShift = (maxYOffset - currentLevelHeight) / 2;

         if (verticalShift > 0 && !isNaN(verticalShift) && modelsInLevel) {
             modelsInLevel.forEach(model => {
                 if (model && typeof model.y === 'number' && !isNaN(model.y)) {
                     model.y += verticalShift;
                 }
             });
         }
    });
}
