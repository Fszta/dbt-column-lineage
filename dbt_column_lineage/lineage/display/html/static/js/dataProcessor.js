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
        visibleModels: new Set(), // Track which models are visible
        modelDownstream: new Map() // Track downstream models for each model
    };
}

// Process input data to build models and indexes
function processData(data, state) {
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
                isKey: node.is_key || false
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

// Initialize visibility: show main model and its direct neighbors
function initializeVisibility(data, state) {
    // Find the main model
    const mainModel = state.models.find(m => m.isMain);
    if (!mainModel) {
        // If no main model, show all models and exposures
        state.models.forEach(m => state.visibleModels.add(m.name));
        state.exposures.forEach(e => state.visibleModels.add(e.name));
        return;
    }

    // Add main model
    state.visibleModels.add(mainModel.name);

    // Find direct upstream and downstream neighbors
    const mainModelColumns = mainModel.columns.map(col => col.id);
    const connectedModels = new Set();

    data.edges.filter(e => e.type === 'lineage').forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);

        if (sourceNode && targetNode) {
            // If source is in main model, target is downstream
            if (mainModelColumns.includes(edge.source) && targetNode.model !== mainModel.name) {
                connectedModels.add(targetNode.model);
            }
            // If target is in main model, source is upstream
            if (mainModelColumns.includes(edge.target) && sourceNode.model !== mainModel.name) {
                connectedModels.add(sourceNode.model);
            }
        }
    });

    // Add all connected models
    connectedModels.forEach(modelName => state.visibleModels.add(modelName));

    // Also check for exposures connected to visible models
    data.edges.filter(e => e.type === 'exposure').forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);

        if (sourceNode && targetNode && targetNode.type === 'exposure') {
            // If source model is visible, show the exposure
            if (state.visibleModels.has(sourceNode.model)) {
                state.visibleModels.add(targetNode.model);
            }
        }
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
