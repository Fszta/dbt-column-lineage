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
        }
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
    layoutModels(data, state);
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

// Position models in the grid layout
function positionModels(state, config) {
    let currentXOffset = config.box.padding;
    const levelWidths = new Map();

    state.models.forEach(model => {
        model.columnsCollapsed = model.columnsCollapsed || false;
        model.height = config.box.titleHeight + 28 +
                      (model.columns.length * config.box.columnHeight) +
                      config.box.padding; 
        if (model.columnsCollapsed) {
            model.height = config.box.titleHeight + 28;
        }
    });
    
    state.exposures.forEach(exposure => {
               const exposureData = exposure.exposureData || {};
               let detailRows = 0;
               if (exposureData.type) detailRows++;
               if (exposureData.url) detailRows++;
               
        exposure.height = config.box.titleHeight + 
                          (detailRows * config.box.columnHeight) +
                          config.box.padding;
    });

    const sortedLevels = Array.from(state.levelGroups.keys()).sort((a, b) => a - b);
    
    sortedLevels.forEach(level => {
        const modelsInLevel = state.levelGroups.get(level);
        let currentYOffset = config.box.padding;
        let maxModelWidthInLevel = 0;

        modelsInLevel.forEach((item, idx) => {
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
                                  (item.columns.length * config.box.columnHeight) +
                                  config.box.padding;
                    if (item.columnsCollapsed) {
                        item.height = config.box.titleHeight + 28;
                    }
                }
            }

            item.x = currentXOffset;
            item.y = currentYOffset + item.height / 2;
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
        const modelsInLevel = state.levelGroups.get(level);
        let levelHeight = 0;
        if (modelsInLevel.length > 0) {
            const lastModel = modelsInLevel[modelsInLevel.length - 1];
            levelHeight = (lastModel.y + lastModel.height / 2);
        }
        maxYOffset = Math.max(maxYOffset, levelHeight);
    });

    sortedLevels.forEach(level => {
        const modelsInLevel = state.levelGroups.get(level);
         let currentLevelHeight = 0;
         if (modelsInLevel.length > 0) {
             const lastModel = modelsInLevel[modelsInLevel.length - 1];
             currentLevelHeight = (lastModel.y + lastModel.height / 2);
         }
         const verticalShift = (maxYOffset - currentLevelHeight) / 2;

         if (verticalShift > 0) {
             modelsInLevel.forEach(model => {
                 model.y += verticalShift;
             });
         }
    });
}