/**
 * Process input data, compute layouts, and build relationships
 */

// Create initial state object to store graph data
function createState() {
    return {
        models: [],
        nodeIndex: new Map(),
        columnPositions: new Map(),
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
        }
    });

    state.models = Object.values(modelGroups);
    
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
    state.models.forEach(model => {
        dependencies.set(model.name, { model, inDegree: 0, outDegree: 0, level: 0 });
    });
    
    // Count dependencies between models
    data.edges.forEach(edge => {
        const sourceNode = state.nodeIndex.get(edge.source);
        const targetNode = state.nodeIndex.get(edge.target);
        
        if (sourceNode && targetNode && sourceNode.model !== targetNode.model) {
            const sourceInfo = dependencies.get(sourceNode.model);
            const targetInfo = dependencies.get(targetNode.model);
            
            if (sourceInfo && targetInfo) {
                sourceInfo.outDegree++;
                targetInfo.inDegree++;
            }
        }
    });
    
    // Assign levels based on topological sort approach
    let currentLevel = 0;
    let modelsInCurrentLevel = [...dependencies.values()]
        .filter(info => info.inDegree === 0)
        .map(info => info.model.name);
    
    while (modelsInCurrentLevel.length > 0) {
        modelsInCurrentLevel.forEach(modelName => {
            const info = dependencies.get(modelName);
            if (info) info.level = currentLevel;
        });
        
        const nextLevelModels = [];
        data.edges.forEach(edge => {
            const sourceNode = state.nodeIndex.get(edge.source);
            const targetNode = state.nodeIndex.get(edge.target);
            
            // Find edges originating from the current level to other levels
            if (sourceNode && targetNode && 
                modelsInCurrentLevel.includes(sourceNode.model) && 
                !modelsInCurrentLevel.includes(targetNode.model)) {
                nextLevelModels.push(targetNode.model);
            }
        });
        
        modelsInCurrentLevel = [...new Set(nextLevelModels)]; // Deduplicate for next iteration
        currentLevel++;
    }
    
    // Handle potential cycles or disconnected models by assigning them a level
    dependencies.forEach((info, modelName) => {
        if (info.level === 0 && info.inDegree > 0) {
            // Assign to a level after the main flow, avoids level 0 if it has inputs
            info.level = Math.max(1, currentLevel); 
        }
    });
    
    // Group models by level
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
    let xOffset = 50;
    state.levelGroups.forEach((modelsInLevel, level) => {
        // Distribute models vertically within the allocated space for the level
        const levelHeight = config.height * config.layout.verticalUsage;
        const verticalSpacing = levelHeight / (modelsInLevel.length + 1);
        
        modelsInLevel.forEach((model, idx) => {
            model.x = xOffset + level * config.layout.xSpacing;
            model.y = verticalSpacing * (idx + 1);
            model.height = config.box.titleHeight + 
                          (model.columns.length * config.box.columnHeight) + 
                          (config.box.padding * 2); 
        });
        
        // Increase offset for the next level if this level had models
        if (modelsInLevel.length > 0) {
            xOffset += 50;
        }
    });
}