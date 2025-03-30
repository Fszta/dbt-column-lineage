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

    // Group nodes by model
    const modelGroups = {};
    data.nodes.forEach(node => {
        if (node.type === 'column') {
            if (!modelGroups[node.model]) {
                modelGroups[node.model] = {
                    name: node.model,
                    columns: [],
                    isMain: node.is_main || false
                };
            }
            modelGroups[node.model].columns.push({
                name: node.label,
                id: node.id,
                dataType: node.data_type
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
        
        // Upstream: what feeds into this column
        if (!upstreamMap.has(targetId)) {
            upstreamMap.set(targetId, new Set());
        }
        upstreamMap.get(targetId).add(sourceId);
        upstreamMap.get(targetId).add(targetId);  // Include self
        
        // Downstream: where this column's data goes
        if (!downstreamMap.has(sourceId)) {
            downstreamMap.set(sourceId, new Set());
        }
        downstreamMap.get(sourceId).add(targetId);
        downstreamMap.get(sourceId).add(sourceId);  // Include self
    });
    
    // Recursively find all connected columns
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
    // Create dependency graph for models
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
    
    // Assign levels based on topological sort
    let currentLevel = 0;
    let modelsInCurrentLevel = [...dependencies.values()]
        .filter(info => info.inDegree === 0)
        .map(info => info.model.name);
    
    while (modelsInCurrentLevel.length > 0) {
        // Set level for current models
        modelsInCurrentLevel.forEach(modelName => {
            const info = dependencies.get(modelName);
            if (info) info.level = currentLevel;
        });
        
        const nextLevelModels = [];
        data.edges.forEach(edge => {
            const sourceNode = state.nodeIndex.get(edge.source);
            const targetNode = state.nodeIndex.get(edge.target);
            
            if (sourceNode && targetNode && 
                modelsInCurrentLevel.includes(sourceNode.model) && 
                !modelsInCurrentLevel.includes(targetNode.model)) {
                nextLevelModels.push(targetNode.model);
            }
        });
        
        modelsInCurrentLevel = [...new Set(nextLevelModels)]; // Deduplicate
        currentLevel++;
    }
    
    // Handle cycles or disconnected models
    dependencies.forEach((info, modelName) => {
        if (info.level === 0 && info.inDegree > 0) {
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
        const levelHeight = config.height * config.layout.verticalUsage;
        const verticalSpacing = levelHeight / (modelsInLevel.length + 1);
        
        modelsInLevel.forEach((model, idx) => {
            model.x = xOffset + level * config.layout.xSpacing;
            model.y = verticalSpacing * (idx + 1);
            model.height = config.box.titleHeight + 
                          (model.columns.length * config.box.columnHeight) + 
                          (config.box.padding * 2);
        });
        
        if (modelsInLevel.length > 0) {
            xOffset += 50;
        }
    });
}