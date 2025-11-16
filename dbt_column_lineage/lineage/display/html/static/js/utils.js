/**
 * Utility functions for graph visualization
 */

// Icon function for different model types
function getModelIcon(modelType) {
    if (!modelType || modelType === 'undefined') {
        modelType = 'model'; // Default to model if no type specified
    }
    
    const icons = {
        source: "M3 5a9 3 0 1 0 18 0a9 3 0 1 0 -18 0 M3 5v14a9 3 0 0 0 18 0V5 M3 12a9 3 0 0 0 18 0",
        seed: "M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z M14 2v6h6",
        model: "M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z M12 22.5v-9 M3.3 7l8.7 5 8.7-5",
        test: "M9 11a2 2 0 1 1 0-4 2 2 0 0 1 0 4z M13 18a2 2 0 1 0 0-4 2 2 0 0 0 0 4z M20 4a2 2 0 1 0 0 4 2 2 0 0 0 0-4z M4 20a2 2 0 1 0 0-4 2 2 0 0 0 0-4z",
        exposure: "M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z M14 2v6h6 M16 13H8 M16 17H8 M10 9H8",
        default: "M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z M12 22.5v-9 M3.3 7l8.7 5 8.7-5"
    };
    
    return icons[modelType] || icons.model; // Default to model icon if type not recognized
}


// Update the getTagColor function with slightly darker colors
function getTagColor(type) {
    const typeStr = type.toLowerCase();
    const defaultColor = '#cbd5e1';
    
    if (typeStr.includes('int') || typeStr.includes('decimal') || 
        typeStr.includes('numeric') || typeStr.includes('double') || 
        typeStr.includes('float')) {
        return '#7cb7fc';
    }
    
    if (typeStr.includes('varchar') || typeStr.includes('char') || 
        typeStr.includes('text') || typeStr.includes('string')) {
        return '#4ddba4';
    }
    
    if (typeStr.includes('date') || typeStr.includes('time')) {
        return '#b29dfc';
    }
    
    if (typeStr.includes('bool')) {
        return '#f98b8b';
    }
    
    if (typeStr.includes('variant')) {
        return '#fca154';
    }
    
    return defaultColor;
}

// Updated createEdgePath to properly handle collapsed models
function createEdgePath(d, state, config) {
    const sourcePos = state.columnPositions.get(d.source);
    const targetPos = state.columnPositions.get(d.target);
    
    if (!sourcePos || !targetPos) return '';
    
    const sourceNode = state.nodeIndex.get(d.source);
    const targetNode = state.nodeIndex.get(d.target);
    
    if (!sourceNode || !targetNode) return '';
    
    const sourceModelName = sourceNode.model;
    const targetModelName = targetNode.model;
    const sourceModel = state.models.find(m => m.name === sourceModelName);
    const targetModel = state.models.find(m => m.name === targetModelName);
    
    if (!sourceModel || !targetModel) return '';
    
    // Determine if models are left-to-right or right-to-left
    const leftToRight = sourceModel.x < targetModel.x;
    
    // Determine connection points and adjust for collapsed state
    let sourceX, sourceY, targetX, targetY;
    
    // Source connection point
    sourceX = sourceModel.x + (leftToRight ? config.box.width - config.box.padding : config.box.padding);
    
    // If columns are collapsed, connect to header
    if (sourceModel.columnsCollapsed) {
        sourceY = sourceModel.y - sourceModel.height/2 + config.box.titleHeight + 14;
    } else {
        sourceY = sourcePos.y;
    }
    
    targetX = targetModel.x + (leftToRight ? config.box.padding : config.box.width - config.box.padding);
    
    // If columns are collapsed, connect to header
    if (targetModel.columnsCollapsed) {
        targetY = targetModel.y - targetModel.height/2 + config.box.titleHeight + 14;
    } else {
        targetY = targetPos.y;
    }
    
    // Create bezier curve path
    const dx = Math.abs(targetX - sourceX);
    const controlX1 = sourceX + (leftToRight ? dx * 0.4 : -dx * 0.4);
    const controlX2 = targetX + (leftToRight ? -dx * 0.4 : dx * 0.4);
    
    return `M${sourceX},${sourceY} 
            C${controlX1},${sourceY} 
             ${controlX2},${targetY} 
             ${targetX},${targetY}`;
}

// Create edge path for exposure connections
function createExposureEdgePath(d, state, config) {
    const sourceNode = state.nodeIndex.get(d.source);
    const targetNode = state.nodeIndex.get(d.target);
    
    if (!sourceNode || !targetNode) return '';
    
    let sourceX, sourceY, targetX, targetY;
    
    // Source is a column
    if (sourceNode.type === 'column') {
        const sourcePos = state.columnPositions.get(d.source);
        if (!sourcePos) return '';
        sourceX = sourcePos.x;
        sourceY = sourcePos.y;
    } else {
        return '';
    }
    
    // Target is an exposure
    if (targetNode.type === 'exposure') {
        const exposure = state.exposures.find(e => e.name === targetNode.model);
        if (!exposure || typeof exposure.x !== 'number' || isNaN(exposure.x) || 
            typeof exposure.y !== 'number' || isNaN(exposure.y)) return '';
        targetX = exposure.x + config.box.width / 2;
        targetY = exposure.y - exposure.height / 2;
    } else {
        return '';
    }
    
    // Create bezier curve path
    const dx = Math.abs(targetX - sourceX);
    const controlX1 = sourceX + dx * 0.4;
    const controlX2 = targetX - dx * 0.4;
    
    return `M${sourceX},${sourceY} 
            C${controlX1},${sourceY} 
             ${controlX2},${targetY} 
             ${targetX},${targetY}`;
} 