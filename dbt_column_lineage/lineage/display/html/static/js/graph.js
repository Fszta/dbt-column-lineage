/**
 * DBT Column Lineage Graph Visualization
 * Main entry point that initializes the graph
 */
function initGraph(data) {
    const config = createConfig(document.getElementById('graph'));
    const state = createState();
    processData(data, state);
    positionModels(state, config);
    
    const svg = setupSvg(config);
    const g = svg.append('g');

    const onColumnClick = (columnId, modelName) => {
        handleColumnClick(columnId, modelName, state, config);
    };
    
    const dragBehavior = createDragBehavior(state, config);
    const nodes = drawModels(g, state, config, dragBehavior);
    drawColumns(nodes, state, config, onColumnClick);
    const edges = drawEdges(g, data, state, config);
    
    setupInteractions(svg, g, data, state, config, edges);
    
    return {
        svg,
        state,
        config
    };
}