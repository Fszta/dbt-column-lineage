/**
 * DBT Column Lineage Graph Visualization
 * Main entry point that initializes the graph
 */
function initGraph(data) {
    const graphElement = document.getElementById('graph');
    
    // Preserve the impact analysis card before clearing
    const impactCard = document.getElementById('impactAnalysisCard');
    const preservedCard = impactCard ? impactCard.cloneNode(true) : null;
    
    graphElement.innerHTML = '';
    
    // Re-append the preserved card if it existed
    if (preservedCard) {
        graphElement.appendChild(preservedCard);
    }
    
    if (!data || !data.nodes || !data.edges) {
        console.warn("No data available to render graph");
        graphElement.innerHTML = '<p class="error-message">No lineage data found to render the graph.</p>'; // User-friendly message
        // Re-append card after error message
        if (preservedCard) {
            graphElement.appendChild(preservedCard);
        }
        return null;
    }
    
    const config = createConfig(graphElement);
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
    drawExposures(g, state, config, dragBehavior);
    const edges = drawEdges(g, data, state, config);
    
    setupInteractions(svg, g, data, state, config, edges);
    
    if (data.main_node) {
        const mainNode = state.nodeIndex.get(data.main_node);
        if (mainNode) {
            // Delay slightly to ensure rendering and transitions are ready
            setTimeout(() => {
                handleColumnClick(data.main_node, mainNode.model, state, config);
            }, 200); 
        }
    }
    
    return { svg, state, config }; // Return instance details if needed
}