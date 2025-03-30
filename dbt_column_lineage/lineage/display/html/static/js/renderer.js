/**
 * Rendering functions for graph visualization
 */

// Set up SVG container and markers
function setupSvg(config) {
    const svg = d3.select('#graph')
        .append('svg')
        .attr('width', config.width)
        .attr('height', config.height);
    
    // Add SVG definitions for arrows and effects
    const defs = svg.append('defs');
    
    // Create a cleaner, subtle drop shadow
    const cleanShadow = defs.append('filter')
        .attr('id', 'clean-shadow')
        .attr('x', '-5%')
        .attr('y', '-5%')
        .attr('width', '110%')
        .attr('height', '110%');
        
    cleanShadow.append('feDropShadow')
        .attr('dx', '0')
        .attr('dy', '1')
        .attr('stdDeviation', '2')
        .attr('flood-color', 'rgba(0,0,0,0.15)')
        .attr('flood-opacity', '0.5');
    
    // Create a subtle gradient for model headers
    const headerGradient = defs.append('linearGradient')
        .attr('id', 'header-gradient')
        .attr('x1', '0%')
        .attr('y1', '0%')
        .attr('x2', '0%')
        .attr('y2', '100%');
        
    headerGradient.append('stop')
        .attr('offset', '0%')
        .attr('stop-color', 'var(--primary-light)')
        .attr('stop-opacity', '0.2');
        
    headerGradient.append('stop')
        .attr('offset', '100%')
        .attr('stop-color', 'var(--primary-light)')
        .attr('stop-opacity', '0.05');
    
    // Arrow markers (regular and highlighted)
    defs.append('marker')
        .attr('id', 'arrowhead')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', 'var(--edge-color)');
    
    defs.append('marker')
        .attr('id', 'arrowhead-highlighted')
        .attr('viewBox', '0 -5 10 10')
        .attr('refX', 10)
        .attr('refY', 0)
        .attr('markerWidth', 6)
        .attr('markerHeight', 6)
        .attr('orient', 'auto')
        .append('path')
        .attr('d', 'M0,-5L10,0L0,5')
        .attr('fill', 'var(--edge-highlight)');
    
    return svg.append('g');
}

// Draw model boxes
function drawModels(g, state, config, dragBehavior) {
    const models = g.selectAll('.model')
        .data(state.models)
        .enter()
        .append('g')
        .attr('class', 'model')
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
        .call(dragBehavior);

    // Main container with rounded corners
    models.append('rect')
        .attr('class', 'model-container')
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('rx', 8)
        .attr('ry', 8)
        .attr('filter', 'url(#clean-shadow)');

    // Add a header section with a subtle gradient
    models.append('rect')
        .attr('class', 'model-header')
        .attr('width', config.box.width)
        .attr('height', config.box.titleHeight)
        .attr('rx', 8)
        .attr('ry', 8)
        .attr('fill', 'url(#header-gradient)');

    // Title separator line
    models.append('line')
        .attr('class', 'title-divider')
        .attr('x1', 0)
        .attr('y1', config.box.titleHeight)
        .attr('x2', config.box.width)
        .attr('y2', config.box.titleHeight)
        .attr('stroke', 'var(--border)')
        .attr('stroke-width', 1);

    // Model title
    models.append('text')
        .attr('class', 'model-title')
        .attr('x', config.box.padding)
        .attr('y', config.box.titleHeight / 2 + 5)
        .text(d => d.name);

    return models;
}

// Draw columns inside model boxes
function drawColumns(nodes, state, config, onColumnClick) {
    nodes.each(function(model) {
        const group = d3.select(this);
        
        model.columns.forEach((col, i) => {
            const yPos = config.box.titleHeight + config.box.padding + (i * config.box.columnHeight);
            
            const columnGroup = group.append('g')
                .attr('class', 'column-group')
                .attr('transform', `translate(${config.box.padding}, ${yPos})`)
                .attr('data-id', col.id)
                .style('cursor', 'pointer')
                .on('click', function() {
                    onColumnClick(col.id, model.name);
                })
                .on('mouseover', function() {
                    d3.select(this).select('rect')
                        .transition().duration(100)
                        .attr('fill', config.colors.columnHover);
                })
                .on('mouseout', function() {
                    if (!d3.select(this).classed('highlighted')) {
                        d3.select(this).select('rect')
                            .transition().duration(100)
                            .attr('fill', config.colors.column);
                    }
                });
        
            // Column background
            columnGroup.append('rect')
                .attr('class', 'column-bg')
                .attr('width', config.box.width - (config.box.padding * 2))
                .attr('height', config.box.columnHeight - config.box.columnPadding)
                .attr('rx', 3)
                .attr('fill', config.colors.column);

            // Column name
            columnGroup.append('text')
                .attr('class', 'column-name')
                .attr('x', 8)
                .attr('y', (config.box.columnHeight - config.box.columnPadding) / 2)
                .attr('dominant-baseline', 'middle')
                .attr('font-size', '12px')
                .text(function() {
                    const maxLength = 20;
                    return col.name.length > maxLength ? col.name.substring(0, maxLength) + '...' : col.name;
                })
                .attr('data-original-text', col.name); // For full text on hover

            if (col.dataType) {
                columnGroup.append('text')
                    .attr('class', 'column-type')
                    .attr('x', config.box.width - (config.box.padding * 3))
                    .attr('y', (config.box.columnHeight - config.box.columnPadding) / 2)
                    .attr('dominant-baseline', 'middle')
                    .attr('text-anchor', 'end')
                    .attr('font-size', '10px')
                    .attr('fill', '#666')
                    .text(col.dataType);
            }

            // Store position and element for edge drawing and highlighting
            state.columnPositions.set(col.id, {
                x: model.x,
                y: model.y - model.height/2 + yPos + (config.box.columnHeight - config.box.columnPadding) / 2
            });
            
            state.columnElements.set(col.id, columnGroup);
        });
    });
}

// Draw edges between columns
function drawEdges(g, data, state, config) {
    state.models.forEach(model => {
        state.modelEdges.set(model.name, []);
    });
    
    const edges = g.selectAll('.edge')
        .data(data.edges.filter(e => e.type === 'lineage'))
        .join('path')
        .attr('class', 'edge')
        .attr('marker-end', 'url(#arrowhead)')
        .attr('data-source', d => d.source)
        .attr('data-target', d => d.target)
        .style('stroke', config.colors.edge)
        .style('stroke-width', 1.5)
        .style('fill', 'none')
        .attr('d', d => createEdgePath(d, state, config))
        .each(function(d) {
            // Store reference to edge elements for faster dragging
            indexEdgeForDragging(d, this, state);
        });
        
    return edges;
}

// Create the path for an edge
function createEdgePath(d, state, config) {
    const sourcePos = state.columnPositions.get(d.source);
    const targetPos = state.columnPositions.get(d.target);
    
    if (!sourcePos || !targetPos) return '';
    
    const sourceX = sourcePos.x + config.box.width - config.box.padding;
    const targetX = targetPos.x + config.box.padding;
    
    // Calculate control points for a smoother curve
    const dx = targetX - sourceX;
    const dy = targetPos.y - sourcePos.y;
    const controlX1 = sourceX + dx * 0.4;
    const controlX2 = sourceX + dx * 0.6;
    
    return `M${sourceX},${sourcePos.y} 
            C${controlX1},${sourcePos.y} 
             ${controlX2},${targetPos.y} 
             ${targetX},${targetPos.y}`;
}

// Store references to edges for efficient dragging
function indexEdgeForDragging(edge, element, state) {
    const sourceNode = state.nodeIndex.get(edge.source);
    const targetNode = state.nodeIndex.get(edge.target);
    
    if (sourceNode && targetNode) {
        const sourceModel = sourceNode.model;
        const targetModel = targetNode.model;
        
        if (!state.modelEdges.has(sourceModel)) state.modelEdges.set(sourceModel, []);
        if (!state.modelEdges.has(targetModel)) state.modelEdges.set(targetModel, []);
        
        const edgeInfo = {
            edge: edge,
            element: element,
            source: edge.source,
            target: edge.target
        };
        
        state.modelEdges.get(sourceModel).push(edgeInfo);
        
        if (sourceModel !== targetModel) {
            state.modelEdges.get(targetModel).push(edgeInfo);
        }
    }
}

// Update node info panel in sidebar
function updateNodeInfo(node) {
    document.getElementById('nodeInfo').innerHTML = `
        <h4>${node.label}</h4>
        <p>Model: ${node.model}</p>
        ${node.data_type ? `<p>Data Type: ${node.data_type}</p>` : ''}
    `;
}