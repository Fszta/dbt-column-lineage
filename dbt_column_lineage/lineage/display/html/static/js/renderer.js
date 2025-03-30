/**
 * Rendering functions for graph visualization
 */

// Set up SVG container and markers
function setupSvg(config) {
    const svg = d3.select(config.container)
        .append('svg')
        .attr('width', config.width)
        .attr('height', config.height)

    // Add arrow markers
    const defs = svg.append("defs");
    
    defs.append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 10)
        .attr("refY", 0)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M0,-5L10,0L0,5")
        .attr("fill", config.colors.edge);

    defs.append("marker")
        .attr("id", "arrowhead-highlighted")
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 10)
        .attr("refY", 0)
        .attr("markerWidth", 6)
        .attr("markerHeight", 6)
        .attr("orient", "auto")
        .append("path")
        .attr("d", "M0,-5L10,0L0,5")
        .attr("fill", config.colors.edgeHighlight);
        
    return svg;
}

// Draw model boxes
function drawModels(g, state, config, dragBehavior) {
    const nodes = g.selectAll('.model')
        .data(state.models)
        .join('g')
        .attr('class', 'model')
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
        .call(dragBehavior)
        .style('cursor', 'move');

    // Model container
    nodes.append('rect')
        .attr('class', 'model-container')
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('rx', config.box.cornerRadius)
        .attr('fill', config.colors.model)
        .attr('stroke', '#333')
        .attr('stroke-width', 1);

    // Title background
    nodes.append('rect')
        .attr('width', config.box.width)
        .attr('height', config.box.titleHeight)
        .attr('rx', config.box.cornerRadius)
        .attr('fill', config.colors.title);

    // Title divider
    nodes.append('line')
        .attr('class', 'title-divider')
        .attr('x1', 0)
        .attr('y1', config.box.titleHeight)
        .attr('x2', config.box.width)
        .attr('y2', config.box.titleHeight)
        .attr('stroke', '#ccc');

    // Model title
    nodes.append('text')
        .attr('class', 'model-title')
        .attr('x', config.box.width / 2)
        .attr('y', config.box.titleHeight / 2)
        .attr('text-anchor', 'middle')
        .attr('dominant-baseline', 'middle')
        .attr('font-weight', 'bold')
        .attr('font-size', '14px')
        .text(function(d) {
            // Truncate with ellipsis if too long
            const maxLength = 25;
            return d.name.length > maxLength ? d.name.substring(0, maxLength) + '...' : d.name;
        })
        .attr('data-original-text', d => d.name); // For full text on hover
        
    return nodes;
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
    const midX = (sourceX + targetX) / 2;
    
    return `M${sourceX},${sourcePos.y} C${midX},${sourcePos.y} ${midX},${targetPos.y} ${targetX},${targetPos.y}`;
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