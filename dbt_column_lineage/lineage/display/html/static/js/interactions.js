/**
 * User interaction handlers
 */

// Handle zoom and control buttons
function setupInteractions(svg, g, data, state, config, edges) {
    // Set up zoom behavior
    const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => g.attr('transform', event.transform));
        
    svg.call(zoom);
    
    // Add background for drag/pan interactions
    svg.insert('rect', ':first-child')
        .attr('class', 'background')
        .attr('width', config.width)
        .attr('height', config.height)
        .attr('fill', 'transparent')
        .style('cursor', 'move');
    
    // Enable dragging on background to pan the entire graph
    svg.on('mousedown', function(event) {
        if (event.target.classList.contains('background')) {
            event.preventDefault();
            
            const startX = event.clientX;
            const startY = event.clientY;
            const transform = d3.zoomTransform(svg.node());
            
            function mousemove(event) {
                const dx = event.clientX - startX;
                const dy = event.clientY - startY;
                svg.call(
                    zoom.transform,
                    transform.translate(dx / transform.k, dy / transform.k)
                );
            }
            
            function mouseup() {
                svg.on('mousemove', null);
                svg.on('mouseup', null);
                document.removeEventListener('mousemove', mousemove);
                document.removeEventListener('mouseup', mouseup);
            }
            
            svg.on('mousemove', mousemove);
            svg.on('mouseup', mouseup);
            document.addEventListener('mousemove', mousemove);
            document.addEventListener('mouseup', mouseup);
        }
    });
    
    // Define reusable functions
    function zoomIn() {
        svg.transition().duration(300).call(zoom.scaleBy, 1.2);
    }
    
    function zoomOut() {
        svg.transition().duration(300).call(zoom.scaleBy, 0.8);
    }
    
    function resetView() {
        const graphBox = g.node().getBBox();
        const scale = Math.min(
            config.width / graphBox.width, 
            config.height / graphBox.height
        ) * 0.9;
        
        return svg.transition()
            .duration(500)
            .call(zoom.transform, d3.zoomIdentity
                .translate(
                    (config.width - graphBox.width * scale) / 2, 
                    (config.height - graphBox.height * scale) / 2
                )
                .scale(scale));
    }
    
    function relayout() {
        positionModels(state, config);
        
        // Update node positions with animation
        d3.selectAll('.model')
            .transition()
            .duration(500)
            .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`);

        // Update column positions
        d3.selectAll('.model').each(function(model) {
            model.columns.forEach((col, i) => {
                const yPos = config.box.titleHeight + config.box.padding + (i * config.box.columnHeight);
                state.columnPositions.set(col.id, {
                    x: model.x,
                    y: model.y - model.height/2 + yPos + (config.box.columnHeight - config.box.columnPadding) / 2
                });
            });
        });

        edges.transition()
            .duration(500)
            .attr('d', d => createEdgePath(d, state, config));
    }
    
    // Attach event listeners
    document.getElementById('zoomIn').addEventListener('click', zoomIn);
    document.getElementById('zoomOut').addEventListener('click', zoomOut);
    document.getElementById('resetView').addEventListener('click', resetView);
    document.getElementById('relayout').addEventListener('click', relayout);
    
    // Add CSS to make drag smoother
    const style = document.createElement('style');
    style.innerHTML = `
        .background {
            pointer-events: all;
        }
        svg {
            user-select: none;
            -webkit-user-select: none;
        }
    `;
    document.head.appendChild(style);
    
    // Save zoom for other functions to use
    state.zoom = zoom;
    
    // Initialize view to show the entire graph
    setTimeout(resetView, 100);
}

// Create drag behavior for models
function createDragBehavior(state, config) {
    return d3.drag()
        .on('start', function(event, d) {
            d3.select(this).raise().classed('active', true);
            d._connectedEdges = state.modelEdges.get(d.name) || [];
        })
        .on('drag', function(event, d) {
            // Update model position
            d.x += event.dx;
            d.y += event.dy;
            
            const modelElement = d3.select(this).node();
            modelElement.setAttribute('transform', `translate(${d.x},${d.y - d.height/2})`);
            
            // Update column positions
            d.columns.forEach((col, i) => {
                const yPos = config.box.titleHeight + config.box.padding + (i * config.box.columnHeight);
                state.columnPositions.set(col.id, {
                    x: d.x,
                    y: d.y - d.height/2 + yPos + (config.box.columnHeight - config.box.columnPadding) / 2
                });
            });

            // Update connected edges directly in DOM
            if (d._connectedEdges) {
                d._connectedEdges.forEach(edgeInfo => {
                    const sourcePos = state.columnPositions.get(edgeInfo.source);
                    const targetPos = state.columnPositions.get(edgeInfo.target);
                    
                    if (sourcePos && targetPos) {
                        const sourceX = sourcePos.x + config.box.width - config.box.padding;
                        const targetX = targetPos.x + config.box.padding;
                        const midX = (sourceX + targetX) / 2;
                        
                        edgeInfo.element.setAttribute('d', 
                            `M${sourceX},${sourcePos.y} C${midX},${sourcePos.y} ${midX},${targetPos.y} ${targetX},${targetPos.y}`
                        );
                    }
                });
            }
        })
        .on('end', function(event, d) {
            d3.select(this).classed('active', false);
            delete d._connectedEdges;
        });
}

// Highlight lineage of a column
function highlightLineage(columnId, state, config) {
    resetHighlights(state, config);
    
    const relatedColumns = new Set();
    
    if (state.lineage.upstream.has(columnId)) {
        state.lineage.upstream.get(columnId).forEach(id => relatedColumns.add(id));
    }
    
    if (state.lineage.downstream.has(columnId)) {
        state.lineage.downstream.get(columnId).forEach(id => relatedColumns.add(id));
    }
    
    if (relatedColumns.size === 0) {
        relatedColumns.add(columnId);
    }
    
    // Highlight related columns
    relatedColumns.forEach(id => {
        if (state.columnElements.has(id)) {
            state.columnElements.get(id)
                .classed('highlighted', true)
                .select('rect')
                .transition().duration(200)
                .attr('fill', id === columnId ? config.colors.selectedColumn : config.colors.relatedColumn);
        }
    });
    
    // Make all edges lighter but still visible
    d3.selectAll('.edge').transition().duration(200)
        .style('stroke', config.colors.edgeDimmed)
        .style('stroke-width', 1)
        .style('stroke-opacity', 0.9)
        .attr('marker-end', 'url(#arrowhead)');
    
    // Highlight relevant edges
    d3.selectAll('.edge').filter(d => {
        return relatedColumns.has(d.source) && relatedColumns.has(d.target);
    })
    .transition().duration(200)
    .style('stroke', config.colors.edgeHighlight)
    .style('stroke-width', 2)
    .style('stroke-opacity', 1)
    .attr('marker-end', 'url(#arrowhead-highlighted)');
}

function resetHighlights(state, config) {
    // Reset column highlighting
    state.columnElements.forEach(col => {
        col.classed('highlighted', false)
            .select('rect')
            .transition().duration(200)
            .attr('fill', config.colors.column);
    });
    
    // Reset edge highlighting
    d3.selectAll('.edge').transition().duration(200)
        .style('stroke', config.colors.edge)
        .style('stroke-width', 1.5)
        .style('stroke-opacity', 1)
        .attr('marker-end', 'url(#arrowhead)');
}

function handleColumnClick(columnId, modelName, state, config) {
    highlightLineage(columnId, state, config);
    
    // Find the column details to display
    const column = state.models
        .find(m => m.name === modelName)
        ?.columns.find(c => c.id === columnId);
        
    if (column) {
        updateNodeInfo({
            label: column.name,
            type: 'column',
            model: modelName,
            data_type: column.dataType
        });
    }
}

function updateNodeInfo(node) {
    const nodeInfoDiv = document.getElementById('nodeInfo');
    if (!nodeInfoDiv) return;
    
    if (!node) {
        nodeInfoDiv.innerHTML = '<div class="node-info-placeholder">Select a column to see details</div>';
        return;
    }
    
    let html = `
        <div class="node-detail">
            <strong class="node-label">${node.label || ''}</strong>
        </div>
        <div class="node-detail">
            <span class="detail-label">Model:</span>
            <span>${node.model || ''}</span>
        </div>
    `;
    
    if (node.data_type) {
        html += `
            <div class="node-detail">
                <span class="detail-label">Data Type:</span>
                <span>${node.data_type}</span>
            </div>
        `;
    }
    
    nodeInfoDiv.innerHTML = html;
}