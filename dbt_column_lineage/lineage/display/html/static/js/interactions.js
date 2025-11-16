/**
 * User interaction handlers
 */

function setupInteractions(svg, g, data, state, config, edges) {
    const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => g.attr('transform', event.transform));
        
    svg.call(zoom);
    
    const background = svg.insert('rect', ':first-child')
        .attr('class', 'background')
        .attr('width', config.width)
        .attr('height', config.height)
        .attr('fill', 'transparent')
        .style('cursor', 'move');
    
    setupBackgroundDrag(svg, zoom);
    setupControlButtons(svg, g, zoom, state, config, edges);
    
    state.zoom = zoom;
    setTimeout(() => resetView(svg, g, zoom, config), 100);
}

function setupBackgroundDrag(svg, zoom) {
    svg.on('mousedown', function(event) {
        if (!event.target.classList.contains('background')) return;
        
        event.preventDefault();
        const startX = event.clientX;
        const startY = event.clientY;
        const transform = d3.zoomTransform(svg.node());
        
        const mousemove = (event) => {
            const dx = event.clientX - startX;
            const dy = event.clientY - startY;
            svg.call(
                zoom.transform,
                transform.translate(dx / transform.k, dy / transform.k)
            );
        };
        
        const mouseup = () => {
            svg.on('mousemove', null).on('mouseup', null);
            document.removeEventListener('mousemove', mousemove);
            document.removeEventListener('mouseup', mouseup);
        };
        
        svg.on('mousemove', mousemove).on('mouseup', mouseup);
        document.addEventListener('mousemove', mousemove);
        document.addEventListener('mouseup', mouseup);
    });
}

function setupControlButtons(svg, g, zoom, state, config, edges) {
    const zoomIn = () => svg.transition().duration(300).call(zoom.scaleBy, 1.2);
    const zoomOut = () => svg.transition().duration(300).call(zoom.scaleBy, 0.8);
    
    document.getElementById('zoomIn').addEventListener('click', zoomIn);
    document.getElementById('zoomOut').addEventListener('click', zoomOut);
    document.getElementById('resetView').addEventListener('click', () => {
        updateLayout(state, config, edges);
        setTimeout(() => resetView(svg, g, zoom, config), 600);
    });
}

function resetView(svg, g, zoom, config) {
    const graphBox = g.node().getBBox();
    const scale = Math.min(
        config.width / graphBox.width, 
        config.height / graphBox.height
    ) * 0.9;
    
    const translateX = (config.width - graphBox.width * scale) / 2 - graphBox.x * scale;
    const translateY = (config.height - graphBox.height * scale) / 2 - graphBox.y * scale;

    return svg.transition()
        .duration(500)
        .call(zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(scale));
}

function updateLayout(state, config, edges) {
    positionModels(state, config);
    
    d3.selectAll('.model:not(.model-exposure)')
        .transition()
        .duration(500)
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`);

    d3.selectAll('.model-exposure')
        .transition()
        .duration(500)
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`);

    d3.selectAll('.model:not(.model-exposure)').each(function(model) {
        if (model.columns && Array.isArray(model.columns)) {
            model.columns.forEach((col, i) => {
                const columnCenter = {
                    x: model.x,
                    y: model.y - model.height/2 + config.box.titleHeight + 28 + 
                       (i * config.box.columnHeight) + 
                       (config.box.columnHeight - config.box.columnPadding) / 2
                };
                state.columnPositions.set(col.id, columnCenter);
            });
        }
    });

    d3.selectAll('.model-exposure').each(function(exposure) {
        const exposureCenter = {
            x: exposure.x,
            y: exposure.y
        };
        state.exposurePositions.set(exposure.name, exposureCenter);
    });

    d3.selectAll('.edge.lineage').transition()
        .duration(500)
        .attr('d', d => createEdgePath(d, state, config));
    
    d3.selectAll('.edge.exposure').transition()
        .duration(500)
        .attr('d', d => createExposureEdgePath(d, state, config));
}

// Highlight lineage of a column
function highlightLineage(columnId, state, config) {
    resetHighlights(state, config);
    
    const relatedColumns = new Set();
    
    relatedColumns.add(columnId);
    
    if (state.lineage.upstream.has(columnId)) {
        state.lineage.upstream.get(columnId).forEach(id => relatedColumns.add(id));
    }
    
    if (state.lineage.downstream.has(columnId)) {
        state.lineage.downstream.get(columnId).forEach(id => relatedColumns.add(id));
    }
    
    relatedColumns.forEach(id => {
        const columnElement = d3.select(`.column-group[data-id="${id}"]`);
        if (!columnElement.empty()) {
            columnElement
                .classed('highlighted', true)
                .select('.column-background')
                .transition().duration(200)
                .attr('fill', id === columnId ? config.colors.selectedColumn : config.colors.relatedColumn);
        }
    });
    
    // Make all edges lighter but still visible
    d3.selectAll('.edge').transition().duration(200)
        .style('stroke', config.colors.edgeDimmed)
        .style('stroke-width', 1)
        .style('stroke-opacity', 0.5)
        .attr('marker-end', 'url(#arrowhead)');
    
    // Highlight relevant edges (both upstream and downstream)
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
    d3.selectAll('.column-group.highlighted')
        .classed('highlighted', false)
        .select('.column-background')
        .transition().duration(200)
        .attr('fill', 'transparent');
    
    // Reset edge highlighting
    d3.selectAll('.edge').transition().duration(200)
        .style('stroke', config.colors.edge)
        .style('stroke-width', 1.5)
        .style('stroke-opacity', 1)
        .attr('marker-end', 'url(#arrowhead)');
}

function handleColumnClick(columnId, modelName, state, config) {
    highlightLineage(columnId, state, config);
}

function createDragBehavior(state, config) {
    return d3.drag()
        .on('start', function(event, d) {
            d3.select(this).raise();
            d3.select(this).classed('active', true);
            d._connectedEdges = state.modelEdges.get(d.name) || []; 
        })
        .on('drag', function(event, d) {
            d.x += event.dx;
            d.y += event.dy;
            
            const modelElement = d3.select(this);
            modelElement.attr('transform', `translate(${d.x},${d.y - d.height/2})`);
            
            if (d.columns && Array.isArray(d.columns)) {
                d.columns.forEach((col, i) => {
                     let columnYOffset = config.box.titleHeight + 28 + 
                                        (i * config.box.columnHeight) + 
                                        (config.box.columnHeight - config.box.columnPadding) / 2;

                     const columnCenter = {
                        x: d.x,
                        y: d.y - d.height/2 + columnYOffset
                    };
                    state.columnPositions.set(col.id, columnCenter);
                });
            }
            
            if (d.type === 'exposure') {
                const exposureCenter = {
                    x: d.x,
                    y: d.y
                };
                state.exposurePositions.set(d.name, exposureCenter);
            }
            
            updateModelEdges(d, state, config); 
        })
        .on('end', function(event, d) {
            d3.select(this).classed('active', false);
            delete d._connectedEdges;
        });
}