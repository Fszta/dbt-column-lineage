/**
 * User interaction handlers
 */

function setupInteractions(svg, g, data, state, config, edges) {
    const zoom = d3.zoom()
        .scaleExtent([0.1, 4])
        .on('zoom', (event) => g.attr('transform', event.transform))
        .filter(function(event) {
            // Disable double-click zoom
            return !event.button && event.type !== 'dblclick';
        });

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
    state.svg = svg;
    state.g = g;
    // Only reset view on initial load, not on expansions
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
    // Reset View only re-fits the CURRENT visible graph; it must NOT re-run
    // layout (which would collapse expanded nodes).
    document.getElementById('resetView').addEventListener('click', () => {
        resetView(svg, g, zoom, config);
    });
    // Re-layout button re-runs the layout (previously the Reset View behaviour).
    const relayoutBtn = document.getElementById('relayout');
    if (relayoutBtn) {
        relayoutBtn.addEventListener('click', () => {
            updateLayout(state, config, edges);
            setTimeout(() => resetView(svg, g, zoom, config), 600);
        });
    }
}

function resetView(svg, g, zoom, config) {
    try {
        const graphBox = g.node().getBBox();

        // Check for valid bounding box
        if (!graphBox || isNaN(graphBox.width) || isNaN(graphBox.height) ||
            graphBox.width <= 0 || graphBox.height <= 0) {
            // Default view if no valid content
            return svg.transition()
                .duration(500)
                .call(zoom.transform, d3.zoomIdentity.translate(0, 0).scale(1));
        }

        // Reserve the footprint of the relationship summary card (a top-right
        // overlay) so fitted nodes are not hidden behind it or under its
        // pointer-capturing area.
        let rightInset = 0;
        const summaryCard = document.getElementById('relationshipSummaryCard');
        if (summaryCard && summaryCard.style.display !== 'none' && summaryCard.offsetParent !== null) {
            const cardRect = summaryCard.getBoundingClientRect();
            if (cardRect.width > 0) {
                rightInset = cardRect.width + 32; // card width + right margin + gap
            }
        }

        const availWidth = Math.max(50, config.width - rightInset);

        const scale = Math.min(
            availWidth / graphBox.width,
            config.height / graphBox.height
        ) * 0.9;

        const translateX = (availWidth - graphBox.width * scale) / 2 - graphBox.x * scale;
        const translateY = (config.height - graphBox.height * scale) / 2 - graphBox.y * scale;

        // Validate scale and translation values
        if (isNaN(scale) || isNaN(translateX) || isNaN(translateY) || scale <= 0) {
            return svg.transition()
                .duration(500)
                .call(zoom.transform, d3.zoomIdentity.translate(0, 0).scale(1));
        }

        return svg.transition()
            .duration(500)
            .call(zoom.transform, d3.zoomIdentity.translate(translateX, translateY).scale(scale));
    } catch (e) {
        console.warn('Error resetting view:', e);
        return svg.transition()
            .duration(500)
            .call(zoom.transform, d3.zoomIdentity.translate(0, 0).scale(1));
    }
}

function updateLayout(state, config, edges) {
    // Only update layout for visible models
    positionModels(state, config);

    // Update model positions with validation
    d3.selectAll('.model:not(.model-exposure)')
        .filter(d => d && d.name && state.visibleModels.has(d.name))
        .transition()
        .duration(500)
        .attr('transform', d => {
            if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
                typeof d.y !== 'number' || isNaN(d.y) ||
                typeof d.height !== 'number' || isNaN(d.height)) {
                return 'translate(0,0)';
            }
            return `translate(${d.x},${d.y - d.height/2})`;
        });

    // Update exposure positions with validation
    d3.selectAll('.model-exposure')
        .filter(d => d && d.name && state.visibleModels.has(d.name))
        .transition()
        .duration(500)
        .attr('transform', d => {
            if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
                typeof d.y !== 'number' || isNaN(d.y) ||
                typeof d.height !== 'number' || isNaN(d.height)) {
                return 'translate(0,0)';
            }
            return `translate(${d.x},${d.y - d.height/2})`;
        });

    // Update column positions for visible models only
    d3.selectAll('.model:not(.model-exposure)').filter(d => d && d.name && state.visibleModels.has(d.name)).each(function(d) {
        if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
            typeof d.y !== 'number' || isNaN(d.y) ||
            typeof d.height !== 'number' || isNaN(d.height)) {
            return;
        }

        if (d.columns && Array.isArray(d.columns)) {
            d.columns.forEach((col, i) => {
                const columnCenter = {
                    x: d.x,
                    y: d.y - d.height/2 + config.box.titleHeight + 28 +
                       (i * config.box.columnHeight) +
                       (config.box.columnHeight - config.box.columnPadding) / 2
                };
                if (!isNaN(columnCenter.x) && !isNaN(columnCenter.y)) {
                    state.columnPositions.set(col.id, columnCenter);
                }
            });
        }
    });

    // Update exposure positions for visible exposures only
    d3.selectAll('.model-exposure').filter(d => d && d.name && state.visibleModels.has(d.name)).each(function(d) {
        if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
            typeof d.y !== 'number' || isNaN(d.y)) {
            return;
        }

        const exposureCenter = {
            x: d.x,
            y: d.y
        };
        state.exposurePositions.set(d.name, exposureCenter);
    });

    // Update edges - only for visible models
    d3.selectAll('.edge.lineage').each(function(d) {
        if (!d || !d.source || !d.target) {
            d3.select(this).style('display', 'none');
            return;
        }

        const sourceNode = state.nodeIndex.get(d.source);
        const targetNode = state.nodeIndex.get(d.target);

        if (sourceNode && targetNode) {
            const sourceVisible = state.visibleModels.has(sourceNode.model);
            const targetVisible = state.visibleModels.has(targetNode.model);

            if (sourceVisible && targetVisible) {
                const path = createEdgePath(d, state, config);
                if (path && path !== '' && !path.includes('NaN')) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl
                        .style('display', 'block')
                        .attr('marker-end', markerEnd)
                        .transition()
                        .duration(500)
                        .attr('d', path);
                } else {
                    d3.select(this).style('display', 'none');
                }
            } else {
                d3.select(this).style('display', 'none');
            }
        } else {
            d3.select(this).style('display', 'none');
        }
    });

    // Update exposure edges
    d3.selectAll('.edge.exposure').each(function(d) {
        if (!d || !d.source || !d.target) {
            d3.select(this).style('display', 'none');
            return;
        }

        const sourceNode = state.nodeIndex.get(d.source);
        const targetNode = state.nodeIndex.get(d.target);

        if (sourceNode && targetNode) {
            const sourceVisible = state.visibleModels.has(sourceNode.model);
            const targetVisible = state.visibleModels.has(targetNode.model);

            if (sourceVisible && targetVisible) {
                const path = createExposureEdgePath(d, state, config);
                if (path && path !== '' && !path.includes('NaN')) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl
                        .style('display', 'block')
                        .attr('marker-end', markerEnd)
                        .transition()
                        .duration(500)
                        .attr('d', path);
                } else {
                    d3.select(this).style('display', 'none');
                }
            } else {
                d3.select(this).style('display', 'none');
            }
        } else {
            d3.select(this).style('display', 'none');
        }
    });
}

// Highlight lineage of a column
function highlightLineage(columnId, state, config) {
    resetHighlights(state, config);

    // First, ensure all column positions are up to date
    d3.selectAll('.model:not(.model-exposure)').filter(d => d && d.name && state.visibleModels.has(d.name)).each(function(d) {
        if (d.columns && Array.isArray(d.columns)) {
            d.columns.forEach((col, i) => {
                const columnCenter = {
                    x: d.x,
                    y: d.y - d.height/2 + config.box.titleHeight + 28 +
                       (i * config.box.columnHeight) +
                       (config.box.columnHeight - config.box.columnPadding) / 2
                };
                state.columnPositions.set(col.id, columnCenter);
            });
        }
    });

    // Update all edge paths before highlighting
    d3.selectAll('.edge.lineage').each(function(d) {
        if (d && d.source && d.target) {
            const sourceNode = state.nodeIndex.get(d.source);
            const targetNode = state.nodeIndex.get(d.target);
            if (sourceNode && targetNode) {
                const sourceVisible = state.visibleModels.has(sourceNode.model);
                const targetVisible = state.visibleModels.has(targetNode.model);
                if (sourceVisible && targetVisible) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl
                        .attr('marker-end', markerEnd)
                        .attr('d', d => createEdgePath(d, state, config));
                }
            }
        }
    });

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
            const isSelected = id === columnId;
            columnElement.classed('highlighted', true);
            columnElement.select('.column-background')
                .transition().duration(200)
                .attr('fill', isSelected ? config.colors.selectedColumn : config.colors.relatedColumn);
            // Thermal rail: the shared left indicator carries the accent current
            columnElement.select('.column-indicator')
                .transition().duration(200)
                .attr('fill', config.colors.edgeHighlight)
                .attr('opacity', isSelected ? 1 : 0.55);
        }
    });

    // Context edges recede so the traced path reads as the one live current
    d3.selectAll('.edge.lineage').transition().duration(200)
        .style('stroke', config.colors.edgeDimmed)
        .style('stroke-width', 1)
        .style('stroke-opacity', 0.32)
        .attr('marker-end', 'url(#arrowhead)');

    // Keep exposure edges visible with their purple color
    d3.selectAll('.edge.exposure').transition().duration(200)
        .style('stroke', 'var(--violet)')
        .style('stroke-width', 2)
        .style('stroke-opacity', 0.7)
        .attr('marker-end', 'url(#arrowhead)');

    // Highlight relevant edges (both upstream and downstream) — the live current
    d3.selectAll('.edge').filter(d => {
        return relatedColumns.has(d.source) && relatedColumns.has(d.target);
    })
    .transition().duration(200)
    .style('stroke', config.colors.edgeHighlight)
    .style('stroke-width', 2.5)
    .style('stroke-opacity', 1)
    .attr('marker-end', 'url(#arrowhead-highlighted)');

    // keep the amber blast path lifted on breaking edges that are NOT part of the
    // currently-traced (indigo) selection — the selection wins where it overlaps, but the
    // "what breaks" thread stays visible everywhere else.
    d3.selectAll('.edge.lineage.breaking')
        .filter(d => !(relatedColumns.has(d.source) && relatedColumns.has(d.target)))
        .transition().duration(200)
        .style('stroke', 'var(--accent)')
        .style('stroke-width', 1.75)
        .style('stroke-opacity', 0.95);
}

function resetHighlights(state, config) {
    // Reset column highlighting (and restore the thermal rail to its base color)
    d3.selectAll('.column-group.highlighted').each(function() {
        const grp = d3.select(this);
        grp.select('.column-background')
            .transition().duration(200)
            .attr('fill', 'transparent');
        const rail = grp.select('.column-indicator');
        rail.transition().duration(200)
            .attr('fill', rail.attr('data-basefill') || '#94a3b8')
            .attr('opacity', 0.7);
        grp.classed('highlighted', false);
    });

    // Reset lineage edge highlighting
    const hasBreaking = state && state.hasBreakingPath === true;
    d3.selectAll('.edge.lineage').transition().duration(200)
        .style('stroke', config.colors.edge)
        .style('stroke-width', 1.5)
        // when a breaking path is present, off-path edges rest dimmed so the amber
        // blast thread stays the read; no breaking context => full opacity (today's look).
        .style('stroke-opacity', hasBreaking ? 0.4 : 1)
        .attr('marker-end', 'url(#arrowhead)');

    // the amber blast path is a property of the change, not the selection — restore it
    // whenever highlights clear so the "what breaks" thread persists at rest.
    d3.selectAll('.edge.lineage.breaking').transition().duration(200)
        .style('stroke', 'var(--accent)')
        .style('stroke-width', 1.75)
        .style('stroke-opacity', 1);

    // Restore exposure edge color
    d3.selectAll('.edge.exposure').transition().duration(200)
        .style('stroke', 'var(--violet)')
        .style('stroke-width', 2)
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
            if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
                typeof d.y !== 'number' || isNaN(d.y) ||
                typeof d.height !== 'number' || isNaN(d.height)) {
                return;
            }

            d.x += event.dx;
            d.y += event.dy;

            // Validate positions are still valid
            if (isNaN(d.x) || isNaN(d.y)) {
                return;
            }

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
                    // Only update if valid
                    if (!isNaN(columnCenter.x) && !isNaN(columnCenter.y)) {
                        state.columnPositions.set(col.id, columnCenter);
                    }
                });
            }

            if (d.type === 'exposure') {
                const exposureCenter = {
                    x: d.x,
                    y: d.y
                };
                if (!isNaN(exposureCenter.x) && !isNaN(exposureCenter.y)) {
                    state.exposurePositions.set(d.name, exposureCenter);
                }
            }

            updateModelEdges(d, state, config);
        })
        .on('end', function(event, d) {
            d3.select(this).classed('active', false);
            delete d._connectedEdges;
        });
}
