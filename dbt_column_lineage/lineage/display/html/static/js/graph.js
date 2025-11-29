/**
 * DBT Column Lineage Graph Visualization
 * Main entry point that initializes the graph
 */
function initGraph(data) {
    const graphElement = document.getElementById('graph');

    // Preserve the impact analysis card and relationship summary card before clearing
    const impactCard = document.getElementById('impactAnalysisCard');
    const relationshipSummaryCard = document.getElementById('relationshipSummaryCard');
    const preservedCard = impactCard ? impactCard.cloneNode(true) : null;
    const preservedSummaryCard = relationshipSummaryCard ? relationshipSummaryCard.cloneNode(true) : null;

    graphElement.innerHTML = '';

    // Re-append the preserved cards if they existed
    if (preservedCard) {
        graphElement.appendChild(preservedCard);
    }
    if (preservedSummaryCard) {
        graphElement.appendChild(preservedSummaryCard);
    }

    // Check if we're in explore mode (where empty data on initial load is expected)
    const isExploreMode = document.getElementById('model-tree-container') !== null ||
                          document.getElementById('columnSelect') !== null;

    if (!data || !data.nodes || !data.edges || data.nodes.length === 0) {
        if (isExploreMode) {
            return null;
        }

        console.warn("No data available to render graph");
        graphElement.innerHTML = '<div class="error-message">No lineage data found to render the graph.</div>';
        if (preservedCard) {
            graphElement.appendChild(preservedCard);
        }
        if (preservedSummaryCard) {
            graphElement.appendChild(preservedSummaryCard);
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

    window.graphState = state;
    window.graphConfig = config;
    window.graphData = data;
    state.svg = svg;

    window.expandDownstream = function(modelName) {
        if (!modelName || !state || !config) return;
        expandDownstreamInternal(modelName, state, config);
    };

    window.collapseDownstream = function(modelName) {
        if (!modelName || !state || !config) return;
        collapseDownstreamInternal(modelName, state, config);
    };

    window.expandUpstream = function(modelName) {
        if (!modelName || !state || !config) return;
        expandUpstreamInternal(modelName, state, config);
    };

    window.collapseUpstream = function(modelName) {
        if (!modelName || !state || !config) return;
        collapseUpstreamInternal(modelName, state, config);
    };

    // Filter models and edges based on visibility
    filterGraphByVisibility(state, config);

    if (data.main_node) {
        const mainNode = state.nodeIndex.get(data.main_node);
        if (mainNode) {
            setTimeout(() => {
                handleColumnClick(data.main_node, mainNode.model, state, config);
            }, 200);
        }
    }

    return { svg, state, config };
}

function filterGraphByVisibility(state, config) {
    d3.selectAll('.model').each(function(d) {
        if (!d || !d.name) return;
        const isVisible = state.visibleModels.has(d.name);
        d3.select(this).style('display', isVisible ? 'block' : 'none');
    });

    d3.selectAll('.model-exposure').each(function(d) {
        const exposureName = d3.select(this).attr('data-name') || (d && d.name);
        if (!exposureName) return;
        const isVisible = state.visibleModels.has(exposureName);
        d3.select(this).style('display', isVisible ? 'block' : 'none');
    });

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
            d3.select(this).style('display', (sourceVisible && targetVisible) ? 'block' : 'none');
        } else {
            d3.select(this).style('display', 'none');
        }
    });

    d3.selectAll('.edge.exposure').each(function(d) {
        if (!d || !d.source || !d.target) {
            d3.select(this).style('display', 'none');
            return;
        }

        const sourceNode = state.nodeIndex.get(d.source);
        const targetNode = state.nodeIndex.get(d.target);

        if (sourceNode && targetNode) {
            const sourceModelVisible = state.visibleModels.has(sourceNode.model);
            const targetExposureVisible = state.visibleModels.has(targetNode.model);
            d3.select(this).style('display', (sourceModelVisible && targetExposureVisible) ? 'block' : 'none');
        } else {
            d3.select(this).style('display', 'none');
        }
    });

    let isExpanding = false;
    for (let key in state) {
        if (key.startsWith('_expanding_')) {
            isExpanding = true;
            break;
        }
    }
    if (!isExpanding) {
        updateExpandIcons(state);
    }
}

function updateExpandIcons(state) {
    d3.selectAll('.expand-icon-group').each(function() {
        const modelName = d3.select(this).attr('data-model-name');
        if (!modelName) return;

        const downstream = state.modelDownstream.get(modelName);
        if (!downstream || downstream.size === 0) {
            d3.select(this).style('display', 'none');
            return;
        }

        let hasVisibleDownstream = false;
        downstream.forEach(downstreamModel => {
            if (state.visibleModels.has(downstreamModel)) {
                hasVisibleDownstream = true;
            }
        });

        d3.select(this).style('display', 'block');

        const expandLines = d3.select(this).selectAll('.expand-line');
        const collapseLine = d3.select(this).select('.collapse-line');

        if (hasVisibleDownstream) {
            expandLines.style('display', 'none');
            collapseLine.style('display', 'block');
        } else {
            expandLines.style('display', 'block');
            collapseLine.style('display', 'none');
        }
    });

    d3.selectAll('.expand-upstream-icon-group').each(function() {
        const modelName = d3.select(this).attr('data-model-name');
        if (!modelName) return;

        const upstream = state.modelUpstream.get(modelName);
        if (!upstream || upstream.size === 0) {
            d3.select(this).style('display', 'none');
            return;
        }

        let hasVisibleUpstream = false;
        upstream.forEach(upstreamModel => {
            if (state.visibleModels.has(upstreamModel)) {
                hasVisibleUpstream = true;
            }
        });

        d3.select(this).style('display', 'block');

        const expandLines = d3.select(this).selectAll('.expand-upstream-line');
        const collapseLine = d3.select(this).select('.collapse-upstream-line');

        if (hasVisibleUpstream) {
            expandLines.style('display', 'none');
            collapseLine.style('display', 'block');
        } else {
            expandLines.style('display', 'block');
            collapseLine.style('display', 'none');
        }
    });
}

function expandDownstreamInternal(modelName, state, config) {
    if (!state || !config || !modelName) return;

    const expandingKey = `_expanding_${modelName}`;
    if (state[expandingKey]) {
        return;
    }
    state[expandingKey] = true;

    const downstreamModels = state.modelDownstream.get(modelName);
    if (!downstreamModels || downstreamModels.size === 0) {
        state[expandingKey] = false;
        return;
    }

    downstreamModels.forEach(downstreamModel => {
        state.visibleModels.add(downstreamModel);
    });

    const currentTransform = state.zoom && state.svg ? d3.zoomTransform(state.svg.node()) : null;

    const parentModel = state.models.find(m => m && m.name === modelName);

    let defaultX, defaultY;

    if (parentModel && typeof parentModel.x === 'number' && !isNaN(parentModel.x) &&
        typeof parentModel.y === 'number' && !isNaN(parentModel.y)) {
        defaultX = parentModel.x + config.box.width + config.layout.xSpacing;
        defaultY = parentModel.y;
    } else {
        defaultX = config.box.padding + config.layout.xSpacing;
        defaultY = config.box.padding;
    }

    const avgHeight = config.box.titleHeight + 28 + config.box.padding;
    let currentYTop = defaultY - avgHeight / 2;

    downstreamModels.forEach((downstreamModel) => {
        let item = state.models.find(m => m && m.name === downstreamModel);
        let isExposure = false;

        if (!item) {
            item = state.exposures.find(e => e && e.name === downstreamModel);
            isExposure = true;
        }

        if (item) {
            if (!item.height || isNaN(item.height)) {
                if (isExposure) {
                    const exposureData = item.exposureData || {};
                    let detailRows = 0;
                    if (exposureData.type) detailRows++;
                    if (exposureData.url) detailRows++;
                    item.height = config.box.titleHeight +
                                  (detailRows * config.box.columnHeight) +
                                  config.box.padding;
                } else {
                    item.columnsCollapsed = item.columnsCollapsed || false;
                    item.height = config.box.titleHeight + 28 +
                                  ((item.columns && item.columns.length) ? (item.columns.length * config.box.columnHeight) : 0) +
                                  config.box.padding;
                    if (item.columnsCollapsed) {
                        item.height = config.box.titleHeight + 28;
                    }
                }
            }

            const hasValidPosition = typeof item.x === 'number' && !isNaN(item.x) &&
                                    typeof item.y === 'number' && !isNaN(item.y) &&
                                    !(item.x === 0 && item.y === 0);

            if (!hasValidPosition) {
                item.x = defaultX;
                item.y = currentYTop + item.height / 2;
                currentYTop = currentYTop + item.height + config.layout.ySpacing;
            } else {
                const itemTop = item.y - item.height / 2;
                const itemBottom = itemTop + item.height;
                if (itemBottom > currentYTop) {
                    currentYTop = itemBottom + config.layout.ySpacing;
                }
            }
        }
    });

    state.models.forEach(model => {
        if (model && state.visibleModels.has(model.name)) {
            if (!model.height || isNaN(model.height)) {
                model.columnsCollapsed = model.columnsCollapsed || false;
                model.height = config.box.titleHeight + 28 +
                              ((model.columns && model.columns.length) ? (model.columns.length * config.box.columnHeight) : 0) +
                              config.box.padding;
                if (model.columnsCollapsed) {
                    model.height = config.box.titleHeight + 28;
                }
            }

            if (typeof model.x !== 'number' || isNaN(model.x) ||
                typeof model.y !== 'number' || isNaN(model.y) ||
                (model.x === 0 && model.y === 0)) {
                console.warn(`Model ${model.name} has invalid position, assigning default`);
                model.x = defaultX;
                model.y = defaultY;
            }
        }
    });

    state.exposures.forEach(exposure => {
        if (exposure && exposure.name && state.visibleModels.has(exposure.name)) {
            if (!exposure.height || isNaN(exposure.height)) {
                const exposureData = exposure.exposureData || {};
                let detailRows = 0;
                if (exposureData.type) detailRows++;
                if (exposureData.url) detailRows++;
                exposure.height = config.box.titleHeight +
                                  (detailRows * config.box.columnHeight) +
                                  config.box.padding;
            }

            if (typeof exposure.x !== 'number' || isNaN(exposure.x) ||
                typeof exposure.y !== 'number' || isNaN(exposure.y) ||
                (exposure.x === 0 && exposure.y === 0)) {
                console.warn(`Exposure ${exposure.name} has invalid position, assigning default`);
                exposure.x = defaultX;
                exposure.y = defaultY;
            }
        }
    });

    if (currentTransform && state.zoom && state.svg &&
        !isNaN(currentTransform.x) && !isNaN(currentTransform.y) &&
        !isNaN(currentTransform.k) && currentTransform.k > 0) {
        state.svg.call(state.zoom.transform, currentTransform);
    }

    d3.selectAll('.model:not(.model-exposure)').each(function(d) {
        if (!d || !d.name) return;

        const isVisible = state.visibleModels.has(d.name);
        const wasVisible = d3.select(this).style('display') !== 'none';

        // Validate positions before showing
        if (isVisible && (typeof d.x !== 'number' || isNaN(d.x) ||
            typeof d.y !== 'number' || isNaN(d.y) ||
            typeof d.height !== 'number' || isNaN(d.height))) {
            console.warn(`Invalid position for model ${d.name}, hiding it`);
            state.visibleModels.delete(d.name);
            d3.select(this).style('display', 'none');
            return;
        }

        if (isVisible && !wasVisible) {
            const modelEl = d3.select(this);
            modelEl
                .style('display', 'block')
                .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
                .style('opacity', 0);

            modelEl
                .transition()
                .duration(300)
                .style('opacity', 1);
        } else if (isVisible) {
            d3.select(this).style('display', 'block');
        } else {
            d3.select(this).style('display', 'none');
        }
    });

    // Update exposure positions in state and display
    state.exposures.forEach(exposure => {
        if (exposure && exposure.name && state.visibleModels.has(exposure.name)) {
            if (typeof exposure.x === 'number' && !isNaN(exposure.x) &&
                typeof exposure.y === 'number' && !isNaN(exposure.y)) {
                state.exposurePositions.set(exposure.name, { x: exposure.x, y: exposure.y });
            }
        }
    });

    // Update exposure display
    d3.selectAll('.model-exposure')
        .data(state.exposures, d => d ? d.name : null)
        .each(function(d) {
            if (!d || !d.name) return;
            const isVisible = state.visibleModels.has(d.name);
            const wasVisible = d3.select(this).style('display') !== 'none';

            const exposureX = d.x;
            const exposureY = d.y;
            const exposureHeight = d.height;

            if (isVisible && (typeof exposureX !== 'number' || isNaN(exposureX) ||
                typeof exposureY !== 'number' || isNaN(exposureY) ||
                typeof exposureHeight !== 'number' || isNaN(exposureHeight))) {
                d3.select(this).style('display', 'none');
                return;
            }

            if (isVisible && !wasVisible) {
                const exposureEl = d3.select(this);
                exposureEl
                    .style('display', 'block')
                    .attr('transform', `translate(${exposureX},${exposureY - exposureHeight/2})`)
                    .style('opacity', 0);

                exposureEl
                    .transition()
                    .duration(300)
                    .style('opacity', 1);
            } else if (isVisible) {
                d3.select(this).style('display', 'block');
            } else {
                d3.select(this).style('display', 'none');
            }
        });

    requestAnimationFrame(() => {
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
                const shouldShow = sourceVisible && targetVisible;
                const wasVisible = d3.select(this).style('display') !== 'none';

                if (shouldShow) {
                    const path = createEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';

                        if (!wasVisible) {
                            edgeEl
                                .style('display', 'block')
                                .style('opacity', 0)
                                .attr('marker-end', markerEnd)
                                .transition()
                                .duration(300)
                                .style('opacity', 1)
                                .attr('d', path);
                        } else {
                            edgeEl
                                .attr('marker-end', markerEnd)
                                .transition()
                                .duration(300)
                                .attr('d', path);
                        }
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
                const sourceModelVisible = state.visibleModels.has(sourceNode.model);
                const targetExposureVisible = state.visibleModels.has(targetNode.model);
                const shouldShow = sourceModelVisible && targetExposureVisible;
                const wasVisible = d3.select(this).style('display') !== 'none';

                if (shouldShow) {
                    const path = createExposureEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';

                        if (!wasVisible) {
                            edgeEl
                                .style('display', 'block')
                                .style('opacity', 0)
                                .attr('marker-end', markerEnd)
                                .transition()
                                .duration(300)
                                .style('opacity', 1)
                                .attr('d', path);
                        } else {
                            edgeEl
                                .attr('marker-end', markerEnd)
                                .transition()
                                .duration(300)
                                .attr('d', path);
                        }
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

        updateExpandIcons(state);
        state[expandingKey] = false;
    });
}

function getAllDownstreamModels(modelName, state, visited = new Set()) {
    if (visited.has(modelName)) {
        return new Set();
    }
    visited.add(modelName);

    const allDownstream = new Set();
    const immediateDownstream = state.modelDownstream.get(modelName);

    if (immediateDownstream && immediateDownstream.size > 0) {
        immediateDownstream.forEach(downstreamModel => {
            allDownstream.add(downstreamModel);
            const nestedDownstream = getAllDownstreamModels(downstreamModel, state, visited);
            nestedDownstream.forEach(model => allDownstream.add(model));
        });
    }

    return allDownstream;
}

function collapseDownstreamInternal(modelName, state, config) {
    if (!state || !config || !modelName) return;

    const expandingKey = `_expanding_${modelName}`;
    if (state[expandingKey]) return;
    state[expandingKey] = true;

    const allDownstreamModels = getAllDownstreamModels(modelName, state);

    if (allDownstreamModels.size === 0) {
        state[expandingKey] = false;
        return;
    }

    allDownstreamModels.forEach(downstreamModel => {
        state.visibleModels.delete(downstreamModel);
    });

    const currentTransform = state.zoom && state.svg ? d3.zoomTransform(state.svg.node()) : null;

    if (currentTransform && state.zoom && state.svg &&
        !isNaN(currentTransform.x) && !isNaN(currentTransform.y) &&
        !isNaN(currentTransform.k) && currentTransform.k > 0) {
        state.svg.call(state.zoom.transform, currentTransform);
    }

    d3.selectAll('.model:not(.model-exposure)').each(function(d) {
        if (!d || !d.name) return;
        const isVisible = state.visibleModels.has(d.name);
        if (!isVisible) {
            d3.select(this)
                .transition()
                .duration(300)
                .style('opacity', 0)
                .on('end', function() {
                    d3.select(this).style('display', 'none');
                });
        }
    });

    // Hide exposures that are no longer visible
    d3.selectAll('.model-exposure').each(function(d) {
        if (!d || !d.name) return;
        const isVisible = state.visibleModels.has(d.name);
        if (!isVisible) {
            d3.select(this)
                .transition()
                .duration(300)
                .style('opacity', 0)
                .on('end', function() {
                    d3.select(this).style('display', 'none');
                });
        }
    });

    requestAnimationFrame(() => {
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
                const shouldShow = sourceVisible && targetVisible;

                if (!shouldShow) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl
                        .attr('marker-end', markerEnd)
                        .transition()
                        .duration(300)
                        .style('opacity', 0)
                        .on('end', function() {
                            d3.select(this).style('display', 'none');
                        });
                } else {
                    const path = createEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                        edgeEl
                            .attr('marker-end', markerEnd)
                            .transition()
                            .duration(300)
                            .attr('d', path);
                    }
                }
            } else {
                d3.select(this).style('display', 'none');
            }
        });

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
                const shouldShow = sourceVisible && targetVisible;

                if (!shouldShow) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl
                        .attr('marker-end', markerEnd)
                        .transition()
                        .duration(300)
                        .style('opacity', 0)
                        .on('end', function() {
                            d3.select(this).style('display', 'none');
                        });
                } else {
                    const path = createExposureEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                        edgeEl
                            .attr('marker-end', markerEnd)
                            .transition()
                            .duration(300)
                            .attr('d', path);
                    }
                }
            } else {
                d3.select(this).style('display', 'none');
            }
        });

        filterGraphByVisibility(state, config);
        state[expandingKey] = false;
    });
}

function getAllUpstreamModels(modelName, state, visited = new Set()) {
    if (visited.has(modelName)) return new Set();
    visited.add(modelName);

    const allUpstream = new Set();
    const immediateUpstream = state.modelUpstream.get(modelName);

    if (immediateUpstream && immediateUpstream.size > 0) {
        immediateUpstream.forEach(upstreamModel => {
            allUpstream.add(upstreamModel);
            const nestedUpstream = getAllUpstreamModels(upstreamModel, state, visited);
            nestedUpstream.forEach(model => allUpstream.add(model));
        });
    }
    return allUpstream;
}

function expandUpstreamInternal(modelName, state, config) {
    if (!state || !config || !modelName) return;

    const expandingKey = `_expanding_upstream_${modelName}`;
    if (state[expandingKey]) return;
    state[expandingKey] = true;

    const upstreamModels = state.modelUpstream.get(modelName);
    if (!upstreamModels || upstreamModels.size === 0) {
        state[expandingKey] = false;
        return;
    }

    upstreamModels.forEach(upstreamModel => {
        state.visibleModels.add(upstreamModel);
    });

    const currentTransform = state.zoom && state.svg ? d3.zoomTransform(state.svg.node()) : null;
    const parentModel = state.models.find(m => m && m.name === modelName);

    let defaultX, defaultY;
    if (parentModel && typeof parentModel.x === 'number' && !isNaN(parentModel.x) &&
        typeof parentModel.y === 'number' && !isNaN(parentModel.y)) {
        defaultX = parentModel.x - config.box.width - config.layout.xSpacing;
        defaultY = parentModel.y;
    } else {
        defaultX = config.box.padding;
        defaultY = config.box.padding;
    }

    const avgHeight = config.box.titleHeight + 28 + config.box.padding;
    let currentYTop = defaultY - avgHeight / 2;

    upstreamModels.forEach((upstreamModel) => {
        let item = state.models.find(m => m && m.name === upstreamModel);
        if (item) {
            if (!item.height || isNaN(item.height)) {
                item.columnsCollapsed = item.columnsCollapsed || false;
                item.height = config.box.titleHeight + 28 +
                    ((item.columns && item.columns.length) ? (item.columns.length * config.box.columnHeight) : 0) +
                    config.box.padding;
                if (item.columnsCollapsed) item.height = config.box.titleHeight + 28;
            }

            const hasValidPosition = typeof item.x === 'number' && !isNaN(item.x) &&
                typeof item.y === 'number' && !isNaN(item.y) &&
                !(item.x === 0 && item.y === 0);

            if (!hasValidPosition) {
                item.x = defaultX;
                item.y = currentYTop + item.height / 2;
                currentYTop = currentYTop + item.height + config.layout.ySpacing;
            } else {
                const itemTop = item.y - item.height / 2;
                const itemBottom = itemTop + item.height;
                if (itemBottom > currentYTop) {
                    currentYTop = itemBottom + config.layout.ySpacing;
                }
            }
        }
    });

    state.models.forEach(model => {
        if (model && state.visibleModels.has(model.name)) {
            if (!model.height || isNaN(model.height)) {
                model.columnsCollapsed = model.columnsCollapsed || false;
                model.height = config.box.titleHeight + 28 +
                    ((model.columns && model.columns.length) ? (model.columns.length * config.box.columnHeight) : 0) +
                    config.box.padding;
                if (model.columnsCollapsed) model.height = config.box.titleHeight + 28;
            }

            if (typeof model.x !== 'number' || isNaN(model.x) ||
                typeof model.y !== 'number' || isNaN(model.y) ||
                (model.x === 0 && model.y === 0)) {
                model.x = defaultX;
                model.y = defaultY;
            }
        }
    });

    if (currentTransform && state.zoom && state.svg &&
        !isNaN(currentTransform.x) && !isNaN(currentTransform.y) &&
        !isNaN(currentTransform.k) && currentTransform.k > 0) {
        state.svg.call(state.zoom.transform, currentTransform);
    }

    d3.selectAll('.model:not(.model-exposure)').each(function(d) {
        if (!d || !d.name) return;

        const isVisible = state.visibleModels.has(d.name);
        const wasVisible = d3.select(this).style('display') !== 'none';

        if (isVisible && (typeof d.x !== 'number' || isNaN(d.x) ||
            typeof d.y !== 'number' || isNaN(d.y) ||
            typeof d.height !== 'number' || isNaN(d.height))) {
            state.visibleModels.delete(d.name);
            d3.select(this).style('display', 'none');
            return;
        }

        if (isVisible && !wasVisible) {
            const modelEl = d3.select(this);
            modelEl
                .style('display', 'block')
                .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
                .style('opacity', 0);
            modelEl.transition().duration(300).style('opacity', 1);
        } else if (isVisible) {
            d3.select(this).style('display', 'block');
        } else {
            d3.select(this).style('display', 'none');
        }
    });

    requestAnimationFrame(() => {
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
                const shouldShow = sourceVisible && targetVisible;
                const wasVisible = d3.select(this).style('display') !== 'none';

                if (shouldShow) {
                    const path = createEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';

                        if (!wasVisible) {
                            edgeEl
                                .style('display', 'block')
                                .style('opacity', 0)
                                .attr('marker-end', markerEnd)
                                .transition().duration(300)
                                .style('opacity', 1)
                                .attr('d', path);
                        } else {
                            edgeEl.attr('marker-end', markerEnd).transition().duration(300).attr('d', path);
                        }
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

        updateExpandIcons(state);
        state[expandingKey] = false;
    });
}

function collapseUpstreamInternal(modelName, state, config) {
    if (!state || !config || !modelName) return;

    const expandingKey = `_expanding_upstream_${modelName}`;
    if (state[expandingKey]) return;
    state[expandingKey] = true;

    const allUpstreamModels = getAllUpstreamModels(modelName, state);

    if (allUpstreamModels.size === 0) {
        state[expandingKey] = false;
        return;
    }

    allUpstreamModels.forEach(upstreamModel => {
        state.visibleModels.delete(upstreamModel);
    });

    const currentTransform = state.zoom && state.svg ? d3.zoomTransform(state.svg.node()) : null;

    if (currentTransform && state.zoom && state.svg &&
        !isNaN(currentTransform.x) && !isNaN(currentTransform.y) &&
        !isNaN(currentTransform.k) && currentTransform.k > 0) {
        state.svg.call(state.zoom.transform, currentTransform);
    }

    d3.selectAll('.model:not(.model-exposure)').each(function(d) {
        if (!d || !d.name) return;
        const isVisible = state.visibleModels.has(d.name);
        if (!isVisible) {
            d3.select(this)
                .transition().duration(300)
                .style('opacity', 0)
                .on('end', function() { d3.select(this).style('display', 'none'); });
        }
    });

    requestAnimationFrame(() => {
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
                const shouldShow = sourceVisible && targetVisible;

                if (!shouldShow) {
                    const edgeEl = d3.select(this);
                    const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                    edgeEl.attr('marker-end', markerEnd)
                        .transition().duration(300)
                        .style('opacity', 0)
                        .on('end', function() { d3.select(this).style('display', 'none'); });
                } else {
                    const path = createEdgePath(d, state, config);
                    if (path && path !== '' && !path.includes('NaN')) {
                        const edgeEl = d3.select(this);
                        const markerEnd = edgeEl.attr('marker-end') || 'url(#arrowhead)';
                        edgeEl.attr('marker-end', markerEnd).transition().duration(300).attr('d', path);
                    }
                }
            } else {
                d3.select(this).style('display', 'none');
            }
        });

        filterGraphByVisibility(state, config);
        state[expandingKey] = false;
    });
}
