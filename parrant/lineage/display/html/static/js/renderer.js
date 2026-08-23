/**
 * Rendering functions for graph visualization
 */

// --- Row-set predicate card -----------------------------------------------------------------
// A click-opened card showing the FULL predicate a row-set dependent filters on, wrapped and
// lightly pretty-printed so long window functions stay readable (the hover tooltip is one line).

function _formatPredicate(sql) {
    // Insert newlines before the main SQL clauses so a long ROW_NUMBER() OVER (...) reads as a
    // small block instead of one runaway line. Purely cosmetic; the SQL text is unchanged.
    return (sql || '')
        .replace(/\s+OVER\s*\(/gi, '\nOVER (')
        .replace(/\s+PARTITION BY\s+/gi, '\n  PARTITION BY ')
        .replace(/\s+ORDER BY\s+/gi, '\n  ORDER BY ')
        .replace(/\s+(AND|OR)\s+/gi, '\n  $1 ')
        .trim();
}

function hideRowsetCard() {
    const card = document.getElementById('rowsetCard');
    if (card) card.style.display = 'none';
}

function showRowsetCard(name, note) {
    const graphContainer = document.getElementById('graph');
    if (!graphContainer) return;

    let card = document.getElementById('rowsetCard');
    if (!card) {
        graphContainer.insertAdjacentHTML(
            'beforeend',
            `<div id="rowsetCard" class="model-details-card" style="display:none; top:230px; z-index:260;">
                <div class="model-details-header">
                    <div class="model-details-title">
                        <div class="model-details-title-text">
                            <h4 id="rowsetCardName"></h4>
                            <span class="model-details-type" style="color:var(--info)">row-set dependency</span>
                        </div>
                    </div>
                    <button class="model-details-close" id="closeRowsetCard">×</button>
                </div>
                <div class="model-details-content">
                    <div class="model-details-section">
                        <label>Column used only in this predicate (not projected)</label>
                        <pre id="rowsetCardPredicate" style="white-space:pre-wrap; word-break:break-word; font-family:var(--font-mono,monospace); font-size:12px; line-height:1.5; background:var(--slate-50,#f7f9fc); border:1px solid var(--border,#e2e8f0); border-radius:6px; padding:10px 12px; margin:6px 0 0; color:var(--text);"></pre>
                    </div>
                </div>
            </div>`
        );
        card = document.getElementById('rowsetCard');
        const closeBtn = document.getElementById('closeRowsetCard');
        if (closeBtn) closeBtn.addEventListener('click', hideRowsetCard);
        // Dismiss on outside click (but not when clicking another row-set node, which reopens it).
        document.addEventListener('click', function (e) {
            const c = document.getElementById('rowsetCard');
            if (!c || c.style.display === 'none') return;
            if (!c.contains(e.target) && !e.target.closest('.is-rowset')) hideRowsetCard();
        });
    }

    document.getElementById('rowsetCardName').textContent = name;
    document.getElementById('rowsetCardPredicate').textContent = _formatPredicate(note);
    card.style.display = 'block';
}

// Set up SVG container and markers
function setupSvg(config) {
    const svg = d3.select('#graph')
        .append('svg')
        .attr('width', config.width)
        .attr('height', config.height);

    const defs = svg.append('defs');

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

    const arrowMarker = (id, color) => {
        defs.append('marker')
            .attr('id', id)
            .attr('viewBox', '0 -5 10 10')
            .attr('refX', 10)
            .attr('refY', 0)
            .attr('markerWidth', 6)
            .attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path')
            .attr('d', 'M0,-5L10,0L0,5')
            .attr('fill', color);
    };

    arrowMarker('arrowhead', 'var(--edge-color)');
    arrowMarker('arrowhead-highlighted', 'var(--edge-highlight)');

    return svg.append('g');
}

function handleModelCollapse(model, isCollapsed, state, config) {
    const modelElement = state.modelElements.get(model.name);
    if (!modelElement) return;

    const container = modelElement.select('.columns-container');
    const icon = modelElement.select('.toggle-icon path');
    // Select the background rect directly for height adjustment
    const modelRect = modelElement.select('.model-background .model-container');

    container.attr('data-expanded', !isCollapsed)
        .style('display', isCollapsed ? 'none' : 'block');

    const iconSVG = icon.node().ownerSVGElement;
    const iconX = parseFloat(iconSVG.getAttribute('x')) + parseFloat(iconSVG.getAttribute('width')) / 2;
    const iconY = parseFloat(iconSVG.getAttribute('y')) + parseFloat(iconSVG.getAttribute('height')) / 2;
    icon.attr('transform', `rotate(${isCollapsed ? -90 : 0}, ${iconX}, ${iconY})`); // Use actual center

    // Adjust height based on the combined header height
    const combinedHeaderHeight = config.box.titleHeight + 28;
    modelRect.attr('height', isCollapsed ? combinedHeaderHeight : model.height);
    model.columnsCollapsed = isCollapsed;

    // Update associated CSS class on the main model group
    modelElement.classed('collapsed-model', isCollapsed);
}


function updateModelEdges(model, state, config) {
    if (!model || !model.name || !state.modelEdges.has(model.name)) {
        return;
    }

    state.modelEdges.get(model.name).forEach(edgeInfo => {
        if (edgeInfo.element && edgeInfo.element.parentNode) {
            const edgeElement = d3.select(edgeInfo.element);

            // Preserve marker-end attribute
            const markerEnd = edgeElement.attr('marker-end') || 'url(#arrowhead)';

            let newPath;
            if (edgeInfo.isExposure) {
                // Use exposure edge path for exposure edges
                newPath = createExposureEdgePath({
                    source: edgeInfo.source,
                    target: edgeInfo.target
                }, state, config);
            } else {
                // Use regular edge path for lineage edges
                newPath = createEdgePath({
                    source: edgeInfo.source,
                    target: edgeInfo.target
                }, state, config);
            }

            // Only update if path is valid (not empty and no NaN)
            if (newPath && newPath !== '' && !newPath.includes('NaN')) {
                edgeElement
                    .attr('marker-end', markerEnd)
                    .attr('d', newPath);
            }
        }
    });

    // Ensure edges remain visually behind models after update
    const edgesGroup = d3.select('.edges-group');
    if (!edgesGroup.empty()) {
        edgesGroup.lower();
    }
}

function drawModels(g, state, config, dragBehavior) {
    state.modelElements = new Map();

    // Store state reference for expand functionality
    window.graphState = state;
    window.graphConfig = config;

    const modelGroups = g.selectAll('.model')
        .data(state.models || [])
        .enter()
        .append('g')
        .attr('class', d => {
            const modelType = (d && d.type) || 'model';
            return `model model-${modelType}`;
        })
        .attr('transform', d => {
            if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
                typeof d.y !== 'number' || isNaN(d.y) ||
                typeof d.height !== 'number' || isNaN(d.height)) {
                return 'translate(0,0)';
            }
            return `translate(${d.x},${d.y - d.height/2})`;
        })
        .call(dragBehavior);

    modelGroups.each(function(d) {
        state.modelElements.set(d.name, d3.select(this));
    });

    const backgroundGroup = modelGroups.append('g')
        .attr('class', 'model-background');

    // Model container rect (border and background)
    backgroundGroup.append('rect')
        .attr('class', 'model-container')
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('rx', 6)
        .attr('ry', 6)
        .style('fill', 'var(--surface)')
        .style('stroke', d => {
            const modelType = d.type || 'model';
            if (modelType === 'snapshot') return '#f59e0b';
            if (modelType === 'source') return '#14b8a6';
            if (modelType === 'seed') return '#22c55e';
            return 'var(--primary)';
        })
        .style('stroke-width', 1.5);

    const foregroundGroup = modelGroups.append('g')
        .attr('class', 'model-foreground');

    // Model header background rect
    foregroundGroup.append('rect')
        .attr('class', 'model-header')
        .attr('width', config.box.width - 2)
        .attr('height', config.box.titleHeight)
        .attr('x', 1)
        .attr('y', 1)
        .attr('rx', 5)
        .style('fill', d => {
            const modelType = d.type || 'model';
            if (modelType === 'source') return 'color-mix(in srgb, var(--success) 14%, var(--surface))';
            if (modelType === 'seed') return 'color-mix(in srgb, #22c55e 14%, var(--surface))';
            if (modelType === 'snapshot') return 'color-mix(in srgb, var(--accent) 16%, var(--surface))';
            return 'color-mix(in srgb, var(--primary) 16%, var(--surface))';  // cool tint for models
        })
        .style('stroke', 'none');

    // Model icon
    foregroundGroup.append('svg')
        .attr('class', 'model-icon')
        .attr('width', 24)
        .attr('height', 24)
        .attr('x', 36)
        .attr('y', config.box.titleHeight / 2 - 12)
        .attr('viewBox', '0 0 24 24')
        .append('path')
        .attr('d', function(d) {
            const modelType = d.type || 'model';
            return getModelIcon(modelType)
        })
        .attr('fill', 'none')
        .attr('stroke', d => {
            const modelType = d.type || 'model';
            if (modelType === 'source') return '#0d9488';
            if (modelType === 'seed') return '#10b981';
            if (modelType === 'snapshot') return '#f59e0b';
            return 'var(--primary)';
        })
        .attr('stroke-width', '2')
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');

    // Model kind eyebrow (MODEL / SOURCE / SEED / SNAPSHOT)
    foregroundGroup.append('text')
        .attr('class', 'model-eyebrow')
        .attr('x', 66)
        .attr('y', config.box.titleHeight / 2 - 6)
        .style('fill', 'var(--text-muted)')
        .style('font-family', 'var(--font-sans)')
        .style('font-size', '8.5px')
        .style('font-weight', '600')
        .style('letter-spacing', '0.09em')
        .style('pointer-events', 'none')
        .text(d => (d.type || 'model').toUpperCase());

    // Model title text (shifted right for upstream expand icon)
    const modelTitleText = foregroundGroup.append('text')
        .attr('class', 'model-title')
        .attr('x', 66)
        .attr('y', config.box.titleHeight / 2 + 10)
        .style('fill', d => {
            const modelType = d.type || 'model';
            if (modelType === 'snapshot') return 'color-mix(in srgb, var(--accent-dark) 55%, var(--text))';
            if (modelType === 'source') return 'color-mix(in srgb, var(--success) 55%, var(--text))';
            return 'var(--text)';
        })
        .style('cursor', 'pointer')
        .text(d => d.name)
        .on('click', function(event, d) {
            event.stopPropagation();
            if (d && d.name && typeof ModelDetailsModule !== 'undefined') {
                ModelDetailsModule.showCard(d.name);
            }
        })
        .each(function(d) {
            // Truncate text if too long (leave space for expand icon)
            const maxWidth = config.box.width - 80; // More space for expand icon
            const self = d3.select(this);
            let textLength = self.node().getComputedTextLength();
            let text = self.text();

            while (textLength > maxWidth && text.length > 0) {
                text = text.slice(0, -1);
                self.text(text + '...');
                textLength = self.node().getComputedTextLength();
            }

            // Store original text for future tooltip
            if (text + '...' !== d.name) {
                self.attr('data-original-text', d.name);
            }
        });

    // Add tooltip for truncated model titles
    modelTitleText.each(function(d) {
        const self = d3.select(this);
        const originalText = self.attr('data-original-text');
        if (originalText) {
            // Native SVG <title> fallback (zero-JS / a11y)
            self.append('title').text(originalText);
            self
                .style('pointer-events', 'all')
                .style('cursor', 'help')
                .on('mouseenter', function(event) {
                    showTooltip(event, originalText);
                })
                .on('mouseleave', function() {
                    hideTooltip();
                })
                .on('mousemove', function(event) {
                    const tooltip = createTooltip();
                    let x, y;
                    if (event.pageX !== undefined && event.pageY !== undefined) {
                        x = event.pageX;
                        y = event.pageY;
                    } else if (event.clientX !== undefined && event.clientY !== undefined) {
                        x = event.clientX + window.scrollX;
                        y = event.clientY + window.scrollY;
                    } else {
                        const sourceEvent = event.sourceEvent || event;
                        x = (sourceEvent.pageX || sourceEvent.clientX || 0) + (window.scrollX || 0);
                        y = (sourceEvent.pageY || sourceEvent.clientY || 0) + (window.scrollY || 0);
                    }
                    tooltip
                        .style('left', (x + 10) + 'px')
                        .style('top', (y - 10) + 'px');
                });
        }
    });

    const upstreamIconX = 20;
    const upstreamIconY = config.box.titleHeight / 2;

    const expandUpstreamIconGroup = foregroundGroup.append('g')
        .attr('class', 'expand-upstream-icon-group')
        .attr('data-model-name', d => d.name)
        .style('display', d => {
            const upstream = state.modelUpstream.get(d.name);
            return (upstream && upstream.size > 0) ? 'block' : 'none';
        })
        .style('cursor', 'pointer')
        .style('opacity', 0.7)
        .on('mouseenter', function() { d3.select(this).style('opacity', 1); })
        .on('mouseleave', function() { d3.select(this).style('opacity', 0.7); })
        .on('click', function(event, d) {
            event.stopPropagation();
            event.preventDefault();
            if (d && d.name) {
                const upstream = state.modelUpstream.get(d.name);
                if (upstream && upstream.size > 0) {
                    let hasVisibleUpstream = false;
                    upstream.forEach(upstreamModel => {
                        if (state.visibleModels.has(upstreamModel)) {
                            hasVisibleUpstream = true;
                        }
                    });
                    if (hasVisibleUpstream) {
                        if (window.collapseUpstream) window.collapseUpstream(d.name);
                    } else {
                        if (window.expandUpstream) window.expandUpstream(d.name);
                    }
                }
            }
        });

    expandUpstreamIconGroup.append('circle')
        .attr('cx', upstreamIconX)
        .attr('cy', upstreamIconY)
        .attr('r', 7)
        .attr('fill', 'white')
        .attr('stroke', '#94a3b8')
        .attr('stroke-width', 1);

    const upstreamPlusSize = 5;
    expandUpstreamIconGroup.append('line')
        .attr('class', 'expand-upstream-line')
        .attr('x1', upstreamIconX - upstreamPlusSize / 2)
        .attr('y1', upstreamIconY)
        .attr('x2', upstreamIconX + upstreamPlusSize / 2)
        .attr('y2', upstreamIconY)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round');

    expandUpstreamIconGroup.append('line')
        .attr('class', 'expand-upstream-line')
        .attr('x1', upstreamIconX)
        .attr('y1', upstreamIconY - upstreamPlusSize / 2)
        .attr('x2', upstreamIconX)
        .attr('y2', upstreamIconY + upstreamPlusSize / 2)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round');

    expandUpstreamIconGroup.append('line')
        .attr('class', 'collapse-upstream-line')
        .attr('x1', upstreamIconX - upstreamPlusSize / 2)
        .attr('y1', upstreamIconY)
        .attr('x2', upstreamIconX + upstreamPlusSize / 2)
        .attr('y2', upstreamIconY)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round')
        .style('display', 'none');

    // Add expand/collapse icon for models with downstream dependencies
    const expandIconGroup = foregroundGroup.append('g')
        .attr('class', 'expand-icon-group')
        .attr('data-model-name', d => d.name)
        .style('display', d => {
            const downstream = state.modelDownstream.get(d.name);
            return (downstream && downstream.size > 0) ? 'block' : 'none';
        })
        .style('cursor', 'pointer')
        .style('opacity', 0.7)
        .on('mouseenter', function() { d3.select(this).style('opacity', 1); })
        .on('mouseleave', function() { d3.select(this).style('opacity', 0.7); })
        .on('click', function(event, d) {
            event.stopPropagation();
            event.preventDefault();
            if (d && d.name) {
                const downstream = state.modelDownstream.get(d.name);
                if (downstream && downstream.size > 0) {
                    let hasVisibleDownstream = false;
                    downstream.forEach(downstreamModel => {
                        if (state.visibleModels.has(downstreamModel)) {
                            hasVisibleDownstream = true;
                        }
                    });
                    if (hasVisibleDownstream) {
                        if (window.collapseDownstream) window.collapseDownstream(d.name);
                    } else {
                        if (window.expandDownstream) window.expandDownstream(d.name);
                    }
                }
            }
        });

    const iconX = config.box.width - 20;
    const iconY = config.box.titleHeight / 2;

    expandIconGroup.append('circle')
        .attr('cx', iconX)
        .attr('cy', iconY)
        .attr('r', 7)
        .attr('fill', 'white')
        .attr('stroke', '#94a3b8')
        .attr('stroke-width', 1);

    // Plus sign (horizontal line) - shown when collapsed
    const plusSize = 5;
    expandIconGroup.append('line')
        .attr('class', 'expand-line')
        .attr('x1', iconX - plusSize / 2)
        .attr('y1', iconY)
        .attr('x2', iconX + plusSize / 2)
        .attr('y2', iconY)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round');

    // Plus sign (vertical line) - shown when collapsed
    expandIconGroup.append('line')
        .attr('class', 'expand-line')
        .attr('x1', iconX)
        .attr('y1', iconY - plusSize / 2)
        .attr('x2', iconX)
        .attr('y2', iconY + plusSize / 2)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round');

    // Minus sign (horizontal line) - shown when expanded
    expandIconGroup.append('line')
        .attr('class', 'collapse-line')
        .attr('x1', iconX - plusSize / 2)
        .attr('y1', iconY)
        .attr('x2', iconX + plusSize / 2)
        .attr('y2', iconY)
        .attr('stroke', '#64748b')
        .attr('stroke-width', 1.5)
        .attr('stroke-linecap', 'round')
        .style('display', 'none');

    const columnsHeader = foregroundGroup.append('g')
        .attr('class', 'columns-header')
        .attr('transform', `translate(0, ${config.box.titleHeight})`)
        .style('cursor', 'pointer');

    columnsHeader.append('rect')
        .attr('width', config.box.width - 2)
        .attr('height', 28)
        .attr('x', 1)
        .attr('fill', d => {
            if (d.type === 'source') return '#f0fdfa';
            if (d.type === 'snapshot') return '#fef3c7';
            return '#f8fafc';
        })
        .style('stroke', 'none');

    columnsHeader.append('text')
        .attr('class', 'columns-label')
        .attr('x', 15)
        .attr('y', 18)
        .attr('dominant-baseline', 'middle')
        .attr('fill', '#64748b')
        .attr('font-size', '13px')
        .text('Columns');

    const toggleIcon = columnsHeader.append('svg')
        .attr('class', 'toggle-icon')
        .attr('x', config.box.width - 28)
        .attr('y', 4)
        .attr('width', 20)
        .attr('height', 20)
        .attr('viewBox', '0 0 24 24');

    toggleIcon.append('path')
.attr('d', 'M6 9l6 6 6-6')
        .attr('stroke', '#64748b')
        .attr('fill', 'none')
        .attr('stroke-width', 2)
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');

    const columnsContainer = foregroundGroup.append('g')
        .attr('class', 'columns-container')
        .attr('transform', `translate(0, ${config.box.titleHeight + 28})`)
        .attr('data-expanded', 'true');

    columnsHeader.on('click', function(event, d) {
        // Prevent click event from propagating to the model drag handler
        event.stopPropagation();

        const modelElement = d3.select(this.parentNode.parentNode);
        const container = modelElement.select('.columns-container');
        const isExpanded = container.attr('data-expanded') === 'true';
        const iconPath = d3.select(this).select('.toggle-icon path');
        const modelRect = modelElement.select('.model-background .model-container');
        const headerRect = d3.select(this).select('rect');
        const combinedHeaderHeight = config.box.titleHeight + 28;

        if (isExpanded) {
            container.attr('data-expanded', 'false')
                .style('display', 'none');

            // Rotate icon using calculated center
            const iconSVG = iconPath.node().ownerSVGElement;
            const iconX = parseFloat(iconSVG.getAttribute('x')) + parseFloat(iconSVG.getAttribute('width')) / 2;
            const iconY = parseFloat(iconSVG.getAttribute('y')) + parseFloat(iconSVG.getAttribute('height')) / 2;
            iconPath.attr('transform', `rotate(-90, ${iconX}, ${iconY})`);

            d3.select(this).attr('data-collapsed', 'true');

            // Store original height before collapsing
            if (!d._originalHeight) {
                d._originalHeight = d.height;
            }

            // Update model height to collapsed height
            d.height = combinedHeaderHeight;
            modelRect.attr('height', combinedHeaderHeight);
            headerRect.attr('rx', 0).attr('ry', 0);
            d.columnsCollapsed = true;

            modelElement.classed('collapsed-model', true);

            // Update model transform immediately to reflect new height
            modelElement.transition()
                .duration(300)
                .attr('transform', `translate(${d.x},${d.y - d.height/2})`);
        } else {
            container.attr('data-expanded', 'true')
                .style('display', 'block');

            // Reset icon rotation using calculated center
            const iconSVG = iconPath.node().ownerSVGElement;
            const iconX = parseFloat(iconSVG.getAttribute('x')) + parseFloat(iconSVG.getAttribute('width')) / 2;
            const iconY = parseFloat(iconSVG.getAttribute('y')) + parseFloat(iconSVG.getAttribute('height')) / 2;
            iconPath.attr('transform', `rotate(0, ${iconX}, ${iconY})`);

            d3.select(this).attr('data-collapsed', 'false');

            // Restore original height
            if (d._originalHeight) {
                d.height = d._originalHeight;
                delete d._originalHeight;
            }

            modelRect.attr('height', d.height);
            headerRect.attr('rx', 0).attr('ry', 0);
            d.columnsCollapsed = false;

            modelElement.classed('collapsed-model', false);

            // Update model transform immediately to reflect new height
            modelElement.transition()
                .duration(300)
                .attr('transform', `translate(${d.x},${d.y - d.height/2})`);
        }

        // Update all edges connected to this model after transform is updated
        // Use requestAnimationFrame to ensure transform is applied first
        requestAnimationFrame(() => {
            // Small delay to ensure transform transition has started
        setTimeout(() => {
            updateEdgesForCollapse(d, state, config);
            }, 50);
        });
    });

    return modelGroups;
}

function drawColumns(nodes, state, config, onColumnClick) {
    nodes.each(function(model) {
        // Find the columns container in the foreground group
        const columnsContainer = d3.select(this).select('.model-foreground').select('.columns-container');

        // Create a group to contain all columns
        const columnsGroup = columnsContainer.append('g')
            .attr('class', 'columns-list')
            .attr('transform', `translate(1, 0)`); // Minimal offset

        model.columns.forEach((col, i) => {
            const yPos = i * config.box.columnHeight;

            const columnGroup = columnsGroup.append('g')
                .attr('class', 'column-group')
                .attr('transform', `translate(${config.box.padding}, ${yPos})`)
                .attr('data-id', col.id);

            // --- Clickable Background ---
            const columnBackground = columnGroup.append('rect')
                .attr('class', 'column-background')
                .attr('x', 0)
                .attr('y', 0)
                .attr('width', config.box.width - (config.box.padding * 2))
                .attr('height', config.box.columnHeight - config.box.columnPadding)
                .attr('fill', 'transparent')
                .style('cursor', 'pointer')
                .on('click', function() {
                    onColumnClick(col.id, model.name);
                });

            // Set up hover handlers - add tooltip if column name is truncated
            if (col.name.length > 18) {
                columnBackground
                    .on('mouseenter', function(event) {
                        d3.select(this).attr('fill', 'rgba(0,0,0,0.03)');
                        showTooltip(event, col.name);
                    })
                    .on('mouseleave', function() {
                        d3.select(this).attr('fill', 'transparent');
                        hideTooltip();
                    })
                    .on('mousemove', function(event) {
                        const tooltip = createTooltip();
                        let x, y;
                        if (event.pageX !== undefined && event.pageY !== undefined) {
                            x = event.pageX;
                            y = event.pageY;
                        } else if (event.clientX !== undefined && event.clientY !== undefined) {
                            x = event.clientX + window.scrollX;
                            y = event.clientY + window.scrollY;
                        } else {
                            const sourceEvent = event.sourceEvent || event;
                            x = (sourceEvent.pageX || sourceEvent.clientX || 0) + (window.scrollX || 0);
                            y = (sourceEvent.pageY || sourceEvent.clientY || 0) + (window.scrollY || 0);
                        }
                        tooltip
                            .style('left', (x + 10) + 'px')
                            .style('top', (y - 10) + 'px');
                    });
            } else {
                columnBackground
                    .on('mouseenter', function() {
                        d3.select(this).attr('fill', 'rgba(0,0,0,0.03)');
                    })
                    .on('mouseleave', function() {
                        d3.select(this).attr('fill', 'transparent');
                    });
            }

            // --- Left Color Indicator (doubles as the thermal rail on select) ---
            columnGroup.append('rect')
                .attr('class', 'column-indicator')
                .attr('x', 2)
                .attr('y', 2)
                .attr('width', 3)
                .attr('height', config.box.columnHeight - config.box.columnPadding - 4)
                .attr('rx', 1.5)
                .attr('data-basefill', col.isKey ? '#3b82f6' : '#94a3b8')
                .attr('fill', col.isKey ? '#3b82f6' : '#94a3b8')
                .attr('opacity', 0.7);

            // --- subject-node ring — the policy verdict on the column being explored.
            //     block = red (--error), warn = amber (--accent), allow/absent = no ring.
            //     A ring (not a fill) so the product stays the hero; at most one color per node.
            if (col.id === state.subjectNodeId &&
                (state.policyDecision === 'block' || state.policyDecision === 'warn')) {
                const rowH = config.box.columnHeight - config.box.columnPadding;
                columnGroup.append('rect')
                    .attr('class', 'column-subject-ring policy-' + state.policyDecision)
                    .attr('x', 0.75)
                    .attr('y', 1.5)
                    .attr('width', config.box.width - (config.box.padding * 2) - 1.5)
                    .attr('height', rowH - 3)
                    .attr('rx', 5)
                    .attr('fill', 'none')
                    .style('pointer-events', 'none');
            }

            // --- breaking mark — a small amber dot on a changed column whose meaning
            //     may have shifted (fail-safe: indeterminate/unknown also renders breaking).
            //     Equivalent/unchanged columns get nothing (absence = safe; color is rationed).
            if (col.breaking) {
                const dotCy = (config.box.columnHeight - config.box.columnPadding) / 2;
                const dotGroup = columnGroup.append('g')
                    .attr('class', 'column-breaking-mark')
                    .style('cursor', 'help');
                dotGroup.append('circle')
                    .attr('class', 'column-breaking-dot')
                    .attr('cx', 8)
                    .attr('cy', dotCy)
                    .attr('r', 2.75);
                const semanticLabel = col.semantic === 'meaning_changed'
                    ? 'Meaning changed — value may differ downstream'
                    : 'Breaking (unproven) — treated as a change (fail-safe)';
                dotGroup
                    .on('mouseenter', function(event) { showTooltip(event, semanticLabel); })
                    .on('mousemove', function(event) {
                        const tt = createTooltip();
                        const x = (event.pageX !== undefined ? event.pageX : event.clientX + window.scrollX);
                        const y = (event.pageY !== undefined ? event.pageY : event.clientY + window.scrollY);
                        tt.style('left', (x + 10) + 'px').style('top', (y - 10) + 'px');
                    })
                    .on('mouseleave', function() { hideTooltip(); });
            }

            // --- Data Type Tag geometry (computed before the name so the name
            //     can be pixel-truncated to the space actually left of the tag) ---
            const nameX = 12;
            const nameTagGap = 8;
            // Default available width = up to the right padding when there is no tag
            let nameAvailableWidth = config.box.width - (config.box.padding * 2) - nameX - nameTagGap;
            let tagInfo = null;

            if (col.dataType) {
                // Get short version of data type first
                const shortType = col.dataType.toLowerCase()
                    .replace('character varying', 'varchar')
                    .replace('double precision', 'double')
                    .replace('timestamp without time zone', 'timestamp')
                    .replace('timestamp with time zone', 'timestamptz');

                const tempText = columnGroup.append('text')
                    .attr('font-size', '11px')
                    .text(shortType)
                    .style('visibility', 'hidden');

                const textWidth = tempText.node().getComputedTextLength();
                tempText.remove();

                // Calculate total tag width including padding
                const tagWidth = textWidth + 16;
                const safetyMargin = 8;

                // Calculate x position to ensure tag stays within bounds
                const xPosition = config.box.width - tagWidth - (config.box.padding * 2) - safetyMargin;

                // Calculate vertical center position
                const yPosition = (config.box.columnHeight - config.box.columnPadding) / 2;

                tagInfo = { shortType, tagWidth, xPosition, yPosition };
                // Name may extend up to the tag's left edge, minus a small gap
                nameAvailableWidth = xPosition - nameX - nameTagGap;
            }

            // --- Test badge geometry (guardrails on the column, shown behind the toggle) ---
            // Reserve space to the LEFT of the type tag so the name truncates around it.
            let testBadgeInfo = null;
            const showTests = (typeof window !== 'undefined') && window.__showTests;
            if (showTests && col.tests && col.tests.length > 0) {
                const safetyMargin = 8;
                const badgeGap = 6;
                const badgePadX = 5;
                const iconW = 9;
                const iconGap = 3;

                const countText = String(col.tests.length);
                const tempCount = columnGroup.append('text')
                    .attr('font-size', '10px')
                    .attr('font-weight', '600')
                    .text(countText)
                    .style('visibility', 'hidden');
                const countWidth = tempCount.node().getComputedTextLength();
                tempCount.remove();

                const badgeWidth = badgePadX * 2 + iconW + iconGap + countWidth;
                // Right anchor: the tag's left edge if a tag exists, else the box's right edge.
                const rightAnchor = tagInfo
                    ? tagInfo.xPosition
                    : (config.box.width - (config.box.padding * 2) - safetyMargin);
                const badgeX = rightAnchor - badgeGap - badgeWidth;
                const badgeY = (config.box.columnHeight - config.box.columnPadding) / 2;

                testBadgeInfo = { badgeWidth, badgeX, badgeY, iconW, iconGap, badgePadX, countText };
                // Shrink the name so it never overlaps the badge.
                nameAvailableWidth = Math.max(0, badgeX - nameX - nameTagGap);
            }

            // --- Column Name Text (pixel-width truncation, matching model-title) ---
            const columnNameText = columnGroup.append('text')
                .attr('class', 'column-name')
                .attr('x', nameX)
                .attr('y', (config.box.columnHeight - config.box.columnPadding) / 2)
                .attr('dominant-baseline', 'middle')
                .attr('font-size', '12px')
                .attr('fill', '#334155')
                .text(col.name)
                .attr('data-original-text', col.name);

            columnNameText.each(function() {
                const node = this;
                let textStr = col.name;
                while (textStr.length > 0 && node.getComputedTextLength() > nameAvailableWidth) {
                    textStr = textStr.slice(0, -1);
                    columnNameText.text(textStr + '...');
                }
            });

            // --- Data Type Tag (drawn after the name) ---
            if (tagInfo) {
                const { shortType, tagWidth, xPosition, yPosition } = tagInfo;

                const tagGroup = columnGroup.append('g')
                    .attr('class', 'column-type-tag')
                    .attr('transform', `translate(${xPosition}, 0)`)
                    .style('pointer-events', 'none');

                // Tag background pill
                tagGroup.append('rect')
                    .attr('rx', 4)
                    .attr('ry', 4)
                    .attr('width', tagWidth)
                    .attr('height', 18)
                    .attr('y', yPosition - 9)
                    .style('fill', getTagColor(shortType))
                    .style('stroke', 'none');

                // Tag text
                tagGroup.append('text')
                    .attr('x', tagWidth / 2)
                    .attr('y', yPosition)
                    .attr('text-anchor', 'middle')
                    .attr('dominant-baseline', 'central')
                    .attr('dy', '-0.1em')
                    .style('fill', 'white')
                    .style('font-size', '11px')
                    .style('font-weight', '500')
                    .text(shortType);
            }

            // --- Test badge (drawn after name + tag so it sits on top) ---
            if (testBadgeInfo) {
                const { badgeWidth, badgeX, badgeY, iconW, iconGap, badgePadX, countText } = testBadgeInfo;
                const badgeHeight = 16;

                const badgeGroup = columnGroup.append('g')
                    .attr('class', 'column-test-badge')
                    .attr('transform', `translate(${badgeX}, 0)`)
                    .style('cursor', 'default');

                badgeGroup.append('rect')
                    .attr('class', 'column-test-badge-bg')
                    .attr('rx', 4)
                    .attr('ry', 4)
                    .attr('width', badgeWidth)
                    .attr('height', badgeHeight)
                    .attr('y', badgeY - badgeHeight / 2);

                // Shield-check glyph: a guardrail metaphor, kept small and muted.
                const iconX = badgePadX;
                const iconTop = badgeY - 5;
                badgeGroup.append('path')
                    .attr('class', 'column-test-badge-icon')
                    .attr('fill', 'none')
                    .attr('stroke-width', 1.3)
                    .attr('stroke-linecap', 'round')
                    .attr('stroke-linejoin', 'round')
                    .attr('transform', `translate(${iconX}, ${iconTop})`)
.attr('d', 'M4.5 0 L9 1.6 V4.6 C9 7.4 7 9.3 4.5 10 C2 9.3 0 7.4 0 4.6 V1.6 Z M2.4 4.8 L4 6.4 L6.8 3.2');

                badgeGroup.append('text')
                    .attr('class', 'column-test-badge-count')
                    .attr('x', badgePadX + iconW + iconGap)
                    .attr('y', badgeY)
                    .attr('dominant-baseline', 'central')
                    .attr('dy', '0.02em')
                    .attr('font-size', '10px')
                    .attr('font-weight', '600')
                    .text(countText);

                const tipNames = col.tests.map(t => {
                    if (t.test_name === 'relationships' && t.referenced_model) {
                        const ref = t.referenced_column
                            ? `${t.referenced_model}.${t.referenced_column}`
                            : t.referenced_model;
                        return `relationships → ${ref}`;
                    }
                    return t.test_name;
                }).join(', ');
                const tipText = `Tests (${col.tests.length}): ${tipNames}`;

                badgeGroup
                    .on('mouseenter', function(event) { showTooltip(event, tipText); })
                    .on('mousemove', function(event) {
                        const tt = createTooltip();
                        const x = (event.pageX !== undefined ? event.pageX : event.clientX + window.scrollX);
                        const y = (event.pageY !== undefined ? event.pageY : event.clientY + window.scrollY);
                        tt.style('left', (x + 10) + 'px').style('top', (y - 10) + 'px');
                    })
                    .on('mouseleave', function() { hideTooltip(); });
            }

            // Calculate and store column position for edge connections
            const columnCenter = {
                x: model.x,
                y: model.y - model.height/2 + config.box.titleHeight + 28 + yPos +
                   (config.box.columnHeight - config.box.columnPadding) / 2
            };

            state.columnPositions.set(col.id, columnCenter);
        });
    });
}

function drawExposures(g, state, config, dragBehavior) {
    if (!state.exposures || state.exposures.length === 0) return;

    // Draw ALL exposures, even if they don't have positions yet
    // They'll be positioned later and hidden/shown based on visibility
    const allExposures = state.exposures.map(e => {
        const exposureData = e.exposureData || {};
        let detailRows = 0;
        if (exposureData.type) detailRows++;
        if (exposureData.url) detailRows++;
        if (exposureData.rowset && exposureData.note) detailRows++;

        if (!e.height || isNaN(e.height)) {
            e.height = config.box.titleHeight +
                       (detailRows * config.box.columnHeight) +
                       config.box.padding;
        }

        // Initialize positions if not set (will be positioned later)
        if (typeof e.x !== 'number' || isNaN(e.x)) {
            e.x = 0;
        }
        if (typeof e.y !== 'number' || isNaN(e.y)) {
            e.y = 0;
        }

        return e;
    });

    if (allExposures.length === 0) return;

    const exposureGroups = g.selectAll('.exposure')
        .data(allExposures)
        .enter()
        .append('g')
        .attr('class', d => 'exposure model-exposure' + ((d.exposureData && d.exposureData.rowset) ? ' is-rowset' : ''))
        .attr('data-name', d => d.name)
        .attr('transform', d => {
            if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
                typeof d.y !== 'number' || isNaN(d.y) ||
                typeof d.height !== 'number' || isNaN(d.height)) {
                return 'translate(0,0)';
            }
            return `translate(${d.x},${d.y - d.height/2})`;
        })
        .style('display', d => {
            // Hide exposures that aren't visible yet
            return (d && d.name && state.visibleModels.has(d.name)) ? 'block' : 'none';
        })
        .call(dragBehavior);

    // Hovering anywhere over a row-set box shows the FULL predicate (the inline "Filter" line
    // only shows the head). Handlers live on the whole group and use mouseover/mouseout (which
    // bubble from every child), with a contains() guard so moving between children doesn't
    // flicker the tooltip.
    const rowsetTip = (event, d) =>
        d && d.exposureData && d.exposureData.rowset && d.exposureData.note
            ? `Row-set dependency — used only in:\n${d.exposureData.note}`
            : null;
    // F6: a reached Metabase dashboard gets a STRUCTURED hover card, not the plain single-line
    // tooltip (which rendered the multi-line reach detail as one unreadable nowrap run-on). The
    // card mirrors the Impact panel's Metabase card: identity + tier, column/table precision,
    // the via-card(s), the affected field(s) (the column-precise chain, when present), and a
    // link. A short hide-delay + the card's own hover keeps it reachable so the link is clickable.
    const escHc = (s) => String(s == null ? '' : s).replace(/[&<>"]/g, (c) =>
        ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));
    const biHovercardHtml = (d) => {
        const ex = (d && d.exposureData) || {};
        const tier = ex.tier ? `<span class="bi-hc-tier">${escHc(ex.tier)}</span>` : '';
        const precision = ex.precision === 'table'
            ? '<span class="bi-hc-precision bi-hc-precision-table">table-level</span>'
            : (ex.precision === 'column'
                ? '<span class="bi-hc-precision bi-hc-precision-column">column-precise</span>'
                : '');
        const cards = (Array.isArray(ex.via_cards) && ex.via_cards.length)
            ? `<div class="bi-hc-cards">via card${ex.via_cards.length !== 1 ? 's' : ''} `
              + `${ex.via_cards.map((c) => '#' + escHc(c)).join(', ')}</div>`
            : '';
        let fields = '';
        if (Array.isArray(ex.via_columns) && ex.via_columns.length) {
            const seen = new Map();
            ex.via_columns.forEach((v) => {
                if (v && v.model && v.column) {
                    const k = v.model + '.' + v.column;
                    if (!seen.has(k)) seen.set(k, v.role || '');
                }
            });
            if (seen.size) {
                const chips = Array.from(seen.entries()).map(([col, role]) =>
                    `<span class="bi-hc-field"><code>${escHc(col)}</code>`
                    + `${role ? `<span class="bi-hc-role">${escHc(role)}</span>` : ''}</span>`).join('');
                fields = `<div class="bi-hc-fields"><span class="bi-hc-fields-label">Affects</span>${chips}</div>`;
            }
        }
        const link = ex.url
            ? `<a class="bi-hc-link" href="${escHc(ex.url)}" target="_blank" rel="noopener">View dashboard →</a>`
            : '';
        const logo = (typeof metabaseLogoSvg === 'function') ? metabaseLogoSvg(12) : '';
        return `<div class="bi-hc-eyebrow">${logo}<span>BI · METABASE — past the dbt edge</span></div>`
            + `<div class="bi-hc-title">${escHc((d && d.name) || 'dashboard')}${tier}</div>`
            + (precision ? `<div class="bi-hc-meta">${precision}</div>` : '')
            + cards + fields + link;
    };
    let biHideTimer = null;
    const ensureBiHovercard = () => {
        let c = document.getElementById('biHovercard');
        if (!c) {
            c = document.createElement('div');
            c.id = 'biHovercard';
            c.className = 'bi-hovercard';
            document.body.appendChild(c);
            c.addEventListener('mouseenter', () => {
                if (biHideTimer) { clearTimeout(biHideTimer); biHideTimer = null; }
            });
            c.addEventListener('mouseleave', () => hideBiHovercard());
        }
        return c;
    };
    const showBiHovercard = (event, d) => {
        const c = ensureBiHovercard();
        if (biHideTimer) { clearTimeout(biHideTimer); biHideTimer = null; }
        c.innerHTML = biHovercardHtml(d);
        c.style.left = (event.pageX + 14) + 'px';
        c.style.top = (event.pageY - 10) + 'px';
        c.classList.add('is-visible');
    };
    const hideBiHovercard = () => {
        if (biHideTimer) { clearTimeout(biHideTimer); biHideTimer = null; }
        biHideTimer = setTimeout(() => {
            const c = document.getElementById('biHovercard');
            if (c) c.classList.remove('is-visible');
        }, 160);
    };

    exposureGroups
        .on('mouseover.biexp', function (event, d) {
            if (d && d.boundary === 'metabase') { showBiHovercard(event, d); return; }
            const text = rowsetTip(event, d);
            if (text) showTooltip(event, text);
        })
        .on('mousemove.biexp', function (event, d) {
            // Metabase card holds a fixed position (set on show) so the user can travel to it;
            // only the plain rowset tooltip tracks the cursor.
            if (d && d.boundary === 'metabase') return;
            if (!rowsetTip(event, d)) return;
            const t = createTooltip();
            t.style('left', event.pageX + 12 + 'px').style('top', event.pageY - 10 + 'px');
        })
        .on('mouseout.biexp', function (event, d) {
            if (event.relatedTarget && this.contains(event.relatedTarget)) return;
            if (d && d.boundary === 'metabase') { hideBiHovercard(); return; }
            if (d && d.exposureData && d.exposureData.rowset) hideTooltip();
        });

    const backgroundGroup = exposureGroups.append('g')
        .attr('class', 'exposure-background');

    backgroundGroup.append('rect')
        .attr('class', d => 'exposure-container' + (d.boundary === 'metabase' ? ' bi-node' : ''))
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('rx', 8)
        .attr('ry', 8)
        .style('fill', 'var(--surface)')
        // a reached Metabase dashboard is NOT a dbt model — a dashed INDIGO-family
        // border (no new hue) + monitor glyph read it as "outside the dbt boundary".
        .style('stroke', d => (d.exposureData && d.exposureData.rowset)
            ? 'var(--info)'
            : (d.boundary === 'metabase' ? 'var(--primary)' : 'var(--violet)'))
        .style('stroke-width', 2)
        // Row-set dependents use the column only in a predicate (not projected): a dashed
        // blue border signals "indirect / row-set" vs a solid violet exposure box. Metabase
        // BI dashboards also dash (indigo) to signal "external artifact, past the dbt edge".
        .style('stroke-dasharray', d => (d.exposureData && d.exposureData.rowset)
            ? '5 4'
            : (d.boundary === 'metabase' ? '6 4' : 'none'));

    const foregroundGroup = exposureGroups.append('g')
        .attr('class', 'exposure-foreground');

    foregroundGroup.append('rect')
        .attr('class', 'exposure-header')
        .attr('width', config.box.width - 2)
        .attr('height', config.box.titleHeight)
        .attr('x', 1)
        .attr('y', 1)
        .attr('rx', 7)
        .style('fill', d => (d.exposureData && d.exposureData.rowset)
            ? 'color-mix(in srgb, var(--info) 14%, var(--surface))'
            : (d.boundary === 'metabase'
                ? 'color-mix(in srgb, var(--primary) 12%, var(--surface))'
                : 'color-mix(in srgb, var(--violet) 12%, var(--surface))'))
        .style('stroke', 'none');

    foregroundGroup.append('svg')
        .attr('class', 'exposure-icon')
        .attr('width', 24)
        .attr('height', 24)
        .attr('x', 12)
        .attr('y', config.box.titleHeight / 2 - 12)
        .attr('viewBox', '0 0 24 24')
        .append('path')
        // Metabase dashboards carry the real Metabase logomark (a FILL path) — monochrome, in
        // the indigo accent, so the mark itself (not a new colour) says "this is Metabase". Other
        // exposures / row-set nodes keep the stroked line glyph.
        .attr('d', d => d.boundary === 'metabase' ? METABASE_LOGO_PATH : getModelIcon('exposure'))
        .attr('fill', d => d.boundary === 'metabase' ? 'var(--primary)' : 'none')
        .attr('stroke', d => d.boundary === 'metabase'
            ? 'none'
            : ((d.exposureData && d.exposureData.rowset) ? 'var(--info)' : 'var(--violet)'))
        .attr('stroke-width', '2')
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');

    foregroundGroup.append('text')
        .attr('class', 'exposure-eyebrow')
        .attr('x', 44)
        .attr('y', config.box.titleHeight / 2 - 6)
        .style('fill', d => (d.exposureData && d.exposureData.rowset)
            ? 'var(--info)'
            : (d.boundary === 'metabase'
                ? 'color-mix(in srgb, var(--primary) 72%, var(--text-muted))'
                : 'color-mix(in srgb, var(--violet) 60%, var(--text-muted))'))
        .style('font-family', 'var(--font-mono)')
        .style('font-size', '8.5px')
        .style('font-weight', '600')
        .style('letter-spacing', '0.12em')
        .style('pointer-events', 'none')
        .text(d => (d.exposureData && d.exposureData.rowset)
            ? 'ROW-SET'
            : (d.boundary === 'metabase' ? 'BI · METABASE' : 'EXPOSURE'));

    const exposureTitleText = foregroundGroup.append('text')
        .attr('class', 'exposure-title')
        .attr('x', 44)
        .attr('y', config.box.titleHeight / 2 + 10)
        .style('cursor', d => (d.exposureData && d.exposureData.rowset) ? 'pointer' : null)
        .text(d => d.name)
        // Row-set title click opens the readable predicate card. Attached to the title text
        // (not the draggable group) so d3-drag doesn't swallow the click — same pattern as
        // the model-details card.
        .on('click', function (event, d) {
            if (!(d && d.exposureData && d.exposureData.rowset && d.exposureData.note)) return;
            event.stopPropagation();
            hideTooltip();
            showRowsetCard(d.name, d.exposureData.note);
        })
        .each(function(d) {
            const maxWidth = config.box.width - 56;
            const self = d3.select(this);
            let textLength = self.node().getComputedTextLength();
            let text = self.text();

            while (textLength > maxWidth && text.length > 0) {
                text = text.slice(0, -1);
                self.text(text + '...');
                textLength = self.node().getComputedTextLength();
            }

            if (text + '...' !== d.name) {
                self.attr('data-original-text', d.name);
            }
        });

    // Add tooltip for truncated exposure titles
    exposureTitleText.each(function(d) {
        const self = d3.select(this);
        const originalText = self.attr('data-original-text');
        if (originalText && originalText !== d.name) {
            self
                .style('pointer-events', 'all')
                .style('cursor', 'help')
                .on('mouseenter', function(event) {
                    showTooltip(event, originalText);
                })
                .on('mouseleave', function() {
                    hideTooltip();
                })
                .on('mousemove', function(event) {
                    const tooltip = createTooltip();
                    let x, y;
                    if (event.pageX !== undefined && event.pageY !== undefined) {
                        x = event.pageX;
                        y = event.pageY;
                    } else if (event.clientX !== undefined && event.clientY !== undefined) {
                        x = event.clientX + window.scrollX;
                        y = event.clientY + window.scrollY;
                    } else {
                        const sourceEvent = event.sourceEvent || event;
                        x = (sourceEvent.pageX || sourceEvent.clientX || 0) + (window.scrollX || 0);
                        y = (sourceEvent.pageY || sourceEvent.clientY || 0) + (window.scrollY || 0);
                    }
                    tooltip
                        .style('left', (x + 10) + 'px')
                        .style('top', (y - 10) + 'px');
                });
        }
    });

    const detailsContainer = foregroundGroup.append('g')
        .attr('class', 'exposure-details')
        .attr('transform', `translate(0, ${config.box.titleHeight})`);

    exposureGroups.each(function(d) {
        const exposureData = d.exposureData || {};
        const type = exposureData.type || 'unknown';
        const url = exposureData.url || '';
        const detailsGroup = d3.select(this).select('.exposure-details');
        let yOffset = 0;

        if (type) {
            const typeRow = detailsGroup.append('g')
                .attr('class', 'exposure-detail-row')
                .attr('transform', `translate(${config.box.padding}, ${yOffset})`);

            typeRow.append('text')
                .attr('class', 'exposure-detail-label')
                .attr('x', 12)
                .attr('y', config.box.columnHeight / 2)
                .attr('dominant-baseline', 'middle')
                .attr('fill', '#334155')
                .attr('font-size', '12px')
                .text('Type');

            const typeTag = type.toLowerCase();
            const tempText = typeRow.append('text')
                .attr('font-size', '11px')
                .text(typeTag)
                .style('visibility', 'hidden');

            const textWidth = tempText.node().getComputedTextLength();
            tempText.remove();

            const tagWidth = textWidth + 16;
            const xPosition = config.box.width - tagWidth - (config.box.padding * 2) - 8;
            const yPosition = config.box.columnHeight / 2;

            const tagGroup = typeRow.append('g')
                .attr('class', 'exposure-type-tag')
                .attr('transform', `translate(${xPosition}, 0)`)
                .style('pointer-events', 'none');

            const exposureTypeColor = exposureData.rowset ? 'var(--info)' : 'var(--violet)';
            tagGroup.append('rect')
                .attr('rx', 4)
                .attr('ry', 4)
                .attr('width', tagWidth)
                .attr('height', 18)
                .attr('y', yPosition - 9)
                .style('fill', exposureTypeColor)
                .style('stroke', 'none');

            // Tag text
            tagGroup.append('text')
                .attr('x', tagWidth / 2)
                .attr('y', yPosition)
                .attr('text-anchor', 'middle')
                .attr('dominant-baseline', 'central')
                .attr('dy', '-0.1em')
                .style('fill', 'white')
                .style('font-size', '11px')
                .style('font-weight', '500')
                .text(typeTag);

            yOffset += config.box.columnHeight;
        }

        if (url) {
            const urlRow = detailsGroup.append('g')
                .attr('class', 'exposure-detail-row')
                .attr('transform', `translate(${config.box.padding}, ${yOffset})`);

            urlRow.append('text')
                .attr('class', 'exposure-detail-label')
                .attr('x', 12)
                .attr('y', config.box.columnHeight / 2)
                .attr('dominant-baseline', 'middle')
                .attr('fill', '#334155')
                .attr('font-size', '12px')
                .text('URL');

            // Calculate label width first
            const labelText = urlRow.select('.exposure-detail-label');
            const labelWidth = labelText.empty() ? 40 : labelText.node().getComputedTextLength() || 40;
            const urlX = 12 + labelWidth + 8;
            const urlWidth = config.box.width - (config.box.padding * 2) - urlX;

            const urlText = urlRow.append('text')
                .attr('class', 'exposure-detail-value exposure-url')
                .attr('x', urlX)
                .attr('y', config.box.columnHeight / 2)
                .attr('dominant-baseline', 'middle')
                .attr('fill', '#9333ea')
                .attr('font-size', '12px')
                .attr('text-decoration', 'underline')
                .style('cursor', 'pointer')
                .text(url)
                .each(function() {
                    const self = d3.select(this);
                    const maxWidth = urlWidth;
                    let textLength = self.node().getComputedTextLength();
                    let text = self.text();

                    while (textLength > maxWidth && text.length > 0) {
                        text = text.slice(0, -1);
                        self.text(text + '...');
                        textLength = self.node().getComputedTextLength();
                    }
                });

            const clickableRect = urlRow.append('rect')
                .attr('class', 'exposure-url-clickable')
                .attr('x', urlX)
                .attr('y', 0)
                .attr('width', urlWidth)
                .attr('height', config.box.columnHeight)
                .attr('fill', 'transparent')
                .style('cursor', 'pointer')
                .on('click', function(event) {
                    event.stopPropagation();
                    window.open(url, '_blank', 'noopener,noreferrer');
                })
                .on('mouseenter', function() {
                    urlText.attr('fill', '#7c3aed');
                })
                .on('mouseleave', function() {
                    urlText.attr('fill', '#9333ea');
                });

            clickableRect.raise();

            yOffset += config.box.columnHeight;
        }

        // Row-set nodes: show the predicate the column appears in (WHERE/JOIN/QUALIFY) as the
        // "why", truncated with a hover tooltip for the full expression.
        if (exposureData.rowset && exposureData.note) {
            const noteRow = detailsGroup.append('g')
                .attr('class', 'exposure-detail-row rowset-note-row')
                .attr('transform', `translate(${config.box.padding}, ${yOffset})`);

            noteRow.append('text')
                .attr('class', 'exposure-detail-label')
                .attr('x', 12)
                .attr('y', config.box.columnHeight / 2)
                .attr('dominant-baseline', 'middle')
                .attr('fill', '#334155')
                .attr('font-size', '12px')
                .text('Filter');

            const noteX = 60;
            const noteMaxWidth = config.box.width - (config.box.padding * 2) - noteX;
            const fullNote = exposureData.note;
            const noteText = noteRow.append('text')
                .attr('class', 'exposure-detail-value')
                .attr('x', noteX)
                .attr('y', config.box.columnHeight / 2)
                .attr('dominant-baseline', 'middle')
                .attr('fill', 'var(--text)')
                .attr('font-family', 'var(--font-mono, monospace)')
                .attr('font-size', '10.5px')
                .text(fullNote)
                .each(function() {
                    const self = d3.select(this);
                    let textLength = self.node().getComputedTextLength();
                    let text = self.text();
                    while (textLength > noteMaxWidth && text.length > 0) {
                        text = text.slice(0, -1);
                        self.text(text + '…');
                        textLength = self.node().getComputedTextLength();
                    }
                });

            // Hover target spans the whole Filter row (label + value) so the full predicate
            // tooltip is easy to trigger, not just over the truncated text.
            noteRow.append('rect')
                .attr('x', 0)
                .attr('y', 0)
                .attr('width', config.box.width - (config.box.padding * 2))
                .attr('height', config.box.columnHeight)
                .attr('fill', 'transparent')
                .style('cursor', 'help')
                .on('mouseenter', function(event) { showTooltip(event, fullNote); })
                .on('mousemove', function(event) {
                    const t = createTooltip();
                    const x = (event.pageX !== undefined) ? event.pageX : (event.clientX + window.scrollX);
                    const y = (event.pageY !== undefined) ? event.pageY : (event.clientY + window.scrollY);
                    t.style('left', (x + 10) + 'px').style('top', (y - 10) + 'px');
                })
                .on('mouseleave', function() { hideTooltip(); });

            yOffset += config.box.columnHeight;
        }
    });

    // Whole-box click target for row-set nodes (appended last → on top): clicking anywhere on
    // the dashed box opens the readable predicate card. A specific element (not the draggable
    // group), so d3-drag doesn't swallow the click.
    exposureGroups
        .filter(d => d && d.exposureData && d.exposureData.rowset && d.exposureData.note)
        .append('rect')
        .attr('class', 'rowset-click-target')
        .attr('x', 0)
        .attr('y', 0)
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('fill', 'transparent')
        .style('cursor', 'pointer')
        .on('click', function (event, d) {
            event.stopPropagation();
            hideTooltip();
            showRowsetCard(d.name, d.exposureData.note);
        });

    state.exposures.forEach(exposure => {
        const exposureCenter = {
            x: exposure.x,
            y: exposure.y
        };
        state.exposurePositions.set(exposure.name, exposureCenter);
    });
}

// the dbt → BI boundary. A thin vertical dashed rule read poorly, so the
// reached Metabase dashboards now sit inside a soft indigo-family LANE with a horizontal header
// — "past here, we've left dbt" reads as a distinct ZONE, not a hairline. A no-op when no
// dashboard was reached (no --metabase context) → the graph renders exactly as today.
function drawBoundaryBand(g, state, config) {
    const biNodes = (state.exposures || []).filter(
        e => e && e.boundary === 'metabase' && typeof e.x === 'number' && !isNaN(e.x)
    );
    if (biNodes.length === 0) return;

    // Horizontal extent: enclose every reached dashboard (exposure d.x is the box's left edge,
    // width is the shared box width), with a little breathing room on each side.
    const boxW = config.box.width;
    const padX = 22;
    const laneLeft = Math.min(...biNodes.map(e => e.x)) - padX;
    const laneRight = Math.max(...biNodes.map(e => e.x + boxW)) + padX;

    // Vertical extent: cover the full laid-out content so the lane reads as a column, with room
    // above the top node for the header so it never overlaps a dashboard.
    const ys = [];
    (state.models || []).forEach(m => {
        if (typeof m.y === 'number' && !isNaN(m.y)) {
            ys.push(m.y - (m.height || 0) / 2, m.y + (m.height || 0) / 2);
        }
    });
    (state.exposures || []).forEach(e => {
        if (typeof e.y === 'number' && !isNaN(e.y)) {
            ys.push(e.y - (e.height || 0) / 2, e.y + (e.height || 0) / 2);
        }
    });
    const padY = 50;
    const headerH = 30;
    const laneTop = (ys.length ? Math.min(...ys) : 0) - padY - headerH;
    const laneBottom = (ys.length ? Math.max(...ys) : config.height) + padY;

    let band = g.select('.bi-boundary-band');
    if (band.empty()) {
        band = g.append('g').attr('class', 'bi-boundary-band');
    }
    band.selectAll('*').remove();
    band.style('pointer-events', 'none');

    // A horizontal header at the lane's top-left — legible at a glance, unlike the old rotated
    // hairline label. The lane encloses the whole consumption layer past the models (dbt
    // exposures AND Metabase dashboards), so the header stays generic; the Metabase-specific
    // "BI · METABASE" identity lives on the individual dashboard node, not the zone.
    // Drawn FIRST so we can measure it and size the lane to enclose it (F7).
    const headerPad = 15;
    const label = band.append('text')
        .attr('class', 'bi-lane-label')
        .attr('x', laneLeft + headerPad)
        .attr('y', laneTop + headerPad)
        .attr('dominant-baseline', 'hanging')
        .text('PAST THE dbt EDGE · BI / CONSUMPTION LAYER');

    // F7: the header is a fixed-length string but the lane width came only from the dashboards'
    // geometry — on a narrow lane the text overran the box. Widen the lane so it always encloses
    // its own header (never shrinks below the geometry-derived width).
    let labelWidth = 0;
    try {
        labelWidth = label.node().getComputedTextLength();
    } catch (e) {
        labelWidth = 0;
    }
    const laneWidth = Math.max(laneRight - laneLeft, labelWidth + headerPad * 2);

    // The lane surface — a tinted rounded rect (dashed indigo border, muted fill; no new hue).
    // Inserted BEFORE the label so it paints underneath it.
    band.insert('rect', '.bi-lane-label')
        .attr('class', 'bi-lane')
        .attr('x', laneLeft)
        .attr('y', laneTop)
        .attr('width', Math.max(0, laneWidth))
        .attr('height', Math.max(0, laneBottom - laneTop))
        .attr('rx', 14);

    // Keep the lane behind the nodes.
    band.lower();
}

// Draw the synthetic "+N more" progressive-disclosure nodes. These are neutral,
// clearly-not-a-model affordances: a dashed muted box carrying an honest count
// and a "click to expand" hint. Styling lives in graph.css (.more-node) so both
// light and dark themes follow the CSS variables.
function drawMoreNodes(g, state, config) {
    if (!state.moreNodes || state.moreNodes.length === 0) return;

    let layer = g.select('.more-nodes-group');
    if (layer.empty()) {
        layer = g.append('g').attr('class', 'more-nodes-group');
    }

    const groups = layer.selectAll('.more-node')
        .data(state.moreNodes, d => d.id)
        .join(
            enter => {
                const grp = enter.append('g')
                    .attr('class', 'more-node')
                    .style('cursor', 'pointer')
                    .on('click', function(event, d) {
                        event.stopPropagation();
                        if (typeof expandMoreNode === 'function') {
                            expandMoreNode(d, state, config);
                        }
                    });

                grp.append('rect')
                    .attr('class', 'more-node-box')
                    .attr('width', config.box.width)
                    .attr('height', d => d.height)
                    .attr('rx', 8)
                    .attr('ry', 8);

                grp.append('text')
                    .attr('class', 'more-node-count')
                    .attr('x', config.box.width / 2)
                    .attr('y', d => d.height / 2 - 5)
                    .attr('text-anchor', 'middle')
                    .attr('dominant-baseline', 'middle');

                grp.append('text')
                    .attr('class', 'more-node-hint')
                    .attr('x', config.box.width / 2)
                    .attr('y', d => d.height / 2 + 13)
                    .attr('text-anchor', 'middle')
                    .attr('dominant-baseline', 'middle')
                    .text('click to expand');

                return grp;
            },
            update => update,
            exit => exit.remove()
        );

    // Honest count + label (singular/plural), refreshed on every draw.
    groups.select('.more-node-count').text(d => {
        const n = (d.hidden && d.hidden.length) || 0;
        const noun = d.kind === 'exposure'
            ? (n === 1 ? 'exposure' : 'exposures')
            : (n === 1 ? 'downstream model' : 'downstream models');
        return `+${n} more ${noun}`;
    });

    // Native <title> for accessibility / hover.
    groups.each(function(d) {
        const sel = d3.select(this);
        sel.select('title').remove();
        const n = (d.hidden && d.hidden.length) || 0;
        const noun = d.kind === 'exposure' ? 'exposures' : 'downstream models';
        sel.append('title').text(`${n} more ${noun} hidden — click to expand`);
    });

    groups.attr('transform', d => {
        if (!d || typeof d.x !== 'number' || isNaN(d.x) ||
            typeof d.y !== 'number' || isNaN(d.y) ||
            typeof d.height !== 'number' || isNaN(d.height)) {
            return 'translate(0,0)';
        }
        return `translate(${d.x},${d.y - d.height / 2})`;
    });
}

function drawEdges(g, data, state, config) {
    state.models.forEach(model => {
        state.modelEdges.set(model.name, []);
    });

    state.exposures.forEach(exposure => {
        state.modelEdges.set(exposure.name, []);
    });

    let edgesGroup = g.select('.edges-group');
    if (edgesGroup.empty()) {
        edgesGroup = g.append('g').attr('class', 'edges-group');
    }

    // when a breaking path exists, the off-path context recedes so the single amber
    // blast thread reads clearly. No breaking context => hasBreakingPath is false => today's look.
    const hasBreaking = state.hasBreakingPath === true;

    // Draw all lineage edges, but hide those between non-visible models
    const lineageEdges = edgesGroup.selectAll('.edge.lineage')
        .data(data.edges.filter(e => e.type === 'lineage'))
        .join('path')
        // tag the blast-path edges (edges leaving a breaking column) so interactions
        // can keep the amber thread lifted through select/reset cycles.
        .attr('class', d => 'edge lineage' + (d.breaking ? ' breaking' : ''))
        .attr('marker-end', 'url(#arrowhead)')
        .attr('data-source', d => d.source)
        .attr('data-target', d => d.target)
        // Breaking edges are the ONE warm accent (amber); everything else stays neutral slate.
        .style('stroke', d => d.breaking ? 'var(--accent)' : config.colors.edge)
        .style('stroke-width', d => d.breaking ? 1.75 : 1.5)
        .style('stroke-opacity', d => d.breaking ? 1 : (hasBreaking ? 0.4 : 1))
        .style('fill', 'none')
        .style('display', d => {
            const sourceNode = state.nodeIndex.get(d.source);
            const targetNode = state.nodeIndex.get(d.target);
            if (!sourceNode || !targetNode) return 'none';
            const sourceVisible = state.visibleModels.has(sourceNode.model);
            const targetVisible = state.visibleModels.has(targetNode.model);
            return (sourceVisible && targetVisible) ? 'block' : 'none';
        })
        .attr('d', d => createEdgePath(d, state, config))
        .each(function(d) {
            indexEdgeForDragging(d, this, state);
        });

    const exposureEdges = edgesGroup.selectAll('.edge.exposure')
        .data(data.edges.filter(e => e.type === 'exposure'))
        .join('path')
        .attr('class', 'edge exposure')
        .attr('marker-end', 'url(#arrowhead)')
        .attr('data-source', d => d.source)
        .attr('data-target', d => d.target)
        .style('stroke', 'var(--violet)')
        .style('stroke-width', 2)
        .style('stroke-dasharray', '8,4')
        .style('fill', 'none')
        .style('display', d => {
            const sourceNode = state.nodeIndex.get(d.source);
            const targetNode = state.nodeIndex.get(d.target);
            if (!sourceNode || !targetNode) return 'none';
            // For exposure edges, source is a column (model), target is an exposure
            // Only show if BOTH source model AND target exposure are visible
            const sourceModelVisible = state.visibleModels.has(sourceNode.model);
            const targetExposureVisible = state.visibleModels.has(targetNode.model);
            return (sourceModelVisible && targetExposureVisible) ? 'block' : 'none';
        })
        .attr('d', d => createExposureEdgePath(d, state, config))
        .each(function(d) {
            indexExposureEdgeForDragging(d, this, state);
        });

    edgesGroup.lower();

    return lineageEdges;
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
            target: edge.target,
            isExposure: false
        };

        state.modelEdges.get(sourceModel).push(edgeInfo);

        if (sourceModel !== targetModel) {
            state.modelEdges.get(targetModel).push(edgeInfo);
        }
    }
}

function indexExposureEdgeForDragging(edge, element, state) {
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
            target: edge.target,
            isExposure: true
        };

        state.modelEdges.get(sourceModel).push(edgeInfo);
        state.modelEdges.get(targetModel).push(edgeInfo);
    }
}

function updateEdgesForCollapse(model, state, config) {
    // Ensure the edges group is lowered before updating paths
    const edgesGroup = d3.select('.edges-group');
    if (!edgesGroup.empty()) {
        edgesGroup.lower();
    }

    // For each edge connected to this model, redraw its path
    if (state.modelEdges.has(model.name)) {
        state.modelEdges.get(model.name).forEach(edgeInfo => {
            if (edgeInfo.element && edgeInfo.element.parentNode) {
                const edgeElement = d3.select(edgeInfo.element);
                // Preserve marker-end attribute
                const markerEnd = edgeElement.attr('marker-end') || 'url(#arrowhead)';

                let path;
                if (edgeInfo.isExposure) {
                    path = createExposureEdgePath({
                        source: edgeInfo.source,
                        target: edgeInfo.target
                    }, state, config);
                } else {
                    path = createEdgePath({
                        source: edgeInfo.source,
                        target: edgeInfo.target
                    }, state, config);
                }

                if (path && path !== '' && !path.includes('NaN')) {
                    // Use transition to smoothly update edge path
                    edgeElement
                        .attr('marker-end', markerEnd)
                        .transition()
                        .duration(300)
                    .attr('d', path);
                }
            }
        });

        // Also update edges connected to other models that might be affected
        // (e.g., if this model is source/target of other edges)
        d3.selectAll('.edge.lineage').each(function(edgeData) {
            if (!edgeData || !edgeData.source || !edgeData.target) return;

            const sourceNode = state.nodeIndex.get(edgeData.source);
            const targetNode = state.nodeIndex.get(edgeData.target);

            if (sourceNode && targetNode) {
                // If this edge connects to the collapsed model, update it
                if (sourceNode.model === model.name || targetNode.model === model.name) {
                    const edgeElement = d3.select(this);
                    const markerEnd = edgeElement.attr('marker-end') || 'url(#arrowhead)';
                    const path = createEdgePath(edgeData, state, config);

                    if (path && path !== '' && !path.includes('NaN')) {
                        edgeElement
                            .attr('marker-end', markerEnd)
                            .transition()
                            .duration(300)
                            .attr('d', path);
                    }
                }
            }
        });

        if (!edgesGroup.empty()) {
            edgesGroup.lower();
        }
    }
}
