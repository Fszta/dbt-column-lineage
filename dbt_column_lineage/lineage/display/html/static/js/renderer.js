/**
 * Rendering functions for graph visualization
 */

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
    if (state.modelEdges.has(model.name)) {
        state.modelEdges.get(model.name).forEach(edgeInfo => {
            if (edgeInfo.element && edgeInfo.element.parentNode) { 
                const edgeElement = d3.select(edgeInfo.element);
                
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
                
                edgeElement.attr('d', newPath);
            } else {
                console.warn("Edge element reference broken or detached for edge:", edgeInfo);
            }
        });
        
        // Ensure edges remain visually behind models after update
        const edgesGroup = d3.select('.edges-group');
        if (!edgesGroup.empty()) {
            edgesGroup.lower();
        }
    }
}

function drawModels(g, state, config, dragBehavior) {
    state.modelElements = new Map();
    
    const modelGroups = g.selectAll('.model')
        .data(state.models)
        .enter()
        .append('g')
        .attr('class', d => {
            const modelType = (d.type || 'model');
            return `model model-${modelType}`;
        })
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
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
        .attr('rx', 8)
        .attr('ry', 8)
        .style('fill', 'white')
        .style('stroke', '#e2e8f0')
        .style('stroke-width', 1);
    
    const foregroundGroup = modelGroups.append('g')
        .attr('class', 'model-foreground');
    
    // Model header background rect
    foregroundGroup.append('rect')
        .attr('class', 'model-header')
        .attr('width', config.box.width - 2) 
        .attr('height', config.box.titleHeight)
        .attr('x', 1)
        .attr('y', 1)
        .attr('rx', 7)
        .style('fill', d => {
            const modelType = d.type || 'model';
            if (modelType === 'source') return '#f0f4f8';
            if (modelType === 'seed') return '#f0f8f4';
            return '#f8fafc';
        })
        .style('stroke', 'none');
    
    // Model icon
    foregroundGroup.append('svg')
        .attr('class', 'model-icon')
        .attr('width', 24)
        .attr('height', 24)
        .attr('x', 12)
        .attr('y', config.box.titleHeight / 2 - 12)
        .attr('viewBox', '0 0 24 24')
        .append('path')
        .attr('d', function(d) {
            const modelType = d.type || 'model';
            return getModelIcon(modelType);
        })
        .attr('fill', 'none')
        .attr('stroke', d => {
            const modelType = d.type || 'model';
            if (modelType === 'source') return '#4a6fa5';
            if (modelType === 'seed') return '#4caf50';
            return '#4a6fa5';
        })
        .attr('stroke-width', '2')
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');
    
    // Model title text
    foregroundGroup.append('text')
        .attr('class', 'model-title')
        .attr('x', 44)
        .attr('y', config.box.titleHeight / 2 + 5)
        .text(d => d.name)
        .each(function(d) {
            // Truncate text if too long
            const maxWidth = config.box.width - 56;
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
    
    const columnsHeader = foregroundGroup.append('g')
        .attr('class', 'columns-header')
        .attr('transform', `translate(0, ${config.box.titleHeight})`)
        .style('cursor', 'pointer');
    
    columnsHeader.append('rect')
        .attr('width', config.box.width - 2)
        .attr('height', 28)
        .attr('x', 1)
        .attr('fill', d => d.type === 'source' ? '#e9f0fa' : '#f1f5f9')
        .style('stroke', 'none');
    
    columnsHeader.append('text')
        .attr('class', 'columns-label')
        .attr('x', 15)
        .attr('y', 18)
        .attr('dominant-baseline', 'middle')
        .attr('fill', '#64748b')
        .attr('font-size', '13px')
        .text(d => `Columns (${d.columns.length})`);
    
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
            
            modelRect.attr('height', combinedHeaderHeight);
            headerRect.attr('rx', 0).attr('ry', 0);
            d.columnsCollapsed = true;
            
            modelElement.classed('collapsed-model', true);
        } else {
            container.attr('data-expanded', 'true')
                .style('display', 'block');
            
            // Reset icon rotation using calculated center
            const iconSVG = iconPath.node().ownerSVGElement;
            const iconX = parseFloat(iconSVG.getAttribute('x')) + parseFloat(iconSVG.getAttribute('width')) / 2;
            const iconY = parseFloat(iconSVG.getAttribute('y')) + parseFloat(iconSVG.getAttribute('height')) / 2;
            iconPath.attr('transform', `rotate(0, ${iconX}, ${iconY})`);
                   
            d3.select(this).attr('data-collapsed', 'false');
            
            modelRect.attr('height', d.height);
            headerRect.attr('rx', 0).attr('ry', 0);
            d.columnsCollapsed = false;
            
            modelElement.classed('collapsed-model', false);
        }
        
        // Update all edges connected to this model after a short delay
        setTimeout(() => {
            updateEdgesForCollapse(d, state, config);
        }, 10); // Delay helps ensure transitions start smoothly
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
            columnGroup.append('rect')
                .attr('class', 'column-background')
                .attr('x', 0)
                .attr('y', 0)
                .attr('width', config.box.width - (config.box.padding * 2))
                .attr('height', config.box.columnHeight - config.box.columnPadding)
                .attr('fill', 'transparent')
                .style('cursor', 'pointer')
                .on('click', function() {
                    onColumnClick(col.id, model.name);
                })
                .on('mouseenter', function() {
                    d3.select(this).attr('fill', 'rgba(0,0,0,0.03)');  // Subtle hover effect
                })
                .on('mouseleave', function() {
                    d3.select(this).attr('fill', 'transparent');
                });

            // --- Left Color Indicator ---
            columnGroup.append('rect')
                .attr('class', 'column-indicator')
                .attr('x', 2)
                .attr('y', 2)
                .attr('width', 3)
                .attr('height', config.box.columnHeight - config.box.columnPadding - 4)
                .attr('rx', 1.5)
                .attr('fill', col.isKey ? '#3b82f6' : '#94a3b8')
                .attr('opacity', 0.7);

            // --- Column Name Text ---
            columnGroup.append('text')
                .attr('class', 'column-name')
                .attr('x', 12)
                .attr('y', (config.box.columnHeight - config.box.columnPadding) / 2)
                .attr('dominant-baseline', 'middle')
                .attr('font-size', '12px')
                .attr('fill', '#334155')
                .text(function() {
                    const maxLength = 18;
                    return col.name.length > maxLength ? col.name.substring(0, maxLength) + '...' : col.name;
                })
                .attr('data-original-text', col.name);

            // --- Data Type Tag (if exists) ---
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
    
    const validExposures = state.exposures.filter(e => {
        if (typeof e.x !== 'number' || isNaN(e.x) || 
            typeof e.y !== 'number' || isNaN(e.y)) {
            return false;
        }
        
        const exposureData = e.exposureData || {};
        let detailRows = 0;
        if (exposureData.type) detailRows++;
        if (exposureData.url) detailRows++;
        
        e.height = config.box.titleHeight + 
                   (detailRows * config.box.columnHeight) +
                   config.box.padding;
        
        return typeof e.height === 'number' && !isNaN(e.height);
    });
    
    if (validExposures.length === 0) return;
    
    const exposureGroups = g.selectAll('.exposure')
        .data(validExposures)
        .enter()
        .append('g')
        .attr('class', 'exposure model-exposure')
        .attr('transform', d => `translate(${d.x},${d.y - d.height/2})`)
        .call(dragBehavior);
    
    const backgroundGroup = exposureGroups.append('g')
        .attr('class', 'exposure-background');
    
    backgroundGroup.append('rect')
        .attr('class', 'exposure-container')
        .attr('width', config.box.width)
        .attr('height', d => d.height)
        .attr('rx', 8)
        .attr('ry', 8)
        .style('fill', 'white')
        .style('stroke', '#a78bfa')
        .style('stroke-width', 2);
    
    const foregroundGroup = exposureGroups.append('g')
        .attr('class', 'exposure-foreground');
    
    foregroundGroup.append('rect')
        .attr('class', 'exposure-header')
        .attr('width', config.box.width - 2)
        .attr('height', config.box.titleHeight)
        .attr('x', 1)
        .attr('y', 1)
        .attr('rx', 7)
        .style('fill', '#f3e8ff')
        .style('stroke', 'none');
    
    foregroundGroup.append('svg')
        .attr('class', 'exposure-icon')
        .attr('width', 24)
        .attr('height', 24)
        .attr('x', 12)
        .attr('y', config.box.titleHeight / 2 - 12)
        .attr('viewBox', '0 0 24 24')
        .append('path')
        .attr('d', getModelIcon('exposure'))
        .attr('fill', 'none')
        .attr('stroke', '#9333ea')
        .attr('stroke-width', '2')
        .attr('stroke-linecap', 'round')
        .attr('stroke-linejoin', 'round');
    
    foregroundGroup.append('text')
        .attr('class', 'exposure-title')
        .attr('x', 44)
        .attr('y', config.box.titleHeight / 2 + 5)
        .text(d => d.name)
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
            
            const exposureTypeColor = '#ec4899';
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
    });
    
    state.exposures.forEach(exposure => {
        const exposureCenter = {
            x: exposure.x,
            y: exposure.y
        };
        state.exposurePositions.set(exposure.name, exposureCenter);
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
    
    const lineageEdges = edgesGroup.selectAll('.edge.lineage')
        .data(data.edges.filter(e => e.type === 'lineage'))
        .join('path')
        .attr('class', 'edge lineage')
        .attr('marker-end', 'url(#arrowhead)')
        .attr('data-source', d => d.source)
        .attr('data-target', d => d.target)
        .style('stroke', config.colors.edge)
        .style('stroke-width', 1.5)
        .style('fill', 'none')
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
        .style('stroke', '#9333ea')
        .style('stroke-width', 2)
        .style('stroke-dasharray', '5,3')
        .style('fill', 'none')
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
                
                d3.select(edgeInfo.element)
                    .attr('d', path);
            }
        });
        
        if (!edgesGroup.empty()) {
            edgesGroup.lower();
        }
    }
}