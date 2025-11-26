const ImpactModule = (function() {
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    function displayImpactAnalysis(data, modelName, columnName) {
        const impactContent = document.getElementById('impactAnalysisContent');
        const summary = data.summary || {};
        const affectedModels = data.affected_models || [];
        const affectedColumns = data.affected_columns || [];
        const affectedExposures = data.affected_exposures || [];

        const criticalColumns = affectedColumns.filter(col => col.severity === 'critical');
        const lowImpactColumns = affectedColumns.filter(col => col.severity === 'low_impact');

        let html = `
            <div class="impact-intro">
                <div class="impact-intro-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M6.34 17.66l-1.41 1.41M19.07 4.93l-1.41 1.41"></path>
                        <circle cx="12" cy="12" r="5"></circle>
                    </svg>
                </div>
                <div class="impact-intro-text">
                    <strong>Understanding the impact:</strong> This analysis shows what will be affected if you modify the logic of <code>${modelName}.${columnName}</code>.
                    Review the transformed columns below as they may need updates when the source column changes.
                </div>
            </div>
            <div class="impact-hero">
                <div class="impact-hero-header">
                    <div class="impact-hero-content">
                        <h2 class="impact-hero-title">Impact Summary</h2>
                        <p class="impact-hero-subtitle">Column: <code>${modelName}.${columnName}</code></p>
                    </div>
                </div>
                <div class="impact-hero-metrics">
                    <div class="hero-metric ${criticalColumns.length > 0 ? 'hero-metric-critical' : ''}">
                        <div class="hero-metric-icon">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
                                <line x1="12" y1="9" x2="12" y2="13"></line>
                                <line x1="12" y1="17" x2="12.01" y2="17"></line>
                            </svg>
                        </div>
                        <div class="hero-metric-content">
                            <div class="hero-metric-value">${criticalColumns.length}</div>
                            <div class="hero-metric-label">Requires Review</div>
                            <div class="hero-metric-desc">Transformations may break</div>
                        </div>
                    </div>
                    <div class="hero-metric hero-metric-pass-through">
                        <div class="hero-metric-icon">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <polyline points="20 6 9 17 4 12"></polyline>
                            </svg>
                        </div>
                        <div class="hero-metric-content">
                            <div class="hero-metric-value">${lowImpactColumns.length}</div>
                            <div class="hero-metric-label">Pass-through</div>
                            <div class="hero-metric-desc">Direct references</div>
                        </div>
                    </div>
                    <div class="hero-metric hero-metric-models">
                        <div class="hero-metric-icon">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <line x1="18" y1="20" x2="18" y2="10"></line>
                                <line x1="12" y1="20" x2="12" y2="4"></line>
                                <line x1="6" y1="20" x2="6" y2="14"></line>
                            </svg>
                        </div>
                        <div class="hero-metric-content">
                            <div class="hero-metric-value">${summary.affected_models || 0}</div>
                            <div class="hero-metric-label">Models Affected</div>
                        </div>
                    </div>
                    <div class="hero-metric hero-metric-exposures">
                        <div class="hero-metric-icon">
                            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline>
                                <polyline points="17 6 23 6 23 12"></polyline>
                            </svg>
                        </div>
                        <div class="hero-metric-content">
                            <div class="hero-metric-value">${summary.affected_exposures || 0}</div>
                            <div class="hero-metric-label">Exposures</div>
                        </div>
                    </div>
                </div>
            </div>
        `;

        if (criticalColumns.length > 0) {
            html += `
                <div class="impact-section critical-section">
                    <div class="section-header critical-header">
                        <div class="section-header-left">
                            <span class="section-icon-badge critical-badge">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
                                    <line x1="12" y1="9" x2="12" y2="13"></line>
                                    <line x1="12" y1="17" x2="12.01" y2="17"></line>
                                </svg>
                            </span>
                            <div>
                                <h3 class="section-title">Requires Review</h3>
                                <p class="section-description">${criticalColumns.length} column${criticalColumns.length !== 1 ? 's' : ''} with transformations that may break</p>
                            </div>
                        </div>
                    </div>
                    <div class="model-groups-list">
            `;

            // Group columns by model
            const columnsByModel = {};
            criticalColumns.forEach(col => {
                const colModelName = col.model || 'unknown';
                if (!columnsByModel[colModelName]) {
                    columnsByModel[colModelName] = [];
                }
                columnsByModel[colModelName].push(col);
            });

            // Sort models alphabetically for consistent display
            const sortedModelNames = Object.keys(columnsByModel).sort();

            sortedModelNames.forEach((modelName, modelIndex) => {
                const modelColumns = columnsByModel[modelName];
                const modelInfo = affectedModels.find(m => m.name === modelName);
                const modelId = `model-group-${modelIndex}`;

                html += `
                    <div class="model-group">
                        <div class="model-group-header" data-model-group-id="${modelId}" data-collapsed="true">
                            <div class="model-group-title">
                                <span class="model-group-toggle-icon" id="${modelId}-icon">
                                    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                        <polyline points="9 18 15 12 9 6"></polyline>
                                    </svg>
                                </span>
                                <span class="model-group-name">${modelName}</span>
                                <span class="model-group-count">${modelColumns.length} transformation${modelColumns.length !== 1 ? 's' : ''}</span>
                            </div>
                        </div>
                        <div class="model-group-content collapsed" id="${modelId}">
                            <div class="columns-list">
                `;

                modelColumns.forEach(col => {
                    const colColumnName = col.column || 'unknown';
                    const transformationType = col.transformation_type || 'unknown';

                    html += `
                        <div class="column-card critical-card">
                            <div class="column-card-header">
                                <div class="column-card-title">
                                    <span class="column-name-bold">${colColumnName}</span>
                                </div>
                                <span class="transformation-badge critical-transformation">${transformationType}</span>
                            </div>
                            ${col.sql_expression ? `
                                <div class="column-card-body">
                                    <div class="sql-expression-card">
                                        <div class="sql-expression-label">Transformation Logic</div>
                                        <code class="sql-expression-code">${escapeHtml(col.sql_expression)}</code>
                                    </div>
                                </div>
                            ` : ''}
                            ${modelInfo && modelInfo.schema_name ? `
                                <div class="column-card-footer">
                                    <span class="schema-info">${modelInfo.database || ''}.${modelInfo.schema_name}</span>
                                </div>
                            ` : ''}
                        </div>
                    `;
                });

                html += `
                            </div>
                        </div>
                    </div>
                `;
            });

            html += `</div></div>`;
        }

        if (affectedExposures.length > 0) {
            html += `
                <div class="impact-section exposure-section">
                    <div class="section-header exposure-header">
                        <div class="section-header-left">
                            <span class="section-icon-badge exposure-badge">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline>
                                    <polyline points="17 6 23 6 23 12"></polyline>
                                </svg>
                            </span>
                            <div>
                                <h3 class="section-title">Affected Exposures</h3>
                                <p class="section-description">${affectedExposures.length} dashboard${affectedExposures.length !== 1 ? 's' : ''} or report${affectedExposures.length !== 1 ? 's' : ''} may be impacted</p>
                            </div>
                        </div>
                    </div>
                    <div class="exposures-grid">
            `;

            affectedExposures.forEach(exposure => {
                const exposureName = exposure.name || 'Unknown';
                const exposureType = exposure.type || 'unknown';
                const dependsOnModels = exposure.depends_on_models || [];
                html += `
                    <div class="exposure-card">
                        <div class="exposure-card-header">
                            <div class="exposure-card-title-group">
                                <span class="exposure-card-name">${exposureName}</span>
                                <span class="exposure-card-type">${exposureType}</span>
                            </div>
                        </div>
                        ${exposure.description ? `
                            <div class="exposure-card-body">
                                <p class="exposure-description">${escapeHtml(exposure.description)}</p>
                            </div>
                        ` : ''}
                        <div class="exposure-card-footer">
                            ${exposure.url ? `
                                <a href="${exposure.url}" target="_blank" class="exposure-link">
                                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                        <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                        <polyline points="15 3 21 3 21 9"></polyline>
                                        <line x1="10" y1="14" x2="21" y2="3"></line>
                                    </svg>
                                    View Dashboard
                                </a>
                            ` : ''}
                            ${dependsOnModels.length > 0 ? `
                                <div class="exposure-models">
                                    Uses: ${dependsOnModels.slice(0, 2).join(', ')}${dependsOnModels.length > 2 ? ` +${dependsOnModels.length - 2} more` : ''}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `;
            });

            html += `</div></div>`;
        }

        if (affectedModels.length > 0) {
            const modelsWithCritical = new Set(criticalColumns.map(col => col.model));

            html += `
                <div class="impact-section">
                    <div class="section-header">
                        <div class="section-header-left">
                            <span class="section-icon-badge">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <line x1="18" y1="20" x2="18" y2="10"></line>
                                    <line x1="12" y1="20" x2="12" y2="4"></line>
                                    <line x1="6" y1="20" x2="6" y2="14"></line>
                                </svg>
                            </span>
                            <div>
                                <h3 class="section-title">Affected Models</h3>
                                <p class="section-description">${affectedModels.length} model${affectedModels.length !== 1 ? 's' : ''} in the dependency chain</p>
                            </div>
                        </div>
                    </div>
                    <div class="model-grid">
            `;

            affectedModels.forEach(model => {
                const modelName = model.name || '';
                const schemaName = model.schema_name || '';
                const hasCritical = modelsWithCritical.has(modelName);
                const isMart = model.resource_type === 'model' && (modelName.includes('mart') || schemaName.includes('mart'));
                const columnsInModel = affectedColumns.filter(col => col.model === modelName);
                const criticalInModel = columnsInModel.filter(col => col.severity === 'critical').length;

                html += `
                    <div class="model-card ${hasCritical ? 'model-card-critical' : ''} ${isMart ? 'model-card-mart' : ''}">
                        <div class="model-card-header">
                            <div class="model-card-title-group">
                                <span class="model-card-name">${modelName}</span>
                                <span class="model-card-type">${model.resource_type || 'model'}</span>
                            </div>
                            ${criticalInModel > 0 ? `
                                <span class="model-critical-badge">${criticalInModel} critical</span>
                            ` : ''}
                        </div>
                        <div class="model-card-body">
                            <div class="model-card-info">
                                <span class="model-schema">${model.database || ''}.${schemaName}</span>
                                <span class="model-columns-count">${columnsInModel.length} column${columnsInModel.length !== 1 ? 's' : ''} affected</span>
                            </div>
                        </div>
                    </div>
                `;
            });

            html += `</div></div>`;
        }

        if (lowImpactColumns.length > 0) {
            html += `
                <div class="impact-section low-impact-section">
                    <div class="section-header low-impact-header">
                        <div class="section-header-left">
                            <span class="section-icon-badge low-impact-badge">
                                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <polyline points="20 6 9 17 4 12"></polyline>
                                </svg>
                            </span>
                            <div>
                                <h3 class="section-title">Pass-through Columns</h3>
                                <p class="section-description">${lowImpactColumns.length} direct reference${lowImpactColumns.length !== 1 ? 's' : ''} - changes propagate automatically</p>
                            </div>
                        </div>
                        <button class="toggle-section-btn" data-toggle-section>
                            Hide
                        </button>
                    </div>
                    <div class="columns-list">
            `;

            lowImpactColumns.forEach(col => {
                const colModelName = col.model || 'unknown';
                const colColumnName = col.column || 'unknown';
                const transformationType = col.transformation_type || 'unknown';

                html += `
                    <div class="column-card low-impact-card">
                        <div class="column-card-header">
                            <div class="column-card-title">
                                <span class="column-model">${colModelName}</span>
                                <span class="column-separator">.</span>
                                <span class="column-name-bold">${colColumnName}</span>
                            </div>
                            <span class="transformation-badge low-impact-transformation">${transformationType}</span>
                        </div>
                    </div>
                `;
            });

            html += `</div></div>`;
        }

        impactContent.innerHTML = html;

        const toggleButtons = impactContent.querySelectorAll('[data-toggle-section]');
        toggleButtons.forEach(button => {
            button.addEventListener('click', function() {
                const section = this.closest('.impact-section');
                const columnsList = section.querySelector('.columns-list');
                if (columnsList) {
                    columnsList.classList.toggle('collapsed');
                    this.textContent = columnsList.classList.contains('collapsed') ? 'Show' : 'Hide';
                }
            });
        });

        const modelGroupHeaders = impactContent.querySelectorAll('.model-group-header');
        modelGroupHeaders.forEach(header => {
            header.addEventListener('click', function() {
                const modelId = this.getAttribute('data-model-group-id');
                const content = document.getElementById(modelId);
                const icon = document.getElementById(`${modelId}-icon`);

                if (content) {
                    const isCollapsed = content.classList.contains('collapsed');
                    content.classList.toggle('collapsed');
                    this.setAttribute('data-collapsed', !isCollapsed);

                    if (icon) {
                        if (isCollapsed) {
                            icon.innerHTML = `
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <polyline points="6 9 12 15 18 9"></polyline>
                                </svg>
                            `;
                        } else {
                            icon.innerHTML = `
                                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <polyline points="9 18 15 12 9 6"></polyline>
                                </svg>
                            `;
                        }
                    }
                }
            });
        });
    }

    function displayRelationshipSummary(summary, container) {
        if (!summary || typeof summary !== 'object') {
            return;
        }

        const transformations = summary.critical_count || 0;
        const passThrough = summary.low_impact_count || 0;
        const relatedExposures = summary.affected_exposures || 0;
        const relatedModels = summary.affected_models || 0;

        container.innerHTML = `
            <div class="summary-metric ${transformations > 0 ? 'summary-metric-critical' : ''}">
                <div class="summary-metric-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path>
                        <line x1="12" y1="9" x2="12" y2="13"></line>
                        <line x1="12" y1="17" x2="12.01" y2="17"></line>
                    </svg>
                </div>
                <div class="summary-metric-content">
                    <div class="summary-metric-value">${transformations}</div>
                    <div class="summary-metric-label">Transformations</div>
                </div>
            </div>
            <div class="summary-metric summary-metric-pass-through">
                <div class="summary-metric-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="20 6 9 17 4 12"></polyline>
                    </svg>
                </div>
                <div class="summary-metric-content">
                    <div class="summary-metric-value">${passThrough}</div>
                    <div class="summary-metric-label">Pass-through</div>
                </div>
            </div>
            <div class="summary-metric summary-metric-models">
                <div class="summary-metric-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <line x1="18" y1="20" x2="18" y2="10"></line>
                        <line x1="12" y1="20" x2="12" y2="4"></line>
                        <line x1="6" y1="20" x2="6" y2="14"></line>
                    </svg>
                </div>
                <div class="summary-metric-content">
                    <div class="summary-metric-value">${relatedModels}</div>
                    <div class="summary-metric-label">Downstream Models</div>
                </div>
            </div>
            <div class="summary-metric summary-metric-exposures">
                <div class="summary-metric-icon">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                        <polyline points="23 6 13.5 15.5 8.5 10.5 1 18"></polyline>
                        <polyline points="17 6 23 6 23 12"></polyline>
                    </svg>
                </div>
                <div class="summary-metric-content">
                    <div class="summary-metric-value">${relatedExposures}</div>
                    <div class="summary-metric-label">Related Exposures</div>
                </div>
            </div>
        `;
    }

    function setupImpactPanel() {
        const impactPanel = document.getElementById('impactAnalysisPanel');
        const impactContent = document.getElementById('impactAnalysisContent');
        const closeImpactPanel = document.getElementById('closeImpactPanel');
        const graphContainer = document.getElementById('graph');

        closeImpactPanel.addEventListener('click', function() {
            impactPanel.style.display = 'none';
        });

        const resizeHandle = document.getElementById('impactPanelResizeHandle');
        if (resizeHandle) {
            let isResizing = false;
            let startX = 0;
            let startWidth = 0;

            resizeHandle.addEventListener('mousedown', function(e) {
                isResizing = true;
                startX = e.clientX;
                startWidth = parseInt(document.defaultView.getComputedStyle(impactPanel).width, 10);
                document.addEventListener('mousemove', handleResize);
                document.addEventListener('mouseup', stopResize);
                e.preventDefault();
            });

            function handleResize(e) {
                if (!isResizing) return;
                const width = startWidth - (e.clientX - startX);
                const minWidth = 400;
                const maxWidth = window.innerWidth * 0.9;
                const newWidth = Math.min(Math.max(width, minWidth), maxWidth);
                impactPanel.style.width = newWidth + 'px';
            }

            function stopResize() {
                isResizing = false;
                document.removeEventListener('mousemove', handleResize);
                document.removeEventListener('mouseup', stopResize);
            }
        }

        graphContainer.addEventListener('click', function(e) {
            let loadImpactBtn = e.target;
            if (e.target.id !== 'loadImpactAnalysisFromCard') {
                loadImpactBtn = e.target.closest('button#loadImpactAnalysisFromCard');
            }
            if (loadImpactBtn && loadImpactBtn.id === 'loadImpactAnalysisFromCard') {
                const columnSelect = document.getElementById('columnSelect');
                const column = columnSelect.value;
                const exploreController = window.app ? window.app.getExploreController() : null;
                const selectedModel = exploreController ? exploreController.getSelectedModel() : null;

                if (selectedModel && selectedModel.model_name && column) {
                    loadImpactBtn.disabled = true;
                    loadImpactBtn.textContent = 'Loading...';

                    fetch(`/api/impact-analysis/${selectedModel.model_name}/${column}`)
                        .then(response => {
                            if (!response.ok) {
                                return response.json().then(data => {
                                    throw new Error(data.error || `HTTP error! status: ${response.status}`);
                                }).catch(() => {
                                    throw new Error(`HTTP error! status: ${response.status}`);
                                });
                            }
                            return response.json();
                        })
                        .then(data => {
                            loadImpactBtn.disabled = false;
                            loadImpactBtn.textContent = 'Analyze Impact';

                            if (data.error) {
                                impactContent.innerHTML = `<p class="error-message">Error: ${data.error}</p>`;
                                impactPanel.style.display = 'block';
                                return;
                            }

                            displayImpactAnalysis(data, selectedModel.model_name, column);
                            impactPanel.style.display = 'block';

                            const impactCard = document.getElementById('impactAnalysisCard');
                            if (impactCard) {
                                impactCard.style.display = 'none';
                            }
                        })
                        .catch(error => {
                            loadImpactBtn.disabled = false;
                            loadImpactBtn.textContent = 'Analyze Impact';
                            console.error("Fetch error:", error);
                            impactContent.innerHTML = `<p class="error-message">Failed to fetch impact analysis: ${error.message}</p>`;
                            impactPanel.style.display = 'block';
                        });
                }
            }
        });
    }

    function init() {
        setupImpactPanel();
    }

    return {
        init,
        displayImpactAnalysis,
        displayRelationshipSummary
    };
})();
