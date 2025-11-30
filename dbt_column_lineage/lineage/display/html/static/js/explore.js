const ExploreModule = (function() {
    const iconFolderClosed = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon folder-icon closed"><path d="M3 7C3 5.89543 3.89543 5 5 5H9.58579C9.851 5 10.1054 5.10536 10.2929 5.29289L12 7H19C20.1046 7 21 7.89543 21 9V18C21 19.1046 20.1046 20 19 20H5C3.89543 20 3 19.1046 3 18V7Z" fill="currentColor" fill-opacity="0.3" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const iconFolderOpen = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon folder-icon open"><path d="M3 7C3 5.89543 3.89543 5 5 5H9.58579C9.851 5 10.1054 5.10536 10.2929 5.29289L12 7H19C20.1046 7 21 7.89543 21 9V10M3 11L4.5 17.5C4.77614 18.6 5.77614 19.5 6.9 19.5H17.1C18.2239 19.5 19.2239 18.6 19.5 17.5L21 11H3Z" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const iconModel = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon model-icon"><path d="M12 3L3 7.5V16.5L12 21L21 16.5V7.5L12 3Z" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M12 12L21 7.5M12 12L3 7.5M12 12V21" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const iconSource = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon source-icon"><path d="M12 5C7 5 4 6.5 4 8.5C4 10.5 7 12 12 12C17 12 20 10.5 20 8.5C20 6.5 17 5 12 5Z" fill="currentColor" fill-opacity="0.3" stroke="currentColor" stroke-width="1.5"/><path d="M4 8.5V15.5C4 17.5 7 19 12 19C17 19 20 17.5 20 15.5V8.5" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/><path d="M4 12C4 14 7 15.5 12 15.5C17 15.5 20 14 20 12" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>`;
    const iconSeed = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon seed-icon"><path d="M3 6C3 4.34315 4.34315 3 6 3H14L18 7V18C18 19.6569 16.6569 21 15 21H6C4.34315 21 3 19.6569 3 18V6Z" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M14 3V7H18" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M7 12H14M7 16H11" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>`;
    const iconSnapshot = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon snapshot-icon"><path d="M12 3a9 9 0 1 0 9 9 9 9 0 0 0-9-9zm0 16a7 7 0 1 1 7-7 7 7 0 0 1-7 7zm0-9a2 2 0 1 0 2 2 2 2 0 0 0-2-2z" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;
    const iconExposure = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon exposure-icon"><path d="M14 2H6C4.89543 2 4 2.89543 4 4V20C4 21.1046 4.89543 22 6 22H18C19.1046 22 20 21.1046 20 20V8L14 2Z" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M14 2V8H20" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/><path d="M16 13H8M16 17H8M10 9H8" stroke="currentColor" stroke-width="1.5" stroke-linecap="round"/></svg>`;
    const iconDefault = `<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" class="tree-icon default-icon"><rect x="4" y="4" width="16" height="16" rx="2" fill="currentColor" fill-opacity="0.2" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/></svg>`;

    let selectedModelData = null;
    let allTreeData = [];
    let currentSearchTerm = '';

    function countItems(node) {
        if (node.type !== 'folder') return 1;
        if (!node.children || node.children.length === 0) return 0;
        return node.children.reduce((sum, child) => sum + countItems(child), 0);
    }

    function highlightText(text, searchTerm) {
        if (!searchTerm) return text;
        const regex = new RegExp(`(${searchTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')})`, 'gi');
        return text.replace(regex, '<span class="highlight">$1</span>');
    }

    function getTypeBadge(resourceType) {
        const typeLabels = {
            'model': 'model',
            'source': 'source',
            'seed': 'seed',
            'snapshot': 'snap',
            'exposure': 'exposure'
        };
        const label = typeLabels[resourceType] || resourceType;
        return `<span class="node-type-badge ${resourceType}">${label}</span>`;
    }

    function getTagColor(type) {
        if (!type) return '#cbd5e1';
        const typeStr = type.toLowerCase();
        if (typeStr.includes('int') || typeStr.includes('decimal') || typeStr.includes('numeric') || typeStr.includes('double') || typeStr.includes('float')) {
            return '#7cb7fc';
        }
        if (typeStr.includes('varchar') || typeStr.includes('char') || typeStr.includes('text') || typeStr.includes('string')) {
            return '#4ddba4';
        }
        if (typeStr.includes('date') || typeStr.includes('time')) {
            return '#b29dfc';
        }
        if (typeStr.includes('bool')) {
            return '#f98b8b';
        }
        if (typeStr.includes('variant')) {
            return '#fca154';
        }
        return '#cbd5e1';
    }

    function getShortType(type) {
        if (!type) return 'unknown';
        return type.toLowerCase()
            .replace('character varying', 'varchar')
            .replace('double precision', 'double')
            .replace('timestamp without time zone', 'timestamp')
            .replace('timestamp with time zone', 'timestamptz');
    }

    let tooltipElement = null;

    function setupTooltip(nodeContent, span, labelText) {
        if (!tooltipElement) {
            tooltipElement = document.createElement('div');
            tooltipElement.style.cssText = 'position:absolute;background:rgba(0,0,0,0.85);color:white;padding:6px 10px;border-radius:4px;font-size:12px;white-space:nowrap;z-index:10000;pointer-events:none;opacity:0;transition:opacity 0.2s;box-shadow:0 2px 8px rgba(0,0,0,0.2)';
            document.body.appendChild(tooltipElement);
        }

        nodeContent.addEventListener('mouseenter', (e) => {
            if (span.scrollWidth > span.clientWidth) {
                tooltipElement.textContent = labelText;
                tooltipElement.style.left = (e.pageX || e.clientX + window.scrollX) + 10 + 'px';
                tooltipElement.style.top = (e.pageY || e.clientY + window.scrollY) - 10 + 'px';
                tooltipElement.style.opacity = '1';
            }
        });

        nodeContent.addEventListener('mouseleave', () => {
            tooltipElement.style.opacity = '0';
        });
    }

    function renderTree(nodes, container, level = 0) {
        const ul = document.createElement('ul');
        ul.classList.add('model-tree');
        if (level > 0) ul.classList.add('nested');

        nodes.forEach(node => {
            const li = document.createElement('li');
            const nodeType = node.resource_type === 'exposure' ? 'exposure' : node.type;
            li.classList.add('tree-node', `tree-node-${nodeType}`);

            const nodeContent = document.createElement('div');
            nodeContent.classList.add('node-content');

            const iconContainer = document.createElement('span');
            iconContainer.classList.add('icon-container');

            const span = document.createElement('span');
            const labelText = node.type === 'model' ? node.display_name : node.name;
            span.innerHTML = currentSearchTerm ? highlightText(labelText, currentSearchTerm) : labelText;
            span.classList.add('node-label');
            span.dataset.originalText = labelText;

            if (node.type === 'folder') {
                nodeContent.classList.add('folder');
                iconContainer.innerHTML = iconFolderClosed;
                nodeContent.appendChild(iconContainer);
                nodeContent.appendChild(span);

                const itemCount = countItems(node);
                if (itemCount > 0) {
                    const countBadge = document.createElement('span');
                    countBadge.classList.add('folder-count');
                    countBadge.textContent = itemCount;
                    nodeContent.appendChild(countBadge);
                }

                li.appendChild(nodeContent);

                nodeContent.addEventListener('click', () => {
                    li.classList.toggle('expanded');
                    const nestedUl = li.querySelector(':scope > ul');
                    if (nestedUl) {
                        nestedUl.classList.toggle('active');
                        if (li.classList.contains('expanded')) {
                            iconContainer.innerHTML = iconFolderOpen;
                        } else {
                            iconContainer.innerHTML = iconFolderClosed;
                        }
                    }
                });
                if (node.children && node.children.length > 0) {
                    renderTree(node.children, li, level + 1);
                    const childUl = li.querySelector(':scope > ul');
                    if (childUl) childUl.classList.remove('active');
                } else {
                    li.classList.add('empty-folder');
                }
            } else {
                if (node.resource_type === 'exposure') {
                    nodeContent.classList.add('exposure');
                    iconContainer.innerHTML = iconExposure;
                    nodeContent.appendChild(iconContainer);
                    nodeContent.appendChild(span);
                    nodeContent.insertAdjacentHTML('beforeend', getTypeBadge('exposure'));
                    li.appendChild(nodeContent);
                    setupTooltip(nodeContent, span, labelText);

                    nodeContent.addEventListener('click', () => {
                        document.querySelectorAll('.model-tree .node-content.selected').forEach(el => el.classList.remove('selected'));
                        nodeContent.classList.add('selected');
                        const columnSelectWrapper = document.getElementById('columnSelectWrapper');
                        const columnSelectValue = document.querySelector('.custom-select-value');
                        const columnSelectOptions = document.getElementById('columnSelectOptions');
                        const columnSelect = document.getElementById('columnSelect');
                        const loadLineageBtn = document.getElementById('loadLineage');
                        columnSelect.innerHTML = '<option value="">Exposures don\'t have columns</option>';
                        columnSelectOptions.innerHTML = '<div class="custom-select-option" data-value=""><span class="option-text">Exposures don\'t have columns</span></div>';
                        const resetContent = document.createElement('div');
                        resetContent.className = 'option-content';
                        const resetText = document.createElement('span');
                        resetText.className = 'option-text';
                        resetText.textContent = 'Exposures don\'t have columns';
                        resetContent.appendChild(resetText);
                        columnSelectValue.innerHTML = '';
                        columnSelectValue.appendChild(resetContent);
                        columnSelectWrapper.classList.add('disabled');
                        columnSelect.disabled = true;
                        loadLineageBtn.disabled = true;
                    });
                } else {
                    nodeContent.classList.add('model');
                    let modelIcon = iconDefault;
                    if (node.resource_type === 'model') modelIcon = iconModel;
                    else if (node.resource_type === 'source') modelIcon = iconSource;
                    else if (node.resource_type === 'seed') modelIcon = iconSeed;
                    else if (node.resource_type === 'snapshot') modelIcon = iconSnapshot;

                    iconContainer.innerHTML = modelIcon;
                    nodeContent.appendChild(iconContainer);
                    nodeContent.appendChild(span);
                    if (node.resource_type) {
                        nodeContent.insertAdjacentHTML('beforeend', getTypeBadge(node.resource_type));
                    }
                    li.appendChild(nodeContent);
                    span.dataset.modelName = node.model_name;
                    setupTooltip(nodeContent, span, labelText);

                    nodeContent.addEventListener('click', () => {
                        document.querySelectorAll('.model-tree .node-content.selected').forEach(el => el.classList.remove('selected'));
                        nodeContent.classList.add('selected');
                        selectedModelData = node;
                        populateColumnSelector(node.columns);
                        document.getElementById('loadLineage').disabled = true;
                    });
                }
            }
            ul.appendChild(li);
        });
        container.appendChild(ul);
    }

    function filterTree(searchTerm, modelTreeContainer) {
        const term = searchTerm.toLowerCase().trim();
        currentSearchTerm = term;
        const allNodes = modelTreeContainer.querySelectorAll('.tree-node');

        let existingEmpty = modelTreeContainer.querySelector('.tree-empty-state');
        if (existingEmpty) existingEmpty.remove();

        allNodes.forEach(node => {
            node.style.display = '';
            const label = node.querySelector('.node-label');
            if (label && label.dataset.originalText) {
                label.innerHTML = term ? highlightText(label.dataset.originalText, term) : label.dataset.originalText;
            }
            if (node.classList.contains('tree-node-folder') && term !== '') {
                node.classList.remove('expanded');
                const nestedUl = node.querySelector(':scope > ul.nested');
                if (nestedUl) nestedUl.classList.remove('active');
                const iconContainer = node.querySelector('.icon-container');
                if (iconContainer) iconContainer.innerHTML = iconFolderClosed;
            }
        });

        if (term === '') return;

        const modelNodes = modelTreeContainer.querySelectorAll('.tree-node-model, .tree-node-exposure');
        const foldersToShow = new Set();
        let matchCount = 0;

        modelNodes.forEach(modelLi => {
            const label = modelLi.querySelector('.node-label');
            const originalText = label ? (label.dataset.originalText || label.textContent) : '';
            const matches = originalText.toLowerCase().includes(term);

            if (matches) {
                matchCount++;
                modelLi.style.display = 'block';
                let current = modelLi.parentElement;
                while (current && current !== modelTreeContainer) {
                    if (current.tagName === 'LI' && current.classList.contains('tree-node-folder')) {
                        foldersToShow.add(current);
                    }
                    current = current.parentElement;
                }
            } else {
                modelLi.style.display = 'none';
            }
        });

        const folderNodes = modelTreeContainer.querySelectorAll('.tree-node-folder');
        folderNodes.forEach(folderLi => {
            if (foldersToShow.has(folderLi)) {
                folderLi.style.display = 'block';
                if (!folderLi.classList.contains('expanded')) {
                    folderLi.classList.add('expanded');
                    const nestedUl = folderLi.querySelector(':scope > ul.nested');
                    if (nestedUl) nestedUl.classList.add('active');
                    const iconContainer = folderLi.querySelector('.icon-container');
                    if (iconContainer) iconContainer.innerHTML = iconFolderOpen;
                }
            } else {
                folderLi.style.display = 'none';
            }
        });

        if (matchCount === 0) {
            const emptyState = document.createElement('div');
            emptyState.className = 'tree-empty-state';
            emptyState.innerHTML = `
                <svg xmlns="http://www.w3.org/2000/svg" width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round">
                    <circle cx="11" cy="11" r="8"></circle>
                    <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
                    <line x1="8" y1="11" x2="14" y2="11"></line>
                </svg>
                <p>No models found for "${term}"</p>
                <span class="empty-hint">Try a different search term</span>
            `;
            modelTreeContainer.appendChild(emptyState);
        }
    }

    function populateColumnSelector(columns) {
        const columnSelect = document.getElementById('columnSelect');
        const columnSelectWrapper = document.getElementById('columnSelectWrapper');
        const columnSelectTrigger = document.getElementById('columnSelectTrigger');
        const columnSelectOptions = document.getElementById('columnSelectOptions');
        const columnSelectOptionsList = document.getElementById('columnSelectOptionsList');
        const columnSelectValue = columnSelectTrigger.querySelector('.custom-select-value');
        const columnSearchInput = document.getElementById('columnSearchInput');

        window.allColumns = columns;

        columnSelect.innerHTML = '<option value="">Select a column</option>';
        columnSelectOptionsList.innerHTML = '<div class="custom-select-option" data-value=""><span class="option-text">Select a column</span></div>';

        if (columnSearchInput) {
            columnSearchInput.value = '';
        }

        columns.forEach(column => {
            const option = document.createElement('option');
            option.value = column.name;
            option.textContent = `${column.name} (${column.type || 'unknown'})`;
            columnSelect.appendChild(option);

            const customOption = document.createElement('div');
            customOption.className = 'custom-select-option';
            customOption.dataset.value = column.name;
            customOption.dataset.columnName = column.name.toLowerCase();
            customOption.dataset.columnType = (column.type || '').toLowerCase();

            const optionContent = document.createElement('div');
            optionContent.className = 'option-content';

            const optionText = document.createElement('span');
            optionText.className = 'option-text';
            optionText.textContent = column.name;

            const typeTag = document.createElement('span');
            typeTag.className = 'option-type-tag';
            const shortType = getShortType(column.type);
            typeTag.textContent = shortType;
            typeTag.style.backgroundColor = getTagColor(column.type);

            optionContent.appendChild(optionText);
            optionContent.appendChild(typeTag);
            customOption.appendChild(optionContent);
            columnSelectOptionsList.appendChild(customOption);
        });

        const resetContent = document.createElement('div');
        resetContent.className = 'option-content';
        const resetText = document.createElement('span');
        resetText.className = 'option-text';
        resetText.textContent = 'Select a column';
        resetContent.appendChild(resetText);
        columnSelectValue.innerHTML = '';
        columnSelectValue.appendChild(resetContent);
        columnSelectWrapper.classList.remove('disabled');
        columnSelect.disabled = false;
    }

    function filterColumns(searchTerm) {
        const term = searchTerm.toLowerCase().trim();
        const optionsList = document.getElementById('columnSelectOptionsList');
        if (!optionsList) return;

        const allOptions = optionsList.querySelectorAll('.custom-select-option');

        allOptions.forEach(option => {
            if (!option.dataset.value) {
                option.style.display = '';
                return;
            }

            const columnName = option.dataset.columnName || '';
            const columnType = option.dataset.columnType || '';

            if (term === '' || columnName.includes(term) || columnType.includes(term)) {
                option.style.display = '';
            } else {
                option.style.display = 'none';
            }
        });
    }

    function setupColumnDropdown() {
        const columnSelect = document.getElementById('columnSelect');
        const columnSelectWrapper = document.getElementById('columnSelectWrapper');
        const columnSelectTrigger = document.getElementById('columnSelectTrigger');
        const columnSelectOptions = document.getElementById('columnSelectOptions');
        const columnSelectValue = columnSelectTrigger.querySelector('.custom-select-value');

        columnSelectTrigger.addEventListener('click', function(e) {
            if (columnSelectWrapper.classList.contains('disabled')) return;
            e.stopPropagation();
            columnSelectWrapper.classList.toggle('open');
        });

        document.addEventListener('click', function(e) {
            if (!columnSelectWrapper.contains(e.target)) {
                columnSelectWrapper.classList.remove('open');
            }
        });

        columnSelectOptions.addEventListener('click', function(e) {
            if (e.target.closest('.column-search-container')) {
                e.stopPropagation();
                return;
            }

            const option = e.target.closest('.custom-select-option');
            if (!option) return;

            const value = option.dataset.value;
            const optionContent = option.querySelector('.option-content');

            columnSelectValue.innerHTML = '';
            if (optionContent) {
                columnSelectValue.appendChild(optionContent.cloneNode(true));
            } else {
                const text = option.textContent;
                columnSelectValue.textContent = text;
            }
            columnSelectOptions.querySelectorAll('.custom-select-option').forEach(opt => {
                opt.classList.remove('selected');
            });
            option.classList.add('selected');
            columnSelectWrapper.classList.remove('open');

            columnSelect.value = value;
            columnSelect.dispatchEvent(new Event('change'));
        });

        const columnSearchInput = document.getElementById('columnSearchInput');
        if (columnSearchInput) {
            const searchContainer = columnSearchInput.closest('.column-search-container');

            columnSearchInput.addEventListener('input', function(e) {
                e.stopPropagation();
                filterColumns(e.target.value);
            });

            columnSearchInput.addEventListener('click', function(e) {
                e.stopPropagation();
            });

            columnSearchInput.addEventListener('focus', function() {
                if (searchContainer) {
                    searchContainer.classList.add('search-focused');
                }
            });

            columnSearchInput.addEventListener('blur', function() {
                if (searchContainer) {
                    searchContainer.classList.remove('search-focused');
                }
            });
        }
    }

    function loadModels(modelTreeContainer) {
        fetch('/api/models')
            .then(response => response.json())
            .then(treeData => {
                modelTreeContainer.innerHTML = '';
                allTreeData = treeData;
                if (treeData && treeData.length > 0) {
                    renderTree(treeData, modelTreeContainer);
                } else {
                    modelTreeContainer.innerHTML = '<p>No models found.</p>';
                }
            })
            .catch(error => {
                console.error("Error fetching model tree:", error);
                modelTreeContainer.innerHTML = '<p>Error loading models.</p>';
            });
    }

    function init(initialData, graphInstanceRef) {
        const columnSelect = document.getElementById('columnSelect');
        const loadLineageBtn = document.getElementById('loadLineage');
        const graphContainer = document.getElementById('graph');
        const modelTreeContainer = document.getElementById('model-tree-container');
        const searchInput = document.getElementById('modelSearchInput');
        const searchContainer = document.getElementById('searchBarContainer');
        const searchClearBtn = document.getElementById('searchClearBtn');

        setupColumnDropdown();

        searchInput.addEventListener('input', (e) => {
            const value = e.target.value;
            filterTree(value, modelTreeContainer);
            if (searchContainer) {
                searchContainer.classList.toggle('has-value', value.length > 0);
            }
        });

        if (searchClearBtn) {
            searchClearBtn.addEventListener('click', () => {
                searchInput.value = '';
                filterTree('', modelTreeContainer);
                if (searchContainer) {
                    searchContainer.classList.remove('has-value');
                }
                searchInput.focus();
        });
        }

        columnSelect.addEventListener('change', function() {
            const hasSelection = this.value && selectedModelData;
            loadLineageBtn.disabled = !hasSelection;
        });

        loadLineageBtn.addEventListener('click', function() {
            const column = columnSelect.value;
            if (selectedModelData && selectedModelData.model_name && column) {
                fetch(`/api/lineage/${selectedModelData.model_name}/${column}`)
                    .then(response => response.json())
                    .then(data => {
                        if (data.error) {
                            console.error(data.error);
                            graphContainer.innerHTML = `<p class="error-message">Error loading lineage: ${data.error}</p>`;
                            document.getElementById('impactAnalysisCard').style.display = 'none';
                            document.getElementById('relationshipSummaryCard').style.display = 'none';
                            return;
                        }

                        const newGraphInstance = initGraph(data);
                        if (graphInstanceRef) {
                            graphInstanceRef.current = newGraphInstance;
                        }
                        if (window.app && window.app.setGraphInstance) {
                            window.app.setGraphInstance(newGraphInstance);
                        }

                        setTimeout(() => {
                            const relationshipSummaryCard = document.getElementById('relationshipSummaryCard');
                            const relationshipSummaryMetrics = document.getElementById('relationshipSummaryMetrics');

                            if (relationshipSummaryCard && relationshipSummaryMetrics) {
                                if (data.impact_summary && typeof data.impact_summary === 'object') {
                                    ImpactModule.displayRelationshipSummary(data.impact_summary, relationshipSummaryMetrics);
                                    relationshipSummaryCard.style.display = 'block';
                                } else {
                                    relationshipSummaryCard.style.display = 'none';
                                }
                            }
                        }, 100);

                        const impactCard = document.getElementById('impactAnalysisCard');
                        if (impactCard) {
                            impactCard.style.display = 'block';
                        }
                    })
                    .catch(error => {
                        console.error("Fetch error:", error);
                        graphContainer.innerHTML = `<p class="error-message">Failed to fetch lineage data.</p>`;
                        const impactCard = document.getElementById('impactAnalysisCard');
                        const impactSummaryCard = document.getElementById('impactSummaryCard');
                        if (impactCard) {
                            impactCard.style.display = 'none';
                        }
                        if (impactSummaryCard) {
                            impactSummaryCard.style.display = 'none';
                        }
                    });
            }
        });

        loadModels(modelTreeContainer);

        if (initialData && initialData.impact_summary) {
            const relationshipSummaryCard = document.getElementById('relationshipSummaryCard');
            const relationshipSummaryMetrics = document.getElementById('relationshipSummaryMetrics');
            if (relationshipSummaryCard && relationshipSummaryMetrics) {
                ImpactModule.displayRelationshipSummary(initialData.impact_summary, relationshipSummaryMetrics);
                relationshipSummaryCard.style.display = 'block';
            }
        }

        return {
            getSelectedModel: () => selectedModelData,
            setSelectedModel: (model) => { selectedModelData = model; }
        };
    }

    return {
        init,
        filterTree,
        populateColumnSelector,
        filterColumns
    };
})();
