const ModelDetailsModule = (function() {
    const modelIcons = {
        model: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3L3 7.5V16.5L12 21L21 16.5V7.5L12 3Z"/><path d="M12 12L21 7.5M12 12L3 7.5M12 12V21"/></svg>`,
        source: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round"><path d="M12 5C7 5 4 6.5 4 8.5C4 10.5 7 12 12 12C17 12 20 10.5 20 8.5C20 6.5 17 5 12 5Z"/><path d="M4 8.5V15.5C4 17.5 7 19 12 19C17 19 20 17.5 20 15.5V8.5"/><path d="M4 12C4 14 7 15.5 12 15.5C17 15.5 20 14 20 12"/></svg>`,
        seed: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 6C3 4.34315 4.34315 3 6 3H14L18 7V18C18 19.6569 16.6569 21 15 21H6C4.34315 21 3 19.6569 3 18V6Z"/><path d="M14 3V7H18"/><path d="M7 12H14M7 16H11"/></svg>`,
        snapshot: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="9"/><circle cx="12" cy="12" r="3"/></svg>`,
        exposure: `<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14 2H6C4.89543 2 4 2.89543 4 4V20C4 21.1046 4.89543 22 6 22H18C19.1046 22 20 21.1046 20 20V8L14 2Z"/><path d="M14 2V8H20"/><path d="M16 13H8M16 17H8M10 9H8"/></svg>`
    };

    let currentModelName = null;

    function ensureCardExists() {
        if (document.getElementById('modelDetailsCard')) return;

        const graphContainer = document.getElementById('graph');
        if (!graphContainer) return;

        const cardHTML = `
            <div id="modelDetailsCard" class="model-details-card" style="display: none; top: 230px; z-index: 250;">
                <div class="model-details-header">
                    <div class="model-details-title">
                        <span class="model-details-icon" id="modelDetailsIcon"></span>
                        <div class="model-details-title-text">
                            <h4 id="modelDetailsName">Model Name</h4>
                            <span class="model-details-type" id="modelDetailsType">model</span>
                        </div>
                    </div>
                    <button class="model-details-close" id="closeModelDetailsCard">×</button>
                </div>
                <div class="model-details-content">
                    <div class="model-details-section" id="modelDescriptionSection">
                        <label>Description</label>
                        <p id="modelDetailsDescription">No description available</p>
                    </div>
                    <div class="model-details-section" id="modelTagsSection" style="display: none;">
                        <label>Tags</label>
                        <div class="model-tags-container" id="modelDetailsTags"></div>
                    </div>
                    <div class="model-details-empty" id="modelDetailsEmpty" style="display: none;">
                        <p>No description or tags available for this model.</p>
                    </div>
                </div>
            </div>
        `;
        graphContainer.insertAdjacentHTML('beforeend', cardHTML);

        const closeBtn = document.getElementById('closeModelDetailsCard');
        if (closeBtn) {
            closeBtn.addEventListener('click', hideCard);
        }
    }

    function init() {
        ensureCardExists();

        const closeBtn = document.getElementById('closeModelDetailsCard');
        if (closeBtn) {
            closeBtn.addEventListener('click', hideCard);
        }

        document.addEventListener('click', function(e) {
            const card = document.getElementById('modelDetailsCard');
            if (!card || card.style.display === 'none') return;

            if (!card.contains(e.target) && !e.target.closest('.model')) {
                hideCard();
            }
        });
    }

    function showCard(modelName) {
        if (!modelName) return;

        currentModelName = modelName;
        ensureCardExists();

        fetch(`/api/model/${encodeURIComponent(modelName)}/details`)
            .then(res => res.json())
            .then(data => {
                if (data.error) {
                    console.error('Error fetching model details:', data.error);
                    return;
                }
                renderCard(data);
            })
            .catch(err => {
                console.error('Failed to fetch model details:', err);
            });
    }

    function renderCard(data) {
        const card = document.getElementById('modelDetailsCard');
        const nameEl = document.getElementById('modelDetailsName');
        const typeEl = document.getElementById('modelDetailsType');
        const iconEl = document.getElementById('modelDetailsIcon');
        const descSection = document.getElementById('modelDescriptionSection');
        const descEl = document.getElementById('modelDetailsDescription');
        const tagsSection = document.getElementById('modelTagsSection');
        const tagsContainer = document.getElementById('modelDetailsTags');
        const emptyEl = document.getElementById('modelDetailsEmpty');

        if (!card) return;

        nameEl.textContent = data.name;

        const resourceType = data.resource_type || 'model';
        typeEl.textContent = resourceType;
        typeEl.className = `model-details-type type-${resourceType}`;

        iconEl.innerHTML = modelIcons[resourceType] || modelIcons.model;
        iconEl.className = `model-details-icon model-type-${resourceType}`;

        const hasDescription = data.description && data.description.trim();
        const hasTags = data.tags && data.tags.length > 0;

        if (hasDescription) {
            descEl.textContent = data.description;
            descSection.style.display = 'block';
        } else {
            descSection.style.display = 'none';
        }

        tagsContainer.innerHTML = '';
        if (hasTags) {
            data.tags.forEach(tag => {
                const tagEl = document.createElement('span');
                tagEl.className = 'model-tag';
                tagEl.textContent = tag;
                tagsContainer.appendChild(tagEl);
            });
            tagsSection.style.display = 'block';
        } else {
            tagsSection.style.display = 'none';
        }

        if (!hasDescription && !hasTags) {
            emptyEl.style.display = 'block';
        } else {
            emptyEl.style.display = 'none';
        }

        card.style.display = 'block';
        card.style.top = '230px';
        card.style.zIndex = '250';
    }

    function hideCard() {
        const card = document.getElementById('modelDetailsCard');
        if (card) {
            card.style.display = 'none';
        }
        currentModelName = null;
    }

    function getCurrentModel() {
        return currentModelName;
    }

    return {
        init,
        showCard,
        hideCard,
        getCurrentModel
    };
})();
