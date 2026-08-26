const ImpactModule = (function() {
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // The dbt tests covering an affected column — the guarantees a change threatens.
    // Gated behind the "Show tests" toggle via the body.show-tests class (see impact.css).
    function testGuardrailsHtml(tests) {
        if (!tests || tests.length === 0) return '';
        const pills = tests.map(t => {
            const label = t.test_name || 'test';
            let title = label;
            if (t.test_name === 'relationships' && t.referenced_model) {
                const ref = t.referenced_column
                    ? `${t.referenced_model}.${t.referenced_column}`
                    : t.referenced_model;
                title = `relationships → ${ref}`;
            }
            return `<span class="test-pill" title="${escapeHtml(title)}">${escapeHtml(label)}</span>`;
        }).join('');
        return `
            <div class="column-test-guardrails">
                <span class="test-guardrails-label">
                    <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.1" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M6 0.5 L10.5 2.1 V5.1 C10.5 7.9 8.5 9.8 6 10.5 C3.5 9.8 1.5 7.9 1.5 5.1 V2.1 Z"></path><path d="M3.9 5.3 L5.5 6.9 L8.3 3.7"></path></svg>
                    Tests
                </span>
                <span class="test-pill-group">${pills}</span>
            </div>`;
    }

    // Semantic categorization folds onto the ONE shared amber "caution" axis: a
    // breaking change is the amber spark; a proven-equivalent change is de-emphasized
    // (neutral slate + an indigo check — the good news is "nothing to see"). Fail-safe:
    // an indeterminate / unknown semantic always reads as breaking (never "safe").
    function isBreakingCol(col) {
        if (!col) return false;
        if (col.breaking === true) return true;
        if (col.breaking === false) return false;
        return !!col.semantic && col.semantic !== 'equivalent';
    }

    function hasSemantic(col) {
        return !!col && (col.breaking === true || col.breaking === false || !!col.semantic);
    }

    // Compact per-column semantic badge for the column cards (breaking vs proven-equivalent).
    function semanticBadgeHtml(col) {
        if (!hasSemantic(col)) return '';
        if (isBreakingCol(col)) {
            const indeterminate = col.semantic === 'indeterminate';
            const label = indeterminate ? 'BREAKING?' : 'BREAKING';
            const title = indeterminate
                ? 'Could not prove equivalence — treated as breaking (value may differ downstream)'
                : 'Meaning changed — value may differ downstream';
            return `<span class="semantic-badge semantic-breaking" title="${escapeHtml(title)}"><span class="semantic-dot"></span>${label}</span>`;
        }
        return `<span class="semantic-badge semantic-equivalent" title="Proven equivalent — cosmetic-only change"><svg width="11" height="11" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>EQUIVALENT</span>`;
    }

    // Hero chip stating the SUBJECT column's own verdict — the change under review. Only
    // rendered when a change context is present (else absence = nothing to review).
    function subjectSemanticChipHtml(data) {
        if (data.subject_breaking === undefined || data.subject_breaking === null) return '';
        if (data.subject_breaking) {
            const indeterminate = data.subject_semantic === 'indeterminate';
            const label = indeterminate ? 'Breaking — unproven' : 'Breaking change';
            const detail = indeterminate
                ? 'Could not prove equivalence, so this is treated as breaking (fail-safe).'
                : 'The expression’s meaning changed — downstream values may differ.';
            return `
                <div class="impact-breaking-chip impact-breaking">
                    <span class="semantic-dot"></span>
                    <span class="impact-breaking-chip-label">${label}</span>
                    <span class="impact-breaking-chip-detail">${escapeHtml(detail)}</span>
                </div>`;
        }
        return `
            <div class="impact-breaking-chip impact-equivalent">
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>
                <span class="impact-breaking-chip-label">Proven equivalent</span>
                <span class="impact-breaking-chip-detail">Cosmetic-only change — downstream values are unaffected.</span>
            </div>`;
    }

    // A neutral note for a column that is NOT part of the reviewed change.
    // Exploring such a column produces no verdict — the change-wide policy decision does not
    // describe it. Only shown when a change context is loaded (subject_in_changeset === false);
    // in pure-explore mode the flag is absent and nothing renders.
    function notInChangesetNoteHtml(data) {
        if (data.subject_in_changeset !== false) return '';
        return `
            <div class="impact-not-in-change" role="note">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>
                <span>Not part of the reviewed change — exploring this column produces no verdict.</span>
            </div>`;
    }

    // A Metabase dashboard reached PAST the dbt edge reads as a distinct card variant:
    // an "outside dbt" eyebrow, honest table-level vs column-precise caption, via-card count.
    function metabaseExposureCardHtml(exposure) {
        const name = escapeHtml(exposure.name || 'Unknown');
        const meta = exposure.meta || {};
        const tier = meta.tier ? `<span class="mb-tier">${escapeHtml(String(meta.tier))}</span>` : '';
        const precision = exposure.precision === 'table'
            ? '<span class="mb-precision mb-precision-table" title="Reached only via a table-grain card — the specific column could not be resolved">table-level</span>'
            : '<span class="mb-precision mb-precision-column" title="Reached via a column-precise card">column-precise</span>';
        const viaCards = Array.isArray(exposure.via_cards) && exposure.via_cards.length
            ? `<span class="mb-via-cards">via card${exposure.via_cards.length !== 1 ? 's' : ''} ${exposure.via_cards.map(c => '#' + escapeHtml(String(c))).join(', ')}</span>`
            : '';
        // F4: name the exact field(s) of the dashboard this change hits, so the reviewer goes
        // straight to it. Distinct model.column (a column may be read by several cards), role
        // shown when known. Absent on a table-grain reach — "table-level" already explains why.
        const viaColumns = (function () {
            if (!Array.isArray(exposure.via_columns) || !exposure.via_columns.length) return '';
            const seen = new Map();
            exposure.via_columns.forEach(v => {
                if (!v || !v.model || !v.column) return;
                const key = v.model + '.' + v.column;
                if (!seen.has(key)) seen.set(key, v.role || '');
            });
            if (!seen.size) return '';
            const chips = Array.from(seen.entries()).map(([col, role]) =>
                `<span class="mb-field"><code>${escapeHtml(col)}</code>${role ? `<span class="mb-field-role">${escapeHtml(role)}</span>` : ''}</span>`
            ).join('');
            return `<div class="mb-fields-line"><span class="mb-fields-label">Affects</span>${chips}</div>`;
        })();
        return `
            <div class="exposure-card exposure-card-metabase">
                <div class="exposure-boundary-eyebrow">${typeof metabaseLogoSvg === 'function' ? metabaseLogoSvg(12) : ''}<span>BI · METABASE</span></div>
                <div class="exposure-card-header">
                    <div class="exposure-card-title-group">
                        <span class="exposure-card-name">${name}</span>
                        <span class="exposure-card-type">dashboard</span>
                    </div>
                    ${tier}
                </div>
                <div class="exposure-card-body">
                    <div class="mb-reach-line">${precision}${viaCards}</div>
                    ${viaColumns}
                </div>
                <div class="exposure-card-footer">
                    ${exposure.url ? `
                        <a href="${exposure.url}" target="_blank" class="exposure-link">
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                <path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"></path>
                                <polyline points="15 3 21 3 21 9"></polyline>
                                <line x1="10" y1="14" x2="21" y2="3"></line>
                            </svg>
                            View dashboard
                        </a>
                    ` : ''}
                </div>
            </div>`;
    }

    // Coverage is a property of the loaded artifacts (not of a single column), so
    // fetch it once and memoize the promise for every panel that wants to show it.
    let coveragePromise = null;
    function fetchCoverage() {
        if (!coveragePromise) {
            coveragePromise = fetch('/api/coverage')
                .then(response => response.ok ? response.json() : null)
                .then(data => (data && !data.error) ? data : null)
                .catch(() => null);
        }
        return coveragePromise;
    }

    // Confidence answers: "can I trust this impact list is complete?" We lead with the
    // consequence (complete vs. lower bound), then explain WHY — how many reachable
    // downstream models we couldn't analyze and the root cause. A model is unanalyzable
    // only when we have no columns to trace: either its SQL couldn't be parsed, or it
    // exposes no column-level information (absent from the catalog with no parseable
    // compiled SQL — e.g. a non-table relation such as a semantic view). The cause is
    // bolded so it stands out. Note: we do NOT claim these are "not built" — a model can
    // be built yet still absent from the catalog (deferred docs, semantic views, ...).
    function confidenceReasonHtml(confidence) {
        const noColumnInfo = confidence.no_column_info || 0;
        const parseFailed = confidence.parse_failed || 0;
        if (noColumnInfo && parseFailed) {
            return `<strong>their SQL couldn't be parsed, or they expose no column-level information</strong> `
                + `(${parseFailed} unparseable, ${noColumnInfo} without a column catalog)`;
        }
        if (parseFailed) {
            return `<strong>their SQL couldn't be parsed</strong>`;
        }
        return `<strong>they expose no column-level information</strong>, so they're absent from the catalog with no parseable compiled SQL`;
    }

    // Drill-down panel: explains the catalog → column-lineage mechanism, lists the models
    // we could only trace at the model level, and shows how to close the gap.
    function confidenceWhyPanelHtml(confidence, sourceModel) {
        const noColumnInfo = confidence.no_column_info_models || [];
        const unparseable = confidence.parse_failed_models || [];
        const items = noColumnInfo
            .map(m => ({ name: m, tag: 'no column info' }))
            .concat(unparseable.map(m => ({ name: m, tag: 'unparseable' })));
        if (!items.length) {
            return '';
        }
        const total = confidence.unanalyzable_models || items.length;
        // The JSON confidence block carries the complete lists; cap the rendered names
        // here so a wide coverage gap stays readable, and show how many were elided.
        const DISPLAY_CAP = 100;
        const shown = items.slice(0, DISPLAY_CAP);
        const more = Math.max(0, items.length - shown.length);
        const listHtml = shown.map(it =>
            `<li><span class="confidence-why-model">${escapeHtml(it.name)}</span>`
            + `<span class="confidence-why-tag">${it.tag}</span></li>`
        ).join('');

        // Remediation: building + regenerating the catalog closes the no-column-info gap
        // for ordinary relations. Parse failures (and non-table relations like semantic
        // views) are a different problem, so only surface the command when building helps.
        let fixHtml = '';
        if (noColumnInfo.length && sourceModel) {
            const cmd = `dbt run --select ${sourceModel}+ --empty && dbt docs generate`;
            fixHtml = `
                <div class="confidence-why-fix">
                    <div class="confidence-why-list-heading">How to close this gap</div>
                    <p class="confidence-why-explain">
                        Build the downstream models so they land in the catalog, then refresh it.
                        <code>--empty</code> creates the table structure without loading data — fast and cheap:
                    </p>
                    <div class="confidence-why-cmd">
                        <code>${escapeHtml(cmd)}</code>
                        <button type="button" class="confidence-why-copy" data-copy="${escapeHtml(cmd)}">Copy</button>
                    </div>
                </div>
            `;
        }

        return `
            <button type="button" class="confidence-why-toggle" aria-expanded="false">
                Why? Show the ${total} model-level-only model${total !== 1 ? 's' : ''}
            </button>
            ${fixHtml}
            <div class="confidence-why-panel" hidden>
                <p class="confidence-why-explain">
                    Column-level lineage is traced from each model's columns in
                    <code>catalog.json</code>, which only records tables actually built in the
                    warehouse. Models not yet built fall back to <strong>model-level</strong>
                    lineage from the manifest DAG — we know they sit downstream, but not whether
                    <em>this column</em> flows into them.
                </p>
                <div class="confidence-why-list-heading">Traced at model level only:</div>
                <ul class="confidence-why-list">${listHtml}</ul>
                ${more > 0 ? `<div class="confidence-why-more">+${more} more not shown</div>` : ''}
            </div>
        `;
    }

    function confidenceBadgeHtml(confidence, sourceModel) {
        if (!confidence || !confidence.level) {
            return '';
        }
        const reachable = confidence.reachable_models || 0;
        const isFull = confidence.level === 'full';
        const ofModel = sourceModel ? ` of <code>${escapeHtml(sourceModel)}</code>` : '';

        let label, detailHtml, whyHtml = '';
        if (isFull) {
            label = 'Complete impact';
            detailHtml = reachable === 0
                ? `No models sit downstream${ofModel}.`
                : `Every one of the ${reachable} model${reachable !== 1 ? 's' : ''} downstream${ofModel} could be analyzed, so nothing is missing below.`;
        } else {
            const unanalyzable = confidence.unanalyzable_models || 0;
            label = 'Lower bound — impact may be larger';
            detailHtml = `${unanalyzable} of ${reachable} model${reachable !== 1 ? 's' : ''} downstream${ofModel} couldn't be checked because ${confidenceReasonHtml(confidence)}.`;
            whyHtml = confidenceWhyPanelHtml(confidence, sourceModel);
        }

        const icon = isFull
            ? `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"></path><polyline points="22 4 12 14.01 9 11.01"></polyline></svg>`
            : `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>`;
        return `
            <div class="confidence-badge confidence-${isFull ? 'full' : 'partial'}">
                <div class="confidence-badge-row">
                    <span class="confidence-badge-icon">${icon}</span>
                    <div class="confidence-badge-text">
                        <span class="confidence-badge-label">${label}</span>
                        <span class="confidence-badge-detail">${detailHtml}</span>
                        ${whyHtml}
                    </div>
                </div>
            </div>
        `;
    }

    // One-line coverage statement, mirroring format_coverage_line() in the CLI.
    function coverageLineHtml(coverage) {
        if (!coverage) {
            return '';
        }
        const manifest = coverage.models_in_manifest || 0;
        const catalog = coverage.models_in_catalog || 0;
        let text;
        if (coverage.complete) {
            text = `Coverage: ${catalog}/${manifest} models, complete.`;
        } else {
            text = `Coverage: analyzed ${coverage.parsed_ok || 0}/${manifest} models `
                + `(${catalog} in catalog; ${coverage.not_in_catalog_count || 0} not in catalog, `
                + `${coverage.parse_failed || 0} parse-failed, ${coverage.skipped_no_sql || 0} no compiled SQL). `
                + `Impact counts are a lower bound.`;
        }
        // Cross-boundary honesty: when a Metabase artifact was joined, add a second
        // muted line about BI reach (column-precise vs table-only, staleness). Absent → skip.
        let metabaseLine = '';
        const mb = coverage.metabase;
        if (mb && mb.level && mb.level !== 'absent') {
            const precise = mb.cards_column_precise || 0;
            const tableOnly = mb.cards_table_only || 0;
            const reached = mb.dashboards_reached || 0;
            const staleTxt = mb.stale ? ', snapshot stale' : '';
            const mbText = `Metabase: ${reached} dashboard${reached !== 1 ? 's' : ''} reached `
                + `(${precise} column-precise, ${tableOnly} table-only${staleTxt}).`;
            metabaseLine = `
                <div class="coverage-line coverage-line-metabase ${mb.level === 'partial' || mb.stale ? 'coverage-line-partial' : ''}">
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>
                    <span>${escapeHtml(mbText)}</span>
                </div>`;
        }
        return `
            <div class="coverage-line ${coverage.complete ? '' : 'coverage-line-partial'}">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="12" y1="16" x2="12" y2="12"></line><line x1="12" y1="8" x2="12.01" y2="8"></line></svg>
                <span>${escapeHtml(text)}</span>
            </div>
            ${metabaseLine}
        `;
    }

    function displayImpactAnalysis(data, modelName, columnName) {
        const impactContent = document.getElementById('impactAnalysisContent');
        const summary = data.summary || {};
        const affectedModels = data.affected_models || [];
        const affectedColumns = data.affected_columns || [];
        const affectedExposures = data.affected_exposures || [];

        const criticalColumns = affectedColumns.filter(col => col.severity === 'critical');
        const lowImpactColumns = affectedColumns.filter(col => col.severity === 'low_impact');

        // Whole-change policy verdict pins ABOVE everything (decision first). Owned by the
        // Policy module so the encoding lives in one place; absent → nothing rendered.
        // The backend attaches policy_verdict ONLY for a column that is part of the reviewed
        // change, so this banner never speaks for an arbitrary explored column;
        // an out-of-change column gets a neutral "not part of this change" note instead.
        const policyBanner = (window.PolicyModule && data.policy_verdict)
            ? window.PolicyModule.bannerHtml(data.policy_verdict)
            : notInChangesetNoteHtml(data);
        // A block/warn verdict owns the primary spark, so the confidence badge steps down to
        // a muted treatment — avoids two amber blocks stacking.
        const verdictDecision = data.policy_verdict && data.policy_verdict.decision;
        const confidenceMuted = verdictDecision === 'block' || verdictDecision === 'warn';

        let html = `
            <div class="impact-hero">
                ${policyBanner}
                <h2 class="impact-question">What breaks if you change <code>${escapeHtml(columnName)}</code>?</h2>
                <p class="impact-question-sub">in <code>${escapeHtml(modelName)}</code></p>
                ${subjectSemanticChipHtml(data)}
                <div class="${confidenceMuted ? 'confidence-muted' : ''}">${confidenceBadgeHtml(data.confidence, modelName)}</div>
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
                            <div class="hero-metric-desc">Downstream value recomputed</div>
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
                                <p class="section-description">${criticalColumns.length} downstream column${criticalColumns.length !== 1 ? 's' : ''} whose value is recomputed from the change</p>
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
                                <span class="model-group-count">${modelColumns.length} recomputed column${modelColumns.length !== 1 ? 's' : ''}</span>
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
                                    ${semanticBadgeHtml(col)}
                                </div>
                                <span class="transformation-badge critical-transformation">${transformationType}</span>
                            </div>
                            ${col.description ? `
                                <p class="column-card-description">${escapeHtml(col.description)}</p>
                            ` : ''}
                            ${testGuardrailsHtml(col.tests)}
                            ${col.sql_expression ? `
                                <div class="column-card-body">
                                    <div class="sql-expression-card">
                                        <div class="sql-expression-label">Expression</div>
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
                // Cross-boundary Metabase dashboards render as a distinct "outside dbt" card.
                if (exposure.source === 'metabase') {
                    html += metabaseExposureCardHtml(exposure);
                    return;
                }
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
                            ${model.description ? `
                                <p class="model-card-description">${escapeHtml(model.description)}</p>
                            ` : ''}
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
                                ${semanticBadgeHtml(col)}
                            </div>
                            <span class="transformation-badge low-impact-transformation">${transformationType}</span>
                        </div>
                        ${col.description ? `
                            <p class="column-card-description">${escapeHtml(col.description)}</p>
                        ` : ''}
                        ${testGuardrailsHtml(col.tests)}
                    </div>
                `;
            });

            html += `</div></div>`;
        }

        impactContent.innerHTML = html;

        // Let the Policy module wire its banner (open the full Policy panel on click).
        if (window.PolicyModule && data.policy_verdict) {
            window.PolicyModule.bindBanner(impactContent, data.policy_verdict);
        }

        // Coverage is artifact-wide; append it once the (memoized) fetch resolves so it
        // never blocks rendering the impact analysis itself.
        fetchCoverage().then(coverage => {
            const line = coverageLineHtml(coverage);
            if (line && impactContent.querySelector('.impact-hero')) {
                const footer = document.createElement('div');
                footer.className = 'impact-coverage-footer';
                footer.innerHTML = line;
                impactContent.appendChild(footer);
            }
        });

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

    function displayRelationshipSummary(summary, container, sourceModel) {
        if (!summary || typeof summary !== 'object') {
            return;
        }

        const transformations = summary.critical_count || 0;
        const passThrough = summary.low_impact_count || 0;
        const relatedExposures = summary.affected_exposures || 0;
        const relatedModels = summary.affected_models || 0;
        // Compact confidence pip beside the Related-Exposures tile: full analysis reads
        // neutral + an indigo check; a partial (lower-bound) analysis reads as an amber dot
        // (it shares the ONE caution slot — an incomplete analysis IS a caution, not an error).
        const confidenceLevel = summary.confidence && summary.confidence.level;
        let exposurePip = '';
        if (confidenceLevel === 'full') {
            exposurePip = `<span class="summary-confidence-pip pip-full" title="Complete impact — every downstream model could be analyzed"><svg width="10" height="10" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg></span>`;
        } else if (confidenceLevel === 'partial') {
            exposurePip = `<span class="summary-confidence-pip pip-partial" title="Lower bound — some downstream models couldn’t be analyzed, so real reach may be larger"></span>`;
        }

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
                    <div class="summary-metric-label">Recomputed</div>
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
                    <div class="summary-metric-value">${relatedExposures}${exposurePip}</div>
                    <div class="summary-metric-label">Related Exposures</div>
                </div>
            </div>
            ${confidenceBadgeHtml(summary.confidence, sourceModel)}
        `;
    }

    function setupImpactPanel() {
        const impactPanel = document.getElementById('impactAnalysisPanel');
        const impactContent = document.getElementById('impactAnalysisContent');
        const closeImpactPanel = document.getElementById('closeImpactPanel');
        const graphContainer = document.getElementById('graph');

        closeImpactPanel.addEventListener('click', function() {
            impactPanel.style.display = 'none';
            // Restore the floating relationship-summary card that was hidden while
            // the panel (which shows the same metrics) was open.
            const relCard = document.getElementById('relationshipSummaryCard');
            if (relCard && relCard.dataset.hiddenByImpact === 'true') {
                relCard.style.display = 'block';
                delete relCard.dataset.hiddenByImpact;
            }
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
                            // Hide the floating relationship-summary card: it shows the same
                            // metrics and otherwise sits under (or bleeds past) the panel.
                            const relCard = document.getElementById('relationshipSummaryCard');
                            if (relCard && getComputedStyle(relCard).display !== 'none') {
                                relCard.dataset.hiddenByImpact = 'true';
                                relCard.style.display = 'none';
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

    function setupCardCloseButtons() {
        document.getElementById('graph')?.addEventListener('click', function(e) {
            // Handle collapse toggle for relationship summary
            const toggleBtn = e.target.closest('#collapseRelationshipSummaryCard, #toggleRelationshipSummary');
            if (toggleBtn) {
                const card = document.getElementById('relationshipSummaryCard');
                if (card) card.classList.toggle('collapsed');
                return;
            }

            // Handle close buttons for other cards
            const closeBtn = e.target.closest('button[id^="close"]');
            if (!closeBtn) return;

            const cardMap = {
                'closeImpactAnalysisCard': 'impactAnalysisCard'
            };

            const card = document.getElementById(cardMap[closeBtn.id]);
            if (card) card.style.display = 'none';
        });
    }

    // Expand/collapse the "why couldn't these be checked" drill-down. Delegated on
    // document because the badge is injected into two different containers (the floating
    // summary card and the impact panel).
    function setupConfidenceWhyToggle() {
        document.addEventListener('click', function(e) {
            const copyBtn = e.target.closest('.confidence-why-copy');
            if (copyBtn) {
                const cmd = copyBtn.getAttribute('data-copy') || '';
                const done = () => {
                    const prev = copyBtn.textContent;
                    copyBtn.textContent = 'Copied!';
                    copyBtn.classList.add('copied');
                    setTimeout(() => {
                        copyBtn.textContent = prev;
                        copyBtn.classList.remove('copied');
                    }, 1500);
                };
                if (navigator.clipboard && navigator.clipboard.writeText) {
                    navigator.clipboard.writeText(cmd).then(done).catch(() => {});
                }
                return;
            }
            const toggle = e.target.closest('.confidence-why-toggle');
            if (!toggle) return;
            const panel = toggle.parentElement.querySelector('.confidence-why-panel');
            if (!panel) return;
            const willShow = panel.hasAttribute('hidden');
            if (willShow) {
                panel.removeAttribute('hidden');
            } else {
                panel.setAttribute('hidden', '');
            }
            toggle.setAttribute('aria-expanded', String(willShow));
            toggle.classList.toggle('is-open', willShow);
        });
    }

    function init() {
        setupImpactPanel();
        setupCardCloseButtons();
        setupConfidenceWhyToggle();
    }

    return {
        init,
        displayImpactAnalysis,
        displayRelationshipSummary
    };
})();
