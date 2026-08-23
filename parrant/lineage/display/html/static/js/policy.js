// Policy panel — the flagship "decision engine" surface.
//
// The metadata-agnostic policy engine returns a whole-change verdict
// (report["policy_verdict"]): a decision (block / warn / allow), the rules that
// fired (subject change + matched reach + actions), the build/test sets, and any
// notifications. The graph answers "what is connected"; this panel answers "what
// the gate decided".
//
// Amber discipline (DESIGN.md): a policy `block` is the ONE red escalation — the
// single hardest signal. `warn` shares the one amber caution axis with semantic-
// breaking and partial-confidence (they never co-fire competing hues). `allow`
// reads neutral slate + an indigo check. No new hue is introduced.
const PolicyModule = (function () {
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text == null ? '' : String(text);
        return div.innerHTML;
    }

    function isActive(verdict) {
        return !!verdict && verdict.decision && verdict.decision !== null;
    }

    // Decision → { cls, label, icon } on the block=red / warn=amber / allow=neutral axis.
    function decisionChrome(decision) {
        if (decision === 'block') {
            return {
                cls: 'policy-block',
                label: 'BLOCK',
                icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"></circle><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"></line></svg>'
            };
        }
        if (decision === 'warn') {
            return {
                cls: 'policy-warn',
                label: 'WARN',
                icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"></path><line x1="12" y1="9" x2="12" y2="13"></line><line x1="12" y1="17" x2="12.01" y2="17"></line></svg>'
            };
        }
        return {
            cls: 'policy-allow',
            label: 'ALLOW',
            icon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>'
        };
    }

    // The pinned banner shown at the top of the Impact panel (and clickable to open the
    // full Policy panel). Returns '' when no policy resolved so nothing new renders.
    function bannerHtml(verdict) {
        if (!isActive(verdict)) return '';
        const chrome = decisionChrome(verdict.decision);
        const fired = verdict.fired_rules || (verdict.hits ? verdict.hits.length : 0);
        const detail = verdict.decision === 'allow'
            ? 'No rule fired — the gate would pass.'
            : `${fired} rule${fired !== 1 ? 's' : ''} fired`;
        return `
            <div class="policy-banner ${chrome.cls}" role="button" tabindex="0" data-policy-open="1">
                <span class="policy-banner-icon">${chrome.icon}</span>
                <span class="policy-banner-verdict">POLICY: ${chrome.label}</span>
                <span class="policy-banner-detail">${escapeHtml(detail)}</span>
                <span class="policy-banner-cta">View policy →</span>
            </div>`;
    }

    function chipListHtml(items) {
        if (!items || items.length === 0) {
            return '<span class="policy-empty">—</span>';
        }
        return items.map(function (name) {
            return `<span class="policy-chip">${escapeHtml(name)}</span>`;
        }).join('');
    }

    function ruleHitHtml(hit) {
        const chrome = decisionChrome(hit.decision);
        const subject = (hit.change_model && hit.change_column)
            ? `${escapeHtml(hit.change_model)}.${escapeHtml(hit.change_column)}`
            : (hit.change_model ? escapeHtml(hit.change_model) : 'change-wide');
        const actions = (hit.actions || []).map(function (a) {
            return `<span class="policy-action">${escapeHtml(a)}</span>`;
        }).join('');
        const reach = (hit.matched_reach && hit.matched_reach.length)
            ? `<div class="policy-rule-reach">
                   <span class="policy-eyebrow">Matched reach</span>
                   <div class="policy-chip-row">${chipListHtml(hit.matched_reach)}</div>
               </div>`
            : '';
        return `
            <div class="policy-rule-card ${chrome.cls}">
                <div class="policy-rule-head">
                    <span class="policy-rule-id">${escapeHtml(hit.rule_id)}</span>
                    <span class="policy-rule-decision ${chrome.cls}">${chrome.label}</span>
                </div>
                <div class="policy-rule-subject">
                    <span class="policy-eyebrow">Subject</span>
                    <code>${subject}</code>
                </div>
                ${reach}
                ${actions ? `<div class="policy-rule-actions">${actions}</div>` : ''}
            </div>`;
    }

    function setListHtml(kind, items) {
        const cmd = items && items.length
            ? `dbt ${kind === 'build' ? 'build' : 'test'} --select ${items.join(' ')}`
            : '';
        return `
            <div class="policy-set">
                <div class="policy-set-head">
                    <span class="policy-eyebrow">${kind === 'build' ? 'Build set' : 'Test set'}</span>
                    ${cmd ? `<button type="button" class="policy-copy-btn" data-copy="${escapeHtml(cmd)}">Copy ${kind} command</button>` : ''}
                </div>
                <div class="policy-chip-row">${chipListHtml(items)}</div>
            </div>`;
    }

    function notificationsHtml(notifications) {
        if (!notifications || notifications.length === 0) return '';
        const rows = notifications.map(function (n) {
            return `
                <div class="policy-notification">
                    <span class="policy-channel">${escapeHtml(n.channel)}</span>
                    <span class="policy-target">${escapeHtml(n.target)}</span>
                    <span class="policy-message">${escapeHtml(n.message)}</span>
                </div>`;
        }).join('');
        return `
            <div class="policy-section">
                <div class="policy-eyebrow">Notifications</div>
                ${rows}
            </div>`;
    }

    function panelInnerHtml(verdict) {
        const chrome = decisionChrome(verdict.decision);
        const fired = verdict.fired_rules || (verdict.hits ? verdict.hits.length : 0);
        const evaluated = verdict.evaluated_rules || 0;
        const hits = (verdict.hits || []).map(ruleHitHtml).join('');
        const honesty = [];
        if (verdict.unresolved_reach_count) {
            honesty.push(`${verdict.unresolved_reach_count} unresolved reach`);
        }
        if (verdict.skipped_missing_meta) {
            honesty.push(`${verdict.skipped_missing_meta} skipped (missing meta)`);
        }
        return `
            <div class="policy-panel-header ${chrome.cls}">
                <div class="policy-panel-title-group">
                    <span class="policy-panel-icon">${chrome.icon}</span>
                    <div>
                        <div class="policy-eyebrow">Policy gate</div>
                        <h2 class="policy-panel-title">${chrome.label}</h2>
                    </div>
                </div>
                <button type="button" class="policy-panel-close" data-policy-close="1" aria-label="Close policy panel">&times;</button>
            </div>
            <div class="policy-panel-body">
                <p class="policy-panel-summary">${fired} of ${evaluated} rule${evaluated !== 1 ? 's' : ''} fired${honesty.length ? ` · ${escapeHtml(honesty.join(' · '))}` : ''}.</p>
                ${hits ? `<div class="policy-section"><div class="policy-eyebrow">Fired rules</div>${hits}</div>` : '<p class="policy-empty">No rule fired.</p>'}
                <div class="policy-section policy-sets">
                    ${setListHtml('build', verdict.build_set)}
                    ${setListHtml('test', verdict.test_set)}
                </div>
                ${notificationsHtml(verdict.notifications)}
            </div>`;
    }

    function ensureOverlay() {
        let overlay = document.getElementById('policyPanelOverlay');
        if (!overlay) {
            overlay = document.createElement('div');
            overlay.id = 'policyPanelOverlay';
            overlay.className = 'policy-overlay';
            overlay.style.display = 'none';
            const panel = document.createElement('div');
            panel.id = 'policyPanel';
            panel.className = 'policy-panel';
            overlay.appendChild(panel);
            document.body.appendChild(overlay);
            overlay.addEventListener('click', function (e) {
                if (e.target === overlay || e.target.closest('[data-policy-close]')) {
                    closePanel();
                }
            });
            overlay.addEventListener('click', function (e) {
                const copyBtn = e.target.closest('.policy-copy-btn');
                if (copyBtn) {
                    const text = copyBtn.getAttribute('data-copy');
                    if (navigator.clipboard) {
                        navigator.clipboard.writeText(text).then(function () {
                            const prev = copyBtn.textContent;
                            copyBtn.textContent = 'Copied';
                            copyBtn.classList.add('is-copied');
                            setTimeout(function () {
                                copyBtn.textContent = prev;
                                copyBtn.classList.remove('is-copied');
                            }, 1400);
                        });
                    }
                }
            });
        }
        return overlay;
    }

    function openPanel(verdict) {
        if (!isActive(verdict)) return;
        const overlay = ensureOverlay();
        const panel = overlay.querySelector('#policyPanel');
        panel.className = 'policy-panel ' + decisionChrome(verdict.decision).cls;
        panel.innerHTML = panelInnerHtml(verdict);
        overlay.style.display = 'flex';
    }

    function closePanel() {
        const overlay = document.getElementById('policyPanelOverlay');
        if (overlay) overlay.style.display = 'none';
    }

    // Wire the pinned banner (inside the impact panel) to open the full panel.
    function bindBanner(container, verdict) {
        if (!container || !isActive(verdict)) return;
        const banner = container.querySelector('[data-policy-open]');
        if (!banner) return;
        banner.addEventListener('click', function () { openPanel(verdict); });
        banner.addEventListener('keydown', function (e) {
            if (e.key === 'Enter' || e.key === ' ') {
                e.preventDefault();
                openPanel(verdict);
            }
        });
    }

    document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') closePanel();
    });

    return {
        bannerHtml: bannerHtml,
        bindBanner: bindBanner,
        openPanel: openPanel,
        closePanel: closePanel,
        isActive: isActive
    };
})();

window.PolicyModule = PolicyModule;
