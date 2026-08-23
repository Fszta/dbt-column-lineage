/**
 * Utility functions for graph visualization
 */

// Icon function for different model types
function getModelIcon(modelType) {
    if (!modelType || modelType === 'undefined') {
        modelType = 'model'; // Default to model if no type specified
    }

    const icons = {
        source: " 5a9 3 0 1 0 18 0a9 3 0 1 0 -18 0 5v14a9 3 0 0 0 18 0V5 12a9 3 0 0 0 18 0",
        seed: "M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z M14 2v6h6",
        model: "M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z M12 22.5v-9.3 7l8.7 5 8.7-5",
        test: "M9 11a2 2 0 1 1 0-4 2 2 0 0 1 0 4z M13 18a2 2 0 1 0 0-4 2 2 0 0 0 0 4z M20 4a2 2 0 1 0 0 4 2 2 0 0 0 0-4z 20a2 2 0 1 0 0-4 2 2 0 0 0 0-4z",
        exposure: "M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z M14 2v6h6 M16 13H8 M16 17H8 M10 9H8",
        // Monitor/BI glyph for reached Metabase dashboards (screen + stand + a mini bar chart).
        dashboard: " 4h18a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V5a1 1 0 0 1 1-1z M8 20h8 M12 16v4 M7 12v-2 M11 12V8 M15 12v-4",
        snapshot: "M12 3a9 9 0 1 0 9 9 9 9 0 0 0-9-9zm0 16a7 7 0 1 1 7-7 7 7 0 0 1-7 7zm0-9a2 2 0 1 0 2 2 2 2 0 0 0-2-2z",
        default: "M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z M12 22.5v-9.3 7l8.7 5 8.7-5"
    };

    return icons[modelType] || icons.model; // Default to model icon if type not recognized
}

// The official Metabase logomark (source: simple-icons, CC0 — free for any use, no attribution
// required). It is a single FILL path (the brand's dot-grid mark) on a 0 0 24 24 viewBox, so it
// tints cleanly to `currentColor` / `--primary`. We render it MONOCHROME (indigo), never in the
// Metabase brand blue, to honor DESIGN.md's single-hue rule — the recognizable shape carries the
// "this is Metabase" meaning without introducing a new colour.
const METABASE_LOGO_PATH = ".385 6.136c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-1.438 2.63c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461zm5.465-2.63c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.499-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm-1.088 5.592c.794 0 1.438-.654 1.438-1.461s-.644-1.461-1.438-1.461-1.438.654-1.438 1.461.643 1.461 1.438 1.461zm5.464-5.592c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111S11.4 7.247 12 7.247s1.088-.498 1.088-1.111zm.35-4.675c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461S11.206 0 12 0s1.438.654 1.438 1.461zm-.35 0C13.088.848 12.6.35 12.35s-1.088.498-1.088 1.111S11.4 2.572 12 2.572s1.088-.498 1.088-1.111zm.35 8.806c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.499 1.088-1.111zm4.376-4.131c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm2.939 1.461c.794 0 1.438-.654 1.438-1.461s-.644-1.461-1.438-1.461-1.438.654-1.438 1.461.644 1.461 1.438 1.461zm-4.027 1.209c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.643-1.461-1.438-1.461zm4.027 0c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461zM3.947 12.857a1.45 1.45 0 0 0-1.438 1.461c0.807.644 1.461 1.438 1.461s1.438-.654 1.438-1.461a1.45 1.45 0 0 0-1.438-1.461zm5.465 1.5c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.655 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zM12 12.896c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461zm5.464 1.461c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.655 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm2.939-1.461c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461zM3.947 16.948c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461zm5.465 1.5c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm4.376 0c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm.35 4.091c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111S11.4 23.65 12 23.65s1.088-.498 1.088-1.111zm4.376-4.091c0.807-.644 1.461-1.438 1.461s-1.438-.654-1.438-1.461.644-1.461 1.438-1.461 1.438.654 1.438 1.461zm-.35 0c0-.613-.488-1.111-1.088-1.111s-1.088.498-1.088 1.111.488 1.111 1.088 1.111 1.088-.498 1.088-1.111zm2.939-1.461c-.794 0-1.438.654-1.438 1.461s.644 1.461 1.438 1.461 1.438-.654 1.438-1.461-.644-1.461-1.438-1.461z";

// Inline monochrome Metabase logomark as an HTML <svg> string (fills with currentColor so it
// inherits the surrounding indigo). Used in the impact-panel card and the graph hover card.
function metabaseLogoSvg(size) {
    const s = size || 13;
    // Size is pinned via inline style (not just width/height attrs) so a broad `svg` rule can't
    // stretch it to fill its flex container.
    return '<svg class="metabase-logo" width="' + s + '" height="' + s + '" viewBox="0 0 24 24" '
        + 'fill="currentColor" aria-hidden="true" focusable="false" '
        + 'style="width:' + s + 'px;height:' + s + 'px;flex:0 0 auto;display:inline-block">'
        + '<path d="' + METABASE_LOGO_PATH + '"/></svg>';
}


// Get color for data type tags - muted, professional palette
function getTagColor(type) {
    const typeStr = type.toLowerCase();
    const defaultColor = '#94a3b8';

    // Numeric types - blue
    if (typeStr.includes('int') || typeStr.includes('decimal') ||
        typeStr.includes('numeric') || typeStr.includes('double') ||
        typeStr.includes('float')) {
        return '#3b82f6';
    }

    // String types - emerald
    if (typeStr.includes('varchar') || typeStr.includes('char') ||
        typeStr.includes('text') || typeStr.includes('string')) {
        return '#10b981';
    }

    // Date/time types - violet
    if (typeStr.includes('date') || typeStr.includes('time')) {
        return '#8b5cf6';
    }

    // Boolean types - rose
    if (typeStr.includes('bool')) {
        return '#f43f5e';
    }

    // Variant/json types - amber
    if (typeStr.includes('variant') || typeStr.includes('json') || typeStr.includes('object')) {
        return '#f59e0b';
    }

    return defaultColor;
}

function createEdgePath(d, state, config) {
    if (!d || !d.source || !d.target) return '';

    const sourcePos = state.columnPositions.get(d.source);
    const targetPos = state.columnPositions.get(d.target);

    if (!sourcePos || !targetPos) return '';

    const sourceNode = state.nodeIndex.get(d.source);
    const targetNode = state.nodeIndex.get(d.target);

    if (!sourceNode || !targetNode) return '';

    const sourceModelName = sourceNode.model;
    const targetModelName = targetNode.model;
    const sourceModel = state.models.find(m => m && m.name === sourceModelName);
    const targetModel = state.models.find(m => m && m.name === targetModelName);

    if (!sourceModel || !targetModel) return '';

    // Check for valid positions
    if (typeof sourceModel.x !== 'number' || isNaN(sourceModel.x) ||
        typeof sourceModel.y !== 'number' || isNaN(sourceModel.y) ||
        typeof targetModel.x !== 'number' || isNaN(targetModel.x) ||
        typeof targetModel.y !== 'number' || isNaN(targetModel.y)) {
        return '';
    }

    const leftToRight = sourceModel.x < targetModel.x;

    let sourceX, sourceY, targetX, targetY;

    sourceX = sourceModel.x + (leftToRight ? config.box.width - config.box.padding : config.box.padding);

    if (sourceModel.columnsCollapsed) {
        sourceY = sourceModel.y - sourceModel.height/2 + config.box.titleHeight + 14;
    } else {
        sourceY = sourcePos.y;
    }

    targetX = targetModel.x + (leftToRight ? config.box.padding : config.box.width - config.box.padding);

    if (targetModel.columnsCollapsed) {
        targetY = targetModel.y - targetModel.height/2 + config.box.titleHeight + 14;
    } else {
        targetY = targetPos.y;
    }

    if (typeof sourceX !== 'number' || isNaN(sourceX) ||
        typeof sourceY !== 'number' || isNaN(sourceY) ||
        typeof targetX !== 'number' || isNaN(targetX) ||
        typeof targetY !== 'number' || isNaN(targetY)) {
        return '';
    }

    const dx = Math.abs(targetX - sourceX);
    const controlX1 = sourceX + (leftToRight ? dx * 0.4 : -dx * 0.4);
    const controlX2 = targetX + (leftToRight ? -dx * 0.4 : dx * 0.4);

    return `M${sourceX},${sourceY}
            C${controlX1},${sourceY}
             ${controlX2},${targetY}
             ${targetX},${targetY}`;
}

function createExposureEdgePath(d, state, config) {
    if (!d || !d.source || !d.target) return '';

    const sourceNode = state.nodeIndex.get(d.source);
    const targetNode = state.nodeIndex.get(d.target);

    if (!sourceNode || !targetNode) return '';

    let sourceX, sourceY, targetX, targetY;

    if (sourceNode.type === 'column') {
        const sourceModelName = sourceNode.model;
        const sourceModel = state.models.find(m => m && m.name === sourceModelName);
        if (!sourceModel) return '';

        const sourcePos = state.columnPositions.get(d.source);
        if (!sourcePos || typeof sourcePos.x !== 'number' || isNaN(sourcePos.x) ||
            typeof sourcePos.y !== 'number' || isNaN(sourcePos.y)) return '';

        sourceX = sourceModel.x + config.box.width - config.box.padding;
        sourceY = sourceModel.columnsCollapsed
            ? sourceModel.y - sourceModel.height/2 + config.box.titleHeight + 14
            : sourcePos.y;
    } else {
        return '';
    }

    if (targetNode.type === 'exposure') {
        const exposure = state.exposures.find(e => e && e.name === targetNode.model);
        if (!exposure || typeof exposure.x !== 'number' || isNaN(exposure.x) ||
            typeof exposure.y !== 'number' || isNaN(exposure.y) ||
            typeof exposure.height !== 'number' || isNaN(exposure.height)) return '';
        targetX = exposure.x + config.box.padding;
        targetY = exposure.y;
    } else {
        return '';
    }

    if (typeof sourceX !== 'number' || isNaN(sourceX) ||
        typeof sourceY !== 'number' || isNaN(sourceY) ||
        typeof targetX !== 'number' || isNaN(targetX) ||
        typeof targetY !== 'number' || isNaN(targetY)) {
        return '';
    }

    const dx = Math.abs(targetX - sourceX);
    const controlX1 = sourceX + dx * 0.4;
    const controlX2 = targetX - dx * 0.4;

    return `M${sourceX},${sourceY}
            C${controlX1},${sourceY}
             ${controlX2},${targetY}
             ${targetX},${targetY}`;
}

let tooltip = null;

function createTooltip() {
    if (tooltip) return tooltip;

    tooltip = d3.select('body')
        .append('div')
        .attr('class', 'column-tooltip')
        .style('position', 'absolute')
        .style('opacity', 0)
        .style('pointer-events', 'none')
        .style('background', '#1e293b')
        .style('color', 'white')
        .style('padding', '5px 8px')
        .style('border-radius', '4px')
        .style('font-size', '12px')
        .style('font-family', "'JetBrains Mono', monospace")
        .style('white-space', 'nowrap')
        .style('z-index', '10000')
        .style('box-shadow', '0 2px 8px rgba(0, 0, 0, 0.15)');

    return tooltip;
}

function showTooltip(event, text) {
    const tooltip = createTooltip();

    // Get coordinates - handle both MouseEvent and D3 event objects
    let x, y;
    if (event.pageX !== undefined && event.pageY !== undefined) {
        x = event.pageX;
        y = event.pageY;
    } else if (event.clientX !== undefined && event.clientY !== undefined) {
        x = event.clientX + window.scrollX;
        y = event.clientY + window.scrollY;
    } else {
        // Fallback - try to get from sourceEvent
        const sourceEvent = event.sourceEvent || event;
        x = (sourceEvent.pageX || sourceEvent.clientX || 0) + (window.scrollX || 0);
        y = (sourceEvent.pageY || sourceEvent.clientY || 0) + (window.scrollY || 0);
    }

    tooltip
        .text(text)
        .style('left', (x + 10) + 'px')
        .style('top', (y - 10) + 'px')
        .transition()
        .duration(200)
        .style('opacity', 1);
}

function hideTooltip() {
    if (tooltip) {
        tooltip.transition()
            .duration(200)
            .style('opacity', 0);
    }
}

// --- Minimal, dependency-free Markdown renderer -------------------------------------------
// dbt descriptions are authored as Markdown most of the time; render the common subset
// (headings, bold/italic, inline + fenced code, links, ordered/unordered lists, paragraphs)
// as HTML. Self-contained on purpose: the explorer ships no external JS and runs offline.
// Input is HTML-escaped FIRST, so raw HTML/scripts in a description can never execute; only
// the tags this renderer emits are produced. Returns an HTML string for `.markdown-body`.

function escapeHtml(text) {
    return String(text)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Only allow safe link schemes (block javascript:, data:, etc.). Accepts http(s), mailto,
// and relative/anchor links. `url` is already HTML-escaped by the caller.
function safeHref(url) {
    const raw = url.trim();
    if (/^(https?:\/\/|mailto:|\/|#|\.\/|\.\.\/)/i.test(raw)) return raw;
    return null;
}

function renderInlineMarkdown(text) {
    // `text` is already HTML-escaped. Protect inline code spans from other transforms by
    // stashing them behind placeholders, transform the rest, then restore.
    const codeSpans = [];
    let s = text.replace(/`([^`]+)`/g, function (_, code) {
        codeSpans.push(code);
        return '\u0000' + (codeSpans.length - 1) + '\u0000';
    });

    // Links: [label](url) — validate the scheme.
    s = s.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, function (m, label, url) {
        const href = safeHref(url);
        if (!href) return label;
        return '<a href="' + href + '" target="_blank" rel="noopener noreferrer">' + label + '</a>';
    });

    // Bold before italic so ** / __ win over single * / _.
    s = s.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    s = s.replace(/__([^_]+)__/g, '<strong>$1</strong>');
    s = s.replace(/\*([^*\n]+)\*/g, '<em>$1</em>');
    s = s.replace(/(^|[^\w])_([^_\n]+)_(?=[^\w]|$)/g, '$1<em>$2</em>');

    // Restore code spans.
    s = s.replace(/\u0000(\d+)\u0000/g, function (_, i) {
        return '<code>' + codeSpans[Number(i)] + '</code>';
    });
    return s;
}

function renderMarkdown(text) {
    if (!text) return '';
    const lines = escapeHtml(text).replace(/\r\n?/g, '\n').split('\n');
    const html = [];
    let i = 0;
    let paragraph = [];
    let listType = null; // 'ul' | 'ol' | null

    const flushParagraph = function () {
        if (paragraph.length) {
            html.push('<p>' + renderInlineMarkdown(paragraph.join(' ')) + '</p>');
            paragraph = [];
        }
    };
    const closeList = function () {
        if (listType) {
            html.push('</' + listType + '>');
            listType = null;
        }
    };

    while (i < lines.length) {
        const line = lines[i];

        // Fenced code block ```
        const fence = line.match(/^\s*```/);
        if (fence) {
            flushParagraph();
            closeList();
            const code = [];
            i++;
            while (i < lines.length && !/^\s*```/.test(lines[i])) {
                code.push(lines[i]);
                i++;
            }
            i++; // skip closing fence
            html.push('<pre><code>' + code.join('\n') + '</code></pre>');
            continue;
        }

        // Heading  #..######
        const heading = line.match(/^\s*(#{1,6})\s+(.*)$/);
        if (heading) {
            flushParagraph();
            closeList();
            const level = heading[1].length;
            html.push('<h' + level + '>' + renderInlineMarkdown(heading[2].trim()) + '</h' + level + '>');
            i++;
            continue;
        }

        // List items  - / * / +  and  1.
        const ul = line.match(/^\s*[-*+]\s+(.*)$/);
        const ol = line.match(/^\s*\d+\.\s+(.*)$/);
        if (ul || ol) {
            flushParagraph();
            const wanted = ul ? 'ul' : 'ol';
            if (listType !== wanted) {
                closeList();
                html.push('<' + wanted + '>');
                listType = wanted;
            }
            html.push('<li>' + renderInlineMarkdown((ul || ol)[1].trim()) + '</li>');
            i++;
            continue;
        }

        // Blank line: paragraph / list boundary
        if (/^\s*$/.test(line)) {
            flushParagraph();
            closeList();
            i++;
            continue;
        }

        // Plain text -> accumulate into the current paragraph
        closeList();
        paragraph.push(line.trim());
        i++;
    }

    flushParagraph();
    closeList();
    return html.join('\n');
}
