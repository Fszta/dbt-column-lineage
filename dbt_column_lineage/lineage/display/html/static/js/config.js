/**
 * Configuration settings for the graph visualization
 */

/**
 * Resolve the graph palette from CSS custom properties on :root so the D3
 * canvas follows the active (light/dark) theme. Falls back to the light
 * values if a variable is missing.
 */
function resolveGraphColors() {
    const cs = getComputedStyle(document.documentElement);
    const v = (name, fallback) => {
        const raw = cs.getPropertyValue(name);
        return (raw && raw.trim()) || fallback;
    };
    return {
        model: v('--surface', '#ffffff'),
        title: v('--surface-2', '#f8fafc'),
        column: v('--surface-2', '#f8fafc'),
        columnHover: v('--surface-hover', '#f1f5f9'),
        edge: v('--edge-color', '#cbd5e1'),
        edgeDimmed: v('--border', '#e2e8f0'),
        edgeHighlight: v('--edge-highlight', '#f59e0b'),
        selectedColumn: v('--col-selected', '#fef3c7'),
        relatedColumn: v('--col-related', '#ecfdf5'),
    };
}

function createConfig(container) {
    return {
        container: container,
        width: container.clientWidth || 800,
        height: container.clientHeight || 600,
        box: {
            width: 250,
            padding: 15,
            titleHeight: 40,
            columnHeight: 28,
            columnPadding: 4,
            cornerRadius: 6
        },
        layout: {
            xSpacing: 150,
            ySpacing: 40,
            verticalUsage: 0.8
        },
        colors: resolveGraphColors()
    };
}
