/**
 * Configuration settings for the graph visualization
 */
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
        colors: {
            model: '#ffffff',
            title: '#f8fafc',
            column: '#f8fafc',
            columnHover: '#f1f5f9',
            edge: '#cbd5e1',
            edgeDimmed: '#e2e8f0',
            edgeHighlight: '#f59e0b',
            selectedColumn: '#fef3c7',
            relatedColumn: '#ecfdf5',
        }
    };
}
