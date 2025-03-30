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
            columnHeight: 30,
            columnPadding: 5,
            cornerRadius: 8
        },
        layout: {
            xSpacing: 350,
            ySpacing: 150,
            verticalUsage: 0.8
        },
        colors: {
            model: '#f5f5f5',
            title: '#f8f9fa',
            column: '#f0f0f0',
            columnHover: '#e0e0e0',
            edge: '#999',
            edgeDimmed: '#97a4b0',
            edgeHighlight: '#ff6b6b',
            selectedColumn: '#ffcccb',
            relatedColumn: '#fff0c0',
        }
    };
}