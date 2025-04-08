/**
 * Model and column exploration functionality
 */

// Alpine.js component for the model explorer
document.addEventListener('alpine:init', () => {
    Alpine.data('explorer', () => ({
        models: [],
        columns: [],
        selectedModel: null,
        searchTerm: '',
        columnSearchTerm: '',
        isLoading: false,
        
        get filteredModels() {
            if (!this.searchTerm.trim()) return this.models;
            const term = this.searchTerm.toLowerCase();
            return this.models.filter(model => 
                model.name.toLowerCase().includes(term)
            );
        },
        
        get filteredColumns() {
            if (!this.columnSearchTerm.trim()) return this.columns;
            const term = this.columnSearchTerm.toLowerCase();
            return this.columns.filter(column => 
                column.name.toLowerCase().includes(term)
            );
        },
        
        init() {
            this.fetchModels();
        },
        
        async fetchModels() {
            this.isLoading = true;
            try {
                const response = await fetch('/api/models');
                if (!response.ok) throw new Error('Failed to fetch models');
                this.models = await response.json();
            } catch (error) {
                console.error('Error fetching models:', error);
                this.models = [];
            } finally {
                this.isLoading = false;
            }
        },
        
        async selectModel(modelName) {
            this.selectedModel = modelName;
            this.columnSearchTerm = '';
            this.isLoading = true;
            
            try {
                // Re-fetch all models to get columns (consider optimizing if model list is huge)
                const response = await fetch('/api/models'); 
                if (!response.ok) throw new Error('Failed to fetch columns');
                const models = await response.json();
                const selectedModel = models.find(m => m.name === modelName);
                this.columns = selectedModel?.columns || [];
            } catch (error) {
                console.error('Error fetching columns:', error);
                this.columns = [];
            } finally {
                this.isLoading = false;
            }
        },
        
        async visualizeLineage(modelName, columnName) {
            this.isLoading = true;
            try {
                const response = await fetch(`/api/lineage/${modelName}/${columnName}`);
                if (!response.ok) throw new Error('Failed to fetch lineage');
                const data = await response.json();
                document.getElementById('graph').innerHTML = '';
                initGraph(data);
            } catch (error) {
                console.error('Error fetching lineage:', error);
            } finally {
                this.isLoading = false;
            }
        }
    }));
}); 