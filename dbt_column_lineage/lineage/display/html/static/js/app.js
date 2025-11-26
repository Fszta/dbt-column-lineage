(function() {
    let graphInstance = null;
    let exploreController = null;

    function init(initialData, isExploreMode) {
        graphInstance = initGraph(initialData);

        if (isExploreMode) {
            const graphInstanceRef = {
                get current() { return graphInstance; },
                set current(value) { graphInstance = value; }
            };
            exploreController = ExploreModule.init(initialData, graphInstanceRef);
            ImpactModule.init();
        }
    }

    window.app = {
        init,
        getGraphInstance: () => graphInstance,
        setGraphInstance: (instance) => { graphInstance = instance; },
        getExploreController: () => exploreController
    };
})();
