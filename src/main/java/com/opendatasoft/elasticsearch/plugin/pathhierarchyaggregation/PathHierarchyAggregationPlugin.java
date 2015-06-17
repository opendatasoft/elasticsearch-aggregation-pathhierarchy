package com.opendatasoft.elasticsearch.plugin.pathhierarchyaggregation;

import com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation.PathHierarchyParser;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation.InternalPathHierarchy;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.search.aggregations.AggregationModule;

public class PathHierarchyAggregationPlugin extends AbstractPlugin {
    @Override
    public String name() {
        return "Path Hierarchy";
    }

    @Override
    public String description() {
        return "Return a path hierarchy aggregation";
    }

    public void onModule(AggregationModule aggModule) {
        aggModule.addAggregatorParser(PathHierarchyParser.class);
        InternalPathHierarchy.registerStreams();
    }

}
