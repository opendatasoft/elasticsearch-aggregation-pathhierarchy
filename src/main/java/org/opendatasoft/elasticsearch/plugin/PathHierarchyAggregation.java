package org.opendatasoft.elasticsearch.plugin;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.InternalPathHierarchy;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.PathHierarchyAggregationBuilder;

import java.util.ArrayList;

public class PathHierarchyAggregation extends Plugin implements SearchPlugin {
    @Override
    public ArrayList<SearchPlugin.AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();

        r.add(
                new AggregationSpec(
                        PathHierarchyAggregationBuilder.NAME,
                        PathHierarchyAggregationBuilder::new,
                        PathHierarchyAggregationBuilder.PARSER)
                        .addResultReader(InternalPathHierarchy::new)
        );
/*
        r.add(
                new AggregationSpec(
                        DateHierarchyAggregationBuilder.NAME,
                        DateHierarchyAggregationBuilder::new,
                        DateHierarchyAggregationBuilder.PARSER)
                        .addResultReader(InternalDateHierarchy::new)
        );
*/
        return r;
    }
}
