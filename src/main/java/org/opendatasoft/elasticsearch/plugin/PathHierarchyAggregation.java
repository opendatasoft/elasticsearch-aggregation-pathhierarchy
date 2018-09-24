package org.opendatasoft.elasticsearch.plugin;

import java.util.ArrayList;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.PathHierarchyAggregationBuilder;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.InternalPathHierarchy;

public class PathHierarchyAggregation extends Plugin implements SearchPlugin {
    @Override
    public ArrayList<SearchPlugin.AggregationSpec> getAggregations() {
        ArrayList<SearchPlugin.AggregationSpec> r = new ArrayList<>();

        r.add(
                new AggregationSpec(
                        PathHierarchyAggregationBuilder.NAME,
                        PathHierarchyAggregationBuilder::new,
                        PathHierarchyAggregationBuilder::parse)
                .addResultReader(InternalPathHierarchy::new)
        );

        return r;
    }
}
