package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;

import java.io.IOException;

public class PathHierarchyBuilder extends AggregationBuilder<PathHierarchyBuilder> {

    private String field;
    private String separator = PathHierarchyParser.DEFAULT_SEPARATOR;
    private int maxDepth = PathHierarchyParser.DEFAULT_MAX_DEPTH;
    private int minDepth = PathHierarchyParser.DEFAULT_MIN_DEPTH;
    private Integer depth;
    private PathHierarchy.Order order;

    public PathHierarchyBuilder(String name) {
        super(name, InternalPathHierarchy.TYPE.name());
    }


    public PathHierarchyBuilder field(String field) {
        this.field = field;
        return this;
    }

    public PathHierarchyBuilder separator(String separator) {
        this.separator = separator;
        return this;
    }

    public PathHierarchyBuilder minDepth(int minDepth) {
        this.minDepth = minDepth;
        return this;
    }

    public PathHierarchyBuilder maxDepth(int maxDepth) {
        this.maxDepth = maxDepth;
        return this;
    }

    public PathHierarchyBuilder depth(int depth) {
        this.depth = depth;
        return this;
    }

    public PathHierarchyBuilder order(PathHierarchy.Order order) {
        this.order = order;
        return this;
    }


    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();


        if (field != null) {
            builder.field("field", field);
        }

        if (order != null) {
            builder.field("order");
            order.toXContent(builder, params);
        }

        if (! separator.equals(PathHierarchyParser.DEFAULT_SEPARATOR)) {
            builder.field("separator", separator);
        }

        if ( minDepth != PathHierarchyParser.DEFAULT_MIN_DEPTH) {
            builder.field("min_depth", minDepth);
        }

        if ( maxDepth != PathHierarchyParser.DEFAULT_MAX_DEPTH) {
            builder.field("max_depth", maxDepth);
        }

        if (depth != null) {
            builder.field("depth", depth);
        }

        return builder.endObject();
    }
}
