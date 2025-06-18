package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface DateHierarchyAggregationSupplier {
    Aggregator build(
        String name,
        AggregatorFactories factories,
        BucketOrder order,
        List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
        long minDocCount,
        DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException;
}
