package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The factory of aggregators.
 * ValuesSourceAggregatorFactory extends {@link AggregatorFactory}
 */
class DateHierarchyAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource.Numeric> {

    private long minDocCount;
    private BucketOrder order;
    private List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo;
    private final DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds;

    DateHierarchyAggregatorFactory(String name,
                                   ValuesSourceConfig<ValuesSource.Numeric> config,
                                   BucketOrder order,
                                   List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
                                   long minDocCount,
                                   DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
                                   SearchContext context,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.order = order;
        this.roundingsInfo = roundingsInfo;
        this.minDocCount = minDocCount;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected Aggregator createUnmapped(
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String,
            Object> metaData) throws IOException {
        final InternalAggregation aggregation = new InternalDateHierarchy(name, new ArrayList<>(), order, minDocCount,
                bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), 0, pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
            {
                // even in the case of an unmapped aggregator, validate the
                // order
                InternalOrder.validate(order, this);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() { return aggregation; }
        };
    }

    @Override
    protected Aggregator doCreateInternal(
            ValuesSource.Numeric valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {

        DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds = new
                DateHierarchyAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (!InternalOrder.isKeyOrder(order)
                && bucketCountThresholds.getShardSize() == DateHierarchyAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();
        return new DateHierarchyAggregator(
                name, factories, context,
                valuesSource, order, minDocCount, bucketCountThresholds, roundingsInfo,
                context, parent, pipelineAggregators, metaData);
    }

}

