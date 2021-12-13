package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The factory of aggregators.
 * ValuesSourceAggregatorFactory extends {@link AggregatorFactory}
 */
class DateHierarchyAggregatorFactory extends ValuesSourceAggregatorFactory {

    private long minDocCount;
    private BucketOrder order;
    private List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo;
    private final DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds;

    DateHierarchyAggregatorFactory(String name,
                                   ValuesSourceConfig config,
                                   BucketOrder order,
                                   List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
                                   long minDocCount,
                                   DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
                                   QueryShardContext context,
                                   AggregatorFactory parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metadata
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.order = order;
        this.roundingsInfo = roundingsInfo;
        this.minDocCount = minDocCount;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(DateHierarchyAggregationBuilder.REGISTRY_KEY, CoreValuesSourceType.DATE, (name,
                                                                                                   factories,
                                                                                                   order,
                                                                                                   roundingsInfo,
                                                                                                   minDocCount,
                                                                                                   bucketCountThresholds,
                                                                                                   valuesSourceConfig,
                                                                                                   aggregationContext,
                                                                                                   parent,
                                                                                                   cardinality,
                                                                                                   metadata) -> null,
                true);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                        Aggregator parent,
                                        Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new InternalDateHierarchy(name, new ArrayList<>(), order, minDocCount,
                bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), 0, metadata);
        return new NonCollectingAggregator(name, searchContext, parent, factories, metadata) {
            {
                // even in the case of an unmapped aggregator, validate the
                // order
                order.validate(this);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() { return aggregation; }
        };
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext, Aggregator parent, CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {

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
        return queryShardContext.getValuesSourceRegistry()
                .getAggregator(DateHierarchyAggregationBuilder.REGISTRY_KEY, config)
                .build(name,
                        factories,
                        order,
                        roundingsInfo,
                        minDocCount,
                        bucketCountThresholds,
                        config,
                        searchContext,
                        parent,
                        cardinality,
                        metadata
                );
    }
}

