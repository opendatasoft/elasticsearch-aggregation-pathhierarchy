package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FutureArrays;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.bucket.BucketUtils;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

/**
 * The factory of aggregators.
 * ValuesSourceAggregatorFactory extends {@link AggregatorFactory}
 */
class PathHierarchyAggregatorFactory extends ValuesSourceAggregatorFactory {

    private BytesRef separator;
    private int minDepth;
    private int maxDepth;
    private BucketOrder order;
    private long minDocCount;
    private boolean keepBlankPath;
    private final PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds;

    PathHierarchyAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        String separator,
        int minDepth,
        int maxDepth,
        boolean keepBlankPath,
        BucketOrder order,
        long minDocCount,
        PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.separator = new BytesRef(separator);
        this.minDepth = minDepth;
        this.maxDepth = maxDepth;
        this.keepBlankPath = keepBlankPath;
        this.order = order;
        this.minDocCount = minDocCount;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            PathHierarchyAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.KEYWORD,
            (
                name,
                factories,
                separator,
                minDepth,
                maxDepth,
                keepBlankPath,
                order,
                minDocCount,
                bucketCountThresholds,
                valuesSourceConfig,
                aggregationContext,
                parent,
                cardinality,
                metadata) -> null,
            true
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        final InternalAggregation aggregation = new InternalPathHierarchy(
            name,
            new ArrayList<>(),
            order,
            minDocCount,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getShardSize(),
            0,
            separator,
            metadata
        );
        return new NonCollectingAggregator(name, context, parent, factories, metadata) {
            {
                // even in the case of an unmapped aggregator, validate the
                // order
                order.validate(this);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() {
                return aggregation;
            }
        };
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        ValuesSource valuesSourceBytes = new HierarchyValuesSource(config.getValuesSource(), separator, minDepth, maxDepth, keepBlankPath);
        PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds = new PathHierarchyAggregator.BucketCountThresholds(
            this.bucketCountThresholds
        );
        if (!InternalOrder.isKeyOrder(order)
            && bucketCountThresholds.getShardSize() == PathHierarchyAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize()));
        }
        bucketCountThresholds.ensureValidity();
        return new PathHierarchyAggregator(
            name,
            factories,
            context,
            valuesSourceBytes,
            order,
            minDocCount,
            bucketCountThresholds,
            separator,
            minDepth,
            parent,
            cardinality,
            metadata
        );
    }

    /**
     * A list of per-document binary values, sorted according to {@link BytesRef}.
     * There might be dups however.
     * @see ValuesSource
     */
    private static class HierarchyValues extends SortingBinaryDocValues {

        /** valuesSource is a list of per-document binary values, sorted according to {@link BytesRef#compareTo(BytesRef)}
         * (warning, there might be dups however).
         */
        private SortedBinaryDocValues valuesSource;
        private BytesRef separator;
        private int minDepth;
        private int maxDepth;
        private boolean keepBlankPath;

        private HierarchyValues(SortedBinaryDocValues valuesSource, BytesRef separator, int minDepth, int maxDepth, boolean keepBlankPath) {
            this.valuesSource = valuesSource;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.keepBlankPath = keepBlankPath;
        }

        /**
         * Handles iterations on doc values:
         *  Advance the iterator to exactly target and return whether target has a value.
         *  target must be greater than or equal to the current doc ID and must be a valid doc ID, ie. &ge; 0 and &lt; maxDoc.
         *  After this method returns, docID() returns target.
         */
        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (valuesSource.advanceExact(docId)) {
                count = 0;
                int t = 0;
                for (int i = 0; i < valuesSource.docValueCount(); i++) {
                    int depth = 0;
                    BytesRef val = valuesSource.nextValue();
                    BytesRefBuilder cleanVal = new BytesRefBuilder();
                    int startNewValOffset = -1;

                    for (int offset = 0; offset < val.length; offset++) {
                        // it is a separator
                        if (val.length - offset >= separator.length
                            && FutureArrays.equals(
                                separator.bytes,
                                separator.offset,
                                separator.offset + separator.length,
                                val.bytes,
                                val.offset + offset,
                                val.offset + offset + separator.length
                            )) {
                            // ignore separator at the beginning
                            if (offset == 0) {
                                offset += separator.length - 1;
                                continue;
                            }

                            // A new path needs to be add
                            if (startNewValOffset != -1) {
                                cleanVal.append(val.bytes, val.offset + startNewValOffset, offset - startNewValOffset);
                                if (depth >= minDepth) {
                                    values[t++].copyBytes(cleanVal);
                                }
                                startNewValOffset = -1;
                                cleanVal.append(separator);
                                depth++;
                                // two separators following each other
                            } else if (keepBlankPath) {
                                count++;
                                growExact();
                                values[t++].copyBytes(cleanVal);
                                cleanVal.append(separator);
                                depth++;
                            }

                            if (maxDepth >= 0 && depth > maxDepth) {
                                break;
                            }
                            offset += separator.length - 1;
                        } else {
                            if (startNewValOffset == -1) {
                                startNewValOffset = offset;
                                if (depth >= minDepth) {
                                    count++;
                                    growExact();
                                }
                            }
                        }
                    }

                    if (startNewValOffset != -1 && minDepth <= depth) {
                        cleanVal.append(val.bytes, val.offset + startNewValOffset, val.length - startNewValOffset);
                        values[t++].copyBytes(cleanVal);
                    }

                }
                sort();  // sort values that are stored between offsets 0 and count of values
                return true;
            } else return false;
        }

        final void growExact() {
            if (values.length < count) {
                final int oldLen = values.length;
                values = ArrayUtil.growExact(values, count);
                for (int i = oldLen; i < count; ++i) {
                    values[i] = new BytesRefBuilder();
                }
            }
        }
    }

    /**
     * To get ValuesSource as sorted bytes.
     */
    private static class HierarchyValuesSource extends ValuesSource.Bytes {
        private final ValuesSource values;
        private final BytesRef separator;
        private final int minDepth;
        private final int maxDepth;
        private final boolean twoSepAsOne;

        private HierarchyValuesSource(ValuesSource values, BytesRef separator, int minDepth, int maxDepth, boolean twoSepAsOne) {
            this.values = values;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.twoSepAsOne = twoSepAsOne;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return new HierarchyValues(values.bytesValues(context), separator, minDepth, maxDepth, twoSepAsOne);
        }

    }
}
