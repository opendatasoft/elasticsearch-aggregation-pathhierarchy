package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
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
class PathHierarchyAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, PathHierarchyAggregatorFactory> {

    private BytesRef separator;
    private int minDepth;
    private int maxDepth;
    private BucketOrder order;
    private boolean twoSepAsOne;
    private final PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds;

    PathHierarchyAggregatorFactory(String name,
                                   ValuesSourceConfig<ValuesSource> config,
                                   String separator,
                                   int minDepth,
                                   int maxDepth,
                                   boolean twoSepAsOne,
                                   BucketOrder order,
                                   PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds,
                                   SearchContext context,
                                   AggregatorFactory<?> parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.separator = new BytesRef(separator);
        this.minDepth = minDepth;
        this.maxDepth = maxDepth;
        this.twoSepAsOne = twoSepAsOne;
        this.order = order;
        this.bucketCountThresholds = bucketCountThresholds;
    }

    @Override
    protected Aggregator createUnmapped(
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String,
            Object> metaData) throws IOException {
        final InternalAggregation aggregation = new InternalPathHierarchy(name, new ArrayList<>(), order,
                bucketCountThresholds.getRequiredSize(), bucketCountThresholds.getShardSize(), 0, separator, pipelineAggregators, metaData);
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
            ValuesSource valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {

        ValuesSource valuesSourceBytes = new HierarchyValuesSource(valuesSource, separator, minDepth, maxDepth, twoSepAsOne);
        PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds = new
                PathHierarchyAggregator.BucketCountThresholds(this.bucketCountThresholds);
        if (!InternalOrder.isKeyOrder(order)
                && bucketCountThresholds.getShardSize() == PathHierarchyAggregationBuilder.DEFAULT_BUCKET_COUNT_THRESHOLDS.getShardSize()) {
            // The user has not made a shardSize selection. Use default
            // heuristic to avoid any wrong-ranking caused by distributed
            // counting
            bucketCountThresholds.setShardSize(BucketUtils.suggestShardSideQueueSize(bucketCountThresholds.getRequiredSize(),
                    context.numberOfShards()));
        }
        bucketCountThresholds.ensureValidity();
        return new PathHierarchyAggregator(
                name, factories, context,
                valuesSourceBytes, order, bucketCountThresholds, separator,
                parent, pipelineAggregators, metaData);
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
        private boolean twoSepAsOne;

        private HierarchyValues(SortedBinaryDocValues valuesSource, BytesRef separator, int minDepth, int maxDepth,
                                boolean twoSepAsOne) {
            this.valuesSource = valuesSource;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.twoSepAsOne = twoSepAsOne;
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
                count = valuesSource.docValueCount();
                grow();
                int t = 0;
                for (int i=0; i < valuesSource.docValueCount(); i++) {
                    int depth = 0;
                    int last_offset_step = 0;
                    BytesRef val = valuesSource.nextValue();
                    BytesRefBuilder cleanVal = new BytesRefBuilder();

                    for (int offset=0; offset < val.length; offset++) {
                        // it is a separator
                        if (new BytesRef(val.bytes, val.offset + offset, separator.length).equals(separator)) {
                            // ignore separator at the beginning
                            if (offset == 0) {
                                last_offset_step += separator.length;
                                continue;
                            }

                            if (minDepth <= depth) {
                                // two separators following each other
                                if (offset - last_offset_step == separator.length) {
                                    if (twoSepAsOne) {
                                        // ignore the second separator
                                        last_offset_step = offset;
                                        continue;
                                    }
                                    else {
                                        BytesRef no_label_bucket = new BytesRef("empty");
                                        cleanVal.append(separator);
                                        cleanVal.append(no_label_bucket);
                                        values[t++].copyBytes(cleanVal);
                                    }
                                }
                                else {
                                    cleanVal.append(val.bytes, val.offset + last_offset_step, offset - last_offset_step);
                                    values[t++].copyBytes(cleanVal);
                                }
                            }
                            if ( !(minDepth <= depth) && (minDepth == depth +1))
                                last_offset_step = offset += separator.length;
                            else
                                last_offset_step = offset;
                            depth++;
                            if (maxDepth >= 0 && depth > maxDepth) {
                                break;
                            }
                            count++;
                            grow();
                        } else if (offset == val.length - 1) {  // last occurrence and no separator at the end
                            if (minDepth <= depth) {
                                cleanVal.append(val.bytes, val.offset + last_offset_step, offset - last_offset_step + 1);
                                values[t++].copyBytes(cleanVal);
                            }
                        }
                    }

                }
                sort();  // sort values that are stored between offsets 0 and count of values
                return true;
            } else
                return false;
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

        private HierarchyValuesSource(ValuesSource values, BytesRef separator, int minDepth, int maxDepth, boolean twoSepAsOne){
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

