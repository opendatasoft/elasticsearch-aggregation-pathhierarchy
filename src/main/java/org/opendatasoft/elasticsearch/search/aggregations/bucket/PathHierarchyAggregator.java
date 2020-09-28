package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

public class PathHierarchyAggregator extends DeferableBucketAggregator {

    public static class BucketCountThresholds implements Writeable, ToXContentFragment {
        private int requiredSize;
        private int shardSize;

        public BucketCountThresholds(int requiredSize, int shardSize) {
            this.requiredSize = requiredSize;
            this.shardSize = shardSize;
        }

        /**
         * Read from a stream.
         */
        public BucketCountThresholds(StreamInput in) throws IOException {
            requiredSize = in.readInt();
            shardSize = in.readInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(requiredSize);
            out.writeInt(shardSize);
        }

        public BucketCountThresholds(PathHierarchyAggregator.BucketCountThresholds bucketCountThresholds) {
            this(bucketCountThresholds.requiredSize, bucketCountThresholds.shardSize);
        }

        public void ensureValidity() {
            // shard_size cannot be smaller than size as we need to at least fetch size entries from every shards in order to return size
            if (shardSize < requiredSize) {
                setShardSize(requiredSize);
            }

            if (requiredSize <= 0 || shardSize <= 0) {
                throw new ElasticsearchException("parameters [required_size] and [shard_size] must be >0 in path-hierarchy aggregation.");
            }
        }

        public int getRequiredSize() {
            return requiredSize;
        }

        public void setRequiredSize(int requiredSize) {
            this.requiredSize = requiredSize;
        }

        public int getShardSize() {
            return shardSize;
        }

        public void setShardSize(int shardSize) {
            this.shardSize = shardSize;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(PathHierarchyAggregationBuilder.SIZE_FIELD.getPreferredName(), requiredSize);
            if (shardSize != -1) {
                builder.field(PathHierarchyAggregationBuilder.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
            }
            return builder;
        }

        @Override
        public int hashCode() {
            return Objects.hash(requiredSize, shardSize);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            PathHierarchyAggregator.BucketCountThresholds other = (PathHierarchyAggregator.BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize)
                    && Objects.equals(shardSize, other.shardSize);
        }
    }


    private final ValuesSource valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketOrder order;
    private final long minDocCount;
    private final int minDepth;
    private final BucketCountThresholds bucketCountThresholds;
    private final BytesRef separator;
    protected final Comparator<InternalPathHierarchy.InternalBucket> partiallyBuiltBucketComparator;
    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final SubAggCollectionMode collectMode;

    public PathHierarchyAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource valuesSource,
            BucketOrder order,
            long minDocCount,
            BucketCountThresholds bucketCountThresholds,
            BytesRef separator,
            int minDepth,
            Aggregator parent,
            Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.separator = separator;
        this.minDocCount = minDocCount;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.bucketCountThresholds = bucketCountThresholds;
        this.minDepth = minDepth;
        this.order = order;
        partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);

        if (subAggsNeedScore() && descendsFromNestedAggregator(parent)) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
             */
            this.collectMode = SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = SubAggCollectionMode.BREADTH_FIRST;
        }

        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof InternalOrder.Aggregation){
            AggregationPath path = ((InternalOrder.Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
        } else if (order instanceof InternalOrder.CompoundOrder) {
            InternalOrder.CompoundOrder compoundOrder = (InternalOrder.CompoundOrder) order;
            for (BucketOrder orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof InternalOrder.Aggregation) {
                    AggregationPath path = ((InternalOrder.Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(this));
                }
            }
        }
    }

    static boolean descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    private boolean subAggsNeedScore() {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected boolean shouldDefer(Aggregator aggregator) {
        return collectMode == SubAggCollectionMode.BREADTH_FIRST
                && !aggsUsedForSorting.contains(aggregator);
    }

    /**
     * The collector collects the docs, including or not some score (depending of the including of a Scorer) in the
     * collect() process.
     *
     * The LeafBucketCollector is a "Per-leaf bucket collector". It collects docs for the account of buckets.
     */
    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            final BytesRefBuilder previous = new BytesRefBuilder();
            /**
             * Collect the given doc in the given bucket.
             * Called once for every document matching a query, with the unbased document number.
             */
            @Override
            public void collect(int doc, long owningBucketOrdinal) throws IOException {
                assert owningBucketOrdinal == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    previous.clear();

                    // SortedBinaryDocValues don't guarantee uniqueness so we need to take care of dups
                    for (int i = 0; i < valuesCount; ++i) {
                        final BytesRef bytesValue = values.nextValue();
                        if (i > 0 && previous.get().equals(bytesValue)) {
                            continue;
                        }
                        long bucketOrdinal = bucketOrds.add(bytesValue);
                        if (bucketOrdinal < 0) { // already seen
                            bucketOrdinal = - 1 - bucketOrdinal;
                            collectExistingBucket(sub, doc, bucketOrdinal);
                        } else {
                            collectBucket(sub, doc, bucketOrdinal);
                        }
                        previous.copyBytes(bytesValue);
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {

        InternalPathHierarchy[] results = new InternalPathHierarchy[owningBucketOrds.length];

        for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {

            //TODO: This is still not clear, maybe ask upstream
            assert owningBucketOrds[ordIdx] == 0;

            final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
            PathSortedTree<String, InternalPathHierarchy.InternalBucket> pathSortedTree =
                    new PathSortedTree<>(partiallyBuiltBucketComparator, size);
            InternalPathHierarchy.InternalBucket spare;

            for (int i = 0; i < bucketOrds.size(); i++) {
                spare = new InternalPathHierarchy.InternalBucket(0, null, null, new BytesRef(), 0, 0, null);
                BytesRef term = new BytesRef();
                bucketOrds.get(i, term);

                String quotedPattern = Pattern.quote(separator.utf8ToString());

                String [] paths = term.utf8ToString().split(quotedPattern, -1);

                String [] pathsForTree;

                if (minDepth > 0) {
                    pathsForTree = Arrays.copyOfRange(paths, minDepth, paths.length);
                } else {
                    pathsForTree = paths;
                }

                spare.termBytes = BytesRef.deepCopyOf(term);
                spare.level = pathsForTree.length - 1;
                spare.docCount = bucketDocCount(i);
                spare.basename = paths[paths.length - 1];
                spare.minDepth = minDepth;
                spare.bucketOrd = i;
                spare.paths = paths;

                pathSortedTree.add(pathsForTree, spare);
            }

            // Get the top buckets
            final List<InternalPathHierarchy.InternalBucket> buckets = new ArrayList<>(size);
            long otherHierarchyNodes = pathSortedTree.getFullSize();
            Iterator<InternalPathHierarchy.InternalBucket> iterator = pathSortedTree.consumer();
            for (int i = 0; i < size; i++) {
                final InternalPathHierarchy.InternalBucket bucket = iterator.next();
                buckets.add(bucket);
                otherHierarchyNodes -= 1;
            }

            results[ordIdx] = new InternalPathHierarchy(name, buckets, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                    bucketCountThresholds.getShardSize(), otherHierarchyNodes, separator, metadata());

        }

        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPathHierarchy(name, null, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), 0, separator, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
