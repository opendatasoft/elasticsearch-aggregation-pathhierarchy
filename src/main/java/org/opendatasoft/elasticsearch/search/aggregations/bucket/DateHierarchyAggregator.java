package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DateHierarchyAggregator extends BucketsAggregator {

    public DateHierarchyAggregator(
        String name,
        AggregatorFactories factories,
        AggregationContext context,
        ValuesSource.Numeric valuesSource,
        BucketOrder order,
        long minDocCount,
        BucketCountThresholds bucketCountThresholds,
        List<DateHierarchyAggregationBuilder.PreparedRounding> preparedRoundings,
        Aggregator parent,
        CardinalityUpperBound cardinalityUpperBound,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, cardinalityUpperBound, metadata);
        this.valuesSource = valuesSource;
        this.preparedRoundings = preparedRoundings;
        this.minDocCount = minDocCount;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.bucketCountThresholds = bucketCountThresholds;
        order.validate(this);
        this.order = order;
        this.partiallyBuiltBucketComparator = order == null ? null : order.partiallyBuiltBucketComparator(b -> b.bucketOrd, this);
    }

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

        public BucketCountThresholds(DateHierarchyAggregator.BucketCountThresholds bucketCountThresholds) {
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
            builder.field(DateHierarchyAggregationBuilder.SIZE_FIELD.getPreferredName(), requiredSize);
            if (shardSize != -1) {
                builder.field(DateHierarchyAggregationBuilder.SHARD_SIZE_FIELD.getPreferredName(), shardSize);
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
            DateHierarchyAggregator.BucketCountThresholds other = (DateHierarchyAggregator.BucketCountThresholds) obj;
            return Objects.equals(requiredSize, other.requiredSize) && Objects.equals(shardSize, other.shardSize);
        }
    }

    private final ValuesSource.Numeric valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketOrder order;
    private final long minDocCount;
    private final BucketCountThresholds bucketCountThresholds;
    private final List<DateHierarchyAggregationBuilder.PreparedRounding> preparedRoundings;
    protected final Comparator<InternalPathHierarchy.InternalBucket> partiallyBuiltBucketComparator;

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
        final SortedNumericDocValues values = valuesSource.longValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0;
                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();

                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        String path = "";
                        for (DateHierarchyAggregationBuilder.PreparedRounding preparedRounding : preparedRoundings) {
                            long roundedValue = preparedRounding.prepared.round(value);
                            path += preparedRounding.roundingInfo.format.format(roundedValue).toString();
                            long bucketOrd = bucketOrds.add(new BytesRef(path));
                            if (bucketOrd < 0) { // already seen
                                bucketOrd = -1 - bucketOrd;
                                collectExistingBucket(sub, doc, bucketOrd);
                            } else {
                                collectBucket(sub, doc, bucketOrd);
                            }
                            path += "/";
                        }
                    }
                }
            }
        };
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrdinals) throws IOException {

        InternalDateHierarchy.InternalBucket[][] topBucketsPerOrd = new InternalDateHierarchy.InternalBucket[owningBucketOrdinals.length][];
        InternalDateHierarchy[] results = new InternalDateHierarchy[owningBucketOrdinals.length];

        for (int ordIdx = 0; ordIdx < owningBucketOrdinals.length; ordIdx++) {
            assert owningBucketOrdinals[ordIdx] == 0;

            // build buckets and store them sorted
            final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

            PathSortedTree<String, InternalDateHierarchy.InternalBucket> pathSortedTree = new PathSortedTree<>(order.comparator(), size);

            InternalDateHierarchy.InternalBucket spare;
            for (int i = 0; i < bucketOrds.size(); i++) {
                spare = new InternalDateHierarchy.InternalBucket(0, null, null, null, 0, null);

                BytesRef term = new BytesRef();
                bucketOrds.get(i, term);
                String[] paths = term.utf8ToString().split("/", -1);

                spare.paths = paths;
                spare.key = term;
                spare.level = paths.length - 1;
                spare.name = paths[spare.level];
                spare.docCount = bucketDocCount(i);
                spare.bucketOrd = i;

                pathSortedTree.add(spare.paths, spare);
            }

            // Get the top buckets
            topBucketsPerOrd[ordIdx] = new InternalDateHierarchy.InternalBucket[size];
            long otherHierarchyNodes = pathSortedTree.getFullSize();
            Iterator<InternalDateHierarchy.InternalBucket> iterator = pathSortedTree.consumer();
            for (int i = 0; i < size; i++) {
                final InternalDateHierarchy.InternalBucket bucket = iterator.next();
                topBucketsPerOrd[ordIdx][i] = bucket;
                otherHierarchyNodes -= 1;
            }

            results[ordIdx] = new InternalDateHierarchy(
                name,
                Arrays.asList(topBucketsPerOrd[ordIdx]),
                order,
                minDocCount,
                bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(),
                otherHierarchyNodes,
                metadata()
            );
        }

        // Build sub-aggregations for pruned buckets
        buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggregations) -> b.aggregations = aggregations);

        return results;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalDateHierarchy(
            name,
            null,
            order,
            minDocCount,
            bucketCountThresholds.getRequiredSize(),
            bucketCountThresholds.getShardSize(),
            0,
            metadata()
        );
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
