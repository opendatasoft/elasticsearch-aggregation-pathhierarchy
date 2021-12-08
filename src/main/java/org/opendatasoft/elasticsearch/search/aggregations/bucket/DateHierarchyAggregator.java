package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;


public class DateHierarchyAggregator extends BucketsAggregator {

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
            return Objects.equals(requiredSize, other.requiredSize)
                    && Objects.equals(shardSize, other.shardSize);
        }
    }

    private final ValuesSource.Numeric valuesSource;
    private final BytesRefHash bucketOrds;
    private final BucketOrder order;
    private final long minDocCount;
    private final BucketCountThresholds bucketCountThresholds;
    private final List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo;

    public DateHierarchyAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource.Numeric valuesSource,
            BucketOrder order,
            long minDocCount,
            BucketCountThresholds bucketCountThresholds,
            List<DateHierarchyAggregationBuilder.RoundingInfo> roundingsInfo,
            Aggregator parent,
            Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, metadata);
        this.valuesSource = valuesSource;
        this.roundingsInfo = roundingsInfo;
        this.minDocCount = minDocCount;
        bucketOrds =  new BytesRefHash(1, context.bigArrays());
        this.order = InternalOrder.validate(order, this);
        this.bucketCountThresholds = bucketCountThresholds;
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
                        for (DateHierarchyAggregationBuilder.RoundingInfo roundingInfo: roundingsInfo) {
                            // A little hacky: Add a microsecond to avoid collision between min dates interval
                            // Since interval cannot be set to microsecond, it is not a problem
                            long roundedValue = roundingInfo.rounding.round(value);
                            path += roundingInfo.format.format(roundedValue).toString();
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        // build buckets and store them sorted
        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());

        PathSortedTree<String, InternalDateHierarchy.InternalBucket> pathSortedTree = new PathSortedTree<>(order.comparator(this), size);

        InternalDateHierarchy.InternalBucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            spare = new InternalDateHierarchy.InternalBucket(0, null, null, null, 0, null);

            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);
            String [] paths = term.utf8ToString().split("/", -1);

            spare.paths = paths;
            spare.key = term;
            spare.level = paths.length - 1;
            spare.name = paths[spare.level];
            spare.aggregations = bucketAggregations(i);
            spare.docCount = bucketDocCount(i);

            pathSortedTree.add(spare.paths, spare);

            consumeBucketsAndMaybeBreak(1);

        }

        // Get the top buckets
        final List<InternalDateHierarchy.InternalBucket> list = new ArrayList<>(size);
        long otherHierarchyNodes = pathSortedTree.getFullSize();
        Iterator<InternalDateHierarchy.InternalBucket> iterator = pathSortedTree.consumer();
        for (int i = 0; i < size; i++) {
            final InternalDateHierarchy.InternalBucket bucket = iterator.next();
            list.add(bucket);
            otherHierarchyNodes -= 1;
        }

        return new InternalDateHierarchy(name, list, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), otherHierarchyNodes, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalDateHierarchy(name, null, order, minDocCount, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), 0, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(bucketOrds);
    }
}
