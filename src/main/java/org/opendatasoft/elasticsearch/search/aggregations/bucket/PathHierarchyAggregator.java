package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
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
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class PathHierarchyAggregator extends BucketsAggregator {

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
            // shard_size cannot be smaller than size as we need to at least fetch <size> entries from every shards in order to return <size>
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
    private final BucketCountThresholds bucketCountThresholds;
    private final BytesRef separator;

    public PathHierarchyAggregator(
            String name,
            AggregatorFactories factories,
            SearchContext context,
            ValuesSource valuesSource,
            BucketOrder order,
            BucketCountThresholds bucketCountThresholds,
            BytesRef separator,
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.valuesSource = valuesSource;
        this.separator = separator;
        bucketOrds = new BytesRefHash(1, context.bigArrays());
        this.order = InternalOrder.validate(order, null);
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
                        if (previous.get().equals(bytesValue)) {
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
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;

        // get back buckets
        if (!InternalOrder.isCountDesc(order) || (bucketOrds.size() < bucketCountThresholds.getRequiredSize())) {
            // we need to fill-in the blanks
            for (LeafReaderContext ctx : context.searcher().getTopReaderContext().leaves()) {
                final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
                // brute force
                for (int docId = 0; docId < ctx.reader().maxDoc(); ++docId) {
                    if (values.advanceExact(docId)) {
                        final int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; ++i) {
                            final BytesRef term = values.nextValue();
                            bucketOrds.add(term);
                        }
                    }
                }
            }
        }

        // build buckets and store them sorted
        final int size = (int) Math.min(bucketOrds.size(), bucketCountThresholds.getShardSize());
        BucketPriorityQueue<InternalPathHierarchy.InternalBucket> ordered = new BucketPriorityQueue<>(size,
                order.comparator(this));
        InternalPathHierarchy.InternalBucket spare = null;
        for (int i = 0; i < bucketOrds.size(); i++) {
            if (spare == null) {
                spare = new InternalPathHierarchy.InternalBucket(0, null, null, new BytesRef(), 0, null);
            }
            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);

            String [] paths = term.utf8ToString().split(Pattern.quote(separator.utf8ToString()));

            spare.termBytes = BytesRef.deepCopyOf(term);
            spare.docCount = bucketDocCount(i);
            spare.aggregations = bucketAggregations(i);
            spare.level = paths.length - 1;
            spare.basename = paths[paths.length - 1];
            spare.path = Arrays.copyOf(paths, paths.length - 1);
            spare.bucketOrd = i;
            spare = ordered.insertWithOverflow(spare);
            if (spare == null) {
                consumeBucketsAndMaybeBreak(1);
            }
        }

        final InternalPathHierarchy.InternalBucket[] list = new InternalPathHierarchy.InternalBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; --i) {
            final InternalPathHierarchy.InternalBucket bucket = ordered.pop();
            list[i] = bucket;
        }

        return new InternalPathHierarchy(name, Arrays.asList(list), order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), separator, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPathHierarchy(name, null, order, bucketCountThresholds.getRequiredSize(),
                bucketCountThresholds.getShardSize(), separator, pipelineAggregators(), metaData());
    }

}
