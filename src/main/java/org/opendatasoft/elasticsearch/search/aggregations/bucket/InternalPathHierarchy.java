package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Arrays;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation} which extends {@link Aggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalPathHierarchy extends InternalMultiBucketAggregation<InternalPathHierarchy,
        InternalPathHierarchy.InternalBucket> implements PathHierarchy {
    protected static final ParseField SUM_OF_OTHER_DOC_COUNTS = new ParseField("sum_other_doc_count");

    /**
     * The bucket class of InternalPathHierarchy.
     * @see MultiBucketsAggregation.Bucket
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements
            PathHierarchy.Bucket, KeyComparable<InternalBucket> {

        BytesRef termBytes;
        long bucketOrd;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected String basename;

        public InternalBucket(long docCount, InternalAggregations aggregations, String basename, BytesRef term, int level) {
            termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.basename = basename;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            termBytes = in.readBytesRef();
            docCount = in.readLong();
            aggregations = InternalAggregations.readAggregations(in);
            level = in.readInt();
            int path_length = in.readInt();
            basename = in.readString();
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(termBytes);
            out.writeLong(docCount);
            aggregations.writeTo(out);
            out.writeInt(level);
            out.writeString(basename);
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public String getKeyAsString() {
            return termBytes.utf8ToString();
        }

        @Override
        public int compareKey(InternalPathHierarchy.InternalBucket other) {
            return termBytes.compareTo(other.termBytes);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        /**
         * Utility method of InternalPathHierarchy.doReduce()
         */
        InternalBucket reduce(List<InternalBucket> buckets, ReduceContext context) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
            InternalBucket reduced = null;
            for (InternalBucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, context);
            return reduced;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

    }


    private List<InternalBucket> buckets;
    private BytesRef separator;
    private BucketOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final long otherDocCount;
    private final long minDocCount = 1;

    public InternalPathHierarchy(
            String name,
            List<InternalBucket> buckets,
            BucketOrder order,
            int requiredSize,
            int shardSize,
            long otherDocCount,
            BytesRef separator,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherDocCount = otherDocCount;
        this.separator = separator;
    }

    /**
     * Read from a stream.
     */
    public InternalPathHierarchy(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherDocCount = in.readVLong();
        separator = in.readBytesRef();
        this.buckets = in.readList(InternalBucket::new);
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        writeSize(requiredSize, out);
        writeSize(shardSize, out);
        out.writeVLong(otherDocCount);
        out.writeBytesRef(separator);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return PathHierarchyAggregationBuilder.NAME;
    }

    protected int getShardSize() {
        return shardSize;
    }

    public long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    public InternalPathHierarchy create(List<InternalBucket> buckets) {
        return new InternalPathHierarchy(this.name, buckets, order, requiredSize, shardSize, otherDocCount,
                this.separator, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.docCount, aggregations, prototype.basename, prototype.termBytes,
                prototype.level);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    /**
     * Reduces the given aggregations to a single one and returns it.
     */
    @Override
    public InternalPathHierarchy doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<BytesRef, List<InternalBucket>> buckets = null;
        long otherDocCount = 0;

        // extract buckets from aggregations
        for (InternalAggregation aggregation : aggregations) {
            InternalPathHierarchy pathHierarchy = (InternalPathHierarchy) aggregation;
            if (buckets == null) {
                buckets = new HashMap<>();
            }

            otherDocCount += pathHierarchy.getSumOfOtherDocCounts();

            for (InternalBucket bucket : pathHierarchy.buckets) {
                List<InternalBucket> existingBuckets = buckets.get(bucket.termBytes);
                if (existingBuckets == null) {
                    existingBuckets = new ArrayList<>(aggregations.size());
                    buckets.put(bucket.termBytes, existingBuckets);
                }
                existingBuckets.add(bucket);
            }
        }

        // reduce and sort buckets depending of ordering rules
        final int size = !reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size());
        final BucketPriorityQueue<InternalBucket> ordered = new BucketPriorityQueue<>(size, order.comparator(null));
        for (List<InternalBucket> sameTermBuckets : buckets.values()) {
            final InternalBucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.docCount >= minDocCount || !reduceContext.isFinalReduce()) {
                InternalBucket removed = ordered.insertWithOverflow(b);
                if (removed != null) {
                    otherDocCount += removed.getDocCount();
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(1);
                }
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
            }
        }
        InternalBucket[] reducedBuckets = new InternalBucket[ordered.size()];
        for (int i = ordered.size() - 1; i >= 0; i--) {
            reducedBuckets[i] = ordered.pop();
        }

        return new InternalPathHierarchy(getName(), Arrays.asList(reducedBuckets), order, requiredSize, shardSize,
                otherDocCount, separator, pipelineAggregators(), getMetaData());
    }

    private void doXContentInternal(XContentBuilder builder, Params params, InternalBucket currentBucket,
                                    Iterator<InternalBucket> bucketIterator) throws IOException {
        builder.startObject();
        builder.field(CommonFields.KEY.getPreferredName(), currentBucket.basename);
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), currentBucket.getDocCount());
        currentBucket.getAggregations().toXContentInternal(builder, params);

        if (bucketIterator.hasNext()) {
            InternalBucket nextBucket = bucketIterator.next();
            if (nextBucket.level == currentBucket.level) {
                builder.endObject();
            } else if (nextBucket.level > currentBucket.level) {
                builder.startObject(name);
                builder.startArray(CommonFields.BUCKETS.getPreferredName());
            } else {
                builder.endObject();
                for (int i=currentBucket.level; i > nextBucket.level; i--) {
                    builder.endArray();
                    builder.endObject();
                    builder.endObject();
                }
            }
            doXContentInternal(builder, params, nextBucket, bucketIterator);
        } else {
            if (currentBucket.level > 0) {
                builder.endObject();
                for (int i=currentBucket.level; i > 0; i--) {
                    builder.endArray();
                    builder.endObject();
                    builder.endObject();
                }
            } else {
                builder.endObject();
            }
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), otherDocCount);
        Iterator<InternalBucket> bucketIterator = buckets.iterator();
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        if (bucketIterator.hasNext()) {
            InternalBucket firstBucket = bucketIterator.next();
            doXContentInternal(builder, params, firstBucket, bucketIterator);
        }
        builder.endArray();
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, separator, order, requiredSize, shardSize, otherDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalPathHierarchy that = (InternalPathHierarchy) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(separator, that.separator)
                && Objects.equals(order, that.order)
                && Objects.equals(requiredSize, that.requiredSize)
                && Objects.equals(shardSize, that.shardSize)
                && Objects.equals(otherDocCount, that.otherDocCount);
    }
}
