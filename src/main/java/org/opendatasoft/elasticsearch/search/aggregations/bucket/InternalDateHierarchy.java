package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation}
 * which extends {@link org.elasticsearch.search.aggregations.Aggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalDateHierarchy extends InternalMultiBucketAggregation<InternalDateHierarchy, InternalDateHierarchy.InternalBucket> {

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        Map<BytesRef, List<InternalBucket>> buckets = new LinkedHashMap<>();

        return new AggregatorReducer() {
            private long otherHierarchyNodes = 0;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalDateHierarchy dateHierarchy = (InternalDateHierarchy) aggregation;

                otherHierarchyNodes += dateHierarchy.getSumOtherHierarchyNodes();

                for (InternalBucket bucket : dateHierarchy.buckets) {
                    List<InternalBucket> existingBuckets = buckets.get(bucket.key);
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>(size);
                        buckets.put(bucket.key, existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }
            }

            @Override
            public InternalAggregation get() {
                final int size = !reduceContext.isFinalReduce() ? buckets.size() : Math.min(requiredSize, buckets.size());
                PathSortedTree<String, InternalBucket> ordered = new PathSortedTree<>(order.comparator(), size);

                for (List<InternalBucket> sameTermBuckets : buckets.values()) {
                    final InternalBucket b = reduceBucket(sameTermBuckets, reduceContext);
                    if (b.getDocCount() >= minDocCount || !reduceContext.isFinalReduce()) {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                        ordered.add(b.paths, b);
                    } else {
                        reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
                    }
                }

                long sum_other_hierarchy_nodes = ordered.getFullSize() - size + otherHierarchyNodes;

                return new InternalDateHierarchy(
                    getName(),
                    ordered.getAsList(),
                    order,
                    minDocCount,
                    requiredSize,
                    shardSize,
                    sum_other_hierarchy_nodes,
                    getMetadata()
                );
            }
        };
    }

    /**
     * The bucket class of InternalDateHierarchy.
     * @see MultiBucketsAggregation.Bucket
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucketWritable
        implements
            KeyComparable<InternalBucket> {

        BytesRef key;
        String name;
        long bucketOrd;
        protected String[] paths;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;

        public InternalBucket(long docCount, InternalAggregations aggregations, BytesRef key, String name, int level, String[] paths) {
            this.key = key;
            this.name = name;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.paths = paths;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            key = in.readBytesRef();
            name = in.readString();
            docCount = in.readLong();
            aggregations = InternalAggregations.readFrom(in);
            level = in.readInt();
            int pathsSize = in.readInt();
            paths = new String[pathsSize];
            for (int i = 0; i < pathsSize; i++) {
                paths[i] = in.readString();
            }
        }

        /**
         * Write to a stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeBytesRef(key);
            out.writeString(name);
            out.writeLong(docCount);
            aggregations.writeTo(out);
            out.writeInt(level);
            out.writeInt(paths.length);
            for (String path : paths) {
                out.writeString(path);
            }
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            return key.utf8ToString();
        }

        @Override
        public int compareKey(InternalBucket other) {
            return key.compareTo(other.key);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public InternalAggregations getAggregations() {
            return aggregations;
        }

        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

    private List<InternalBucket> buckets;
    private BucketOrder order;
    private final int requiredSize;
    private final int shardSize;
    private final long otherHierarchyNodes;
    private final long minDocCount;

    public InternalDateHierarchy(
        String name,
        List<InternalBucket> buckets,
        BucketOrder order,
        long minDocCount,
        int requiredSize,
        int shardSize,
        long otherHierarchyNodes,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.order = order;
        this.minDocCount = minDocCount;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherHierarchyNodes = otherHierarchyNodes;
    }

    /**
     * Read from a stream.
     */
    public InternalDateHierarchy(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherHierarchyNodes = in.readVLong();
        int bucketsSize = in.readInt();
        this.buckets = new ArrayList<>(bucketsSize);
        for (int i = 0; i < bucketsSize; i++) {
            this.buckets.add(new InternalBucket(in));
        }
    }

    /**
     * Write to a stream.
     */
    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVLong(minDocCount);
        writeSize(requiredSize, out);
        writeSize(shardSize, out);
        out.writeVLong(otherHierarchyNodes);
        out.writeInt(buckets.size());
        for (InternalBucket bucket : buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return DateHierarchyAggregationBuilder.NAME;
    }

    protected int getShardSize() {
        return shardSize;
    }

    public long getSumOtherHierarchyNodes() {
        return otherHierarchyNodes;
    }

    @Override
    public InternalDateHierarchy create(List<InternalBucket> buckets) {
        return new InternalDateHierarchy(
            this.name,
            buckets,
            order,
            minDocCount,
            requiredSize,
            shardSize,
            otherHierarchyNodes,
            this.metadata
        );
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.docCount, aggregations, prototype.key, prototype.name, prototype.level, prototype.paths);
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    protected InternalBucket reduceBucket(List<InternalBucket> buckets, AggregationReduceContext context) {
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
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        Iterator<InternalBucket> bucketIterator = buckets.iterator();
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        InternalBucket prevBucket = null;
        InternalBucket currentBucket = null;
        while (bucketIterator.hasNext()) {
            currentBucket = bucketIterator.next();

            if (prevBucket != null) {
                if (prevBucket.level == currentBucket.level) {
                    builder.endObject();
                } else if (prevBucket.level < currentBucket.level) {
                    builder.startObject(name);
                    builder.startArray(CommonFields.BUCKETS.getPreferredName());
                } else {
                    for (int i = currentBucket.level; i < prevBucket.level; i++) {
                        builder.endObject();
                        builder.endArray();
                        builder.endObject();
                    }
                    builder.endObject();
                }
            }

            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), currentBucket.name);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), currentBucket.docCount);
            currentBucket.getAggregations().toXContentInternal(builder, params);

            prevBucket = currentBucket;
        }

        if (currentBucket != null) {
            for (int i = 0; i < currentBucket.level; i++) {
                builder.endObject();
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
        }

        builder.endArray();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(buckets, order, requiredSize, shardSize, otherHierarchyNodes, minDocCount);
    }

    @Override
    public boolean equals(Object obj) {
        InternalDateHierarchy that = (InternalDateHierarchy) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(order, that.order)
            && Objects.equals(minDocCount, that.minDocCount)
            && Objects.equals(requiredSize, that.requiredSize)
            && Objects.equals(shardSize, that.shardSize)
            && Objects.equals(otherHierarchyNodes, that.otherHierarchyNodes);
    }
}
