package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.KeyComparable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * An internal implementation of {@link InternalMultiBucketAggregation} which extends {@link Aggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalPathHierarchy extends InternalMultiBucketAggregation<InternalPathHierarchy,
        InternalPathHierarchy.InternalBucket> implements PathHierarchy {
    protected static final ParseField SUM_OF_OTHER_HIERARCHY_NODES = new ParseField("sum_other_hierarchy_nodes");

    /**
     * The bucket class of InternalPathHierarchy.
     * @see MultiBucketsAggregation.Bucket
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket implements
            PathHierarchy.Bucket, KeyComparable<InternalBucket> {

        BytesRef termBytes;
        long bucketOrd;
        protected String[] paths;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected String basename;

        public InternalBucket(long docCount, InternalAggregations aggregations, String basename, BytesRef term, int level, String[] paths) {
            termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.basename = basename;
            this.paths = paths;
        }

        /**
         * Read from a stream.
         */
        public InternalBucket(StreamInput in) throws IOException {
            termBytes = in.readBytesRef();
            docCount = in.readLong();
            aggregations = InternalAggregations.readAggregations(in);
            level = in.readInt();
            basename = in.readString();
            int pathsSize = in.readInt();
            paths = new String[pathsSize];
            for (int i=0; i < pathsSize; i++) {
                paths[i] = in.readString();
            }
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
            out.writeInt(paths.length);
            for (String path: paths) {
                out.writeString(path);
            }
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
        public int compareKey(InternalBucket other) {
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
    private final long otherHierarchyNodes;
    private final long minDocCount;

    public InternalPathHierarchy(
            String name,
            List<InternalBucket> buckets,
            BucketOrder order,
            long minDocCount,
            int requiredSize,
            int shardSize,
            long otherHierarchyNodes,
            BytesRef separator,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.minDocCount = minDocCount;
        this.requiredSize = requiredSize;
        this.shardSize = shardSize;
        this.otherHierarchyNodes = otherHierarchyNodes;
        this.separator = separator;
    }

    /**
     * Read from a stream.
     */
    public InternalPathHierarchy(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        minDocCount = in.readVLong();
        requiredSize = readSize(in);
        shardSize = readSize(in);
        otherHierarchyNodes = in.readVLong();
        separator = in.readBytesRef();
        int bucketsSize = in.readInt();
        this.buckets = new ArrayList<>(bucketsSize);
        for (int i=0; i<bucketsSize; i++) {
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
        out.writeBytesRef(separator);
        out.writeInt(buckets.size());
        for (InternalBucket bucket: buckets) {
            bucket.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return PathHierarchyAggregationBuilder.NAME;
    }

    protected int getShardSize() {
        return shardSize;
    }

    public long getSumOtherHierarchyNodes() {
        return otherHierarchyNodes;
    }

    @Override
    public InternalPathHierarchy create(List<InternalBucket> buckets) {
        return new InternalPathHierarchy(this.name, buckets, order, minDocCount, requiredSize, shardSize, otherHierarchyNodes,
                this.separator, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.docCount, aggregations, prototype.basename, prototype.termBytes,
                prototype.level, prototype.paths);
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
        long otherHierarchyNodes = 0;

        // extract buckets from aggregations
        for (InternalAggregation aggregation : aggregations) {
            InternalPathHierarchy pathHierarchy = (InternalPathHierarchy) aggregation;
            if (buckets == null) {
                buckets = new TreeMap<>();
            }

            otherHierarchyNodes += pathHierarchy.getSumOtherHierarchyNodes();

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
        PathSortedTree<String, InternalBucket> ordered = new PathSortedTree<>(order.comparator(null), size);
        for (List<InternalBucket> sameTermBuckets : buckets.values()) {
            final InternalBucket b = sameTermBuckets.get(0).reduce(sameTermBuckets, reduceContext);
            if (b.getDocCount() >= minDocCount || !reduceContext.isFinalReduce()) {
                reduceContext.consumeBucketsAndMaybeBreak(1);
                ordered.add(b.paths, b);
            } else {
                reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(b));
            }
        }

        long sum_other_hierarchy_nodes = ordered.getFullSize() - size + otherHierarchyNodes;
        return new InternalPathHierarchy(getName(), ordered.getAsList(), order, minDocCount, requiredSize, shardSize,
                sum_other_hierarchy_nodes, separator, pipelineAggregators(), getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
//        builder.field(SUM_OF_OTHER_HIERARCHY_NODES.getPreferredName(), otherHierarchyNodes);
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
            builder.field(CommonFields.KEY.getPreferredName(), currentBucket.basename);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), currentBucket.docCount);
            currentBucket.getAggregations().toXContentInternal(builder, params);

            prevBucket = currentBucket;
        }

        if (currentBucket != null) {
            for (int i=0; i < currentBucket.level; i++) {
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
    protected int doHashCode() {
        return Objects.hash(buckets, separator, order, requiredSize, shardSize, otherHierarchyNodes);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalPathHierarchy that = (InternalPathHierarchy) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(separator, that.separator)
                && Objects.equals(order, that.order)
                && Objects.equals(requiredSize, that.requiredSize)
                && Objects.equals(shardSize, that.shardSize)
                && Objects.equals(otherHierarchyNodes, that.otherHierarchyNodes);
    }
}
