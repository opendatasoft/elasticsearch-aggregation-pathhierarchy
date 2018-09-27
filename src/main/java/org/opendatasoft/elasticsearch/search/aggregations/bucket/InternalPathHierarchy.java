package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.BytesRef;
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

/**
 * An internal implementation of {@link InternalMultiBucketAggregation} which extends {@link Aggregation}.
 * Mainly, returns the builder and makes the reduce of buckets.
 */
public class InternalPathHierarchy extends InternalMultiBucketAggregation<InternalPathHierarchy,
        InternalPathHierarchy.InternalBucket> implements PathHierarchy {
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
        protected String[] path;
        protected String basename;

        public InternalBucket(long docCount, InternalAggregations aggregations, String basename, BytesRef term, int level, String[] path) {
            termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.path = path;
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
            path = new String[path_length];
            for (int i = 0; i < path_length; i++) {
                path[i] = in.readString();
            }
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
            out.writeInt(path.length);
            for (String path: this.path) {
                out.writeString(path);
            }
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
    private final long minDocCount = 1;

    public InternalPathHierarchy(
            String name,
            List<InternalBucket> buckets,
            BucketOrder order,
            int requiredSize,
            BytesRef separator,
            List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData
    ) {
        super(name, pipelineAggregators, metaData);
        this.buckets = buckets;
        this.order = order;
        this.requiredSize = requiredSize;
        this.separator = separator;
    }

    /**
     * Read from a stream.
     */
    public InternalPathHierarchy(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = readSize(in);
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
        out.writeBytesRef(separator);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return PathHierarchyAggregationBuilder.NAME;
    }

    @Override
    public InternalPathHierarchy create(List<InternalBucket> buckets) {
        return new InternalPathHierarchy(this.name, buckets, order, requiredSize,
                this.separator, this.pipelineAggregators(), this.metaData);
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(prototype.docCount, aggregations, prototype.basename, prototype.termBytes,
                prototype.level, prototype.path);
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

        // extract buckets from aggregations
        for (InternalAggregation aggregation : aggregations) {
            InternalPathHierarchy pathHierarchy = (InternalPathHierarchy) aggregation;
            if (buckets == null) {
                buckets = new HashMap<>();
            }

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

        // cast the buckets set-list to a Map object which is a tree of buckets containers
        Map<String, List<InternalBucket>> res = new HashMap<>();
        for (InternalBucket bucket: reducedBuckets) {
            String key = bucket.path.length > 0 ? String.join(separator.utf8ToString(), bucket.path) : separator.utf8ToString();

            List<InternalBucket> listBuckets = res.get(key);
            if (listBuckets == null) {
                listBuckets = new ArrayList<>();
            }
            listBuckets.add(bucket);
            res.put(key, listBuckets);
        }

        return new InternalPathHierarchy(getName(), createBucketListFromMap(res), order, requiredSize,
                separator, pipelineAggregators(), getMetaData());
    }

    private List<InternalBucket> createBucketListFromMap(Map<String, List<InternalBucket>> buckets) {
        List<InternalBucket> res = new ArrayList<>();

        if (buckets.size() > 0) {
            List<InternalBucket> rootList = buckets.get(separator.utf8ToString());
            createBucketListFromMapRecurse(res, buckets, rootList);
        }

        return res;
    }

    private void createBucketListFromMapRecurse(List<InternalBucket> res, Map<String, List<InternalBucket>> mapBuckets,
                                                List<InternalBucket> buckets) {
        for (InternalBucket bucket: buckets) {
            res.add(bucket);

            List<InternalBucket> children =  mapBuckets.get(bucket.getKey());
            if (children != null && ! children.isEmpty()) {
                createBucketListFromMapRecurse(res, mapBuckets, children);
            }
        }
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
        return Objects.hash(buckets, separator, order);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalPathHierarchy that = (InternalPathHierarchy) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(separator, that.separator)
                && Objects.equals(order, that.order);
    }
}
