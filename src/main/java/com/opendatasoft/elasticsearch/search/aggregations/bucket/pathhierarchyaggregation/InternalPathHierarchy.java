package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.AggregationStreams;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.*;

public class InternalPathHierarchy extends InternalAggregation implements PathHierarchy {

    public static final Type TYPE = new Type("path_hierarchy", "phierarchy");

    public static final AggregationStreams.Stream STREAM = new AggregationStreams.Stream() {
        @Override
        public InternalPathHierarchy readResult(StreamInput in) throws IOException {
            InternalPathHierarchy buckets = new InternalPathHierarchy();
            buckets.readFrom(in);
            return buckets;
        }
    };

    public static void registerStreams() {
        AggregationStreams.registerStream(STREAM, TYPE.stream());
    }

    static class Bucket implements PathHierarchy.Bucket {

        BytesRef termBytes;
        Long hash;

        protected long docCount;
        protected InternalAggregations aggregations;
        protected int level;
        protected String[] path;
        protected String val;
        protected List<Bucket> children;

        public Bucket(Long hash, String val, BytesRef term, long docCount, InternalAggregations aggregations, int level, String[] path) {
            this.hash = hash;
            this.termBytes = term;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.level = level;
            this.path = path;
            this.val = val;
            this.children = new ArrayList<>();
        }

        @Override
        public String getKey() {
            return termBytes.utf8ToString();
        }

        @Override
        public Text getKeyAsText() {
            return new BytesText(new BytesArray(termBytes));
        }

        @Override
        public int compareTerm(PathHierarchy.Bucket other) {
            return BytesRef.getUTF8SortedAsUnicodeComparator().compare(termBytes, ((Bucket) other).termBytes);
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        public Bucket reduce(List<? extends Bucket> buckets, ReduceContext reduceContext) {
            List<InternalAggregations> aggregationsList = new ArrayList<InternalAggregations>(buckets.size());
            Bucket reduced = null;
            for (Bucket bucket : buckets) {
                if (reduced == null) {
                    reduced = bucket;
                } else {
                    reduced.docCount += bucket.docCount;
                }
                aggregationsList.add(bucket.aggregations);
            }
            reduced.aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            return reduced;
        }

    }

    private Map<String, List<Bucket>> buckets;
    protected Map<BytesRef, Bucket> bucketMap;
    private InternalOrder order;
    private String separator;

    InternalPathHierarchy() {
    } // for serialization

    public InternalPathHierarchy(String name, Map<String, List<Bucket>> buckets, InternalOrder order, String separator) {
        super(name);
        this.buckets = buckets;
        this.order = order;
        this.separator = separator;
    }

    @Override
    public Type type() {
        return TYPE;
    }

    @Override
    public Collection<PathHierarchy.Bucket> getBuckets() {
        Object o = buckets;
        return (Collection<PathHierarchy.Bucket>) o;
    }

    @Override
    public PathHierarchy.Bucket getBucketByKey(String path) {
        if (bucketMap == null) {
            bucketMap = new HashMap<>();
            for(List<Bucket> buckets: this.buckets.values()) {
                for (Bucket bucket: buckets) {
                    bucketMap.put(bucket.termBytes, bucket);
                }
            }
        }
        return bucketMap.get(new BytesRef(path));
    }

    @Override
    public InternalPathHierarchy reduce(ReduceContext reduceContext) {
        List<InternalAggregation> aggregations = reduceContext.aggregations();

        Map<BytesRef, List<Bucket>> buckets = null;
        for (InternalAggregation aggregation : aggregations) {
            InternalPathHierarchy pathHierarchy = (InternalPathHierarchy) aggregation;
            if (buckets == null) {
                buckets = new HashMap<>();
            }

            for (List<Bucket> tmpBuckets: pathHierarchy.buckets.values()) {
                for (Bucket bucket: tmpBuckets) {
                    List<Bucket> existingBuckets = buckets.get(bucket.termBytes);
                    if (existingBuckets == null) {
                        existingBuckets = new ArrayList<>(aggregations.size());
                        buckets.put(bucket.termBytes, existingBuckets);
                    }
                    existingBuckets.add(bucket);
                }
            }
        }

        List<Bucket> reduced = new ArrayList<>((int) buckets.size());

        for (Map.Entry<BytesRef, List<Bucket>> entry : buckets.entrySet()) {
            List<Bucket> sameCellBuckets = entry.getValue();
            reduced.add(sameCellBuckets.get(0).reduce(sameCellBuckets, reduceContext));
        }

        Map<String, List<Bucket>> res = new HashMap<>();
        for (Bucket bucket: reduced) {
            String key = bucket.path.length > 0 ? StringUtils.join(bucket.path, separator) : separator;

            List<Bucket> listBuckets = res.get(key);
            if (listBuckets == null) {
                listBuckets = new ArrayList<>();
            }
            listBuckets.add(bucket);
            res.put(key, listBuckets);
        }

        for (List<InternalPathHierarchy.Bucket> bucket: res.values()) {
            CollectionUtil.introSort(bucket, order.comparator());
        }

        return new InternalPathHierarchy(getName(), res, order, separator);
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        this.name = in.readString();
        order = InternalOrder.Streams.readOrder(in);
        int mapSize = in.readVInt();
        this.buckets = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            String key = in.readString();
            int listSize = in.readInt();
            List<Bucket> bucketList = new ArrayList<>(listSize);
            for (int j=0; j < listSize; j++) {
                Bucket bucket = new Bucket(in.readLong(), in.readString(), in.readBytesRef(), in.readLong(), InternalAggregations.readAggregations(in), in.readInt(), null);
                int sizePath = in.readInt();
                String [] paths = new String[sizePath];
                for (int k=0; k < sizePath; k++) {
                    paths[k] = in.readString();
                }
                bucket.path = paths;
                bucketList.add(bucket);
            }
            this.buckets.put(key, bucketList);
        }
        this.bucketMap = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        InternalOrder.Streams.writeOrder(order, out);
        out.writeVInt(buckets.size());
        for (String bucketKey : buckets.keySet()) {
            out.writeString(bucketKey);
            List<Bucket> bucketList = buckets.get(bucketKey);
            out.writeInt(bucketList.size());
            for (Bucket bucket: bucketList) {
                out.writeLong(bucket.hash);
                out.writeString(bucket.val);
                out.writeBytesRef(bucket.termBytes);
                out.writeLong(bucket.docCount);
                ((InternalAggregations) bucket.getAggregations()).writeTo(out);
                out.writeInt(bucket.level);
                out.writeInt(bucket.path.length);
                for (String path: bucket.path) {
                    out.writeString(path);
                }
            }
        }
    }


    private void doXcontentRecurse(XContentBuilder builder, Params params, List<Bucket> buckets) throws IOException {
        for (Bucket bucket: buckets) {
            List<Bucket> childBuckets = this.buckets.get(bucket.getKey());
            builder.startObject();
            builder.field(CommonFields.KEY, bucket.val);
            builder.field(CommonFields.DOC_COUNT, bucket.getDocCount());
//            builder.field("level", bucket.level);
//            builder.field("path", bucket.path);
            ((InternalAggregations) bucket.getAggregations()).toXContentInternal(builder, params);
            if (childBuckets != null && ! childBuckets.isEmpty()) {
                builder.startObject(name);
                builder.startArray(CommonFields.BUCKETS);
                doXcontentRecurse(builder, params, childBuckets);
                builder.endArray();
                builder.endObject();
            }
            builder.endObject();
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(CommonFields.BUCKETS);

        doXcontentRecurse(builder, params, buckets.get(separator));

        builder.endArray();
        return builder;
    }

}
