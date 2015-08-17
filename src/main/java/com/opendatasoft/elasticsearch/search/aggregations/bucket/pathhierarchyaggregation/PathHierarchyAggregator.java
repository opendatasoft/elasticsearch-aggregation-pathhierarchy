package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class PathHierarchyAggregator extends BucketsAggregator {

    private static final int INITIAL_CAPACITY = 50; // TODO sizing

    private final ValuesSource valuesSource;
    protected final BytesRefHash bucketOrds;
    private SortedBinaryDocValues values;
    private final BytesRefBuilder previous;
    private final BytesRef separator;
    private final InternalOrder order;


    public PathHierarchyAggregator(String name, AggregatorFactories factories, ValuesSource valuesSource,
                                   AggregationContext aggregationContext, Aggregator parent, BytesRef separator, InternalOrder order) {
        super(name, BucketAggregationMode.PER_BUCKET, factories, INITIAL_CAPACITY, aggregationContext, parent);
        this.valuesSource = valuesSource;
        bucketOrds = new BytesRefHash(estimatedBucketCount, aggregationContext.bigArrays());
        previous = new BytesRefBuilder();
        this.separator = separator;
        this.order = order;
    }

    @Override
    public boolean shouldCollect() {
        return true;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.bytesValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        assert owningBucketOrdinal == 0;
        values.setDocument(doc);
        final int valuesCount = values.count();

        previous.clear();
        // SortedBinaryDocValues don't guarantee uniqueness so we need to take care of dups
        for (int i = 0; i < valuesCount; ++i) {

            final BytesRef bytes = values.valueAt(i);

            if (previous.get().equals(bytes)) {
                continue;
            }
            long bucketOrdinal = bucketOrds.add(bytes);
            if (bucketOrdinal < 0) { // already seen
                bucketOrdinal = - 1 - bucketOrdinal;
                collectExistingBucket(doc, bucketOrdinal);
            } else {
                collectBucket(doc, bucketOrdinal);
            }
            previous.copyBytes(bytes);
        }
    }

    @Override
    public InternalPathHierarchy buildAggregation(long owningBucketOrdinal) {
        assert owningBucketOrdinal == 0;

        List<InternalPathHierarchy.Bucket> buckets = new ArrayList<>();
        InternalPathHierarchy.Bucket spare;

        for (long i = 0; i < bucketOrds.size(); i++) {

            spare = new InternalPathHierarchy.Bucket(null, new BytesRef(), 0, null, 0, null);

            BytesRef term = new BytesRef();
            bucketOrds.get(i, term);

            String [] paths = term.utf8ToString().split(Pattern.quote(separator.utf8ToString()));

            spare.termBytes = BytesRef.deepCopyOf(term);
            spare.docCount = bucketDocCount(i);
            spare.aggregations = bucketAggregations(i);
            spare.level = paths.length - 1;

            spare.val = paths[paths.length - 1];
            spare.path = Arrays.copyOf(paths, paths.length - 1);

            buckets.add(spare);
        }

        return new InternalPathHierarchy(name, buckets, order, separator);
    }

    @Override
    public InternalPathHierarchy buildEmptyAggregation() {
        return new InternalPathHierarchy(name, new ArrayList<InternalPathHierarchy.Bucket>(), order, separator);
    }


    @Override
    public void doClose() {
        Releasables.close(bucketOrds);
    }

}