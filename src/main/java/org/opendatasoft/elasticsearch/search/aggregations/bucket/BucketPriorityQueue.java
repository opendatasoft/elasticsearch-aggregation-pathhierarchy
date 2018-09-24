package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.util.PriorityQueue;
import java.util.Comparator;

/**
 * BucketQueue used in aggregator's buildAggregation() and aggregation's InternalPathHierarchy.doReduce() for storing
 * ordered buckets.
 */
public class BucketPriorityQueue <B extends InternalPathHierarchy.InternalBucket> extends PriorityQueue<B> {

    private final Comparator<? super B> comparator;

    public BucketPriorityQueue(int size, Comparator<? super B> comparator) {
        super(size);
        this.comparator = comparator;
    }

    @Override
    protected boolean lessThan(B a, B b) {
        return comparator.compare(a, b) > 0; // reverse, since we reverse again when adding to a list
    }
}
