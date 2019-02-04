package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

public interface PathHierarchyComparable<T extends Bucket & PathHierarchyComparable<T>> {

    /**
     * Compare this {@link PathHierarchy.Bucket}s {@link PathHierarchy.Bucket#getKey() key} with another bucket.
     *
     * @param other the bucket that contains the key to compare to.
     * @return a negative integer, zero, or a positive integer as this buckets key
     * is less than, equal to, or greater than the other buckets key.
     */
    int compareCount(T other, boolean isAsc);

    int compareKey(T other, boolean isAsc);
}
