package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A multi bucket aggregation which returns buckets as a tree hierarchy based on a path field.
 * The aggregation will result in nested buckets: each bucket is a node (or a leaf) of the tree, and will contain all
 * documents having the concerned path segment in their own base path.
 * Strongly inspired by {@link org.elasticsearch.search.aggregations.bucket.terms.Terms}.
 */
public interface PathHierarchy extends MultiBucketsAggregation {
    interface Bucket extends MultiBucketsAggregation.Bucket {}

    @Override
    List<? extends Bucket> getBuckets();

}
