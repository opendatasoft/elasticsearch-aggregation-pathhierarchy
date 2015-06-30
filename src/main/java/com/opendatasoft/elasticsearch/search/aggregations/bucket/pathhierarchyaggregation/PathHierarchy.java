package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import java.util.Comparator;
import java.util.List;


public interface PathHierarchy extends MultiBucketsAggregation {

    public static interface Bucket extends MultiBucketsAggregation.Bucket {

        int compareTerm(Bucket other);

    }

    List<Bucket> getBuckets();

    Bucket getBucketByKey(String term);


    /**
     * A strategy defining the order in which the buckets in this path hierarchy are ordered.
     */
    static abstract class Order implements ToXContent {

        public static final Order KEY_ASC = new InternalOrder((byte) 1, "_term", true, new Comparator<InternalPathHierarchy.Bucket>() {
            @Override
            public int compare(InternalPathHierarchy.Bucket b1, InternalPathHierarchy.Bucket b2) {
                return b1.compareTerm(b2);
            }
        });

        public static final Order KEY_DESC = new InternalOrder((byte) 2, "_term", false, new Comparator<InternalPathHierarchy.Bucket>() {
            @Override
            public int compare(InternalPathHierarchy.Bucket b1, InternalPathHierarchy.Bucket b2) {
                return b2.compareTerm(b1);
            }
        });

        public static final Order COUNT_ASC = new InternalOrder((byte) 3, "_count", true, new Comparator<InternalPathHierarchy.Bucket>() {
            @Override
            public int compare(InternalPathHierarchy.Bucket b1, InternalPathHierarchy.Bucket b2) {
                int cmp = Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = b1.compareTerm(b2);
                }
                return cmp;
            }
        });


        public static final Order COUNT_DESC = new InternalOrder((byte) 4, "_count", false, new Comparator<InternalPathHierarchy.Bucket>() {
            @Override
            public int compare(InternalPathHierarchy.Bucket b1, InternalPathHierarchy.Bucket b2) {
                int cmp = -Long.compare(b1.getDocCount(), b2.getDocCount());
                if (cmp == 0) {
                    cmp = b1.compareTerm(b2);
                }
                return cmp;
            }
        });

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a single-valued calc sug-aggregation
         *
         * @param path the name of the aggregation
         * @param asc             The direction of the order (ascending or descending)
         */
        public static Order aggregation(String path, boolean asc) {
            return new InternalOrder.Aggregation(path, asc);
        }

        /**
         * Creates a bucket ordering strategy that sorts buckets based on a multi-valued calc sug-aggregation
         *
         * @param aggregationName the name of the aggregation
         * @param valueName       The name of the value of the multi-value get by which the sorting will be applied
         * @param asc             The direction of the order (ascending or descending)
         */
        public static Order aggregation(String aggregationName, String valueName, boolean asc) {
            return new InternalOrder.Aggregation(aggregationName + "." + valueName, asc);
        }

        /**
         * @return The bucket comparator by which the order will be applied.
         */
        abstract Comparator<InternalPathHierarchy.Bucket> comparator();

    }

}
