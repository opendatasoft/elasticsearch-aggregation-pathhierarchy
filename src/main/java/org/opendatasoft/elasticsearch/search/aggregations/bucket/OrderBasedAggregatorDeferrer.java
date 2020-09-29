package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.HashSet;
import java.util.Set;

/**
 * Contains logic shared by Aggregator for deferring sub aggregator collection.
 * Copy/Pasted from TermsAggregator
 */
public class OrderBasedAggregatorDeferrer {

    protected final Set<Aggregator> aggsUsedForSorting = new HashSet<>();
    protected final Aggregator.SubAggCollectionMode collectMode;

    public OrderBasedAggregatorDeferrer(Aggregator parent, Aggregator[] subAggregators, BucketOrder order) {
        if (subAggsNeedScore(subAggregators) && descendsFromNestedAggregator(parent)) {
            /**
             * Force the execution to depth_first because we need to access the score of
             * nested documents in a sub-aggregation and we are not able to generate this score
             * while replaying deferred documents.
             */
            this.collectMode = Aggregator.SubAggCollectionMode.DEPTH_FIRST;
        } else {
            this.collectMode = Aggregator.SubAggCollectionMode.BREADTH_FIRST;
        }

        // Don't defer any child agg if we are dependent on it for pruning results
        if (order instanceof InternalOrder.Aggregation) {
            AggregationPath path = ((InternalOrder.Aggregation) order).path();
            aggsUsedForSorting.add(path.resolveTopmostAggregator(parent));
        } else if (order instanceof InternalOrder.CompoundOrder) {
            InternalOrder.CompoundOrder compoundOrder = (InternalOrder.CompoundOrder) order;
            for (BucketOrder orderElement : compoundOrder.orderElements()) {
                if (orderElement instanceof InternalOrder.Aggregation) {
                    AggregationPath path = ((InternalOrder.Aggregation) orderElement).path();
                    aggsUsedForSorting.add(path.resolveTopmostAggregator(parent));
                }
            }
        }
    }

    static boolean descendsFromNestedAggregator(Aggregator parent) {
        while (parent != null) {
            if (parent.getClass() == NestedAggregator.class) {
                return true;
            }
            parent = parent.parent();
        }
        return false;
    }

    static boolean subAggsNeedScore(Aggregator[] subAggregators) {
        for (Aggregator subAgg : subAggregators) {
            if (subAgg.scoreMode().needsScores()) {
                return true;
            }
        }
        return false;
    }


     public boolean shouldDefer(Aggregator subAggregator) {
        return collectMode == Aggregator.SubAggCollectionMode.BREADTH_FIRST
                && !aggsUsedForSorting.contains(subAggregator);
    }

}
