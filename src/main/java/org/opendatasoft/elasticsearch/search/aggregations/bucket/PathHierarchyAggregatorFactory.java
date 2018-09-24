package org.opendatasoft.elasticsearch.search.aggregations.bucket;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalOrder;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The factory of aggregators.
 * ValuesSourceAggregatorFactory extends {@link AggregatorFactory}
 */
class PathHierarchyAggregatorFactory extends ValuesSourceAggregatorFactory<ValuesSource, PathHierarchyAggregatorFactory> {

    private BytesRef separator;
    private int maxDepth;
    private int minDepth;
    private BucketOrder order;

    PathHierarchyAggregatorFactory(String name,
                                   ValuesSourceConfig<ValuesSource> config,
                                   String separator,
                                   int minDepth,
                                   int maxDepth,
                                   BucketOrder order,
                                   SearchContext context,
                                   AggregatorFactory<?> parent,
                                   AggregatorFactories.Builder subFactoriesBuilder,
                                   Map<String, Object> metaData
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metaData);
        this.separator = new BytesRef(separator);
        this.minDepth = minDepth;
        this.maxDepth = maxDepth;
        this.order = order;
    }

    @Override
    protected Aggregator createUnmapped(
            Aggregator parent,
            List<PipelineAggregator> pipelineAggregators,
            Map<String,
            Object> metaData) throws IOException {
        final InternalAggregation aggregation = new InternalPathHierarchy(name, new ArrayList<>(), order, separator,
                pipelineAggregators, metaData);
        return new NonCollectingAggregator(name, context, parent, factories, pipelineAggregators, metaData) {
            {
                // even in the case of an unmapped aggregator, validate the
                // order
                InternalOrder.validate(order, this);
            }

            @Override
            public InternalAggregation buildEmptyAggregation() { return aggregation; }
        };
    }

    /**
     * Crée le (ou les) aggregator.
     * récupère les docValues brutes, qu'on passe ensuite au constructeur de l'aggregator
     */
    @Override
    protected Aggregator doCreateInternal(
            ValuesSource valuesSource, Aggregator parent,
            boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        ValuesSource valuesSourceBytes = new HierarchyValuesSource(valuesSource, separator, minDepth, maxDepth);
        return new PathHierarchyAggregator(
                name, factories, context,
                valuesSourceBytes, order, separator,
                parent, pipelineAggregators, metaData);
    }

    /**
     * A list of per-document binary values, sorted according to {@link BytesRef}.
     * There might be dups however.
     * @see ValuesSource
     */
    private static class HierarchyValues extends SortingBinaryDocValues {

        /** valuesSource is a list of per-document binary values, sorted according to {@link BytesRef#compareTo(BytesRef)}
         * (warning, there might be dups however).
         */
        private SortedBinaryDocValues valuesSource;
        private BytesRef separator;
        private int minDepth;
        private int maxDepth;

        private HierarchyValues(SortedBinaryDocValues valuesSource, BytesRef separator, int minDepth, int maxDepth) {
            this.valuesSource = valuesSource;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
        }

        /**
         * Handles iterations on doc values:
         *  Advance the iterator to exactly target and return whether target has a value.
         *  target must be greater than or equal to the current doc ID and must be a valid doc ID, ie. &ge; 0 and &lt; maxDoc.
         *  After this method returns, docID() returns target.
         */
        @Override
        public boolean advanceExact(int docId) throws IOException {
            if (valuesSource.advanceExact(docId)) {
                count = valuesSource.docValueCount();
                grow();
                int t = 0;
                for (int i=0; i < valuesSource.docValueCount(); i++) {
                    int depth = 0;
                    int lastOff = 0;
                    BytesRef val = valuesSource.nextValue();

                    BytesRefBuilder cleanVal = new BytesRefBuilder();

                    for (int offset=0; offset < val.length; offset++) {
                        //  if it is a separator
                        if (new BytesRef(val.bytes, val.offset + offset, separator.length).equals(separator)) {
                            if (offset - lastOff > 1) {
                                if (cleanVal.length() > 0) {
                                    cleanVal.append(separator);
                                }
                                if (minDepth > depth) {
                                    depth++;
                                    offset += separator.length - 1;
                                    lastOff = offset + 1;
                                    continue;
                                }
                                cleanVal.append(val.bytes, val.offset + lastOff, offset - lastOff);
                                values[t++].copyBytes(cleanVal);
                                depth++;
                                if (maxDepth >= 0 && depth > maxDepth) {
                                    break;
                                }
                                offset += separator.length - 1;
                                lastOff = offset + 1;
                                count++;
                                grow();
                            } else {
                                lastOff = offset + separator.length;
                            }
                        } else if (offset == val.length -1) {
                            if (cleanVal.length() > 0) {
                                cleanVal.append(separator);
                            }
                            if (depth >= minDepth) {
                                cleanVal.append(val.bytes, val.offset + lastOff, offset - lastOff + 1);
                            }
                        }
                    }
                    if (maxDepth >= 0 && depth > maxDepth) {
                        continue;
                    }
                    values[t++].copyBytes(cleanVal);
                }
                sort();  // sort values that are stored between offsets 0 and count of values
                return true;
            } else {
                grow();
                return false;
            }
        }
    }


    /**
     * To get ValuesSource as sorted bytes.
     */
    private static class HierarchyValuesSource extends ValuesSource.Bytes {
        private final ValuesSource values;
        private final BytesRef separator;
        private final int minDepth;
        private final int maxDepth;

        private HierarchyValuesSource(ValuesSource values, BytesRef separator, int minDepth, int maxDepth){
            this.values = values;
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
            return new HierarchyValues(values.bytesValues(context), separator, minDepth, maxDepth);
        }

    }
}

