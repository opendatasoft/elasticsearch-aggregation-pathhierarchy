package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.NonCollectingAggregator;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.*;

public class PathHierarchyParser implements Aggregator.Parser {

    @Override
    public String type() {
        return InternalPathHierarchy.TYPE.name();
    }

    public static final String DEFAULT_SEPARATOR = "/";
    public static final int DEFAULT_MIN_DEPTH = 0;
    public static final int DEFAULT_MAX_DEPTH = 2;

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalPathHierarchy.TYPE, context).scriptable(true).build();

        String separator = DEFAULT_SEPARATOR;
        int maxDepth = DEFAULT_MAX_DEPTH;
        int minDepth = DEFAULT_MIN_DEPTH;
        boolean depth = false;
        InternalOrder order = (InternalOrder) PathHierarchy.Order.KEY_ASC;

        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (vsParser.token(currentFieldName, token, parser)) {
                continue;
            } else if (token == XContentParser.Token.VALUE_STRING) {
                if ("separator".equals(currentFieldName)) {
                    separator = parser.text();
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                if (!depth && "max_depth".equals(currentFieldName)) {
                    maxDepth = parser.intValue();
                } else if (!depth && "min_depth".equals(currentFieldName)) {
                    minDepth = parser.intValue();
                } else if ("depth".equals(currentFieldName)) {
                    minDepth = maxDepth = parser.intValue();
                    depth = true;
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if ("order".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_STRING) {
                            String dir = parser.text();
                            boolean asc = "asc".equals(dir);
                            order = resolveOrder(currentFieldName, asc);
                            //TODO should we throw an error if the value is not "asc" or "desc"???
                        }
                    }
                }
            }
        }

        if (minDepth > maxDepth) {
            throw new SearchParseException(context, "min_depth paramater must be lower than max_depth parameter");
        }

        return new PathHierarchyFactory(aggregationName, vsParser.config(), new BytesRef(separator), minDepth, maxDepth, order);
    }


    private static class PathHierarchyFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

        private BytesRef separator;
        private int maxDepth;
        private int minDepth;
        private InternalOrder order;

        public PathHierarchyFactory(String name, ValuesSourceConfig<ValuesSource> config, BytesRef separator, int minDepth, int maxDepth, InternalOrder order) {
            super(name, InternalPathHierarchy.TYPE.name(), config);
            this.separator = separator;
            this.minDepth = minDepth;
            this.maxDepth = maxDepth;
            this.order = order;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            final InternalAggregation aggregation = new InternalPathHierarchy(name, new ArrayList<InternalPathHierarchy.Bucket>(), order, separator);
            return new NonCollectingAggregator(name, aggregationContext, parent) {
                public InternalAggregation buildEmptyAggregation() {
                    return aggregation;
                }
            };
        }

        @Override
        protected Aggregator create(final ValuesSource valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            final HierarchyValues hierarchyValues = new HierarchyValues(valuesSource, separator, minDepth, maxDepth);
            ValuesSource.Bytes hierarchySource = new PathHierarchySource(hierarchyValues, valuesSource.metaData());
            return new PathHierarchyAggregator(name, factories, hierarchySource, aggregationContext, parent, separator, order);

        }

        private static class HierarchyValues extends SortingBinaryDocValues {

            private ValuesSource hierarchyValues;
            private SortedBinaryDocValues hieraValues;
            private BytesRef separator;
            private int minDepth;
            private int maxDepth;

            protected HierarchyValues(ValuesSource hierarchyValues, BytesRef separator, int minDepth, int maxDepth) {
                this.hierarchyValues = hierarchyValues;
                this.separator = separator;
                this.minDepth = minDepth;
                this.maxDepth = maxDepth;
            }

            @Override
            public void setDocument(int docId) {
                hieraValues = hierarchyValues.bytesValues();
                hieraValues.setDocument(docId);
                count = hieraValues.count();
                grow();
                int t = 0;
                for (int i=0; i < hieraValues.count(); i++) {
                    int depth = 0;
                    int lastOff = 0;
                    BytesRef val = hieraValues.valueAt(i);

                    BytesRefBuilder cleanVal = new BytesRefBuilder();

                    for (int off=0; off < val.length; off++) {
//                        if (isSeparator(val.bytes, val.offset + off)) {
                        if (new BytesRef(val.bytes, val.offset + off, separator.length).equals(separator)) {
                            if (off - lastOff > 1) {
                                if (cleanVal.length() > 0) {
                                    cleanVal.append(separator);
                                }
                                if (minDepth > depth) {
                                    depth++;
                                    off += separator.length - 1;
                                    lastOff = off + 1;
                                    continue;
                                }
                                cleanVal.append(val.bytes, val.offset + lastOff, off - lastOff);
                                values[t++].copyBytes(cleanVal);
                                depth++;
                                if (maxDepth >= 0 && depth > maxDepth) {
                                    break;
                                }
                                off += separator.length - 1;
                                lastOff = off + 1;
                                count++;
                                grow();
                            } else {
                                lastOff = off + separator.length;
                            }
                        } else if (off == val.length -1) {
                            if (cleanVal.length() > 0) {
                                cleanVal.append(separator);
                            }
                            if (depth >= minDepth) {
                                cleanVal.append(val.bytes, val.offset + lastOff, off - lastOff + 1);
                            }
                        }

                    }
                    if (maxDepth >= 0 && depth > maxDepth) {
                        continue;
                    }
                    values[t++].copyBytes(cleanVal);
                }
                sort();
            }

            private boolean isSeparator(byte[] val, int currentOffset) {
                for (int i=0; i < separator.length; i++) {
                    if (val[currentOffset+i] != separator.bytes[i])
                        return false;
                }
                return true;
            }
        }

        private static class PathHierarchySource extends ValuesSource.Bytes {

            SortedBinaryDocValues values;
            MetaData metaData;

            public PathHierarchySource(SortedBinaryDocValues values, MetaData delegates) {
                this.values = values;
                this.metaData = MetaData.builder(delegates).uniqueness(MetaData.Uniqueness.UNKNOWN).build();
            }


            @Override
            public SortedBinaryDocValues bytesValues() {
                return values;
            }

            @Override
            public MetaData metaData() {
                return metaData;
            }
        }
    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_term".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        return new InternalOrder.Aggregation(key, asc);
    }

}