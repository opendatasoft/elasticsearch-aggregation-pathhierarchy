package com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortingBinaryDocValues;
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
    public static final int DEFAULT_MAX_DEPTH = 3;

    @Override
    public AggregatorFactory parse(String aggregationName, XContentParser parser, SearchContext context) throws IOException {

        ValuesSourceParser vsParser = ValuesSourceParser.any(aggregationName, InternalPathHierarchy.TYPE, context).scriptable(true).build();

        String separator = DEFAULT_SEPARATOR;
        int maxDepth = DEFAULT_MAX_DEPTH;
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
                if ("max_depth".equals(currentFieldName)) {
                    maxDepth = parser.intValue();
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

        return new PathHierarchyFactory(aggregationName, vsParser.config(), separator, maxDepth, order);
    }


    private static class PathHierarchyFactory extends ValuesSourceAggregatorFactory<ValuesSource> {

        private String separator;
        private int maxDepth;
        private InternalOrder order;

        public PathHierarchyFactory(String name, ValuesSourceConfig<ValuesSource> config, String separator, int maxDepth, InternalOrder order) {
            super(name, InternalPathHierarchy.TYPE.name(), config);
            this.separator = separator;
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
            final HierarchyValues hierarchyValues = new HierarchyValues(valuesSource, separator, maxDepth);
            ValuesSource.Bytes hierarchySource = new PathHierarchySource(hierarchyValues, valuesSource.metaData());
            return new PathHierarchyAggregator(name, factories, hierarchySource, aggregationContext, parent, separator, order);

        }

        private static class HierarchyValues extends SortingBinaryDocValues {

            private ValuesSource hierarchyValues;
            private SortedBinaryDocValues hieraValues;
            private String separator;
            private int maxDepth;

            protected HierarchyValues(ValuesSource hierarchyValues, String separator, int maxDepth) {
                this.hierarchyValues = hierarchyValues;
                this.separator = separator;
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
                    String path = "";
                    int depth = 0;
                    BytesRef val = hieraValues.valueAt(i);

                    // FIXME : make a better sizing
                    List<String> listHiera = new ArrayList<>();

                    for (String s: val.utf8ToString().split(separator)) {
                        if (s.length() == 0) {
                            continue;
                        }
                        listHiera.add(s);
                    }
                    count += listHiera.size() - 1;
                    grow();
                    for(int j=0; j < listHiera.size(); j++) {
                        if (maxDepth >=0 && j > maxDepth) break;
                        String s = listHiera.get(j);
                        if (depth > 0) path += separator;
                        path += s;
                        depth++;
                        values[t++].copyChars(path);
                    }
                }
                sort();
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