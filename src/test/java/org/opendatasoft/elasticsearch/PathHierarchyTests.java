package org.opendatasoft.elasticsearch;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.test.ESTestCase;
import org.opendatasoft.elasticsearch.search.aggregations.bucket.PathHierarchyAggregationBuilder;

public class PathHierarchyTests extends ESTestCase {
    public void testParser() throws Exception {
        // can create the factory with utf8 separator
        String separator = "夢";
        XContentParser stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"path\", \"separator\": \"" + separator + "\"}");
        XContentParser.Token token = stParser.nextToken();
        assertSame(XContentParser.Token.START_OBJECT, token);
        assertNotNull(PathHierarchyAggregationBuilder.parse("path_hierarchy", stParser));

        // can create the factory with an array of orders
        String orders = "[{\"_key\": \"asc\"}, {\"_count\": \"desc\"}]";
        stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"path\", \"order\": " + orders + "}");
        assertNotNull(PathHierarchyAggregationBuilder.parse("path_hierarchy", stParser));
        stParser = createParser(JsonXContent.jsonXContent,
                "{\"field\":\"path\", \"separator\":\"/\", \"order\": " + orders + ", \"min_depth\": 0, \"max_depth\": 3}");
        AggregationBuilder builder = PathHierarchyAggregationBuilder.parse("path_hierarchy", stParser);
        assertNotNull(builder);
    }
}
