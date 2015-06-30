package com.opendatasoft.search.aggregations.bucket;

import com.opendatasoft.elasticsearch.plugin.pathhierarchyaggregation.PathHierarchyAggregationPlugin;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation.PathHierarchy;
import com.opendatasoft.elasticsearch.search.aggregations.bucket.pathhierarchyaggregation.PathHierarchyBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.hppc.ObjectIntMap;
import org.elasticsearch.common.hppc.ObjectIntOpenHashMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;

@ElasticsearchIntegrationTest.SuiteScopeTest
public class PathHierarchyTests extends ElasticsearchIntegrationTest{

    private static final String PATH_FIELD_NAME = "path";
    private static final String VIEWS_FIELD_NAME = "views";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugin.types", PathHierarchyAggregationPlugin.class.getName())
                .put(super.nodeSettings(nodeOrdinal)).build();
    }

    static ObjectIntMap<String> expectedDocCountsForPath = null;

    @Override
    protected void setupSuiteScopeCluster() throws Exception {

        expectedDocCountsForPath = new ObjectIntOpenHashMap<>();

        assertAcked(prepareCreate("idx")
                .addMapping("path", PATH_FIELD_NAME, "type=string,index=not_analyzed"));

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Spreadsheets/Budget_2013.xls")
                .field(VIEWS_FIELD_NAME, 10)
                .endObject()));
        builders.add(client().prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Spreadsheets/Budget_2014.xls")
                .field(VIEWS_FIELD_NAME, 7)
                .endObject()));
        builders.add(client().prepareIndex("idx", "path").setSource(jsonBuilder()
                .startObject()
                .field(PATH_FIELD_NAME, "/My documents/Test.txt")
                .field(VIEWS_FIELD_NAME, 1)
                .endObject()));

        expectedDocCountsForPath.put("My documents", 3);
        expectedDocCountsForPath.put("My documents/Spreadsheets", 2);
        expectedDocCountsForPath.put("My documents/Spreadsheets/Budget_2013.xls", 1);
        expectedDocCountsForPath.put("My documents/Spreadsheets/Budget_2014.xls", 1);
        expectedDocCountsForPath.put("My documents/Test.txt", 1);

        indexRandom(true, builders);
        ensureSearchable();
    }

    @Test
    public void simple() {
        SearchResponse response = client().prepareSearch("idx").setTypes("path")
                .addAggregation(new PathHierarchyBuilder("path")
                        .field(PATH_FIELD_NAME)
                        .separator("/")
                ).execute().actionGet();

        assertSearchResponse(response);

        PathHierarchy pathHierarchy = response.getAggregations().get("path");
        List<PathHierarchy.Bucket> buckets = pathHierarchy.getBuckets();
        assertNotSame(buckets.size(), 0);
        for (PathHierarchy.Bucket bucket: buckets) {
            assertEquals(expectedDocCountsForPath.get(bucket.getKey()), bucket.getDocCount());
        }
    }
}
