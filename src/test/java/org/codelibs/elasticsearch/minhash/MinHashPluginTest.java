package org.codelibs.elasticsearch.minhash;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.Map;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import com.google.common.collect.Lists;

import junit.framework.TestCase;

public class MinHashPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    private String clusterName;

    @Override
    protected void setUp() throws Exception {
        clusterName = "es-minhash-" + System.currentTimeMillis();
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
                settingsBuilder.put("http.cors.enabled", true);
                settingsBuilder.put("http.cors.allow-origin", "*");
                settingsBuilder.put("discovery.type", "single-node");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1).pluginTypes(
                "org.codelibs.elasticsearch.minhash.MinHashPlugin"));

        // wait for yellow status
        runner.ensureYellow();
    }

    @Override
    protected void tearDown() throws Exception {
        // close runner
        runner.close();
        // delete all files
        runner.clean();
    }

    public void test_runEs() throws Exception {

        final String index = "dataset";
        final String type = "_doc";

        {
            // create an index
            final String indexSettings = "{\"index\":{\"analysis\":{\"analyzer\":{"
                    + "\"minhash_analyzer1\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"minhash\"]},"
                    + "\"minhash_analyzer2\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhashfilter1\"]},"
                    + "\"minhash_analyzer3\":{\"type\":\"custom\",\"tokenizer\":\"standard\",\"filter\":[\"my_minhashfilter2\"]}"
                    + "},\"filter\":{"
                    + "\"my_minhashfilter1\":{\"type\":\"minhash\",\"seed\":1000},"
                    + "\"my_minhashfilter2\":{\"type\":\"minhash\",\"bit\":2,\"size\":32,\"seed\":1000}"
                    + "}}}}";
            runner.createIndex(index, Settings.builder()
                    .loadFromSource(indexSettings, XContentType.JSON).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory.jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // num
                    .startObject("num")//
                    .field("type", "integer")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to",
                            Lists.newArrayList("minhash_value1",
                                    "minhash_value2", "minhash_value3",
                                    "minhash_value4"))//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value1")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer1")//
                    .field("copy_bits_to", "bits")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value2")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer2")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value3")//
                    .field("type", "minhash")//
                    .field("store", true)//
                    .field("minhash_analyzer", "minhash_analyzer3")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value4")//
                    .field("type", "minhash")//
                    .field("bit_string", true)//
                    .field("minhash_analyzer", "minhash_analyzer1")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i % 100 + "\",\"num\":" + i + "}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        final Client client = runner.client();

        test_get(client, index, type, "1", "Uji99jenq7da3aNKTYc8yQ==",
                "fUkN7K0iiMHp1MxiGXnIaw==", "W51pEPuK8tw=");

        test_get(client, index, type, "2", "AGB9/Yen+yf/lBvJKtMdQA==",
                "8ShNb6UVCgPh16yxOd2Lew==", "i11gJHsY/zw=");

        test_get(client, index, type, "101", "Uji99jenq7da3aNKTYc8yQ==",
                "fUkN7K0iiMHp1MxiGXnIaw==", "W51pEPuK8tw=");

    }

    private void test_get(final Client client, final String index,
            final String type, final String id, final String hash1,
            final String hash2, final String hash3) {
        final GetResponse response = client.prepareGet(index, type, id)
                .setStoredFields(new String[] { "_source", "minhash_value1",
                        "minhash_value2", "minhash_value3", "minhash_value4" })
                .execute().actionGet();
        assertTrue(response.isExists());
        final Map<String, Object> source = response.getSourceAsMap();
        assertEquals("test " + Integer.parseInt(id) % 100, source.get("msg"));

        final DocumentField field1 = response.getField("minhash_value1");
        final String value1 = (String) field1.getValue();
        assertEquals(hash1, value1);

        final DocumentField field2 = response.getField("minhash_value2");
        final String value2 = (String) field2.getValue();
        assertEquals(hash2, value2);

        final DocumentField field3 = response.getField("minhash_value3");
        final String value3 = (String) field3.getValue();
        assertEquals(hash3, value3);
    }
}
