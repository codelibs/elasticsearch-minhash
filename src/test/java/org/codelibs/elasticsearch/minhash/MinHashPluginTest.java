package org.codelibs.elasticsearch.minhash;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.Map;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;

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
                settingsBuilder.putList("discovery.zen.ping.unicast.hosts", "localhost:9301-9310");
            }
        }).build(newConfigs().clusterName(clusterName).numOfNode(1).pluginTypes("org.codelibs.elasticsearch.minhash.MinHashPlugin"));

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

        final String index = "test_index";
        final String type = "test_type";

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
            final XContentBuilder mappingBuilder = XContentFactory
                    .jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "keyword")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "text")//
                    .field("copy_to", Lists.newArrayList("minhash_value1", "minhash_value2", "minhash_value3"))//
                    .endObject()//

                    // bits
                    .startObject("bits")//
                    .field("type", "keyword")//
                    .field("store", true)//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value1")//
                    .field("type", "minhash")//
                    .field("minhash_analyzer", "minhash_analyzer1")//
                    .field("copy_bits_to", "bits")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value2")//
                    .field("type", "minhash")//
                    .field("minhash_analyzer", "minhash_analyzer2")//
                    .endObject()//

                    // minhash
                    .startObject("minhash_value3")//
                    .field("type", "minhash")//
                    .field("minhash_analyzer", "minhash_analyzer3")//
                    .endObject()//

                    .endObject()//
                    .endObject()//
                    .endObject();
            runner.createMapping(index, type, mappingBuilder);
        }

        if (!runner.indexExists(index)) {
            fail();
        }

        // create 1000 documents
        for (int i = 1; i <= 1000; i++) {
            final IndexResponse indexResponse1 = runner.insert(index, type,
                    String.valueOf(i), "{\"id\":\"" + i + "\",\"msg\":\"test "
                            + i % 100 + "\"}");
            assertEquals(Result.CREATED, indexResponse1.getResult());
        }
        runner.refresh();

        final Client client = runner.client();

        test_get(client, index, type, "1", new byte[] { 82, 56, -67, -10, 55,
                -89, -85, -73, 90, -35, -93, 74, 77, -121, 60, -55 },
                new byte[] { 125, 73, 13, -20, -83, 34, -120, -63, -23, -44,
                        -52, 98, 25, 121, -56, 107 }, new byte[] { 91, -99,
                        105, 16, -5, -118, -14, -36 });

        test_get(client, index, type, "2", new byte[] { 0, 96, 125, -3, -121,
                -89, -5, 39, -1, -108, 27, -55, 42, -45, 29, 64 }, new byte[] {
                -15, 40, 77, 111, -91, 21, 10, 3, -31, -41, -84, -79, 57, -35,
                -117, 123 }, new byte[] { -117, 93, 96, 36, 123, 24, -1, 60 });

        test_get(client, index, type, "101", new byte[] { 82, 56, -67, -10, 55,
                -89, -85, -73, 90, -35, -93, 74, 77, -121, 60, -55 },
                new byte[] { 125, 73, 13, -20, -83, 34, -120, -63, -23, -44,
                        -52, 98, 25, 121, -56, 107 }, new byte[] { 91, -99,
                        105, 16, -5, -118, -14, -36 });

    }

    private void test_get(final Client client, final String index,
            final String type, final String id, final byte[] hash1,
            final byte[] hash2, final byte[] hash3) {
        final GetResponse response = client.prepareGet(index, type, id)
                .setStoredFields(new String[] { "_source", "minhash_value1", "minhash_value2", "minhash_value3" }).execute()
                .actionGet();
        assertTrue(response.isExists());
        final Map<String, Object> source = response.getSourceAsMap();
        assertEquals("test " + Integer.parseInt(id) % 100, source.get("msg"));

        final DocumentField field1 = response.getField("minhash_value1");
        final BytesArray value1 = (BytesArray) field1.getValue();
        assertEquals(hash1.length, value1.length());
        Assert.assertArrayEquals(hash1, value1.array());

        final DocumentField field2 = response.getField("minhash_value2");
        final BytesArray value2 = (BytesArray) field2.getValue();
        assertEquals(hash2.length, value2.length());
        Assert.assertArrayEquals(hash2, value2.array());

        final DocumentField field3 = response.getField("minhash_value3");
        final BytesArray value3 = (BytesArray) field3.getValue();
        assertEquals(hash3.length, value3.length());
        Assert.assertArrayEquals(hash3, value3.array());
    }
}
