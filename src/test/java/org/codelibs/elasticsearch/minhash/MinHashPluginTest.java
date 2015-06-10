package org.codelibs.elasticsearch.minhash;

import static org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner.newConfigs;

import java.util.Map;
import java.util.UUID;

import junit.framework.TestCase;

import org.codelibs.elasticsearch.runner.ElasticsearchClusterRunner;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.ImmutableSettings.Builder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.get.GetField;
import org.junit.Assert;

public class MinHashPluginTest extends TestCase {

    private ElasticsearchClusterRunner runner;

    @Override
    protected void setUp() throws Exception {
        // create runner instance
        runner = new ElasticsearchClusterRunner();
        // create ES nodes
        runner.onBuild(new ElasticsearchClusterRunner.Builder() {
            @Override
            public void build(final int number, final Builder settingsBuilder) {
            }
        }).build(newConfigs().ramIndexStore().numOfNode(1)
                .clusterName(UUID.randomUUID().toString()));

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
            runner.createIndex(index, ImmutableSettings.builder()
                    .loadFromSource(indexSettings).build());
            runner.ensureYellow(index);

            // create a mapping
            final XContentBuilder mappingBuilder = XContentFactory
                    .jsonBuilder()//
                    .startObject()//
                    .startObject(type)//
                    .startObject("properties")//

                    // id
                    .startObject("id")//
                    .field("type", "string")//
                    .field("index", "not_analyzed")//
                    .endObject()//

                    // msg
                    .startObject("msg")//
                    .field("type", "string")//
                    .field("copy_to", "minhash_value1", "minhash_value2",
                            "minhash_value3")//
                    .endObject()//

                    // bits
                    .startObject("bits")//
                    .field("type", "string")//
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
            assertTrue(indexResponse1.isCreated());
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
        final GetResponse response = client
                .prepareGet(index, type, id)
                .setFields("_source", "minhash_value1", "minhash_value2",
                        "minhash_value3").execute().actionGet();
        assertTrue(response.isExists());
        final Map<String, Object> source = response.getSourceAsMap();
        assertEquals("test " + Integer.parseInt(id) % 100, source.get("msg"));

        final GetField field1 = response.getField("minhash_value1");
        final BytesArray value1 = (BytesArray) field1.getValue();
        assertEquals(hash1.length, value1.length());
        Assert.assertArrayEquals(hash1, value1.array());

        final GetField field2 = response.getField("minhash_value2");
        final BytesArray value2 = (BytesArray) field2.getValue();
        assertEquals(hash2.length, value2.length());
        Assert.assertArrayEquals(hash2, value2.array());

        final GetField field3 = response.getField("minhash_value3");
        final BytesArray value3 = (BytesArray) field3.getValue();
        assertEquals(hash3.length, value3.length());
        Assert.assertArrayEquals(hash3, value3.array());
    }
}
