package org.codelibs.elasticsearch.minhash;

import java.io.IOException;
import java.io.Reader;

import junit.framework.TestCase;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.util.Version;
import org.codelibs.elasticsearch.minhash.index.analysis.MinHashTokenFilter;

import com.google.common.hash.HashFunction;

public class MinHashTest extends TestCase {

    public void test_calcMinHash_1bit_128funcs_seed0() throws IOException {

        final int hashBit = 1;
        final int seed = 0;
        final int num = 128;
        final Analyzer minhashAnalyzer = createAnalyzer(hashBit, seed, num);
        final StringBuilder[] texts = createTexts();
        final byte[][] data = createMinHashes(minhashAnalyzer, texts);

        assertEquals(1.0f, MinHash.compare(data[0], data[0]));
        assertEquals(0.890625f, MinHash.compare(data[0], data[1]));
        assertEquals(0.7890625f, MinHash.compare(data[0], data[2]));
        assertEquals(0.7421875f, MinHash.compare(data[0], data[3]));
        assertEquals(0.6953125f, MinHash.compare(data[0], data[4]));
        assertEquals(0.609375f, MinHash.compare(data[0], data[5]));
        assertEquals(0.578125f, MinHash.compare(data[0], data[6]));
        assertEquals(0.546875f, MinHash.compare(data[0], data[7]));
        assertEquals(0.546875f, MinHash.compare(data[0], data[8]));
        assertEquals(0.5625f, MinHash.compare(data[0], data[9]));
    }

    public void test_calcMinHash_1bit_128funcs_seed100() throws IOException {

        final int hashBit = 1;
        final int seed = 100;
        final int num = 128;
        final Analyzer minhashAnalyzer = createAnalyzer(hashBit, seed, num);
        final StringBuilder[] texts = createTexts();
        final byte[][] data = createMinHashes(minhashAnalyzer, texts);

        assertEquals(1.0f, MinHash.compare(data[0], data[0]));
        assertEquals(0.9296875f, MinHash.compare(data[0], data[1]));
        assertEquals(0.8515625f, MinHash.compare(data[0], data[2]));
        assertEquals(0.8046875f, MinHash.compare(data[0], data[3]));
        assertEquals(0.7265625f, MinHash.compare(data[0], data[4]));
        assertEquals(0.640625f, MinHash.compare(data[0], data[5]));
        assertEquals(0.640625f, MinHash.compare(data[0], data[6]));
        assertEquals(0.5703125f, MinHash.compare(data[0], data[7]));
        assertEquals(0.53125f, MinHash.compare(data[0], data[8]));
        assertEquals(0.484375f, MinHash.compare(data[0], data[9]));
    }

    public void test_calcMinHash_2bit_128funcs_seed0() throws IOException {

        final int hashBit = 2;
        final int seed = 0;
        final int num = 128;
        final Analyzer minhashAnalyzer = createAnalyzer(hashBit, seed, num);
        final StringBuilder[] texts = createTexts();
        final byte[][] data = createMinHashes(minhashAnalyzer, texts);

        assertEquals(1.0f, MinHash.compare(data[0], data[0]));
        assertEquals(0.89453125f, MinHash.compare(data[0], data[1]));
        assertEquals(0.80859375f, MinHash.compare(data[0], data[2]));
        assertEquals(0.7734375f, MinHash.compare(data[0], data[3]));
        assertEquals(0.7265625f, MinHash.compare(data[0], data[4]));
        assertEquals(0.66015625f, MinHash.compare(data[0], data[5]));
        assertEquals(0.625f, MinHash.compare(data[0], data[6]));
        assertEquals(0.59765625f, MinHash.compare(data[0], data[7]));
        assertEquals(0.5859375f, MinHash.compare(data[0], data[8]));
        assertEquals(0.55078125f, MinHash.compare(data[0], data[9]));
    }

    public void test_calcMinHash_1bit_256funcs_seed0() throws IOException {

        final int hashBit = 1;
        final int seed = 0;
        final int num = 256;
        final Analyzer minhashAnalyzer = createAnalyzer(hashBit, seed, num);
        final StringBuilder[] texts = createTexts();
        final byte[][] data = createMinHashes(minhashAnalyzer, texts);

        assertEquals(1.0f, MinHash.compare(data[0], data[0]));
        assertEquals(0.90625f, MinHash.compare(data[0], data[1]));
        assertEquals(0.82421875f, MinHash.compare(data[0], data[2]));
        assertEquals(0.76953125f, MinHash.compare(data[0], data[3]));
        assertEquals(0.703125f, MinHash.compare(data[0], data[4]));
        assertEquals(0.625f, MinHash.compare(data[0], data[5]));
        assertEquals(0.6015625f, MinHash.compare(data[0], data[6]));
        assertEquals(0.55078125f, MinHash.compare(data[0], data[7]));
        assertEquals(0.53125f, MinHash.compare(data[0], data[8]));
        assertEquals(0.51171875f, MinHash.compare(data[0], data[9]));
    }

    public void test_calcMinHash_1bit_128funcs_seed0_moreSimilar()
            throws IOException {

        final int hashBit = 1;
        final int seed = 0;
        final int num = 128;
        final Analyzer minhashAnalyzer = createAnalyzer(hashBit, seed, num);
        final StringBuilder[] texts = createMoreSimilarTexts();
        final byte[][] data = createMinHashes(minhashAnalyzer, texts);

        assertEquals(1.0f, MinHash.compare(data[0], data[0]));
        assertEquals(0.984375f, MinHash.compare(data[0], data[1]));
        assertEquals(0.9765625f, MinHash.compare(data[0], data[2]));
        assertEquals(0.96875f, MinHash.compare(data[0], data[3]));
        assertEquals(0.953125f, MinHash.compare(data[0], data[4]));
        assertEquals(0.9375f, MinHash.compare(data[0], data[5]));
        assertEquals(0.9296875f, MinHash.compare(data[0], data[6]));
        assertEquals(0.921875f, MinHash.compare(data[0], data[7]));
        assertEquals(0.921875f, MinHash.compare(data[0], data[8]));
        assertEquals(0.921875f, MinHash.compare(data[0], data[9]));
    }

    private byte[][] createMinHashes(final Analyzer minhashAnalyzer,
            final StringBuilder[] texts) throws IOException {
        final byte[][] data = new byte[10][];
        for (int i = 0; i < 10; i++) {
            // System.out.println("texts" + i + ": " + texts[i]);
            data[i] = MinHash.calcMinHash(minhashAnalyzer, texts[i].toString());
        }
        return data;
    }

    private Analyzer createAnalyzer(final int hashBit, final int seed,
            final int num) {
        final HashFunction[] hashFunctions = MinHash.createHashFunctions(seed,
                num);
        final Analyzer minhashAnalyzer = new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(
                    final String fieldName, final Reader reader) {
                @SuppressWarnings("deprecation")
                final Tokenizer tokenizer = new WhitespaceTokenizer(
                        Version.LUCENE_CURRENT, reader);
                final TokenStream stream = new MinHashTokenFilter(tokenizer,
                        hashFunctions, hashBit);
                return new TokenStreamComponents(tokenizer, stream);
            }
        };
        return minhashAnalyzer;
    }

    private StringBuilder[] createTexts() {
        final StringBuilder[] texts = new StringBuilder[10];
        for (int i = 0; i < 10; i++) {
            texts[i] = new StringBuilder();
        }
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                if (i - j * 10 >= 0) {
                    texts[j].append(" aaa" + i);
                } else {
                    texts[j].append(" bbb" + i);
                }
            }
        }
        return texts;
    }

    private StringBuilder[] createMoreSimilarTexts() {
        final StringBuilder[] texts = new StringBuilder[10];
        for (int i = 0; i < 10; i++) {
            texts[i] = new StringBuilder();
        }
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10; j++) {
                if (i - 90 - j >= 0) {
                    texts[j].append(" aaa" + i);
                } else {
                    texts[j].append(" bbb" + i);
                }
            }
        }
        return texts;
    }

    public void test_compare() {
        assertEquals(1f,
                MinHash.compare(new byte[] { 0x1 }, new byte[] { 0x1 }));
        assertEquals(1f, MinHash.compare(new byte[] { 0x1, 0x1 }, new byte[] {
                0x1, 0x1 }));

        assertEquals(0.5f,
                MinHash.compare(new byte[] { 0xf }, new byte[] { 0x0 }));
        assertEquals(0.5f, MinHash.compare(new byte[] { 0xf, 0x0 }, new byte[] {
                0x0, 0xf }));

        assertEquals(0.0f,
                MinHash.compare(new byte[] { 0xf }, new byte[] { (byte) 0xf0 }));
        assertEquals(
                0.0f,
                MinHash.compare(new byte[] { 0xf, (byte) 0xf0 }, new byte[] {
                        (byte) 0xf0, 0xf }));
    }

    public void test_countSameBits() {
        assertEquals(8,
                MinHash.countSameBits(new byte[] { 0x0 }, new byte[] { 0x0 }));
        assertEquals(7,
                MinHash.countSameBits(new byte[] { 0x1 }, new byte[] { 0x0 }));
        assertEquals(7,
                MinHash.countSameBits(new byte[] { 0x2 }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0x3 }, new byte[] { 0x0 }));
        assertEquals(7,
                MinHash.countSameBits(new byte[] { 0x4 }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0x5 }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0x6 }, new byte[] { 0x0 }));
        assertEquals(5,
                MinHash.countSameBits(new byte[] { 0x7 }, new byte[] { 0x0 }));
        assertEquals(7,
                MinHash.countSameBits(new byte[] { 0x8 }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0x9 }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0xa }, new byte[] { 0x0 }));
        assertEquals(5,
                MinHash.countSameBits(new byte[] { 0xb }, new byte[] { 0x0 }));
        assertEquals(6,
                MinHash.countSameBits(new byte[] { 0xc }, new byte[] { 0x0 }));
        assertEquals(5,
                MinHash.countSameBits(new byte[] { 0xd }, new byte[] { 0x0 }));
        assertEquals(5,
                MinHash.countSameBits(new byte[] { 0xe }, new byte[] { 0x0 }));
        assertEquals(4,
                MinHash.countSameBits(new byte[] { 0xf }, new byte[] { 0x0 }));
        assertEquals(4,
                MinHash.countSameBits(new byte[] { 0x0f }, new byte[] { 0x0 }));
        assertEquals(3,
                MinHash.countSameBits(new byte[] { 0x1f }, new byte[] { 0x0 }));
        assertEquals(3,
                MinHash.countSameBits(new byte[] { 0x2f }, new byte[] { 0x0 }));
        assertEquals(2,
                MinHash.countSameBits(new byte[] { 0x3f }, new byte[] { 0x0 }));
        assertEquals(3,
                MinHash.countSameBits(new byte[] { 0x4f }, new byte[] { 0x0 }));
        assertEquals(2,
                MinHash.countSameBits(new byte[] { 0x5f }, new byte[] { 0x0 }));
        assertEquals(2,
                MinHash.countSameBits(new byte[] { 0x6f }, new byte[] { 0x0 }));
        assertEquals(1,
                MinHash.countSameBits(new byte[] { 0x7f }, new byte[] { 0x0 }));
        assertEquals(3, MinHash.countSameBits(new byte[] { (byte) 0x8f },
                new byte[] { 0x0 }));
        assertEquals(2, MinHash.countSameBits(new byte[] { (byte) 0x9f },
                new byte[] { 0x0 }));
        assertEquals(2, MinHash.countSameBits(new byte[] { (byte) 0xaf },
                new byte[] { 0x0 }));
        assertEquals(1, MinHash.countSameBits(new byte[] { (byte) 0xbf },
                new byte[] { 0x0 }));
        assertEquals(2, MinHash.countSameBits(new byte[] { (byte) 0xcf },
                new byte[] { 0x0 }));
        assertEquals(1, MinHash.countSameBits(new byte[] { (byte) 0xdf },
                new byte[] { 0x0 }));
        assertEquals(1, MinHash.countSameBits(new byte[] { (byte) 0xef },
                new byte[] { 0x0 }));
        assertEquals(0, MinHash.countSameBits(new byte[] { (byte) 0xff },
                new byte[] { 0x0 }));

        assertEquals(
                16,
                MinHash.countSameBits(new byte[] { 0x0, 0x0 }, new byte[] {
                        0x0, 0x0 }));
        assertEquals(
                15,
                MinHash.countSameBits(new byte[] { 0x0, 0x0 }, new byte[] {
                        0x0, 0x1 }));

    }
}
