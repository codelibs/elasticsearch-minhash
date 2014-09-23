package org.codelibs.elasticsearch.minhash;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.elasticsearch.common.bytes.BytesArray;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.BaseEncoding;

public class MinHash {
    private MinHash() {
    }

    /**
     * Compare base64 strings for MinHash.
     *
     * @param str1 MinHash base64 string
     * @param str2 MinHash base64 string
     * @return similarity (0 to 1.0f)
     */
    public static float compare(final String str1, final String str2) {
        return compare(BaseEncoding.base64().decode(str1), BaseEncoding
                .base64().decode(str2));
    }

    /**
     * Compare byte arrays for MinHash.
     *
     * @param array1 MinHash byte array
     * @param array2 MinHash byte array
     * @return
     */
    public static float compare(final BytesArray array1, final BytesArray array2) {
        return compare(array1.toBytes(), array2.toBytes());
    }

    /**
     * Compare bytes for MinHash.
     *
     * @param data1 MinHash bytes
     * @param data2 MinHash bytes
     * @return similarity (0 to 1.0f)
     */
    public static float compare(final byte[] data1, final byte[] data2) {
        final int size = data1.length;
        if (size != data2.length) {
            return 0;
        }
        final int count = countSameBits(data1, data2);
        return (float) count / (float) (size * 8);
    }

    protected static int countSameBits(final byte[] data1, final byte[] data2) {
        int count = 0;
        for (int i = 0; i < data1.length; i++) {
            byte b1 = data1[i];
            byte b2 = data2[i];
            for (int j = 0; j < 8; j++) {
                if ((b1 & 1) == (b2 & 1)) {
                    count++;
                }
                b1 >>= 1;
                b2 >>= 1;
            }
        }
        return count;
    }

    public static HashFunction[] createHashFunctions(final int seed,
            final int num) {
        final HashFunction[] hashFunctions = new HashFunction[num];
        for (int i = 0; i < num; i++) {
            hashFunctions[i] = Hashing.murmur3_128(seed + i);
        }
        return hashFunctions;
    }

    public static byte[] calcMinHash(final Analyzer minhashAnalyzer,
            final String text) throws IOException {
        byte[] value = null;
        try (TokenStream stream = minhashAnalyzer.tokenStream("minhash", text)) {
            final CharTermAttribute termAtt = stream
                    .addAttribute(CharTermAttribute.class);
            stream.reset();
            if (stream.incrementToken()) {
                final String minhashValue = termAtt.toString();
                value = BaseEncoding.base64().decode(minhashValue);
            }
            stream.end();
        }
        return value;
    }
}
