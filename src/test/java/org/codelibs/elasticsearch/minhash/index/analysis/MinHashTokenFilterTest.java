package org.codelibs.elasticsearch.minhash.index.analysis;

import junit.framework.TestCase;

public class MinHashTokenFilterTest extends TestCase {

    public void test_calcMinHash() throws Exception {
        int hashBit;
        long[] minHashValues;
        byte[] bytes;

        hashBit = 1;
        minHashValues = new long[] { 0, 1, 2, 3, 4, 5, 6, 7 };
        bytes = MinHashTokenFilter.calcMinHash(minHashValues, hashBit);
        assertEquals(1, bytes.length);
        assertEquals(0x0, bytes[0] & 0x1);
        assertEquals(0x1, bytes[0] >> 1 & 0x1);
        assertEquals(0x0, bytes[0] >> 2 & 0x1);
        assertEquals(0x1, bytes[0] >> 3 & 0x1);
        assertEquals(0x0, bytes[0] >> 4 & 0x1);
        assertEquals(0x1, bytes[0] >> 5 & 0x1);
        assertEquals(0x0, bytes[0] >> 6 & 0x1);
        assertEquals(0x1, bytes[0] >> 7 & 0x1);

        hashBit = 1;
        minHashValues = new long[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        bytes = MinHashTokenFilter.calcMinHash(minHashValues, hashBit);
        assertEquals(1, bytes.length);
        assertEquals(0x1, bytes[0] & 0x1);
        assertEquals(0x0, bytes[0] >> 1 & 0x1);
        assertEquals(0x1, bytes[0] >> 2 & 0x1);
        assertEquals(0x0, bytes[0] >> 3 & 0x1);
        assertEquals(0x1, bytes[0] >> 4 & 0x1);
        assertEquals(0x0, bytes[0] >> 5 & 0x1);
        assertEquals(0x1, bytes[0] >> 6 & 0x1);
        assertEquals(0x0, bytes[0] >> 7 & 0x1);

        hashBit = 1;
        minHashValues = new long[] { 0, 1, 2, 3, };
        bytes = MinHashTokenFilter.calcMinHash(minHashValues, hashBit);
        assertEquals(1, bytes.length);
        assertEquals(0x0, bytes[0] & 0x1);
        assertEquals(0x1, bytes[0] >> 1 & 0x1);
        assertEquals(0x0, bytes[0] >> 2 & 0x1);
        assertEquals(0x1, bytes[0] >> 3 & 0x1);
        assertEquals(0x0, bytes[0] >> 4 & 0x1);
        assertEquals(0x0, bytes[0] >> 5 & 0x1);
        assertEquals(0x0, bytes[0] >> 6 & 0x1);
        assertEquals(0x0, bytes[0] >> 7 & 0x1);

        hashBit = 2;
        minHashValues = new long[] { 0, 1, 2, 3, };
        bytes = MinHashTokenFilter.calcMinHash(minHashValues, hashBit);
        assertEquals(1, bytes.length);
        assertEquals(0x0, bytes[0] & 0x1);
        assertEquals(0x0, bytes[0] >> 1 & 0x1);
        assertEquals(0x1, bytes[0] >> 2 & 0x1);
        assertEquals(0x0, bytes[0] >> 3 & 0x1);
        assertEquals(0x0, bytes[0] >> 4 & 0x1);
        assertEquals(0x1, bytes[0] >> 5 & 0x1);
        assertEquals(0x1, bytes[0] >> 6 & 0x1);
        assertEquals(0x1, bytes[0] >> 7 & 0x1);

        hashBit = 1;
        minHashValues = new long[] { 0, 1, 2, 3, 4, 5, 6, 7, 7, 6, 5, 4, 3, 2,
                1, 0 };
        bytes = MinHashTokenFilter.calcMinHash(minHashValues, hashBit);
        assertEquals(2, bytes.length);
        assertEquals(0x0, bytes[0] & 0x1);
        assertEquals(0x1, bytes[0] >> 1 & 0x1);
        assertEquals(0x0, bytes[0] >> 2 & 0x1);
        assertEquals(0x1, bytes[0] >> 3 & 0x1);
        assertEquals(0x0, bytes[0] >> 4 & 0x1);
        assertEquals(0x1, bytes[0] >> 5 & 0x1);
        assertEquals(0x0, bytes[0] >> 6 & 0x1);
        assertEquals(0x1, bytes[0] >> 7 & 0x1);
        assertEquals(0x1, bytes[1] & 0x1);
        assertEquals(0x0, bytes[1] >> 1 & 0x1);
        assertEquals(0x1, bytes[1] >> 2 & 0x1);
        assertEquals(0x0, bytes[1] >> 3 & 0x1);
        assertEquals(0x1, bytes[1] >> 4 & 0x1);
        assertEquals(0x0, bytes[1] >> 5 & 0x1);
        assertEquals(0x1, bytes[1] >> 6 & 0x1);
        assertEquals(0x0, bytes[1] >> 7 & 0x1);

    }
}
