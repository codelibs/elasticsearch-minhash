package org.codelibs.elasticsearch.minhash;

import junit.framework.TestCase;

public class MinHashTest extends TestCase {
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
