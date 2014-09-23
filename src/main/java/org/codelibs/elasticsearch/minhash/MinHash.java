package org.codelibs.elasticsearch.minhash;

import org.elasticsearch.common.bytes.BytesArray;

public class MinHash {
    private MinHash() {
    }

    public static float compare(BytesArray array1, BytesArray array2) {
        return compare(array1.toBytes(), array2.toBytes());
    }

    public static float compare(byte[] data1, byte[] data2) {
        int size = data1.length;
        if (size != data2.length) {
            return 0;
        }
        int count = countSameBits(data1, data2);
        return (float) count / (float) (size * 8);
    }

    protected static int countSameBits(byte[] data1, byte[] data2) {
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
}
