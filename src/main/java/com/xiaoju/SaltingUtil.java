package com.xiaoju;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;


import com.google.common.collect.Lists;


/**
 * Utility methods related to transparent salting of row keys.
 */
public class SaltingUtil {
    public static final int NUM_SALTING_BYTES = 1;
    public static final Integer MAX_BUCKET_NUM = 256; // Unsigned byte.
    public static final String SALTING_COLUMN_NAME = "_SALT";
    public static final String SALTED_ROW_KEY_NAME = "_SALTED_KEY";

    public static byte[][] getSalteByteSplitPoints(int saltBucketNum) {
        byte[][] splits = new byte[saltBucketNum-1][];
        for (int i = 1; i < saltBucketNum; i++) {
            splits[i-1] = new byte[] {(byte) i};
        }
        return splits;
    }

    // Compute the hash of the key value stored in key and set its first byte as the value. The
    // first byte of key should be left empty as a place holder for the salting byte.
    public static byte[] getSaltedKey(ImmutableBytesWritable key, int bucketNum) {
        byte[] keyBytes = new byte[key.getLength()];
        byte saltByte = getSaltingByte(key.get(), key.getOffset() + 1, key.getLength() - 1, bucketNum);
        keyBytes[0] = saltByte;
        System.arraycopy(key.get(), key.getOffset() + 1, keyBytes, 1, key.getLength() - 1);
        return keyBytes;
    }

    // Generate the bucket byte given a byte array and the number of buckets.
    public static byte getSaltingByte(byte[] value, int offset, int length, int bucketNum) {
        int hash = calculateHashCode(value, offset, length);
        return (byte) Math.abs(hash % bucketNum);
    }

    private static int calculateHashCode(byte a[], int offset, int length) {
        if (a == null)
            return 0;
        int result = 1;
        for (int i = offset; i < offset + length; i++) {
            result = 31 * result + a[i];
        }
        return result;
    }

    public static void addRegionStartKeyToScanStartAndStopRows(byte[] startKey, byte[] endKey, Scan scan) {
        if (startKey.length == 0 && endKey.length == 0) return;
        byte[] prefixBytes = startKey.length != 0 ? startKey : new byte[endKey.length];
        byte[] newStartRow = new byte[scan.getStartRow().length + prefixBytes.length];
        System.arraycopy(prefixBytes, 0, newStartRow, 0, prefixBytes.length);
        System.arraycopy(scan.getStartRow(), 0, newStartRow, prefixBytes.length, scan.getStartRow().length);
        scan.setStartRow(newStartRow);
        if (scan.getStopRow().length != 0) {
            byte[] newStopRow = new byte[scan.getStopRow().length + prefixBytes.length];
            System.arraycopy(prefixBytes, 0, newStopRow, 0, prefixBytes.length);
            System.arraycopy(scan.getStopRow(), 0, newStopRow, prefixBytes.length, scan.getStopRow().length);
            scan.setStopRow(newStopRow);
        }
    }
}