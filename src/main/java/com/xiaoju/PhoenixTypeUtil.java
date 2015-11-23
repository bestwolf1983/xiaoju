package com.xiaoju;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by cenyuhai on 2015/9/17.
 */
public class PhoenixTypeUtil {

	public static byte[] toBytes(Long object) {
		byte[] b = new byte[Bytes.SIZEOF_LONG];
		return encodeLong(object, b, 0);
	}
	
	public static byte[] toBytes(int object) {
		byte[] b = new byte[Bytes.SIZEOF_INT];
		return encodeInt(object, b, 0);
		
	}
	
	public static byte[] encodeInt(int v, byte[] b, int o) {
        b[o + 0] = (byte) ((v >> 24) ^ 0x80); // Flip sign bit so that INTEGER is binary comparable
        b[o + 1] = (byte) (v >> 16);
        b[o + 2] = (byte) (v >> 8);
        b[o + 3] = (byte) v;
        return b;
      }

	public static int toInt(byte[] bytes) {
		return decodeInt(bytes, 0);
	}
	
	public static int decodeInt(byte[] bytes, int o) {
		int v;
		v = bytes[o] ^ 0x80; // Flip sign bit back
		for (int i = 1; i < Bytes.SIZEOF_INT; i++) {
			v = (v << 8) + (bytes[o + i] & 0xff);
		}
		return v;
	}

	public static long toLong(byte[] bytes) {
		return decodeLong(bytes, 0);
	}

	public static long decodeLong(byte[] bytes, int o) {
		long v;
		byte b = bytes[o];
		v = b ^ 0x80; // Flip sign bit back
		for (int i = 1; i < Bytes.SIZEOF_LONG; i++) {
			b = bytes[o + i];
			v = (v << 8) + (b & 0xff);
		}
		return v;
	}

	public static byte[] encodeLong(long v, byte[] b, int o) {
		b[o + 0] = (byte) ((v >> 56) ^ 0x80);
		b[o + 1] = (byte) (v >> 48);
		b[o + 2] = (byte) (v >> 40);
		b[o + 3] = (byte) (v >> 32);
		b[o + 4] = (byte) (v >> 24);
		b[o + 5] = (byte) (v >> 16);
		b[o + 6] = (byte) (v >> 8);
		b[o + 7] = (byte) v;
		return b;
	}
}
