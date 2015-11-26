package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Created by cenyuhai on 2015/11/26.
 */
public class CreateHbaseTable {
  public static void main(String[] args) throws Exception {
    if(args.length < 2) {
      throw new Exception("args length < 2, args[0]:TableName, args[1]:is Compress,value is [true or false]");
    }
    Configuration conf = HBaseConfiguration.create();
    HBaseAdmin admin = new HBaseAdmin(conf);
    HTableDescriptor tableDesc = new HTableDescriptor(args[0].toUpperCase());
    //MemStore大小
    tableDesc.setMemStoreFlushSize(128 * 1024 * 1024);
    HColumnDescriptor colDesc = new HColumnDescriptor("a");
    colDesc.setBloomFilterType(BloomType.ROW);
    //下面的压缩建议在正式环境使用
    if(args[1].equals("true")) {
      colDesc.setCompressionType(Compression.Algorithm.LZO);
      colDesc.setCompactionCompressionType(Compression.Algorithm.LZO);
    }
    //colDesc.setDataBlockEncoding(DataBlockEncoding.FAST_DIFF);
    byte[][] splitKeys = new byte[99][2];
    for(int i=1;i<100;i++) {
      String key = String.format("%02d", i);
      System.out.println(key);
      splitKeys[i-1] = Bytes.toBytes(key);
    }

    colDesc.setMaxVersions(1);
    tableDesc.addFamily(colDesc);
    admin.createTable(tableDesc, splitKeys);
  }
}
