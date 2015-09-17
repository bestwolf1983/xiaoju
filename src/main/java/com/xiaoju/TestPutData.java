package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * Created by cenyuhai on 2015/9/17.
 */
public class TestPutData {
  public static void main(String[] args) throws IOException {
    System.out.println("App start!");
    String tableName = args[0];
    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, tableName);
    Put put = new Put(Bytes.toBytes("10000"));
    byte[] family = Bytes.toBytes("0");
    byte[] order_id = Bytes.toBytes("ORDER_ID");
    byte[] order_type =  Bytes.toBytes("ORDER_TYPE");
    byte[] p_ip = Bytes.toBytes("P_IP");
    byte[] id = Bytes.toBytes("ID");

    put.add(family, order_id, Bytes.toBytes(1000000000L));
    put.add(family, order_type, Bytes.toBytes(0));
    put.add(family, p_ip, Bytes.toBytes("127.0.0.1"));
    put.add(family, id, Bytes.toBytes(10000L));

    table.put(put);
    table.flushCommits();
    table.close();
    System.out.println("Put complete!");

  }
}
