package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by cenyuhai on 2015/9/17.
 */
public class TestPutData {

  public static byte[] longToByte(long number) {
    long temp = number;
    byte[] b = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Long(temp & 0xff).byteValue();
      temp = temp >> 8;
    }
/*    byte head = b[0];
    int h = (int)head;
    h = h == 0?1:0;
    head = (byte)h;*/
    b[0] = (byte)(b[0] ^ 0x80);
    //b[0] = head;
    return b;
  }

  public static void main(String[] args) throws IOException {
/*    byte[] b = longToByte(0);
    System.out.println(b);*/

    /*byte[] a = Bytes.toBytes("20150917");
    byte[] b = Bytes.toBytes(20150917);
    boolean result = a.equals(b);
    System.out.println(result);*/

    System.out.println("App start!");
    String tableName = args[0];
    Configuration conf = HBaseConfiguration.create();
    HTable table = new HTable(conf, tableName);
    Put put = new Put(Bytes.toBytes(20000000L));
    byte[] family = Bytes.toBytes("0");
    byte[] order_id = Bytes.toBytes("ORDER_ID");
    byte[] order_type =  Bytes.toBytes("ORDER_TYPE");
    byte[] p_ip = Bytes.toBytes("P_IP");
    byte[] id = Bytes.toBytes("ID");
    put.add(family, order_id, Bytes.toBytes(21111111111L));
    put.add(family, order_type, longToByte(0L));
    put.add(family, p_ip, Bytes.toBytes("127.0.0.1"));
    put.add(family, id, Bytes.toBytes(20000000L));

    table.put(put);
    table.flushCommits();
    table.close();
    System.out.println("Put complete!");
  }
}
