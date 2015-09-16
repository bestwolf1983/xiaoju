package com.xiaoju

import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding
import org.apache.hadoop.hbase.regionserver.BloomType
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.HBaseAdmin

/**
 * Created by cenyuhai on 2015/9/16.
 */
object CreateHbaseTable {

  def main(args: Array[String]) = {
    var tableName = args(0)
    var conf = HBaseConfiguration.create()
    var hBaseAdmin = new HBaseAdmin(conf)
    if (hBaseAdmin.tableExists(tableName)) {
/*      hBaseAdmin.disableTable(tableName)
      hBaseAdmin.deleteTable(tableName)*/
      println(tableName + " is exist,detele....")
    }
    var family = new HColumnDescriptor("0")
    family.setBloomFilterType(BloomType.ROW)
    family.setMaxVersions(1)
    family.setKeepDeletedCells(false)
    family.setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE)
    var tableDescriptor = new HTableDescriptor(tableName)
    tableDescriptor.addFamily(family)
    var points: Array[Long] = Array(11, 12, 13 , 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29)
    var splitPoints:Array[Array[Byte]] = points.map(x=> Bytes.toBytes(x))
    hBaseAdmin.createTable(tableDescriptor, splitPoints)
  }

}
