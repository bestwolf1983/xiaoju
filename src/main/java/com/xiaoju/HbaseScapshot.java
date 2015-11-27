package com.xiaoju;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.NavigableMap;
import java.util.Properties;

public class HbaseScapshot {

  public static class ReaderHbaseMap extends TableMapper<NullWritable, Text> {

    public enum ColumnType {
      LONG, BIGINT, INT, STRING, DECIMAL
    }

    public int changeColByType(String type) {
      if (type.contains("long")) {
        return ColumnType.LONG.ordinal();
      } else if (type.contains("bigint")) {
        return ColumnType.BIGINT.ordinal();
      } else if (type.contains("int")) {
        return ColumnType.INT.ordinal();
      } else if (type.equals("string")) {
        return ColumnType.STRING.ordinal();
      }
      if (type.contains("decimal")) {
        return ColumnType.DECIMAL.ordinal();
      } else {
        return ColumnType.STRING.ordinal();
      }
    }

    public String tranformationColName(byte[] bytes, int type) {
      if (type == ColumnType.LONG.ordinal()) {
        return String.valueOf(Bytes.toLong(bytes));
      } else if (type == ColumnType.BIGINT.ordinal()) {
        return String.valueOf(Bytes.toLong(bytes));
      } else if (type == ColumnType.INT.ordinal()) {
        return String.valueOf(Bytes.toInt(bytes));
      } else if (type == ColumnType.STRING.ordinal()) {
        return Bytes.toString(bytes);
      } else if (type == ColumnType.DECIMAL.ordinal()) {
        return String.valueOf(Bytes.toBigDecimal(bytes));
      } else {
        return Bytes.toString(bytes);
      }
    }


    public void map(ImmutableBytesWritable key, Result value, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String schema = conf.get("Schema");

      HashMap<String, Integer> colname2Index = new HashMap<String, Integer>();
      HashMap<String, Integer> colname2Type = new HashMap<String, Integer>();
      String fieldsStr = schema.substring("struct<".length(), schema.length() - 1 - (">".length()));
      String[] fields = fieldsStr.split(",");
      String[] splits = null;

      byte[] cellName = null;
      byte[] cellValue = null;
      for (int i = 0; i < fields.length; i++) {
        splits = fields[i].split(":");
        colname2Index.put(splits[0], i);
        colname2Type.put(splits[0], changeColByType(splits[1].toLowerCase()));
        System.out.println(splits[0] + " type: " + splits[1]);
      }



      String[] values = new String[fields.length];
      String colName = null;
      Integer colType = null;
      Integer index = null;
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> resmap = value.getNoVersionMap();
      for (java.util.Map.Entry res : resmap.entrySet()) {
        NavigableMap<byte[], byte[]> qualifiers = (NavigableMap<byte[], byte[]>) res.getValue();
        for (java.util.Map.Entry entry : qualifiers.entrySet()) {
            cellName = (byte[]) entry.getKey();
            cellValue = (byte[]) entry.getValue();
            colName = Bytes.toString(cellName);
            System.out.println("Col Name:"  + colName);
            index = colname2Index.get(colName.toLowerCase());
            if(index != null) {
              System.out.println("find index:" + colName + " " + index);
              colType = colname2Type.get(colName.toLowerCase());
              try{
                values[index] = tranformationColName(cellValue, colType);
              } catch(Exception ex) {
                System.out.println("*******Exception Cell Name:" + colName + " is Wrong!");
              }
            } else {
              System.out.println("find index:" + colName);
            }

        }
      }

      StringBuilder sb = new StringBuilder();
      for (String s : values) {
        if (s != null) {
          sb.append(s);
        }
        sb.append("\001");
      }
      sb.deleteCharAt(sb.length() - 1);

      context.write(null, new Text(sb.toString()));
    }
  }

  public static Properties readProperties(String file) throws Exception {
    InputStream in = new FileInputStream(file);
    Properties p = new Properties();
    p.load(in);
    return p;
  }


  public static byte[] getBytes(String key) {
    try {
      return Bytes.toBytes(Long.valueOf(key));
    } catch (Exception ex) {
      return Bytes.toBytes(key);
    }
  }

  public static String trimType(String type) {
    if(type.contains("bigint")) {
      return "bigint";
    }

    if(type.contains("long")) {
      return "long";
    }

    if(type.contains("decimal")) {
      return "decimal";
    }

    return type.toLowerCase();
  }

  public static void main(String[] args) throws Exception {
    Properties properties = readProperties(args[0]);
    String hbaseTableName = args[1];
    String hiveDb = properties.getProperty("HiveDb");
    String hiveTableName = hbaseTableName.toLowerCase();
    String familyName = properties.getProperty("FamilyName");
    String zookeeperList = properties.getProperty("ZookeeperList");
    String startKey = args[2];
    String endKey = args[3];
    Configuration conf = HBaseConfiguration.create(new Configuration());
    conf.set("Table", hbaseTableName);
    conf.set("StartKey", startKey.trim());
    conf.set("EndKey", endKey.trim());
    conf.set("FamilyName", familyName);
    String url = properties.getProperty("JdbcUrl");
    String user = properties.getProperty("User");
    String password = properties.getProperty("Password");
    conf.set("hbase.zookeeper.quorum",zookeeperList);
    Connection conn = DriverManager.getConnection(url, user, password);
    Statement statement = conn.createStatement();
    String queryLocationSql = "select LOCATION from SDS where SD_ID in "
        + " (select t.SD_ID from DBS d left join TBLS t on d.DB_ID=t.DB_ID "
        + "  where d.NAME='" + hiveDb +"' and t.TBL_NAME='" + hiveTableName + "')";

    ResultSet locationSet = statement.executeQuery(queryLocationSql);
    locationSet.next();
    String tableDir = locationSet.getString(1);

    String querySql = "select COLUMN_NAME,TYPE_NAME from COLUMNS_V2 where CD_ID in "
        + "(select CD_ID from SDS where SD_ID in "
        + "(select t.SD_ID from DBS d left join TBLS t on d.DB_ID=t.DB_ID "
        + "where d.NAME='" + hiveDb + "' and t.TBL_NAME='" + hiveTableName + "')"
        + ") order by INTEGER_IDX";
    System.out.println(querySql);
    System.out.println("Table Dir:" + tableDir);
    conf.set("HdfsDir", tableDir.toString());

    FileSystem fs = FileSystem.get(conf);
    Path antiExportPath = new Path("/tmp/anti-export");
    if(!fs.exists(antiExportPath)) {
      fs.mkdirs(antiExportPath);
    }
    Path outputDir = new Path("/tmp/anti-export/" + hiveTableName + "/output");
    boolean isExist = fs.exists(outputDir);
    if (isExist) {
      fs.delete(outputDir);
    }

    Path restoreDir = new Path("/tmp/anti-export/" + hiveTableName + "/restore");
    isExist = fs.exists(restoreDir);
    if (isExist) {
      fs.delete(restoreDir);
    }

    StringBuilder sb = new StringBuilder();
    ResultSet cols = statement.executeQuery(querySql);
    sb.append("struct<");
    while (cols.next()) {
      sb.append(cols.getString(1).toLowerCase() + ":" + trimType(cols.getString(2).toLowerCase()) + ",");
    }

    sb.deleteCharAt(sb.length() - 1);
    sb.append(">");
    System.out.println("Table Schema: " + sb.toString());
    conf.set("Schema", sb.toString());
    statement.close();

    String snapshotString = hbaseTableName + System.currentTimeMillis();
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.snapshot(snapshotString, hbaseTableName, HBaseProtos.SnapshotDescription.Type.FLUSH);

    List<HBaseProtos.SnapshotDescription> snapshots = admin.listSnapshots();
    boolean foundSnapShot = false;
    for(HBaseProtos.SnapshotDescription s: snapshots) {
      if(s.getTable().equals(hbaseTableName) && s.getName().equals(snapshotString)) {
        foundSnapShot = true;
      }
    }

    if(!foundSnapShot) {
      System.out.println("can not found the snapshot we take!!! program exit");
    }

    Job job = new Job(conf, "Read Table:" + hbaseTableName);
    job.setJarByClass(HbaseScapshot.class);

    Scan scan = new Scan();
    scan.setCaching(1000);
    scan.setBatch(Integer.MAX_VALUE);
    scan.setMaxVersions(1);
    if(!startKey.equals("0")) {
      scan.setStartRow(getBytes(startKey));
    }
    if(!endKey.equals("0")) {
      scan.setStopRow(getBytes(endKey));
    }

    TableSnapshotMapReduceUtil.addDependencyJars(job.getConfiguration(),
        HbaseScapshot.class);

    TableSnapshotMapReduceUtil.initTableSnapshotMapperJob(
        snapshotString,
        scan,
        ReaderHbaseMap.class,
        NullWritable.class,
        Text.class,
        job,
        true,
        restoreDir);

    boolean b = job.waitForCompletion(true);
    FileOutputFormat.setOutputPath(job, outputDir);

/*    if (b) {
      Path tablePath = new Path(tableDir);
      fs.delete(tablePath);
      fs.rename(outputDir, tablePath);
      System.out.println("delete snapshot:" + snapshotString);
      admin.deleteSnapshot(snapshotString);
      admin.close();
    } else {
      throw new IOException("error with job!");
    }*/


  }

}
