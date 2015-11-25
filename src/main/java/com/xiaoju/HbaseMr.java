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
import org.apache.hadoop.hbase.regionserver.RegionSplit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

public class HbaseMr {

  public static class ReaderHbaseMap extends TableMapper<NullWritable, Text> {

    public enum ColumnType {
      LONG, BIGINT, INT, STRING, DECIMAL
    }

    public int changeColByType(String type) {
      if (type.contains(ColumnType.LONG.toString())) {
        return ColumnType.LONG.ordinal();
      } else if (type.contains(ColumnType.BIGINT.toString())) {
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
        return Long.valueOf(Bytes.toLong(bytes)).toString();
      } else if (type == ColumnType.BIGINT.ordinal()) {
        return Long.valueOf(Bytes.toLong(bytes)).toString();
      } else if (type == ColumnType.INT.ordinal()) {
        return Integer.valueOf(Bytes.toInt(bytes)).toString();
      } else if (type == ColumnType.STRING.ordinal()) {
        return Bytes.toString(bytes);
      } else if (type == ColumnType.DECIMAL.ordinal()) {
        return Bytes.toBigDecimal(bytes).toString();
      } else {
        return Bytes.toString(bytes);
      }
    }

    private String regionPath;

    protected void setup(Context context) {
      regionPath = ((RegionSplit) context.getInputSplit()).getRegionPath();
    }

    public byte[] getBytes(String key) {
      try {
        return Bytes.toBytes(Long.valueOf(key));
      } catch (Exception ex) {
        return Bytes.toBytes(key);
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
      }

      String[] values = new String[fields.length];
      String colName = null;
      Integer colType = null;
      Integer index = null;
      NavigableMap<byte[], NavigableMap<byte[], byte[]>> resmap = value.getNoVersionMap();
      for (java.util.Map.Entry res : resmap.entrySet()) {
        NavigableMap<byte[], byte[]> qualifiers = (NavigableMap<byte[], byte[]>) res.getValue();
        for (java.util.Map.Entry entry : qualifiers.entrySet()) {
          for (java.util.Map.Entry col : qualifiers.entrySet()) {
            cellName = (byte[]) col.getKey();
            cellValue = (byte[]) col.getValue();
            colName = Bytes.toString(cellName);
            colType = colname2Type.get(colName.toLowerCase());
            index = colname2Index.get(colName.toLowerCase());
            values[index] = tranformationColName(cellValue, colType);
          }
        }
      }

      StringBuilder sb = new StringBuilder();
      for (String s : values) {
        if (s != null) {
          sb.append(s + "\t");
        } else {
          sb.append("\t");
        }
      }

      context.write(null, new Text(sb.toString()));
    }
  }

  public static Properties readProperties(String file) throws Exception {
    InputStream in = new FileInputStream(file);
    Properties p = new Properties();
    p.load(in);
    return p;
  }

  public static void main(String[] args) throws Exception {
    Properties properties = readProperties(args[0]);
    String hbaseTableName = properties.getProperty("HbaseTable");
    String hiveDb = properties.getProperty("HiveDb");
    String hiveTableName = properties.getProperty("HiveTable");
    String familyName = properties.getProperty("FamilyName");
    String tableDir = properties.getProperty("TableDir");
    String startKey = args[1];
    String endKey = args[2];
    Configuration conf = HBaseConfiguration.create(new Configuration());
    conf.set("Table", hbaseTableName);
    conf.set("StartKey", startKey.trim());
    conf.set("EndKey", endKey.trim());
    conf.set("FamilyName", familyName);
    HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
    hbaseAdmin.flush(hbaseTableName);
    String url = properties.getProperty("JdbcUrl");
    String user = properties.getProperty("User");
    String password = properties.getProperty("Password");


    Connection conn = DriverManager.getConnection(url, user, password);
    Statement statement = conn.createStatement();
    String querySql = "select COLUMN_NAME,TYPE_NAME from COLUMNS_V2 where CD_ID in "
        + "(select CD_ID from SDS where SD_ID in "
        + "(select t.SD_ID from DBS d left join TBLS t on d.DB_ID=t.DB_ID "
        + "where d.NAME='" + hiveDb + "' and t.TBL_NAME='" + hiveTableName + "')"
        + ") order by column_name";
    System.out.println(querySql);
    conf.set("HdfsDir", tableDir.toString());

    FileSystem fs = FileSystem.get(conf);
    Path outputDir = new Path(tableDir, "tmp");
    boolean isExist = fs.exists(outputDir);
    if (isExist) {
      fs.delete(outputDir);
    }

    StringBuilder sb = new StringBuilder();
    ResultSet cols = statement.executeQuery(querySql);
    sb.append("struct<");
    while (cols.next()) {
      sb.append(cols.getString(1).toLowerCase() + ":" + cols.getString(2).toLowerCase() + ",");
    }

    sb.deleteCharAt(sb.length() - 1);
    sb.append(">");
    System.out.println("Table Schema: " + sb.toString());
    conf.set("Schema", sb.toString());
    statement.close();

    Job job = new Job(conf, "Read Table:" + hbaseTableName);
    job.setJarByClass(Map.class);

    Scan scan = new Scan();
    scan.setCaching(100);
    scan.setBatch(Integer.MAX_VALUE);
    scan.setMaxVersions(1);

    TableMapReduceUtil.initTableMapperJob(
        hbaseTableName,
        scan,
        ReaderHbaseMap.class,
        NullWritable.class,
        Text.class,
        job);
    job.setNumReduceTasks(0);

    boolean b = job.waitForCompletion(true);
    if (!b) {
      throw new IOException("error with job!");
    }


  }

}
