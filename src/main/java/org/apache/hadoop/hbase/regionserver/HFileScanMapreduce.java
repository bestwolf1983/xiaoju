package org.apache.hadoop.hbase.regionserver;

import com.xiaoju.PhoenixTypeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

public class HFileScanMapreduce {

  public static class Map extends Mapper<LongWritable, Text, NullWritable, Writable> {

    public enum ColumnType {
      LONG, BIGINT, INT, STRING, DECIMAL
    }

    public int changeColByType(String type) {
      if(type.contains(ColumnType.LONG.toString())) {
        return ColumnType.LONG.ordinal();
      } else if (type.contains(ColumnType.BIGINT.toString())){
        return ColumnType.BIGINT.ordinal();
      } else if(type.contains("int")) {
        return ColumnType.INT.ordinal();
      } else if (type.equals("string")){
        return ColumnType.STRING.ordinal();
      } if(type.contains("decimal")) {
        return ColumnType.DECIMAL.ordinal();
      } else {
        return ColumnType.STRING.ordinal();
      }
    }

    public Object tranformationColName(byte[] bytes, int type) {
      if(type == ColumnType.LONG.ordinal()) {
        return Bytes.toLong(bytes);
      } else if(type == ColumnType.BIGINT.ordinal()) {
        return Bytes.toLong(bytes);
      } else if(type == ColumnType.INT.ordinal()) {
        return Bytes.toInt(bytes);
      } else if(type == ColumnType.STRING.ordinal()) {
        return Bytes.toString(bytes);
      } else if(type == ColumnType.DECIMAL.ordinal()) {
        return Bytes.toBigDecimal(bytes);
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

    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      Configuration conf = context.getConfiguration();
      String startId = conf.get("StartKey");
      String endId = conf.get("EndKey");
      String familyName = conf.get("FamilyName");
      String schema = conf.get("Schema");
      String hdfsDir = conf.get("HdfsDir");
      int bucketNumber = 100;
      //int bucketNumber = Integer.valueOf(conf.get("BucketNumber"));
      String regionDir = value.toString();
      FileSystem fs = FileSystem.get(URI.create(regionDir), conf);
      FileStatus[] files = fs.listStatus(new Path(regionDir, familyName));
      Scan regionScan = new Scan();
      regionScan.setBatch(Integer.MAX_VALUE);
      regionScan.setCaching(Integer.MAX_VALUE);
      regionScan.setMaxVersions(1);
      regionScan.addFamily(Bytes.toBytes(familyName));

      //byte[] startRow = Bytes.toBytes(startId);
      //byte[] stopRow = Bytes.toBytes(endId);
      byte[] startRow = new byte[5];
      startRow[0] = (byte)0;
      System.arraycopy(PhoenixTypeUtil.toBytes(20150709), 0, startRow, 1, 4);
      byte[] stopRow = new byte[6];
      stopRow[0] = (byte)(bucketNumber - 1);
      System.arraycopy(PhoenixTypeUtil.toBytes(20150709), 0, stopRow, 1, 4);
      stopRow[5] = (byte)(bucketNumber - 1);
      regionScan.setStartRow(getBytes(startId));
      regionScan.setStopRow(getBytes(endId));

      ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
      for (FileStatus status : files) {
        if (!StoreFileInfo.isValid(status)) continue;
        storeFiles.add(new StoreFileInfo(conf, fs, status));
      }

      System.out.println("Start to scan " + files.length + " files");
      KeyValue kv = null;
      CustomRegionScanner heap = new CustomRegionScanner(regionScan, conf, storeFiles);
      LinkedList<Cell> list = new LinkedList<Cell>();
      boolean isOver = false;
      byte[] cellValue = null;
      String colName = null;
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
      ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

      HashMap<String, Integer> colname2Index = new HashMap<String, Integer>();
      HashMap<String, Integer> colname2Type = new HashMap<String, Integer>();
      String fieldsStr = schema.substring("struct<".length(), schema.length() - 1 - (">".length()));
      String[] fields = fieldsStr.split(",");
      String[] splits = null;

      for (int i = 0; i < fields.length; i++) {
        splits = fields[i].split(":");
        colname2Index.put(splits[0], i);
        colname2Type.put(splits[0], changeColByType(splits[1].toLowerCase()));
      }

      final OrcSerde serde = new OrcSerde();
      Writable row;
      List<Object> struct = new ArrayList<Object>(fields.length);

      long line = 0;
      Integer colType;
      Integer index;
      do {
        heap.next(list);
        if (!list.isEmpty()) {
          for (Cell cell : list) {
            colName = Bytes.toString(cell.getQualifier());
            colType = colname2Type.get(colName.toLowerCase());
            cellValue = cell.getValue();
            index = colname2Index.get(colName);
            if(index != null) {
              struct.add(colname2Index.get(colName), tranformationColName(cellValue, colType));
            }
          }
          list.clear();
          line = line + 1;
          if (line % 10000 == 0) {
            System.out.println("scan " + line + " record");
          }

          row = serde.serialize(struct, inspector);
          context.write(null, row);
        } else {
          isOver = true;
          System.out.println("game over");
        }

      } while (!isOver);
      heap.close();
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

    try {
      Connection conn = DriverManager.getConnection(url, user, password);
      Statement statement = conn.createStatement();
      String querySql = "select COLUMN_NAME,TYPE_NAME from COLUMNS_V2 where CD_ID in "
          + "(select CD_ID from SDS where SD_ID in "
          + "(select t.SD_ID from DBS d left join TBLS t on d.DB_ID=t.DB_ID "
          + "where d.NAME='" + hiveDb + "' and t.TBL_NAME='" + hiveTableName+ "')"
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
      while(cols.next()) {
        sb.append(cols.getString(1).toLowerCase() + ":" + cols.getString(2).toLowerCase() + ",");
      }

      sb.deleteCharAt(sb.length() - 1);
      sb.append(">");
      System.out.println("Table Schema: " + sb.toString());
      conf.set("Schema", sb.toString());
      statement.close();
      Job job = new Job(conf, "Read Table " + hbaseTableName);
      job.setJarByClass(HFileScanMapreduce.class);
      job.setMapperClass(Map.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(RegionInputformat.class);
      job.setOutputFormatClass(OrcNewOutputFormat.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setMapOutputValueClass(Writable.class);
      FileInputFormat.setInputPaths(job, new Path(tableDir));
      FileOutputFormat.setOutputPath(job, new Path(tableDir, "tmp"));
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
