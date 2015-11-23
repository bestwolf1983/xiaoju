package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class HFileScanMapreduce {

  public static class Map extends Mapper<LongWritable, Text, NullWritable, Writable> {

    public Object changeColByType(byte[] bytes, String type) {
      if(type.contains("long") || type.contains("bigint")) {
        return Bytes.toLong(bytes);
      } else if(type.contains("int")) {
        return Bytes.toInt(bytes);
      } else if (type.equals("string")){
        return Bytes.toString(bytes);
      } if(type.contains("decimal")) {
        return Bytes.toBigDecimal(bytes);
      } else {
        return Bytes.toString(bytes);
      }

/*      switch (type) {
        case "SHORT":
          return Bytes.toShort(bytes);
        case "INT":
          return Bytes.toInt(bytes);
        case "LONG":
          return Bytes.toLong(bytes);
        case "FLOAT":
          return Bytes.toFloat(bytes);
        case "DOUBLE":
          return Bytes.toDouble(bytes);
        case "DECIMAL":
          return Bytes.toBigDecimal(bytes);
        default:
          return Bytes.toString(bytes);
      }*/
    }

    private MultipleOutputs output;
    private String regionPath;

    protected void setup(Context context) {
      output = new MultipleOutputs(context);
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
      FileSystem fs = FileSystem.get(conf);
      String regionDir = value.toString();
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
      System.arraycopy(PhoenixTypeUtil.toBytes(20150709L), 0, startRow, 1, 4);
      byte[] stopRow = new byte[6];
      stopRow[0] = (byte)1;
      System.arraycopy(PhoenixTypeUtil.toBytes(20150709L), 0, stopRow, 1, 4);
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
      StringBuilder sb = null;
      String colName = null;
      TypeInfo typeInfo = TypeInfoUtils.getTypeInfoFromTypeString(schema);
      ObjectInspector inspector = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

      HashMap<String, Integer> colname2Index = new HashMap<String, Integer>();
      HashMap<String, String> colname2Type = new HashMap<String, String>();
      String fieldsStr = schema.substring("struct<".length(), schema.length() - 1 - (">".length()));
      String[] fields = fieldsStr.split(",");
      String[] splits = null;

      for (int i = 0; i < fields.length; i++) {
        splits = fields[i].split(",");
        colname2Index.put(splits[0], i);
        colname2Type.put(splits[0], splits[1]);
      }

      final OrcSerde serde = new OrcSerde();
      Writable row;
      List<Object> struct = new ArrayList<Object>(fields.length);

      long line = 0;
      String colType = null;
      do {
        heap.next(list);
        if (!list.isEmpty()) {
          for (Cell cell : list) {
            colName = Bytes.toString(cell.getQualifier());
            colType = colname2Type.get(colName.toLowerCase());
            cellValue = cell.getValue();
            struct.add(colname2Index.get(colName), changeColByType(cellValue, colType.toLowerCase()));
          }
          list.clear();
          line = line + 1;
          if (line % 10000 == 0) {
            System.out.println("scan " + line + " record");
          }

          row = serde.serialize(struct, inspector);
          output.write(regionPath, null, row, hdfsDir + "/tmp");
        } else {
          isOver = true;
          System.out.println("game over");
        }

      } while (!isOver);
      heap.close();
    }
  }

  public static void main(String[] args) throws Exception {
    String hbaseTableName = args[0];
    String hiveTableName = args[1];
    String familyName = args[2];
    String startKey = args[3];
    String endKey = args[4];
    Configuration conf = HBaseConfiguration.create(new Configuration());
    conf.set("Table", hbaseTableName);
    conf.set("StartKey", startKey.trim());
    conf.set("EndKey", endKey.trim());
    conf.set("FamilyName", familyName);
    HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
    hbaseAdmin.flush(hbaseTableName);

    try {
      Hive hive = Hive.get(new HiveConf());
      Table table = hive.getTable(hiveTableName);
      Path tableDir = table.getPath();
      System.out.println("Table hdfs dir is :" + tableDir.toString());
      conf.set("HdfsDir", tableDir.toString());

      FileSystem fs = FileSystem.get(conf);
      Path outputDir = new Path(tableDir, "tmp");
      boolean isExist = fs.exists(outputDir);
      if (isExist) {
        fs.delete(outputDir);
        fs.mkdirs(outputDir);
      }

      List<FieldSchema> cols = table.getAllCols();
      StringBuilder sb = new StringBuilder();
      sb.append("struct<");
      for (FieldSchema col : cols) {
        sb.append(col.getName().toLowerCase() + ":" + col.getType().toLowerCase() + ",");
      }

      if (cols.size() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }

      sb.append(">");
      System.out.println("Table Schema: " + sb.toString());
      conf.set("Schema", sb.toString());

      Job job = new Job(conf, "Read Table " + hbaseTableName);
      job.setJarByClass(HFileScanMapreduce.class);
      job.setMapperClass(Map.class);
      job.setNumReduceTasks(0);
      job.setInputFormatClass(RegionInputformat.class);
      job.setMapOutputKeyClass(NullWritable.class);
      job.setOutputValueClass(OrcNewOutputFormat.class);
      System.exit(job.waitForCompletion(true) ? 0 : 1);

    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
