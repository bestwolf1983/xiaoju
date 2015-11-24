package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * ���hbase��region���з֣��ж��ٸ�region���ж��ٸ�split
 * @author cenyuhai
 *
 */
public class RegionInputformat extends InputFormat<LongWritable, Text>{

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit inputSplit,
			TaskAttemptContext context) throws IOException, InterruptedException {
		return new RegionRecordReader();
	}

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException,
			InterruptedException {
		Configuration conf = context.getConfiguration();
		String tableName = conf.get("Table");
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		//Configuration hbaseConf = HBaseConfiguration.create();
		Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), TableName.valueOf(tableName));
		NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
		Set<HRegionInfo> regions= locations.keySet();
		List<InputSplit> splits = new ArrayList<InputSplit>();
		for(HRegionInfo regionInfo : regions) {
			String regionDir = HRegion.getRegionDir(tableDir, regionInfo.getEncodedName()).toString();
			splits.add(new RegionSplit(regionDir));
		}
		
		return splits;
	}
	
	public static class RegionRecordReader extends RecordReader<LongWritable, Text> {
		
		private int pos = 0; 
		private long length = 0;
		private LongWritable key = new LongWritable();
		private Text value = new Text();
		private RegionSplit split = null;

		@Override
		public LongWritable getCurrentKey() throws IOException, InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public float getProgress() throws IOException, InterruptedException {
			return pos / length;
		}

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			length = split.getLength();
			this.split = (RegionSplit)split;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			if(pos >= length) {
				return false;
			}
			value = new Text(this.split.getRegionPath());
			pos ++;
			return true;
		}

		@Override
		public void close() throws IOException {
			
		}
		
	}


}


