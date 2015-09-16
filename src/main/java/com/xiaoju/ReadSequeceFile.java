package com.xiaoju;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;  
import org.apache.hadoop.io.Writable;  
import org.apache.hadoop.util.ReflectionUtils;

public class ReadSequeceFile {
	public static void main(String[] args) throws IOException {
		String pathStr = "/SF_ORDER_BK/part-m-00000";
	    Configuration conf = new Configuration();  
	    FileSystem fs = FileSystem.get(URI.create(pathStr), conf);  
	    Path path = new Path(pathStr);
			Result valueResult = null;
	    SequenceFile.Reader reader = null;
	    try {  
	      reader = new SequenceFile.Reader(fs, path, conf);
	      ImmutableBytesWritable key = (ImmutableBytesWritable)  
	        ReflectionUtils.newInstance(reader.getKeyClass(), conf);
	      Writable value = (Writable)
	        ReflectionUtils.newInstance(reader.getValueClass(), conf);//ͬ��  
	      long position = reader.getPosition();  
	     
	      while (reader.next(key, value)) {
					valueResult = (Result)value;
					for  (Cell cell : valueResult.rawCells()) {
						System. out .println(
								"Rowkey : " + SaltingUtil.getSaltedKey(key, 20) +
										"   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
										"   Value : " +Bytes. toString (CellUtil. cloneValue (cell))

						);

					}
	        /*String syncSeen = reader.syncSeen() ? "*" : "";
	        System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
	        position = reader.getPosition(); // beginning of next record
*/	      }  
	    } finally {  
	      IOUtils.closeStream(reader);  
	    }  
	  }  
}
