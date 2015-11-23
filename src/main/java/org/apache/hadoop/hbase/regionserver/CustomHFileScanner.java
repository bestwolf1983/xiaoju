package org.apache.hadoop.hbase.regionserver;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import com.xiaoju.PhoenixTypeUtil;

public class CustomHFileScanner {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		String tableName = "SF_ORDER2";
		FileSystem fs = FileSystem.get(conf);
		HTable table = new HTable(conf, Bytes.toBytes(tableName));
		Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), 
				TableName.valueOf(tableName));
		Path regionDir = null;
		NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
		Set<HRegionInfo> regions= locations.keySet();
		byte[] startKey = null;
		for(HRegionInfo region : regions) {
			 startKey = region.getStartKey();
			 if(startKey.length == 13) {
				 try {
					 int bucketNumber = (int)startKey[0];
					 if(bucketNumber == 1) {
						 regionDir = HRegion.getRegionDir(tableDir, region.getEncodedName());
					 } else {
						 System.out.println("region start key is " 
								 + bucketNumber + " not 1 !");
					 }
				 }catch(Exception ex) {
					 ex.printStackTrace();
				 }
			 } else {
				 System.out.println("not match, startKey length " 
						 + startKey.length + " not equal 1 !");
			 }
		}
		System.out.println("fs dir: " + regionDir.getName());
		FileStatus[] files = fs.listStatus(new Path(regionDir, "0"));
		
		Scan regionScan = new Scan();
		regionScan.setBatch(100000000);
		regionScan.setCaching(1100);
		regionScan.setMaxVersions(1);
		regionScan.addFamily(Bytes.toBytes("0"));
		//ֻ����20150709��һ���
		byte[] startRow = new byte[5];
		startRow[0] = (byte)1;
		System.arraycopy(PhoenixTypeUtil.toBytes(20150709L), 0, startRow, 1, 4);
		byte[] stopRow = new byte[6];
		stopRow[0] = (byte)1;
		System.arraycopy(PhoenixTypeUtil.toBytes(20150709L), 0, stopRow, 1, 4);
		stopRow[5] = (byte)9;
		regionScan.setStartRow(startRow);
		// regionScan.setStopRow(stopRow);
		
	    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
	    for (FileStatus status: files) {
	      if (!StoreFileInfo.isValid(status)) continue;
	      storeFiles.add(new StoreFileInfo(conf, fs, status));
	    }
	    
	    System.out.println("Start to scan " + files.length + " files");
	    
	    KeyValue kv = null;
	    CustomRegionScanner heap = new CustomRegionScanner(regionScan, conf, storeFiles);
	    LinkedList<Cell> list = new LinkedList<Cell>();
	    boolean isOver = false;
	    byte[] rowkey = null;
	    byte[] value = null;
	    StringBuilder sb = null; 
	    FileOutputStream outputStream = new FileOutputStream("/home/hfile.txt");
	    long line = 0;
	    long startTime = System.currentTimeMillis();
	    int date = 0;
	    do {
	    	heap.next(list);
	    	if(!list.isEmpty()) {
	    		sb = new StringBuilder();
	    		rowkey = list.get(0).getRow();
	    		date = PhoenixTypeUtil.decodeInt(rowkey, 1);
	    		if(date != 20150709) {
	    			System.out.println("game over! date is not 20150709...");
	    			isOver = true;
	    			break;
	    		}
    			sb.append(date + ",");
    			sb.append(PhoenixTypeUtil.decodeLong(rowkey, 5) + ",");
	    		for(Cell cell : list) {
	    			value = cell.getValue();
	    			if(value.length == 4) {
	    				try {
	    					sb.append(PhoenixTypeUtil.decodeInt(value, 0));
	    				} catch(Exception ex) {
	    					ex.printStackTrace();
	    					sb.append(Bytes.toString(value));
	    				}
	    			} else if(value.length == 8) {
	    				try {
	    					sb.append(PhoenixTypeUtil.decodeLong(value, 0));
	    				} catch(Exception ex) {
	    					ex.printStackTrace();
	    					sb.append(Bytes.toString(value));
	    				}
	    			} else if(value.length == 0){
	    				
	    			} else {
	    				sb.append(Bytes.toString(value));
	    			}
	    			sb.append(",");
	    		}
	    		sb.append("\r\n");
	    		outputStream.write(sb.toString().getBytes());
	    		list.clear();
	    		line = line + 1;
	    		if(line % 10000 == 0) {
	    			System.out.println("scan " + line + " record");
	    		}
	    	} else {
	    		isOver = true;
	    		System.out.println("game over");
	    	}
	    	
	    } while(!isOver);
	    heap.close();
	    long endTime = System.currentTimeMillis();
	    System.out.println("write " + line + "lines use " 
	    		+ (endTime - startTime)/1000 + "s");
	}
}
