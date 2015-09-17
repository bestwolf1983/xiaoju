package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
//import org.apache.hadoop.hbase.thrift.generated.Hbase.Processor.scannerClose;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

public class CustomHFileScanner {

	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
/*		HTable table = new HTable(conf, Bytes.toBytes("test"));
		NavigableMap<HRegionInfo, ServerName> locations = table.getRegionLocations();
		Set<HRegionInfo> regions= locations.keySet();

		ArrayList<byte[]> keys = new ArrayList<byte[]>();
		keys.add(Bytes.toBytes("1"));
		keys.add(Bytes.toBytes("2"));
		HashMap<byte[], HRegionInfo> querys = new HashMap<byte[], HRegionInfo>();
		
		// ����
		// scan.setStartRow(TableName.valueOf("test").getName());
		// scan.setStopRow(Bytes.toBytes("test"));
		//ResultScanner scanner = table.getScanner(scan);
		HRegionInfo regionInfo = null;
		// ʵ����һ���Ƚ���
		Comparator<byte[]> compare = new Comparator<byte[]>() {
			@Override
			public int compare(byte[] left, byte[] right) {

				return Bytes.compareTo(left, right);
			}
		};

		NavigableMap<byte[], HRegionInfo> key2Region = new TreeMap<byte[], HRegionInfo>(compare);
		for (HRegionInfo region : regions) {
			if (!region.isSplit() && !region.isOffline()) {
				key2Region.put(region.getStartKey(), region);
			}
			System.out.println(region.toString());
		}

		//regions.put(Bytes.toBytes("1"), regionInfo);
		byte[] key = null;
		// ͨ��starkey���ҳ���ӽ����key��region��λ��
		key = key2Region.floorKey(Bytes.toBytes("111"));
		System.out.println(Bytes.toString(key));
		regionInfo = key2Region.get(key);
		System.out.println(regionInfo.toString());
		// �ж��Ƿ�������
		boolean exists = regionInfo.containsRow(Bytes.toBytes("111"));
		System.out.println(exists);
		// �����������encodedName�������rowkey����region��Ŀ¼���
		String encodedRegionName = regionInfo.getEncodedName();
		// ��¼(encodedRegionName, rowkey)
		Path tableDir = FSUtils.getTableDir(FSUtils.getRootDir(conf), TableName.valueOf("test"));
		Path regionDir = HRegion.getRegionDir(tableDir, encodedRegionName);
		FileSystem fs = tableDir.getFileSystem(conf);
		// ����ֱ��д�ˣ����������µ�����HFile
		FileStatus[] files = fs.listStatus(new Path(regionDir, "cf"));*/
		
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] files = fs.listStatus(new Path("/hbase-data/data/default/SF_ORDER2/d9f958f6b0f77ace9527eb433be15e26/0/"));
		System.out.println("has " + files.length + " files!");
		Scan regionScan = new Scan();

		regionScan.setBatch(100);
		regionScan.setCaching(100);
		regionScan.setMaxVersions(1);
		regionScan.addFamily(Bytes.toBytes("0"));
		regionScan.setStartRow((new byte[]{(byte) 0}));
/*		regionScan.setStartRow(Bytes.toBytes("1"));
		regionScan.setStopRow(Bytes.toBytes("3"));*/

	    ArrayList<StoreFileInfo> storeFiles = new ArrayList<StoreFileInfo>(files.length);
	    for (FileStatus status: files) {
	      if (!StoreFileInfo.isValid(status)) continue;
	      storeFiles.add(new StoreFileInfo(conf, fs, status));
	    }
	    LinkedList<Cell> list = new LinkedList<Cell>();
	    
	    CustomRegionScanner heap = new CustomRegionScanner(regionScan, conf, storeFiles);
	    //boolean exist = true;
	    int i=0;
	    do {
	    	heap.next(list);
	    	//heap.reseek(Bytes.toBytes("2"));
	    	i++;
	    } while(i<10);
	    heap.close();
	    KeyValue kv = null;
			System.out.println("scan is over, start to print " + list.size() + " elements!");
	    for(Cell cell : list) {
	    	kv = KeyValueUtil.ensureKeyValue(cell);
	    	System.out.println("Row: " + Bytes.toLong(kv.getRow()));
	    	//System.out.println(Bytes.toString(kv.getValue()));
	    }


	}
}
