package org.apache.hadoop.hbase.regionserver;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class RegionSplit extends InputSplit implements Writable {
	
	private String regionPath;
	
	public RegionSplit() {
		
	}
	
	public RegionSplit(String regionPath) {
		this.regionPath = regionPath;
	}
	
	public String getRegionPath() {
		return regionPath;
	}
	
	@Override
	public long getLength() throws IOException, InterruptedException {
		return 1;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return null;
	}

	public void readFields(DataInput in) throws IOException {
		regionPath = in.readUTF();
	}

	public void write(DataOutput out) throws IOException {
		out.writeUTF(regionPath);
	}

}
