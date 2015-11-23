package org.apache.hadoop.hbase.regionserver;

import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Scan;

public class CustomScanQueryMatcher extends ScanQueryMatcher{

	public CustomScanQueryMatcher(Scan scan, ScanInfo scanInfo,
			NavigableSet<byte[]> columns, ScanType scanType,
			long readPointToUse, long earliestPutTs, long oldestUnexpiredTS) {
		super(scan, scanInfo, columns, scanType, readPointToUse, earliestPutTs,
				oldestUnexpiredTS);
		// TODO Auto-generated constructor stub
	}

	public CustomScanQueryMatcher(Scan scan, ScanInfo scanInfo,
			NavigableSet<byte[]> columns, long readPointToUse,
			long earliestPutTs, long oldestUnexpiredTS,
			byte[] dropDeletesFromRow, byte[] dropDeletesToRow) {
		super(scan, scanInfo, columns, readPointToUse, earliestPutTs,
				oldestUnexpiredTS, dropDeletesFromRow, dropDeletesToRow);
		// TODO Auto-generated constructor stub
		
		super.reset();
	}

}
