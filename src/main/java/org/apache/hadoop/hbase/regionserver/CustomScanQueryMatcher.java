package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.ScanInfo;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher;
import org.apache.hadoop.hbase.regionserver.ScanType;

public class CustomScanQueryMatcher extends ScanQueryMatcher{

	public CustomScanQueryMatcher(Scan scan, ScanInfo scanInfo,
			NavigableSet<byte[]> columns, ScanType scanType,
			long readPointToUse, long earliestPutTs, long oldestUnexpiredTS) throws IOException {
		super(scan, scanInfo, columns, readPointToUse, earliestPutTs);
		// TODO Auto-generated constructor stub
	}

	public CustomScanQueryMatcher(Scan scan, ScanInfo scanInfo,
			NavigableSet<byte[]> columns, long readPointToUse,
			long earliestPutTs, long oldestUnexpiredTS,
			byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
		super(scan, scanInfo, columns, readPointToUse, earliestPutTs,
				oldestUnexpiredTS, System.currentTimeMillis(), dropDeletesFromRow, dropDeletesToRow, null);
		// TODO Auto-generated constructor stub

		super.reset();
	}

}
