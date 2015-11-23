package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterWrapper;
import org.apache.hadoop.hbase.filter.IncompatibleFilterException;
import org.apache.hadoop.hbase.util.Bytes;

public class CustomRegionScanner implements RegionScanner {

	private static final List<Cell> MOCKED_LIST = new AbstractList<Cell>() {

		@Override
		public void add(int index, Cell element) {
			// do nothing
		}

		@Override
		public boolean addAll(int index, Collection<? extends Cell> c) {
			return false; // this list is never changed as a result of an update
		}

		@Override
		public KeyValue get(int index) {
			throw new UnsupportedOperationException();
		}

		@Override
		public int size() {
			return 0;
		}
	};


	
	KVComparator comparator = KeyValue.COMPARATOR;

	/** ���store��scanner */
	KeyValueHeap storeHeap = null;

	/**
	 * If the joined heap data gathering is interrupted due to scan limits, this
	 * will contain the row for which we are populating the values.
	 */
	private KeyValue joinedContinuationRow = null;
	/** KeyValue indicating that limit is reached when scanning */
	private final KeyValue KV_LIMIT = new KeyValue();
	private byte[] stopRow;
	private Filter filter;
	private int batch;
	private int isScan;
	private boolean filterClosed = false;
	/** �������ʱ���ģ��������ˣ������� */
	private long readPt;
	private long maxResultSize;

	public HRegionInfo getRegionInfo() {
		return null;
	}

	CustomRegionScanner(Scan scan, Configuration conf, Collection<StoreFileInfo> files) throws IOException {

		this.maxResultSize = scan.getMaxResultSize();
		if (scan.hasFilter()) {
			this.filter = new FilterWrapper(scan.getFilter());
		} else {
			this.filter = null;
		}

		this.batch = scan.getBatch();
		if (Bytes.equals(scan.getStopRow(), HConstants.EMPTY_END_ROW)
				&& !scan.isGetScan()) {
			this.stopRow = null;
		} else {
			this.stopRow = scan.getStopRow();
		}
		// If we are doing a get, we want to be [startRow,endRow] normally
		// it is [startRow,endRow) and if startRow=endRow we get nothing.
		this.isScan = scan.isGetScan() ? -1 : 0;

		// synchronize on scannerReadPoints so that nobody calculates
		// getSmallestReadPoint, before scannerReadPoints is updated.
		//IsolationLevel isolationLevel = scan.getIsolationLevel();

		this.readPt = Long.MAX_VALUE;
		//MultiVersionConsistencyControl.setThreadReadPoint(this.readPt);

		// Here we separate all scanners into two lists - scanner that provide
		// data required
		// by the filter to operate (scanners list) and all others
		// (joinedScanners list).
		List<KeyValueScanner> scanners = new ArrayList<KeyValueScanner>();
		ScanInfo scanInfo = new ScanInfo(scan.getFamilies()[0], 0, Integer.MAX_VALUE,
			      Long.MAX_VALUE, false, 0, KeyValue.COMPARATOR);
		/*scanners.add(new CustomStoreScanner(null, scanInfo, scan, scan.getFamilyMap().get(scan.getFamilies()[0]),
				conf, files));*/
		scanners.add(new CustomStoreScanner(null, scanInfo, scan, null,
				conf, files));
		this.storeHeap = new KeyValueHeap(scanners, KeyValue.COMPARATOR);

	}
	
	public void setStopRow(byte[] stopRow) {
		this.stopRow = stopRow;
	}

	public long getMaxResultSize() {
		return maxResultSize;
	}

	public long getMvccReadPoint() {
		return this.readPt;
	}

	/**
	 * Reset both the filter and the old filter. ����filter
	 * 
	 * @throws IOException
	 *             in case a filter raises an I/O exception.
	 */
	protected void resetFilters() throws IOException {
		if (filter != null) {
			filter.reset();
		}
	}

	public boolean next(List<Cell> outResults) throws IOException {
		// apply the batching limit by default
		return next(outResults, batch);
	}

	public synchronized boolean next(List<Cell> outResults, int limit)
			throws IOException {
		if (this.filterClosed) {
			throw new UnknownScannerException(
					"Scanner was closed (timed out?) "
							+ "after we renewed it. Could be caused by a very slow scanner "
							+ "or a lengthy garbage collection");
		}

		// This could be a new thread from the last time we called next().
		//MultiVersionConsistencyControl.setThreadReadPoint(this.readPt);
		return nextRaw(outResults, limit);

	}

	public boolean nextRaw(List<Cell> outResults) throws IOException {
		return nextRaw(outResults, batch);
	}

	/** �������Ľ��浽outResults���� */
	public boolean nextRaw(List<Cell> outResults, int limit) throws IOException {
		boolean returnResult;
		if (outResults.isEmpty()) {
			// Usually outResults is empty. This is true when next is called
			// to handle scan or get operation. �ѽ��浽outResults����
			returnResult = nextInternal(outResults, limit);
		} else {
			List<Cell> tmpList = new ArrayList<Cell>();
			returnResult = nextInternal(tmpList, limit);
			outResults.addAll(tmpList);
		}
		resetFilters();
		if (isFilterDone()) {
			return false;
		}

		return returnResult;
	}


	/**
	 * Fetches records with currentRow into results list, until next row or
	 * limit (if not -1). ��nextRow���ƵĹ��ܣ���΢��ͬ����nextRow�ѽ��浽��MOCKED_LIST
	 * ��ѽ�����results���У����ҷ�����һ��kv
	 * 
	 * @param results
	 * @param heap
	 *            KeyValueHeap to fetch data from.It must be positioned on
	 *            correct row before call.
	 * @param limit
	 *            Max amount of KVs to place in result list, -1 means no limit.
	 * @param currentRow
	 *            Byte array with key we are fetching.
	 * @param offset
	 *            offset for currentRow
	 * @param length
	 *            length for currentRow
	 * @return KV_LIMIT if limit reached, next KeyValue otherwise.
	 */
	private KeyValue populateResult(List<Cell> results, KeyValueHeap heap,
			int limit, byte[] currentRow, int offset, short length)
			throws IOException {
		KeyValue nextKv;
		do {
			// ��heap����ȡ��ʣ�µĽ�����results����
			heap.next(results, limit - results.size());
			// ������ˣ��ͷ�����
			if (limit > 0 && results.size() == limit) {
				return KV_LIMIT;
			}
			nextKv = heap.peek();
		} while (nextKv != null
				&& nextKv.matchingRow(currentRow, offset, length));

		return nextKv;
	}

	/**
	 * ���û�й������������Ѿ����˹�ȫ����
	 * 
	 * @return True if a filter rules the scanner is over, done.
	 */
	public synchronized boolean isFilterDone() throws IOException {
		return this.filter != null && this.filter.filterAllRemaining();
	}

	/** �Ѳ�ѯ�����Ľ��浽results���� */
	private boolean nextInternal(List<Cell> results, int limit)
			throws IOException {
		if (!results.isEmpty()) {
			throw new IllegalArgumentException(
					"First parameter should be an empty list");
		}

		// The loop here is used only when at some point during the next we
		// determine
		// that due to effects of filters or otherwise, we have an empty row in
		// the result.
		// Then we loop and try again. Otherwise, we must get out on the first
		// iteration via return,
		// "true" if there's more data to read, "false" if there isn't
		// (storeHeap is at a stop row,
		// and joinedHeap has no more data to read for the last row (if set,
		// joinedContinuationRow).
		while (true) {
			// storeHeap�����˶��KeyValueScanner
			// Let's see what we have in the storeHeap. ��storeHeap����ȡ��һ����
			KeyValue current = this.storeHeap.peek();

			byte[] currentRow = null;
			int offset = 0;
			short length = 0;
			if (current != null) {
				currentRow = current.getBuffer();
				offset = current.getRowOffset();
				length = current.getRowLength();
			}
			// ���һ�µ����row�Ƿ�Ӧ��ֹͣ��
			boolean stopRow = isStopRow(currentRow, offset, length);
			// Check if we were getting data from the joinedHeap and hit the
			// limit.
			// If not, then it's main path - getting results from storeHeap.
			if (joinedContinuationRow == null) {
				// First, check if we are at a stop row. If so, there are no
				// more results.
				if (stopRow) {
					if (filter != null && filter.hasFilterRow()) {
						// ʹ��filter���˵�һЩcells
						filter.filterRowCells(results);
					}
					return false;
				}

				// Check if rowkey filter wants to exclude this row. If so, loop
				// to next.
				// Technically, if we hit limits before on this row, we don't
				// need this call.
				// �����filter�Ļ���ûͨ��filter�Ŀ��飬��������row���е����ݣ��л�����һ��Scanner
				if (filterRowKey(currentRow, offset, length)) {
					boolean moreRows = nextRow(currentRow, offset, length);
					if (!moreRows)
						return false;
					results.clear();
					continue;
				}
				// �ѽ��浽results����
				KeyValue nextKv = populateResult(results, this.storeHeap,
						limit, currentRow, offset, length);
				// Ok, we are good, let's try to get some results from the main
				// heap.
				// ��populateResult�ҵ����㹻limit������
				if (nextKv == KV_LIMIT) {
					if (this.filter != null && filter.hasFilterRow()) {
						throw new IncompatibleFilterException(
								"Filter whose hasFilterRow() returns true is incompatible with scan with limit!");
					}
					return true; // We hit the limit.
				}

				stopRow = nextKv == null
						|| isStopRow(nextKv.getBuffer(), nextKv.getRowOffset(),
								nextKv.getRowLength());
				// save that the row was empty before filters applied to it.
				final boolean isEmptyRow = results.isEmpty();

				// We have the part of the row necessary for filtering (all of
				// it, usually).
				// First filter with the filterRow(List). ����һ�¸ղ��ҳ�����
				if (filter != null && filter.hasFilterRow()) {
					filter.filterRowCells(results);
				}
				// ���result�Ŀյģ�ɶҲû�ҵ������ǡ��������簡
				if (isEmptyRow) {
					boolean moreRows = nextRow(currentRow, offset, length);
					if (!moreRows)
						return false;
					results.clear();
					// This row was totally filtered out, if this is NOT the
					// last row,
					// we should continue on. Otherwise, nothing else to do.
					if (!stopRow)
						continue;
					return false;
				}


			} 

			// We may have just called populateFromJoinedMap and hit the limits.
			// If that is
			// the case, we need to call it again on the next next() invocation.
			if (joinedContinuationRow != null) {
				return true;
			}

			// Finally, we are done with both joinedHeap and storeHeap.
			// Double check to prevent empty rows from appearing in result. It
			// could be
			// the case when SingleColumnValueExcludeFilter is used.
			if (results.isEmpty()) {
				boolean moreRows = nextRow(currentRow, offset, length);
				if (!moreRows)
					return false;
				if (!stopRow)
					continue;
			}

			// We are done. Return the result.
			return !stopRow;
		}
	}

	/** ���filter��Ϊ�յĻ�������ͨ����filter�Ĺ��ˣ�����true */
	private boolean filterRowKey(byte[] row, int offset, short length)
			throws IOException {
		return filter != null && filter.filterRowKey(row, offset, length);
	}

	/** �ҳ����е�rowkey����currentRow��cells������� */
	protected boolean nextRow(byte[] currentRow, int offset, short length)
			throws IOException {
		assert this.joinedContinuationRow == null : "Trying to go to next row during joinedHeap read.";
		KeyValue next;
		while ((next = this.storeHeap.peek()) != null
				&& next.matchingRow(currentRow, offset, length)) {
			// rowkey��ͬ�Ļ����Ͱ�storeHeap�е��ܲ�ó���������
			this.storeHeap.next(MOCKED_LIST);
		}
		resetFilters();

		return true;
	}

	/** ������һ���Ƿ�Ӧ��ֹͣ���� */
	private boolean isStopRow(byte[] currentRow, int offset, short length) {
		return currentRow == null
				|| (stopRow != null && comparator.compareRows(stopRow, 0,
						stopRow.length, currentRow, offset, length) <= isScan);
	}

	public synchronized void close() {
		if (storeHeap != null) {
			storeHeap.close();
			storeHeap = null;
		}

		// no need to sychronize here.
		this.filterClosed = true;
	}

	KeyValueHeap getStoreHeapForTesting() {
		return storeHeap;
	}

	public synchronized boolean reseek(byte[] row) throws IOException {
		if (row == null) {
			throw new IllegalArgumentException("Row cannot be null.");
		}
		boolean result = false;
		try {
			// This could be a new thread from the last time we called next().
			//MultiVersionConsistencyControl.setThreadReadPoint(this.readPt);
			KeyValue kv = KeyValue.createFirstOnRow(row);
			// use request seek to make use of the lazy seek option. See
			// HBASE-5520
			result = this.storeHeap.requestSeek(kv, true, true);
		} finally {
		}
		return result;
	}
}
