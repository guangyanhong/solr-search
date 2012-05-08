package com.taobao.terminator.core.realtime;

import java.util.Arrays;

/**
 * UID --> DocIdµÄÓ³Éä
 * 
 * @author yusen
 *
 */
public class DocIdMapper {
	private final int[] docs;
	private final long[] uids;
	private final int[] starts;
	private final long[] filter;

	private final int _mask;
	private final int MIXER = 2147482951;
	
	public DocIdMapper(long[] uidArray) {
		int len = uidArray.length;

		int mask = len / 4;
		mask |= (mask >> 1);
		mask |= (mask >> 2);
		mask |= (mask >> 4);
		mask |= (mask >> 8);
		mask |= (mask >> 16);
		_mask = mask;

		filter = new long[mask + 1];

		for (long uid : uidArray) {
			if (uid != Integer.MIN_VALUE) {
				int h = (int) ((uid >>> 32) ^ uid) * MIXER;

				long bits = filter[h & _mask];
				bits |= ((1L << (h >>> 26)));
				bits |= ((1L << ((h >> 20) & 0x3F)));
				filter[h & _mask] = bits;
			}
		}

		starts = new int[_mask + 1 + 1];
		len = 0;
		for (long uid : uidArray) {
			if (uid != Integer.MIN_VALUE) {
				starts[((int) ((uid >>> 32) ^ uid) * MIXER) & _mask]++;
				len++;
			}
		}
		int val = 0;
		for (int i = 0; i < starts.length; i++) {
			val += starts[i];
			starts[i] = val;
		}
		starts[_mask] = len;

		long[] partitionedUidArray = new long[len];
		int[] docArray = new int[len];

		for (long uid : uidArray) {
			if (uid != Integer.MIN_VALUE) {
				int i = --(starts[((int) ((uid >>> 32) ^ uid) * MIXER) & _mask]);
				partitionedUidArray[i] = uid;
			}
		}

		int s = starts[0];
		for (int i = 1; i < starts.length; i++) {
			int e = starts[i];
			if (s < e) {
				Arrays.sort(partitionedUidArray, s, e);
			}
			s = e;
		}

		for (int docid = 0; docid < uidArray.length; docid++) {
			long uid = uidArray[docid];
			if (uid != Integer.MIN_VALUE) {
				final int p = ((int) ((uid >>> 32) ^ uid) * MIXER) & _mask;
				int idx = findIndex(partitionedUidArray, uid, starts[p], starts[p + 1]);
				if (idx >= 0) {
					docArray[idx] = docid;
				}
			}
		}

		uids = partitionedUidArray;
		docs = docArray;
	}

	public int getDocID(final long uid) {
		final int h = (int) ((uid >>> 32) ^ uid) * MIXER;
		final int p = h & _mask;

		// check the filter
		final long bits = filter[p];
		if ((bits & (1L << (h >>> 26))) == 0 || (bits & (1L << ((h >> 20) & 0x3F))) == 0)
			return -1;

		// do binary search in the partition
		int begin = starts[p];
		int end = starts[p + 1] - 1;
		// we have some uids in this partition, so we assume (begin <= end)
		while (true) {
			int mid = (begin + end) >>> 1;
			long midval = uids[mid];

			if (midval == uid)
				return docs[mid];
			if (mid == end)
				return -1;

			if (midval < uid)
				begin = mid + 1;
			else
				end = mid;
		}
	}

	private static final int findIndex(final long[] arr, final long uid, int begin, int end) {
		if (begin >= end)
			return -1;
		end--;

		while (true) {
			int mid = (begin + end) >>> 1;
			long midval = arr[mid];
			if (midval == uid)
				return mid;
			if (mid == end)
				return -1;

			if (midval < uid)
				begin = mid + 1;
			else
				end = mid;
		}
	}

	public int quickGetDocID(long uid) {
		// getDocID()
		final int h = (int) ((uid >>> 32) ^ uid) * MIXER;
		final int p = h & _mask;

		// check the filter
		final long bits = filter[p];
		if ((bits & (1L << (h >>> 26))) == 0 || (bits & (1L << ((h >> 20) & 0x3F))) == 0)
			return -1;

		// do binary search in the partition
		int begin = starts[p];
		int end = starts[p + 1] - 1;
		// we have some uids in this partition, so we assume (begin <= end)
		while (true) {
			int mid = (begin + end) >>> 1;
			long midval = uids[mid];

			if (midval == uid)
				return docs[mid];
			if (mid == end)
				return -1;

			if (midval < uid)
				begin = mid + 1;
			else
				end = mid;
		}
	}
}
