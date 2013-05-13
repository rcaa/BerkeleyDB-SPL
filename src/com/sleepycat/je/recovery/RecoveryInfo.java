/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: RecoveryInfo.java,v 1.1.6.1.2.1 2006/10/16 20:30:09 ckaestne Exp $
 */

package com.sleepycat.je.recovery;

import com.sleepycat.je.utilint.DbLsn;

/**
 * RecoveryInfo keeps information about recovery processing.
 */
public class RecoveryInfo {

	/* Locations found during recovery. */
	public long lastUsedLsn = DbLsn.NULL_LSN; // location of last entry

	public long nextAvailableLsn = DbLsn.NULL_LSN; // EOF, location of first
													// unused spot

	public long firstActiveLsn = DbLsn.NULL_LSN;

	public long checkpointStartLsn = DbLsn.NULL_LSN;

	public long checkpointEndLsn = DbLsn.NULL_LSN;

	public long useRootLsn = DbLsn.NULL_LSN;

	/*
	 * Represents the first CkptStart following the CkptEnd. It is a CkptStart
	 * with no CkptEnd, and is used for counting provisional INs obsolete.
	 */
	public long partialCheckpointStartLsn = DbLsn.NULL_LSN;

	// Checkpoint record used for this recovery.
	public CheckpointEnd checkpointEnd;

	// Ids
	public long useMaxNodeId;

	public int useMaxDbId;

	public long useMaxTxnId;

	// num nodes read
	public int numMapINs;

	public int numOtherINs;

	public int numBinDeltas;

	public int numDuplicateINs;

	// ln processing
	public int lnFound;

	public int lnNotFound;

	public int lnInserted;

	public int lnReplaced;


	public String toString() {
		StringBuffer sb = new StringBuffer();
		hook_fillToStringBuffer(sb);
		return sb.toString();
	}
	
	

	private void hook_fillToStringBuffer(StringBuffer sb) {
		sb.append("Recovery Info");
		appendLsn(sb, " lastUsed=", lastUsedLsn);
		appendLsn(sb, " nextAvail=", nextAvailableLsn);
		appendLsn(sb, " ckptStart=", checkpointStartLsn);
		appendLsn(sb, " firstActive=", firstActiveLsn);
		appendLsn(sb, " ckptEnd=", checkpointEndLsn);
		appendLsn(sb, " useRoot=", useRootLsn);
		sb.append(" ckptEnd=<").append(checkpointEnd).append(">");
		sb.append(" useMaxNodeId=").append(useMaxNodeId);
		sb.append(" useMaxDbId=").append(useMaxDbId);
		sb.append(" useMaxTxnId=").append(useMaxTxnId);
		sb.append(" numMapINs=").append(numMapINs);
		sb.append(" numOtherINs=").append(numOtherINs);
		sb.append(" numBinDeltas=").append(numBinDeltas);
		sb.append(" numDuplicateINs=").append(numDuplicateINs);
		sb.append(" lnFound=").append(lnFound);
		sb.append(" lnNotFound=").append(lnNotFound);
		sb.append(" lnInserted=").append(lnInserted);
		sb.append(" lnReplaced=").append(lnReplaced);
	}



	private void appendLsn(StringBuffer sb, String name, long lsn) {
		if (lsn != DbLsn.NULL_LSN) {
			sb.append(name).append(DbLsn.getNoFormatString(lsn));
		}
	}
}
