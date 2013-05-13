/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TxnPrepare.java,v 1.1.6.1 2006/07/28 09:01:36 ckaestne Exp $
 */

package com.sleepycat.je.txn;

import java.nio.ByteBuffer;

import javax.transaction.xa.Xid;

import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogReadable;
import com.sleepycat.je.log.LogUtils;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.utilint.DbLsn;

/**
 * This class writes out a transaction prepare record.
 */
public class TxnPrepare extends TxnEnd {

	private Xid xid;

	public TxnPrepare(long id, Xid xid) {
		/* LastLSN is never used. */
		super(id, DbLsn.NULL_LSN);
		this.xid = xid;
	}

	/**
	 * For constructing from the log.
	 */
	public TxnPrepare() {
	}

	public Xid getXid() {
		return xid;
	}

	/*
	 * Log support
	 */

	/**
	 * @see TxnEnd#getLogType
	 */
	public LogEntryType getLogType() {
		return LogEntryType.LOG_TXN_PREPARE;
	}

	protected String getTagName() {
		return "TxnPrepare";
	}

	/**
	 * @see LoggableObject#getLogSize
	 */
	public int getLogSize() {
		return LogUtils.LONG_BYTES + // id
				LogUtils.getTimestampLogSize() + // timestamp
				LogUtils.getXidSize(xid); // Xid
	}

	/**
	 * @see LoggableObject#writeToLog
	 */
	public void writeToLog(ByteBuffer logBuffer) {
		LogUtils.writeLong(logBuffer, id);
		LogUtils.writeTimestamp(logBuffer, time);
		LogUtils.writeXid(logBuffer, xid);
	}

	/**
	 * @see LogReadable#readFromLog
	 */
	public void readFromLog(ByteBuffer logBuffer, byte entryTypeVersion) {
		id = LogUtils.readLong(logBuffer);
		time = LogUtils.readTimestamp(logBuffer);
		xid = LogUtils.readXid(logBuffer);
	}

	/**
	 * @see LogReadable#dumpLog
	 */
	public void dumpLog(StringBuffer sb, boolean verbose) {
		sb.append("<").append(getTagName());
		sb.append(" id=\"").append(id);
		sb.append("\" xid=\"").append(xid);
		sb.append("\" time=\"").append(time);
		sb.append("\">");
		sb.append("</").append(getTagName()).append(">");
	}
}
