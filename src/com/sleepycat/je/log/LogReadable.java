/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: LogReadable.java,v 1.1.6.1 2006/07/28 09:01:54 ckaestne Exp $
 */

package com.sleepycat.je.log;

import java.nio.ByteBuffer;

/**
 * A class that implements LogReadable knows how to read itself from the JE log.
 */
public interface LogReadable {

	/**
	 * Initialize this object from the data in itemBuf.
	 * 
	 * @param itemBuf
	 *            the source buffer
	 */
	public void readFromLog(ByteBuffer itemBuffer, byte entryTypeVersion)
			throws LogException;

	/**
	 * Write the object into the string buffer for log dumping. Each object
	 * should be dumped without indentation or new lines and should be valid
	 * XML.
	 * 
	 * @param sb
	 *            destination string buffer
	 * @param verbose
	 *            if true, dump the full, verbose version
	 */
	public void dumpLog(StringBuffer sb, boolean verbose);

	/**
	 * @return true if the LogEntry is a transactional log entry type.
	 */
	public boolean logEntryIsTransactional();

	/**
	 * @return return the transaction id if this log entry is transactional, 0
	 *         otherwise.
	 */
	public long getTransactionId();
}
