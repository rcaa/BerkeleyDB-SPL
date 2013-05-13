/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: LoggableObject.java,v 1.1.6.1 2006/07/28 09:01:45 ckaestne Exp $
 */

package com.sleepycat.je.log;

import com.sleepycat.je.DatabaseException;

/**
 * A class that implements LoggableObject can be stored as a JE log entry.
 */
public interface LoggableObject extends LogWritable {

	/**
	 * All objects that are reponsible for a generating a type of log entry must
	 * implement this.
	 * 
	 * @return the type of log entry
	 */
	public LogEntryType getLogType();

	/**
	 * Do any processing we need to do after logging, while under the logging
	 * latch.
	 */
	public void postLogWork(long justLoggedLsn) throws DatabaseException;

	/**
	 * Return true if this item can be marshalled outside the log write latch.
	 */
	public boolean marshallOutsideWriteLatch();

	/**
	 * Returns true if this item should be counted as obsoleted when logged.
	 * This currently applies to deleted LNs only.
	 */
	public boolean countAsObsoleteWhenLogged();
}
