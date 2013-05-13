/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: LogFileNotFoundException.java,v 1.1.6.1 2006/07/28 09:01:46 ckaestne Exp $
 */

package com.sleepycat.je.log;

/**
 * Log file doesn't exist.
 */
public class LogFileNotFoundException extends LogException {

	public LogFileNotFoundException(String message) {
		super(message);
	}
}
