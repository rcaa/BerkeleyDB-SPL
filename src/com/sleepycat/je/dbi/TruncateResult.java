/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TruncateResult.java,v 1.1.6.1 2006/07/28 09:02:45 ckaestne Exp $
 */

package com.sleepycat.je.dbi;

/**
 * Holds the result of a database truncate operation.
 */
public class TruncateResult {

	private DatabaseImpl db;

	private int count;

	TruncateResult(DatabaseImpl db, int count) {
		this.db = db;
		this.count = count;
	}

	public DatabaseImpl getDatabase() {
		return db;
	}

	public int getRecordCount() {
		return count;
	}
}
