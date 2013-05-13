/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: ForeignKeyNullifier.java,v 1.1.6.1 2006/07/28 09:02:22 ckaestne Exp $
 */

package com.sleepycat.je;

/**
 * Javadoc for this public method is generated via the doc templates in the
 * doc_src directory.
 */
public interface ForeignKeyNullifier {

	/**
	 * Javadoc for this public method is generated via the doc templates in the
	 * doc_src directory.
	 */
	public boolean nullifyForeignKey(SecondaryDatabase secondary,
			DatabaseEntry data) throws DatabaseException;
}
