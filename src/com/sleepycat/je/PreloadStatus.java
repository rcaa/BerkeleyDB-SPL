/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: PreloadStatus.java,v 1.1.6.1 2006/07/28 09:02:18 ckaestne Exp $
 */

package com.sleepycat.je;

import java.io.Serializable;

/**
 * Javadoc for this public class is generated via the doc templates in the
 * doc_src directory.
 */
public class PreloadStatus implements Serializable {
	/* For toString */
	private String statusName;

	private PreloadStatus(String statusName) {
		this.statusName = statusName;
	}

	public String toString() {
		return "PreloadStatus." + statusName;
	}

	/* preload() was successful. */
	public static final PreloadStatus SUCCESS = new PreloadStatus("SUCCESS");

	/* preload() filled maxBytes of the cache. */
	public static final PreloadStatus FILLED_CACHE = new PreloadStatus(
			"FILLED_CACHE");

	/* preload() took more than maxMillisecs. */
	public static final PreloadStatus EXCEEDED_TIME = new PreloadStatus(
			"EXCEEDED_TIME");
}
