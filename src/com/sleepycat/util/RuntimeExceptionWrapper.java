/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2000-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: RuntimeExceptionWrapper.java,v 1.1.6.1 2006/07/28 09:02:37 ckaestne Exp $
 */

package com.sleepycat.util;

/**
 * A RuntimeException that can contain nested exceptions.
 * 
 * @author Mark Hayes
 */
public class RuntimeExceptionWrapper extends RuntimeException implements
		ExceptionWrapper {

	private Throwable e;

	public RuntimeExceptionWrapper(Throwable e) {

		super(e.getMessage());
		this.e = e;
	}

	/**
	 * @deprecated replaced by {@link #getCause}.
	 */
	public Throwable getDetail() {

		return e;
	}

	public Throwable getCause() {

		return e;
	}
}
