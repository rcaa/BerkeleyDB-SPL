/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: FileHandleSource.java,v 1.1.6.1.2.1 2006/08/25 08:37:44 ckaestne Exp $
 */

package com.sleepycat.je.log;

import com.sleepycat.je.DatabaseException;

/**
 * FileHandleSource is a file source built on top of a cached file handle.
 */
class FileHandleSource extends FileSource {

	private FileHandle fileHandle;

	FileHandleSource(FileHandle fileHandle, int readBufferSize,
			FileManager fileManager) {
		super(fileHandle.getFile(), readBufferSize, fileManager);
		this.fileHandle = fileHandle;
	}

	/**
	 * @see LogSource#release
	 */
	public void release() throws DatabaseException {
//		fileHandle.release();
	}
}
