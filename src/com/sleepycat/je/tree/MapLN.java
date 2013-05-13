/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: MapLN.java,v 1.1.6.1.2.1 2006/08/25 08:37:17 ckaestne Exp $
 */

package com.sleepycat.je.tree;

import java.nio.ByteBuffer;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogException;
import com.sleepycat.je.log.LogUtils;

/**
 * A MapLN represents a Leaf Node in the JE DatabaseImpl Naming Tree.
 */
public final class MapLN extends LN {

	private static final String BEGIN_TAG = "<mapLN>";

	private static final String END_TAG = "</mapLN>";

	private DatabaseImpl databaseImpl;

	private boolean deleted;

	/**
	 * Create a new MapLn to hold a new databaseImpl. In the ideal world, we'd
	 * have a base LN class so that this MapLN doesn't have a superfluous data
	 * field, but we want to optimize the LN class for size and speed right now.
	 */
	public MapLN(DatabaseImpl db) {
		super(new byte[0]);
		databaseImpl = db;
		deleted = false;
	}

	/**
	 * Create an empty MapLN, to be filled in from the log.
	 */
	public MapLN() throws DatabaseException {

		super();
		databaseImpl = new DatabaseImpl();
	}

	public boolean isDeleted() {
		return deleted;
	}

	void makeDeleted() {
		deleted = true;

		/* Release all references to nodes held by this database. */
		databaseImpl.getTree().setRoot(null);
	}

	public DatabaseImpl getDatabase() {
		return databaseImpl;
	}

	/**
	 * Initialize a node that has been faulted in from the log.
	 */
	public void postFetchInit(DatabaseImpl db, long sourceLsn)
			throws DatabaseException {

		databaseImpl.setEnvironmentImpl(db.getDbEnvironment());
	}

	/*
	 * Dumping
	 */

	public String toString() {
		return dumpString(0, true);
	}

	public String beginTag() {
		return BEGIN_TAG;
	}

	public String endTag() {
		return END_TAG;
	}

	public String dumpString(int nSpaces, boolean dumpTags) {
		StringBuffer sb = new StringBuffer();
		sb.append(super.dumpString(nSpaces, dumpTags));
		sb.append('\n');
		sb.append(TreeUtils.indent(nSpaces));
		sb.append("<deleted val=\"").append(Boolean.toString(deleted));
		sb.append("\">");
		sb.append('\n');
		sb.append(databaseImpl.dumpString(nSpaces));
		return sb.toString();
	}

	/*
	 * Logging
	 */

	/**
	 * Log type for transactional entries.
	 */
	protected LogEntryType getTransactionalLogType() {
		return LogEntryType.LOG_MAPLN_TRANSACTIONAL;
	}

	/**
	 * @see LN#getLogType
	 */
	public LogEntryType getLogType() {
		return LogEntryType.LOG_MAPLN;
	}

	/**
	 * @see LN#getLogSize
	 */
	public int getLogSize() {
		return super.getLogSize() + databaseImpl.getLogSize()
				+ LogUtils.getBooleanLogSize();
	}

	/**
	 * @see LN#writeToLog
	 */
	public void writeToLog(ByteBuffer logBuffer) {
		/* Ask ancestors to write to log. */
		super.writeToLog(logBuffer);
		databaseImpl.writeToLog(logBuffer);
		LogUtils.writeBoolean(logBuffer, deleted);
	}

	/**
	 * @see LN#readFromLog
	 */
	public void readFromLog(ByteBuffer itemBuffer, byte entryTypeVersion)
			throws LogException {

		super.readFromLog(itemBuffer, entryTypeVersion);
		databaseImpl.readFromLog(itemBuffer, entryTypeVersion);
		deleted = LogUtils.readBoolean(itemBuffer);
	}

	/**
	 * Dump additional fields. Done this way so the additional info can be
	 * within the XML tags defining the dumped log entry.
	 */
	protected void dumpLogAdditional(StringBuffer sb, boolean verbose) {
		databaseImpl.dumpLog(sb, true);
	}
}
