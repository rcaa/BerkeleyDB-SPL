/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: BIN.java,v 1.1.6.1.2.6 2006/10/26 16:36:47 ckaestne Exp $
 */

package com.sleepycat.je.tree;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.cleaner.Cleaner;
import com.sleepycat.je.dbi.CursorImpl;
import com.sleepycat.je.dbi.DatabaseImpl;
import com.sleepycat.je.log.LogEntryType;
import com.sleepycat.je.log.LogManager;
import com.sleepycat.je.log.LoggableObject;
import com.sleepycat.je.txn.BasicLocker;
import com.sleepycat.je.txn.LockGrantType;
import com.sleepycat.je.txn.LockResult;
import com.sleepycat.je.txn.LockType;
import com.sleepycat.je.utilint.DbLsn;
import com.sleepycat.je.utilint.TinyHashSet;

/**
 * A BIN represents a Bottom Internal Node in the JE tree.
 */
public class BIN extends IN implements LoggableObject {

	private static final String BEGIN_TAG = "<bin>";

	private static final String END_TAG = "</bin>";

	/*
	 * The set of cursors that are currently referring to this BIN.
	 */
	private TinyHashSet cursorSet;

	/*
	 * Support for logging BIN deltas. (Partial BIN logging)
	 */

	/* Location of last delta, for cleaning. */
	private long lastDeltaVersion = DbLsn.NULL_LSN;

	private int numDeltasSinceLastFull; // num deltas logged

	private boolean prohibitNextDelta; // disallow delta on next log

//	public BIN() {
//		cursorSet = new TinyHashSet();
//		numDeltasSinceLastFull = 0;
//		prohibitNextDelta = false;
//	}

	public BIN(DatabaseImpl db, byte[] identifierKey, int maxEntriesPerNode,
			int level) {
		super(db, identifierKey, maxEntriesPerNode, level);

		cursorSet = new TinyHashSet();
		numDeltasSinceLastFull = 0;
		prohibitNextDelta = false;
	}

	/**
	 * Create a holder object that encapsulates information about this BIN for
	 * the INCompressor.
	 */
	public BINReference createReference() {
		return new BINReference(getNodeId(), getDatabase().getId(),
				getIdentifierKey());
	}

	/**
	 * Create a new BIN. Need this because we can't call newInstance() without
	 * getting a 0 for nodeid.
	 */
	protected IN createNewInstance(byte[] identifierKey, int maxEntries,
			int level) {
		return new BIN(getDatabase(), identifierKey, maxEntries, level);
	}

	/**
	 * Get the key (dupe or identifier) in child that is used to locate it in
	 * 'this' node. For BIN's, the child node has to be a DIN so we use the Dup
	 * Key to cross the main-tree/dupe-tree boundary.
	 */
	public byte[] getChildKey(IN child) throws DatabaseException {

		return child.getDupKey();
	}

	/**
	 * @return the log entry type to use for bin delta log entries.
	 */
	LogEntryType getBINDeltaType() {
		return LogEntryType.LOG_BIN_DELTA;
	}

	/**
	 * @return location of last logged delta version. If never set, return null.
	 */
	public long getLastDeltaVersion() {
		return lastDeltaVersion;
	}

	/**
	 * If cleaned or compressed, must log full version.
	 * 
	 * @Override
	 */
	public void setProhibitNextDelta() {
		prohibitNextDelta = true;
	}

	/*
	 * If this search can go further, return the child. If it can't, and you are
	 * a possible new parent to this child, return this IN. If the search can't
	 * go further and this IN can't be a parent to this child, return null.
	 */
	protected void descendOnParentSearch(SearchResult result,
			boolean targetContainsDuplicates, boolean targetIsRoot,
			long targetNodeId, Node child, boolean requireExactMatch)
			throws DatabaseException {

		if (child.canBeAncestor(targetContainsDuplicates)) {
			if (targetContainsDuplicates && targetIsRoot) {

				/*
				 * Don't go further -- the target is a root of a dup tree, so
				 * this BIN will have to be the parent.
				 */
				long childNid = child.getNodeId();
				((IN) child).hook_releaseLatch();

				result.keepSearching = false; // stop searching

				if (childNid == targetNodeId) { // set if exact find
					result.exactParentFound = true;
				} else {
					result.exactParentFound = false;
				}

				/*
				 * Return a reference to this node unless we need an exact match
				 * and this isn't exact.
				 */
				if (requireExactMatch && !result.exactParentFound) {
					result.parent = null;
					hook_releaseLatch();
				} else {
					result.parent = this;
				}

			} else {
				/*
				 * Go further down into the dup tree.
				 */
				hook_releaseLatch();
				result.parent = (IN) child;
			}
		} else {
			/*
			 * Our search ends, we didn't find it. If we need an exact match,
			 * give up, if we only need a potential match, keep this node
			 * latched and return it.
			 */
			result.exactParentFound = false;
			result.keepSearching = false;
			if (!requireExactMatch && targetContainsDuplicates) {
				result.parent = this;
			} else {
				hook_releaseLatch();
				result.parent = null;
			}
		}
	}

	/*
	 * A BIN can be the ancestor of an internal node of the duplicate tree. It
	 * can't be the parent of an IN or another BIN.
	 */
	protected boolean canBeAncestor(boolean targetContainsDuplicates) {
		/* True if the target is a DIN or DBIN */
		return targetContainsDuplicates;
	}

	/**
	 * @Override
	 */
	boolean isEvictionProhibited() {
		return (nCursors() > 0);
	}

	/**
	 * @Override
	 */
	boolean hasNonLNChildren() {

		for (int i = 0; i < getNEntries(); i++) {
			Node node = getTarget(i);
			if (node != null) {
				if (!(node instanceof LN)) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * @Override
	 */
	int getChildEvictionType() {

		Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();

		for (int i = 0; i < getNEntries(); i++) {
			Node node = getTarget(i);
			if (node != null) {
				if (node instanceof LN) {
					if (cleaner.isEvictable(this, i)) {
						return MAY_EVICT_LNS;
					}
				} else {
					return MAY_NOT_EVICT;
				}
			}
		}
		return MAY_EVICT_NODE;
	}

	/**
	 * Indicates whether entry 0's key is "special" in that it always compares
	 * less than any other key. BIN's don't have the special key, but IN's do.
	 */
	boolean entryZeroKeyComparesLow() {
		return false;
	}

	/**
	 * Mark this entry as deleted, using the delete flag. Only BINS may do this.
	 * 
	 * @param index
	 *            indicates target entry
	 */
	public void setKnownDeleted(int index) {

		/*
		 * The target is cleared to save memory, since a known deleted entry
		 * will never be fetched. The migrate flag is also cleared since
		 * migration is never needed for known deleted entries either.
		 */
		super.setKnownDeleted(index);
		setMigrate(index, false);
		super.setTarget(index, null);
		setDirty(true);
	}

	/**
	 * Mark this entry as deleted, using the delete flag. Only BINS may do this.
	 * Don't null the target field.
	 * 
	 * This is used so that an LN can still be locked by the compressor even if
	 * the entry is knownDeleted. See BIN.compress.
	 * 
	 * @param index
	 *            indicates target entry
	 */
	public void setKnownDeletedLeaveTarget(int index) {

		/*
		 * The migrate flag is cleared since migration is never needed for known
		 * deleted entries.
		 */
		setMigrate(index, false);
		super.setKnownDeleted(index);
		setDirty(true);
	}

	/**
	 * Clear the known deleted flag. Only BINS may do this.
	 * 
	 * @param index
	 *            indicates target entry
	 */
	public void clearKnownDeleted(int index) {
		super.clearKnownDeleted(index);
		setDirty(true);
	}



	/*
	 * Cursors
	 */

	/* public for the test suite. */
	public Set getCursorSet() {
		return cursorSet.copy();
	}

	/**
	 * Register a cursor with this bin. Caller has this bin already latched.
	 * 
	 * @param cursor
	 *            Cursor to register.
	 */
	public void addCursor(CursorImpl cursor) {
		cursorSet.add(cursor);
	}

	/**
	 * Unregister a cursor with this bin. Caller has this bin already latched.
	 * 
	 * @param cursor
	 *            Cursor to unregister.
	 */
	public void removeCursor(CursorImpl cursor) {
		cursorSet.remove(cursor);
	}

	/**
	 * @return the number of cursors currently referring to this BIN.
	 */
	public int nCursors() {
		return cursorSet.size();
	}

	/**
	 * The following four methods access the correct fields in a cursor
	 * depending on whether "this" is a BIN or DBIN. For BIN's, the
	 * CursorImpl.index and CursorImpl.bin fields should be used. For DBIN's,
	 * the CursorImpl.dupIndex and CursorImpl.dupBin fields should be used.
	 */
	BIN getCursorBIN(CursorImpl cursor) {
		return cursor.getBIN();
	}

	BIN getCursorBINToBeRemoved(CursorImpl cursor) {
		return cursor.getBINToBeRemoved();
	}

	int getCursorIndex(CursorImpl cursor) {
		return cursor.getIndex();
	}

	void setCursorBIN(CursorImpl cursor, BIN bin) {
		cursor.setBIN(bin);
	}

	void setCursorIndex(CursorImpl cursor, int index) {
		cursor.setIndex(index);
	}

	/**
	 * Called when we know we are about to split on behalf of a key that is the
	 * minimum (leftSide) or maximum (!leftSide) of this node. This is achieved
	 * by just forcing the split to occur either one element in from the left or
	 * the right (i.e. splitIndex is 1 or nEntries - 1).
	 */
	void splitSpecial(IN parent, int parentIndex, int maxEntriesPerNode,
			byte[] key, boolean leftSide) throws DatabaseException {

		int index = findEntry(key, true, false);
		int nEntries = getNEntries();
		boolean exact = (index & IN.EXACT_MATCH) != 0;
		index &= ~IN.EXACT_MATCH;
		if (leftSide && index < 0) {
			splitInternal(parent, parentIndex, maxEntriesPerNode, 1);
		} else if (!leftSide && !exact && index == (nEntries - 1)) {
			splitInternal(parent, parentIndex, maxEntriesPerNode, nEntries - 1);
		} else {
			split(parent, parentIndex, maxEntriesPerNode);
		}
	}

	/**
	 * Adjust any cursors that are referring to this BIN. This method is called
	 * during a split operation. "this" is the BIN being split. newSibling is
	 * the new BIN into which the entries from "this" between newSiblingLow and
	 * newSiblingHigh have been copied.
	 * 
	 * @param newSibling -
	 *            the newSibling into which "this" has been split.
	 * @param newSiblingLow,
	 *            newSiblingHigh - the low and high entry of "this" that were
	 *            moved into newSibling.
	 */
	void adjustCursors(IN newSibling, int newSiblingLow, int newSiblingHigh) {
		int adjustmentDelta = (newSiblingHigh - newSiblingLow);
		Iterator iter = cursorSet.iterator();
		while (iter.hasNext()) {
			CursorImpl cursor = (CursorImpl) iter.next();
			if (getCursorBINToBeRemoved(cursor) == this) {

				/*
				 * This BIN will be removed from the cursor by CursorImpl
				 * following advance to next BIN; ignore it.
				 */
				continue;
			}
			int cIdx = getCursorIndex(cursor);
			BIN cBin = getCursorBIN(cursor);
			assert cBin == this : "nodeId=" + getNodeId() + " cursor="
					+ cursor.dumpToString(true);
			assert newSibling instanceof BIN;

			/*
			 * There are four cases to consider for cursor adjustments,
			 * depending on (1) how the existing node gets split, and (2) where
			 * the cursor points to currently. In cases 1 and 2, the id key of
			 * the node being split is to the right of the splitindex so the new
			 * sibling gets the node entries to the left of that index. This is
			 * indicated by "new sibling" to the left of the vertical split line
			 * below. The right side of the node contains entries that will
			 * remain in the existing node (although they've been shifted to the
			 * left). The vertical bar (^) indicates where the cursor currently
			 * points.
			 * 
			 * case 1:
			 * 
			 * We need to set the cursor's "bin" reference to point at the new
			 * sibling, but we don't need to adjust its index since that
			 * continues to be correct post-split.
			 * 
			 * +=======================================+ | new sibling |
			 * existing node | +=======================================+ cursor ^
			 * 
			 * case 2:
			 * 
			 * We only need to adjust the cursor's index since it continues to
			 * point to the current BIN post-split.
			 * 
			 * +=======================================+ | new sibling |
			 * existing node | +=======================================+ cursor ^
			 * 
			 * case 3:
			 * 
			 * Do nothing. The cursor continues to point at the correct BIN and
			 * index.
			 * 
			 * +=======================================+ | existing Node | new
			 * sibling | +=======================================+ cursor ^
			 * 
			 * case 4:
			 * 
			 * Adjust the "bin" pointer to point at the new sibling BIN and also
			 * adjust the index.
			 * 
			 * +=======================================+ | existing Node | new
			 * sibling | +=======================================+ cursor ^
			 */
			BIN ns = (BIN) newSibling;
			if (newSiblingLow == 0) {
				if (cIdx < newSiblingHigh) {
					/* case 1 */
					setCursorBIN(cursor, ns);
					iter.remove();
					ns.addCursor(cursor);
				} else {
					/* case 2 */
					setCursorIndex(cursor, cIdx - adjustmentDelta);
				}
			} else {
				if (cIdx >= newSiblingLow) {
					/* case 4 */
					setCursorIndex(cursor, cIdx - newSiblingLow);
					setCursorBIN(cursor, ns);
					iter.remove();
					ns.addCursor(cursor);
				}
			}
		}
	}



	/**
	 * Adjust cursors referring to this BIN following an insert.
	 * 
	 * @param insertIndex -
	 *            The index of the new entry.
	 */
	void adjustCursorsForInsert(int insertIndex) {
		/*
		 * cursorSet may be null if this is being created through
		 * createFromLog()
		 */
		if (cursorSet != null) {
			Iterator iter = cursorSet.iterator();
			while (iter.hasNext()) {
				CursorImpl cursor = (CursorImpl) iter.next();
				if (getCursorBINToBeRemoved(cursor) != this) {
					int cIdx = getCursorIndex(cursor);
					if (insertIndex <= cIdx) {
						setCursorIndex(cursor, cIdx + 1);
					}
				}
			}
		}
	}

	/**
	 * Adjust cursors referring to the given binIndex in this BIN following a
	 * mutation of the entry from an LN to a DIN. The entry was moved from a BIN
	 * to a newly created DBIN so each cursor must be added to the new DBIN.
	 * 
	 * @param binIndex -
	 *            The index of the DIN (previously LN) entry in the BIN.
	 * 
	 * @param dupBin -
	 *            The DBIN into which the LN entry was moved.
	 * 
	 * @param dupBinIndex -
	 *            The index of the moved LN entry in the DBIN.
	 * 
	 * @param excludeCursor -
	 *            The cursor being used for insertion and that should not be
	 *            updated.
	 */
	void adjustCursorsForMutation(int binIndex, DBIN dupBin, int dupBinIndex,
			CursorImpl excludeCursor) {
		/*
		 * cursorSet may be null if this is being created through
		 * createFromLog()
		 */
		if (cursorSet != null) {
			Iterator iter = cursorSet.iterator();
			while (iter.hasNext()) {
				CursorImpl cursor = (CursorImpl) iter.next();
				if (getCursorBINToBeRemoved(cursor) != this
						&& cursor != excludeCursor
						&& cursor.getIndex() == binIndex) {
					assert cursor.getDupBIN() == null;
					cursor.addCursor(dupBin);
					cursor.updateDBin(dupBin, dupBinIndex);
				}
			}
		}
	}

	/**
	 * Compress this BIN by removing any entries that are deleted. Deleted
	 * entries are those that have LN's marked deleted or if the knownDeleted
	 * flag is set. Caller is responsible for latching and unlatching this node.
	 * 
	 * @param binRef
	 *            is used to determine the set of keys to be checked for
	 *            deletedness, or is null to check all keys.
	 * @param canFetch
	 *            if false, don't fetch any non-resident children. We don't want
	 *            some callers of compress, such as the evictor, to fault in
	 *            other nodes.
	 * 
	 * @return true if we had to requeue the entry because we were unable to get
	 *         locks, false if all entries were processed and therefore any
	 *         remaining deleted keys in the BINReference must now be in some
	 *         other BIN because of a split.
	 */
	public boolean compress(BINReference binRef, boolean canFetch)
			throws DatabaseException {

		boolean ret = false;
		boolean setNewIdKey = false;
		boolean anyLocksDenied = false;
		DatabaseImpl db = getDatabase();
		BasicLocker lockingTxn = new BasicLocker(db.getDbEnvironment());

		try {
			for (int i = 0; i < getNEntries(); i++) {

				/*
				 * We have to be able to lock the LN before we can compress the
				 * entry. If we can't, then, skip over it.
				 * 
				 * We must lock the LN even if isKnownDeleted is true, because
				 * locks protect the aborts. (Aborts may execute multiple
				 * operations, where each operation latches and unlatches. It's
				 * the LN lock that protects the integrity of the whole
				 * multi-step process.)
				 * 
				 * For example, during abort, there may be cases where we have
				 * deleted and then added an LN during the same txn. This means
				 * that to undo/abort it, we first delete the LN (leaving
				 * knownDeleted set), and then add it back into the tree. We
				 * want to make sure the entry is in the BIN when we do the
				 * insert back in.
				 */
				boolean deleteEntry = false;

				if (binRef == null || isEntryPendingDeleted(i)
						|| isEntryKnownDeleted(i)
						|| binRef.hasDeletedKey(new Key(getKey(i)))) {

					Node n = null;
					if (canFetch) {
						n = fetchTarget(i);
					} else {
						n = getTarget(i);
						if (n == null) {
							/* Punt, we don't know the state of this child. */
							continue;
						}
					}

					if (n == null) {
						/* Cleaner deleted the log file. Compress this LN. */
						deleteEntry = true;
					} else if (isEntryKnownDeleted(i)) {
						LockResult lockRet = lockingTxn.nonBlockingLock(n
								.getNodeId(), LockType.READ, db);
						if (lockRet.getLockGrant() == LockGrantType.DENIED) {
							anyLocksDenied = true;
							continue;
						}

						deleteEntry = true;
					} else {
						if (!n.containsDuplicates()) {
							LN ln = (LN) n;
							LockResult lockRet = lockingTxn.nonBlockingLock(ln
									.getNodeId(), LockType.READ, db);
							if (lockRet.getLockGrant() == LockGrantType.DENIED) {
								anyLocksDenied = true;
								continue;
							}

							if (ln.isDeleted()) {
								deleteEntry = true;
							}
						}
					}

					/* Remove key from BINReference in case we requeue it. */
					if (binRef != null) {
						binRef.removeDeletedKey(new Key(getKey(i)));
					}
				}

				/* At this point, we know we can delete. */
				if (deleteEntry) {
					boolean entryIsIdentifierKey = Key.compareKeys(getKey(i),
							getIdentifierKey(), getKeyComparator()) == 0;
					if (entryIsIdentifierKey) {

						/*
						 * We're about to remove the entry with the idKey so the
						 * node will need a new idkey.
						 */
						setNewIdKey = true;
					}

					boolean deleteSuccess = deleteEntry(i, true);
					assert deleteSuccess;

					/*
					 * Since we're deleting the current entry, bump the current
					 * index back down one.
					 */
					i--;
				}
			}
		} finally {
			if (lockingTxn != null) {
				lockingTxn.operationEnd();
			}
		}

		if (anyLocksDenied && binRef != null) {
			db.getDbEnvironment().addToCompressorQueue(binRef, false);
			ret = true;
		}

		if (getNEntries() != 0 && setNewIdKey) {
			setIdentifierKey(getKey(0));
		}

		/* This BIN is empty and expendable. */
		if (getNEntries() == 0) {
			setGeneration(0);
		}

		return ret;
	}

	public boolean isCompressible() {
		return true;
	}



	/* For debugging. Overrides method in IN. */
	boolean validateSubtreeBeforeDelete(int index) throws DatabaseException {

		return true;
	}

	/**
	 * Check if this node fits the qualifications for being part of a deletable
	 * subtree. It can only have one IN child and no LN children.
	 */
	boolean isValidForDelete() throws DatabaseException {

		/*
		 * Can only have one valid child, and that child should be deletable.
		 */
		int validIndex = 0;
		int numValidEntries = 0;
			for (int i = 0; i < getNEntries(); i++) {
				if (!isEntryKnownDeleted(i)) {
					numValidEntries++;
					validIndex = i;
				}
			}

			if (numValidEntries > 1) { // more than 1 entry
				return false;
			} else {
				if (nCursors() > 0) { // cursors on BIN, not eligable
					return false;
				}
				if (numValidEntries == 1) { // need to check child (DIN or LN)
					Node child = fetchTarget(validIndex);
					return child != null && child.isValidForDelete();
				} else {
					return true; // 0 entries.
				}
			}
	}



	/**
	 * Return the relevant user defined comparison function for this type of
	 * node. For IN's and BIN's, this is the BTree Comparison function.
	 * Overriden by DBIN.
	 */
	public Comparator getKeyComparator() {
		return getDatabase().getBtreeComparator();
	}

	public String beginTag() {
		return BEGIN_TAG;
	}

	public String endTag() {
		return END_TAG;
	}

	/*
	 * Logging support
	 */

	/**
	 * @see LoggableObject#getLogType
	 */
	public LogEntryType getLogType() {
		return LogEntryType.LOG_BIN;
	}

	public String shortClassName() {
		return "BIN";
	}

	/**
	 * Decide how to log this node. BINs may be logged provisionally. If logging
	 * a delta, return an null for the LSN.
	 */
	protected long logInternal(LogManager logManager, boolean allowDeltas,
			boolean isProvisional, boolean proactiveMigration, IN parent)
			throws DatabaseException {

		boolean doDeltaLog = false;
		long lastFullVersion = getLastFullVersion();

		/* Allow the cleaner to migrate LNs before logging. */
		Cleaner cleaner = getDatabase().getDbEnvironment().getCleaner();
		cleaner.lazyMigrateLNs(this, proactiveMigration);

		/*
		 * We can log a delta rather than full version of this BIN if - this has
		 * been called from the checkpointer with allowDeltas=true - there is a
		 * full version on disk - we meet the percentage heuristics defined by
		 * environment params. - this delta is not prohibited because of
		 * cleaning or compression All other logging should be of the full
		 * version.
		 */
		BINDelta deltaInfo = null;
		if ((allowDeltas) && (lastFullVersion != DbLsn.NULL_LSN)
				&& !prohibitNextDelta) {
			deltaInfo = new BINDelta(this);
			doDeltaLog = doDeltaLog(deltaInfo);
		}

		long returnLsn = DbLsn.NULL_LSN;
		if (doDeltaLog) {

			/*
			 * Don't change the dirtiness of the node -- leave it dirty. Deltas
			 * are never provisional, they must be processed at recovery time.
			 */
			lastDeltaVersion = logManager.log(deltaInfo);
			returnLsn = DbLsn.NULL_LSN;
			numDeltasSinceLastFull++;
		} else {
			/* Log a full version of the IN. */
			returnLsn = super.logInternal(logManager, allowDeltas,
					isProvisional, proactiveMigration, parent);
			lastDeltaVersion = DbLsn.NULL_LSN;
			numDeltasSinceLastFull = 0;
		}
		prohibitNextDelta = false;

		return returnLsn;
	}

	/**
	 * Decide whether to log a full or partial BIN, depending on the ratio of
	 * the delta size to full BIN size, and the number of deltas that have been
	 * logged since the last full.
	 * 
	 * @return true if we should log the deltas of this BIN
	 */
	private boolean doDeltaLog(BINDelta deltaInfo) throws DatabaseException {

		int maxDiffs = (getNEntries() * getDatabase().getBinDeltaPercent()) / 100;
		if ((deltaInfo.getNumDeltas() <= maxDiffs)
				&& (numDeltasSinceLastFull < getDatabase().getBinMaxDeltas())) {
			return true;
		} else {
			return false;
		}
	}
}
