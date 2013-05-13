/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: TreeUtils.java,v 1.1.6.1 2006/07/28 09:02:03 ckaestne Exp $
 */

package com.sleepycat.je.tree;

/**
 * Miscellaneous Tree utilities.
 */
public class TreeUtils {

	static private final String SPACES = "                                "
			+ "                                "
			+ "                                "
			+ "                                ";

	/**
	 * For tree dumper.
	 */
	public static String indent(int nSpaces) {
		return SPACES.substring(0, nSpaces);
	}

	public static String dumpByteArray(byte[] b) {
		StringBuffer sb = new StringBuffer();
		if (b != null) {
			if (Key.DUMP_BINARY) {
				for (int i = 0; i < b.length; i++) {
					// sb.append(Integer.toHexString(b[i] & 0xFF));
					sb.append(b[i] & 0xFF);
					sb.append(" ");
				}
			} else {
				sb.append(new String(b));
			}
		} else {
			sb.append("null");
		}
		return sb.toString();
	}
}
