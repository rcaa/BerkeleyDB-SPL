/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002-2006
 *      Sleepycat Software.  All rights reserved.
 *
 * $Id: IntConfigParam.java,v 1.1.6.1 2006/07/28 09:02:42 ckaestne Exp $
 */

package com.sleepycat.je.config;

/**
 * A JE configuration parameter with an integer value.
 */
public class IntConfigParam extends ConfigParam {

	private static final String DEBUG_NAME = IntConfigParam.class.getName();

	private Integer min;

	private Integer max;

	IntConfigParam(String configName, Integer minVal, Integer maxVal,
			Integer defaultValue, boolean mutable, String description) {
		// defaultValue must not be null
		super(configName, defaultValue.toString(), mutable, description);

		min = minVal;
		max = maxVal;
	}

	/*
	 * Self validate. Check mins and maxs
	 */
	private void validate(Integer value) throws IllegalArgumentException {

		if (value != null) {
			if (min != null) {
				if (value.compareTo(min) < 0) {
					throw new IllegalArgumentException(DEBUG_NAME + ":"
							+ " param " + name + " doesn't validate, " + value
							+ " is less than min of " + min);
				}
			}
			if (max != null) {
				if (value.compareTo(max) > 0) {
					throw new IllegalArgumentException(DEBUG_NAME + ":"
							+ " param " + name + " doesn't validate, " + value
							+ " is greater than max of " + max);
				}
			}
		}
	}

	public void validateValue(String value) throws IllegalArgumentException {

		try {
			validate(new Integer(value));
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException(DEBUG_NAME + ": " + value
					+ " not valid value for " + name);
		}
	}

	public String getExtraDescription() {
		StringBuffer minMaxDesc = new StringBuffer();
		if (min != null) {
			minMaxDesc.append("# minimum = ").append(min);
		}
		if (max != null) {
			if (min != null) {
				minMaxDesc.append("\n");
			}
			minMaxDesc.append("# maximum = ").append(max);
		}
		return minMaxDesc.toString();
	}

}
