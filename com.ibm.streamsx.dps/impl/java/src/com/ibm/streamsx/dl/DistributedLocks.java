/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/

package com.ibm.streamsx.dl;

import com.ibm.streamsx.dl.impl.LockFactoryImpl;
/**
 * This is the main entry point into the locking API, used to create a {@link LockFactory}.
 * It is assumed that the {@link DistributedStores} class has been configured using its initialize method before attempts to utilize the <code>DistributedLocks</code> are made.
 */
public class DistributedLocks {
	// Get a Lock factory ONLY ONCE per object of this class per Process (i.e. PE).
	private static LockFactory factory = new LockFactoryImpl();
	
	/**
	 * Get a LockFactory instance
	 * @return  the one and only factory we will have per process.
	 */
	public static LockFactory getLockFactory() {
		return factory;
	}
}
