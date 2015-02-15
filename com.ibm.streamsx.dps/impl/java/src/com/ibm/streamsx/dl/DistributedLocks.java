/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the very first thing a Java dps user should obtain i.e. a lock factory.
package com.ibm.streamsx.dl;

import com.ibm.streamsx.dl.impl.LockFactoryImpl;

public class DistributedLocks {
	// Get a Lock factory ONLY ONCE per object of this class per Process (i.e. PE).
	private static LockFactory factory = new LockFactoryImpl();
	
	// Return the one and only factory we will have per process.
	public static LockFactory getLockFactory() {
		return factory;
	}
}
