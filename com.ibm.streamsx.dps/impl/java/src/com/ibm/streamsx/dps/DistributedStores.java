/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the very first thing a Java dps user should obtain i.e. a store factory.
package com.ibm.streamsx.dps;

import com.ibm.streamsx.dps.impl.StoreFactoryImpl;

public class DistributedStores {
	// Get a Store factory ONLY ONCE per object of this class per Process (i.e. PE).
	private static StoreFactory factory = new StoreFactoryImpl();
	
	// Return the one and only factory we will have per process.
	public static StoreFactory getStoreFactory() {
		return factory;
	}
}
