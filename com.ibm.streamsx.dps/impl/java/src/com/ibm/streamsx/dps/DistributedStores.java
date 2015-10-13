/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/

package com.ibm.streamsx.dps;

import com.ibm.streamsx.dps.impl.StoreFactoryImpl;
/**
 * This is the main entry point for using the Distributed Store API - this class 
 * is used to get a {@link StoreFactory}, which in turn, is used to create {@link Store}s.
 * 
 */
public class DistributedStores {
	// Get a Store factory ONLY ONCE per object of this class per Process (i.e. PE).
	private static StoreFactory factory = new StoreFactoryImpl();
	
	/** 
	 * Return a store factory 
	 * @return the  one and only factory we will have per process.
	 * 
	 */
	public static StoreFactory getStoreFactory() {
		return factory;
	}
}
