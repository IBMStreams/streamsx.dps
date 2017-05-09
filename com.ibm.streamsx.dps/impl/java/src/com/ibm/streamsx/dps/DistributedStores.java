/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/

package com.ibm.streamsx.dps;

import com.ibm.streams.operator.PERuntime;
import com.ibm.streamsx.dps.impl.DpsHelperHolder;
import com.ibm.streamsx.dps.impl.StoreFactoryImpl;
/**
 * This is the main entry point for using the Distributed Store API - this class 
 * is used to get a {@link StoreFactory}, which in turn, is used to create {@link Store}s.
 * The <code>initialize</code> method of this class must be called once before trying to create Stores.
 */
public class DistributedStores {
	private static final String DPS_TOOLKIT = "com.ibm.streamsx.dps";
	// Get a Store factory ONLY ONCE per object of this class per Process (i.e. PE).
	private static StoreFactory factory;
	/**
	 * This method must be called before <code>getStoreFactory()</code> can be used.
	 * */
	public static void initialize() throws Exception {
		if (factory == null){
			String dpsToolkitPath = PERuntime.getPE().getToolkitDirectory(DPS_TOOLKIT).getAbsolutePath();
			DpsHelperHolder.initialize(dpsToolkitPath);
			factory = new StoreFactoryImpl();
		}
	}
	
	/** 
	 * Return a store factory.  
	 * Calls to this method must be preceded by a successful call to the initialize() method of this class.
	 * @return the  one and only factory we will have per process.
	 * @throws Exception 
	 */
	public static StoreFactory getStoreFactory() {
		if (factory == null){
			factory = new StoreFactoryImpl();
		}
		return factory;
	}
}
