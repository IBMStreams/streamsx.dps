/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This class will hold ONE and ONLY instance of the dpsHelper.
// This is done only once per Process i.e. PE.
package com.ibm.streamsx.dps.impl;

public class DpsHelperHolder 
{
	private static DpsHelper dps = null;

	/***
	 * Accessor method to get a reference to the single DpsHelper instance for this application.
	 * The DpsHelper class contains all the store and lock related functions.
	 * @throws Exception if initialization has not taken place by a successful call to the  initialize method 
	 */
	public static synchronized DpsHelper getDpsHelper() throws Exception {
		if (dps == null) {
			throw new IllegalStateException("The store must be initialized using DistributedStores.initialize before calls to getDpsHelper can be made. ");
		}
		
		return(dps);
	}

	/**
	 * This method must be called before getDpsHelper can be called.
	 * @param toolkitPath the path to the DPS toolkit within the application bundle.
	 * See the Java toolkit samples for an example of how to retrieve the toolkit path.
	 * */
	public static void initialize(String toolkitPath) throws Exception {
		if (dps == null){
			dps = new DpsHelper(toolkitPath);
		}
	}
}
