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

	public static synchronized DpsHelper getDpsHelper() throws Exception {
		if (dps == null) {
			dps = new DpsHelper();
		}
		
		return(dps);
	}
}
