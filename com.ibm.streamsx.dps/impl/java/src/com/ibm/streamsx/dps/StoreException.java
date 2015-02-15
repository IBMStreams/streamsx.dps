/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// Any errors during store operation will result in this exception getting thrown.
// Users of the dps facility can then catch the exception and obtain the error code and error message.
package com.ibm.streamsx.dps;

public class StoreException extends Exception {
	private long errorCode;
	private String errorMessage;
	
	public StoreException(long errorCode) {
		this.errorCode = errorCode;
	}

	public StoreException(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
	public StoreException(long errorCode, String errorMessage) {
		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}	

	public long getErrorCode() {
		return(errorCode);
	}
	
	public String getErrorMessage() {
		return(errorMessage);
	}
}
