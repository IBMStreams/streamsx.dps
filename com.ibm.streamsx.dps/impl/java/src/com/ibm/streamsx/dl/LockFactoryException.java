/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// Any errors during lock creation will result in this exception getting thrown.
// Users of the dps facility can then catch the exception and obtain the error code and error message.
package com.ibm.streamsx.dl;

public class LockFactoryException extends Exception {
	private long errorCode;
	private String errorMessage;
	
	public LockFactoryException(long errorCode) {
		this.errorCode = errorCode;
	}

	public LockFactoryException(String errorMessage) {
		this.errorMessage = errorMessage;
	}
	
	public LockFactoryException(long errorCode, String errorMessage) {
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
