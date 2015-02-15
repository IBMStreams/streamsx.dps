/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface file where all the available distributed lock (dl) methods are declared.
// A lock implemntation class will provide concrete logic for these methods.
package com.ibm.streamsx.dl;

public interface Lock {
	public long getId();
	public void acquireLock() throws LockException;
	public void acquireLock(double leaseTime, double maxWaitTimeToAcquireLock) throws LockException;
	public void releaseLock() throws LockException;
}
