/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the implementation class for the lock (in fact, distributed lock).
// Through the object of this class, a Java dps user can execute all the available dl APIs to
// perform distributed lock operations.
package com.ibm.streamsx.dl.impl;

import com.ibm.streamsx.dl.Lock;
import com.ibm.streamsx.dl.LockException;
import com.ibm.streamsx.dps.impl.DpsHelperHolder;
import com.ibm.streamsx.dps.impl.DpsHelper;

public class LockImpl implements Lock
{	
	private long id;
	
	// Create a lock implementation.
	public LockImpl(long id) {
		this.id = id;
	}
	
	// Get the lock id.
	public long getId() {
		return(id);
	}
		
	// Acquire possession of this distributed lock. (Infinite lock until the user releases it.)
	public void acquireLock() throws LockException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dlAcquireLock(id, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or acquireLock went wrong.
			// An error code of 65534 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual lock functions.
			throw new LockException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockException(dps.dlGetLastDistributedLockErrorCode(), 
					dps.dlGetLastDistributedLockErrorString());
		}
		
		return;	
	}
	
	// Acquire possession of this distributed lock for a user-specified lease duration.
	public void acquireLock(double leaseTime, double maxWaitTimeToAquireLock) throws LockException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dlAcquireLock(id, leaseTime, maxWaitTimeToAquireLock, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or acquireLock went wrong.
			// An error code of 65534 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual lock functions.
			throw new LockException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockException(dps.dlGetLastDistributedLockErrorCode(), 
					dps.dlGetLastDistributedLockErrorString());
		}
		
		return;	
	}
	
	// Release the possession of this distributed lock.
	public void releaseLock() throws LockException {
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			dps.dlReleaseLock(id, err);
		} catch(Exception e) {
			// Either dps cannot be initialized or releaseLock went wrong.
			// An error code of 65534 indicates that this error occurred inside
			// the dps JNI glue layer and not inside the actual lock functions.
			throw new LockException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockException(dps.dlGetLastDistributedLockErrorCode(), 
					dps.dlGetLastDistributedLockErrorString());
		}
		
		return;	
	}
}
