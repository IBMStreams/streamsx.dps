/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is the implementation class for the distributed lock factory.
// Through the object of this class, a Java dps user can create a new lock or get an existing lock, or
// remove an existing lock.
package com.ibm.streamsx.dl.impl;

import com.ibm.streamsx.dl.Lock;
import com.ibm.streamsx.dl.LockFactory;
import com.ibm.streamsx.dl.LockFactoryException;
import com.ibm.streamsx.dps.impl.DpsHelperHolder;
import com.ibm.streamsx.dps.impl.DpsHelper;

public class LockFactoryImpl implements LockFactory
{	
	// Create a brand new lock or get an existing lock. 
	public Lock createOrGetLock(String name) throws LockFactoryException {
		long result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dlCreateOrGetLock(name, err);
		} catch(Exception e) { 
			// Either dps cannot be initialized or createOrGetLock went wrong.
			throw new LockFactoryException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockFactoryException(dps.dlGetLastDistributedLockErrorCode(), 
				dps.dlGetLastDistributedLockErrorString());
		}
		
		return new LockImpl(result);
	}
	
	// Remove an existing lock.
	public boolean removeLock(Lock lock) throws LockFactoryException {
		boolean result;
		long[] err = new long[1];
		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dlRemoveLock(((LockImpl)lock).getId(), err);
		} catch(Exception e) { 
			// either dps cannot be initialized or removeLock went wrong
			throw new LockFactoryException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockFactoryException(dps.dlGetLastDistributedLockErrorCode(), 
				dps.dlGetLastDistributedLockErrorString());
		}
		
		return(result);
	}	
	
	// Get the Linux process id of the current owner of the given lock.
	public int getPidForLock(String name) throws LockFactoryException {
		int result;
		long[] err = new long[1];

		DpsHelper dps = null;
		
		try {
			dps = DpsHelperHolder.getDpsHelper();
			result = dps.dlGetPidForLock(name, err);
		} catch(Exception e) { 
			// either dps cannot be initialized or getPidForLock went wrong
			throw new LockFactoryException(65534, e.getMessage());
		}
		
		if (err[0] != 0) {
			throw new LockFactoryException(dps.dlGetLastDistributedLockErrorCode(), 
				dps.dlGetLastDistributedLockErrorString());
		}
		
		return(result);		
	}
}
