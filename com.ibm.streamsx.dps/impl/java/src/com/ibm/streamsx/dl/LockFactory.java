/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface for the lock factory with the available methods declared here.
// A lock factory implementaton class will provide concrete logic for these methods.
package com.ibm.streamsx.dl;

public interface LockFactory {	
	public Lock createOrGetLock(String name) throws LockFactoryException;
	public boolean removeLock(Lock lock) throws LockFactoryException;
	// Get the Linux process id that currently owns this lock.
	public int getPidForLock(String name) throws LockFactoryException;
}
