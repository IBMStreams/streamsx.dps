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
	/**
	 * This function can be used to create a new distributed lock with a given lock name or obtain an existing distributed lock in the event that a distributed lock with the given lock name already exists. 
	 * 
	 * <pre>
 {@code
* LockFactory lf = DistributedLocks.getLockFactory(); 
* Lock myLock = lf.createOrGetLock("Lock_For_Test_Store1");
* }
* </pre>
* @param name an arbitrary string representing the intended lock name.
* @return a {@link Lock} object.
* @throws LockFactoryException if an error occurs.
	 * */
	public Lock createOrGetLock(String name) throws LockFactoryException;

/**
 * This function can be used to remove a given distributed lock id. For the final function argument, you must provide a mutable variable to receive the DPS error code in the event of a problem in the remove lock operation. 
 * Since the whole purpose of distributed locks is to ensure operations are performed safely on shared resources by multiple unrelated application components,
 * don't remove the distributed locks abruptly unless you are very clear about what you are doing. Do it only when you are sure that no one is using the lock at the time of removing it.
 * <pre>{@code
 * LockFactory lf = DistributedLocks.getLockFactory(); 
   Lock myLock = lf.createOrGetLock("Lock_For_Test_Store1");
   // access shared data
  // once you are certain the lock is no longer needed, the lock can be removed.
	lf.removeLock(myLock);

 * }</pre>
 * @param lock the lock to remove
 * @return true if the lock was successfully removed.
 * @throws LockFactoryException
 */

	public boolean removeLock(Lock lock) throws LockFactoryException;
	
	/** Get the Linux process id that currently owns this lock.
	 * 
	 * @param name the name of the lock
	 * @return the id of the Linux process that owns this lock, or 0 if no one owns the lock.
	 * @throws LockFactoryException if an error occurs
	 */
	public int getPidForLock(String name) throws LockFactoryException;
}
