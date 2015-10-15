/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/

package com.ibm.streamsx.dl;
/** This is an interface defining the available distributed lock (dl) methods.
 * A lock implementation class will provide concrete logic for these methods.
 * Use {@link LockFactory} to get a usable instance of this class.
 * <br>
 * Example usage:
  <pre>
 * 
 * <code>
 LockFactory lf = DistributedLocks.getLockFactory(); 
 Lock myLock = lf.createOrGetLock("Lock_For_Test_Store1");

// Acquire the lock
try {
   myLock.acquireLock();
  
} catch (LockException le) {
   System.out.println("Unable to acquire the lock named 'Lock_For_Test_Store1'. Error = " +
      le.getErrorCode() + ", Error msg = " + le.getErrorMessage());
      throw le;
}

 System.out.println("Successfully acquired the lock named 'Lock_For_Test_Store1'.");
 // Exclusive access to the distributed lock obtained.
 // Perform your operations on a shared data store safely now.
 // ....
 // ....
 //release the lock
 try {
  myLock.releaseLock();
} catch (LockException le) {}

 * </code>
 * </pre>
 */
public interface Lock {
	/**Get the id for this lock
	 * 
	 * @return lock id
	 */
	public long getId();
	
	/***Acquire a distributed lock before accessing a critical shared resource.
	 * <p>
	 * <b>IMPORTANT NOTE:</b>
Once called, this function could spin loop for a very long time (three or four minutes) until it acquires the distributed lock or it times out due to the distributed lock being not yet available.
Another important factor is that once the lock is granted, it is given to you for an infinite period of time. Hence, it is your responsibility to properly release the lock at the end of your task via <code>releaseLock</code>. There is no external mechanism to forcibly release a lock.
If either the  behavior of waiting for four minutes to acquire the lock or the indefinite allocation of lock to yourself is undesired, then use the alternative overloaded function, {@code acquireLock}, instead.
</p>
	 * @throws LockException
	 */
	public void acquireLock() throws LockException;
/***Acquire a distributed lock with lease time and max wait time before accessing a critical shared resource
 * 
 * @param leaseTime time, in seconds, for which the lock will be owned by the caller.  If the caller doesn't release the lock after the requested lease time due to a program crash or a programmatic error, DPS internal code will automatically release the stale lock for others to acquire. 
 * @param maxWaitTimeToAcquireLock maximum amount of time, in seconds, the caller is willing to wait to acquire the lock. If the lock acquisition is not done within the specified max wait time, this API will exit with an error instead of spin looping forever.
 * @throws LockException if the lock could not be acquired within the time specified.
 */
	public void acquireLock(double leaseTime, double maxWaitTimeToAcquireLock) throws LockException;
	/***
	 * Release ownership of the lock.  It  will be made available for other callers to acquire.
	 * @throws LockException if an error occurs 
	 */
	 public void releaseLock() throws LockException;
}
