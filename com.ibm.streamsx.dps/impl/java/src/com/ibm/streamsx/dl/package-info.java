/**
 * This package provides distributed locking functions which can be used for safe, concurrent access to distributed stores from multiple threads and processes. 
 * This is achieved using a trust based cooperative locking scheme to gain exclusive access into the stores for performing a set of transaction based store activities.
 * The main class is a {@link com.ibm.streamsx.dl.Lock}.
 * Use the {@link com.ibm.streamsx.dl.DistributedLocks} class which provides access to a {@link com.ibm.streamsx.dl.LockFactory}, from which {@link com.ibm.streamsx.dl.Lock} objects can be created.
 * Each Lock has methods for acquiring and releasing it.

 */
package com.ibm.streamsx.dl;