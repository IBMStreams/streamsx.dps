/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef DISTRIBUTED_LOCK_WRAPPERS_H_
#define DISTRIBUTED_LOCK_WRAPPERS_H_

#include "DistributedProcessStore.h"

namespace com {
namespace ibm {
namespace streamsx {
namespace lock {
namespace distributed
{

  /// Create a lock or get it if it already exists
  /// @param name of the lock
  /// @return lock handle
  /// @param err lock error code
  inline SPL::uint64 dlCreateOrGetLock(SPL::rstring const & name, SPL::uint64 & err) 
  {
    return store::distributed::DistributedProcessStore::getGlobalStore().createOrGetLock(name, err);    
  }
  
  /// Remove the lock
  /// @param lock lock handle
  /// @return true if the lock was there, false otherwise
  /// @param err lock error code
  inline SPL::boolean dlRemoveLock(SPL::uint64 lock, SPL::uint64 & err)
  {
    return store::distributed::DistributedProcessStore::getGlobalStore().removeLock(lock, err);    
  }
        
  /// Acquire the lock
  /// @return lock lock handle
  /// @param err lock error code
  inline void dlAcquireLock(SPL::uint64 lock, SPL::uint64 & err) 
  {
    store::distributed::DistributedProcessStore::getGlobalStore().acquireLock(lock, err); 
  }
  
  /// Acquire the lock
  /// @param leaseTime lease time (in seconds) 
  /// @return lock lock handle
  /// @param err lock error code
  inline void dlAcquireLock(SPL::uint64 lock, SPL::float64 leaseTime, SPL::float64 maxWaitTimeToAcquireLock, SPL::uint64 & err)
  {
    store::distributed::DistributedProcessStore::getGlobalStore().acquireLock(lock, leaseTime, maxWaitTimeToAcquireLock, err);
  }

  /// Release the lock
  /// @return lock lock handle
  /// @param err GlobalStore error code
  inline void dlReleaseLock(SPL::uint64 lock, SPL::uint64 & err) 
  {
    store::distributed::DistributedProcessStore::getGlobalStore().releaseLock(lock, err);             
  }

  /// Get the process id for a given distributed lock name
  /// @param name of the lock
  /// @return Process id that currently owns the lock
  /// @param err GlobalStore error code
  inline SPL::uint32 dlGetPidForLock(SPL::rstring name, SPL::uint64 & err) {
	  return store::distributed::DistributedProcessStore::getGlobalStore().getPidForLock(name, err);
  }

  /// Get the last lock error string
  /// @return the last lock error string
  inline SPL::rstring dlGetLastDistributedLockErrorString()
  {
    return store::distributed::DistributedProcessStore::getGlobalStore().getLastLockErrorString();
  }
  
  /// Get the last lock error code
  /// @return the last lock error code
  inline SPL::uint64 dlGetLastDistributedLockErrorCode()
  {
    return store::distributed::DistributedProcessStore::getGlobalStore().getLastLockErrorCode();
  }

} } } } }

#endif /* DISTRIBUTED_LOCK_WRAPPERS_H_ */
