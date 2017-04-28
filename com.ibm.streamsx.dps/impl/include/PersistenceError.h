/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef PERSISTENCE_ERROR_H_
#define PERSISTENCE_ERROR_H_

#include <string>
#include <inttypes.h>
#include "DpsConstants.h"

namespace com {
namespace ibm {
namespace streamsx {
namespace store
{
  class PersistenceError
  {
  public:
    /// Default constructor
    PersistenceError() 
      : errorCode_(DPS_NO_ERROR), errorCodeTTL_(DPS_NO_ERROR) {}
        
    /// Constructor
    /// @param errorStr error string
    /// @param errorCode error code
    PersistenceError(std::string const & errorStr, uint64_t errorCode,
    	std::string const & errorStrTTL, uint64_t errorCodeTTL)
      : errorStr_(errorStr), errorCode_(errorCode), errorStrTTL_(errorStrTTL), errorCodeTTL_(errorCodeTTL)
    {}
        
    /// Check if there is error or not
    /// @return true if there is an error, false otherwise
    bool hasError() const
    {
      return errorCode_ != DPS_NO_ERROR;
    }

    /// Check if there is error or not for the TTL operation.
    /// @return true if there is an error, false otherwise
    bool hasErrorTTL() const
    {
      return errorCodeTTL_ != DPS_NO_ERROR;
    }

    /// Reset the error code
    void reset()
    {
      set("", DPS_NO_ERROR);
    }

    /// Reset the error code for TTL operations
    void resetTTL()
    {
      setTTL("", DPS_NO_ERROR);
    }

    /// Get the error string
    /// @return the error string
    std::string const & getErrorStr() const
    {
      return errorStr_;
    }
        
    /// Get the error string (mutating)
    /// @return the error string  
    std::string & getErrorStr() 
    {
      return errorStr_;
    }

    /// Get the error string for TTL operation.
    /// @return the error string
    std::string const & getErrorStrTTL() const
    {
      return errorStrTTL_;
    }
        
    /// Get the error string (mutating) for TTL operation.
    /// @return the error string
    std::string & getErrorStrTTL()
    {
      return errorStrTTL_;
    }

    /// Get the error code
    /// @return the error code
    uint64_t const & getErrorCode() const
    {
      return errorCode_;
    }
        
    /// Get the error code (mutating)
    /// @return the error code
    uint64_t & getErrorCode() 
    {
      return errorCode_;
    }

    /// Get the error code for TTL operation.
    /// @return the error code
    uint64_t const & getErrorCodeTTL() const
    {
      return errorCodeTTL_;
    }
        
    /// Get the error code (mutating) for TTL operation.
    /// @return the error code
    uint64_t & getErrorCodeTTL()
    {
      return errorCodeTTL_;
    }

    /// Set the error code and string
    /// @param errorStr error string
    /// @param errorCode error code
    void set(std::string const & errorStr, uint64_t errorCode)
    {
      errorStr_ = errorStr;
      errorCode_ = errorCode;
    }

    /// Set the error code and string for TTL operations.
    /// @param errorStr error string
    /// @param errorCode error code
    void setTTL(std::string const & errorStr, uint64_t errorCode)
    {
      errorStrTTL_ = errorStr;
      errorCodeTTL_ = errorCode;
    }

  private:
    std::string errorStr_;
    uint64_t errorCode_;
    std::string errorStrTTL_;
    uint64_t errorCodeTTL_;
  };
} } } }

#endif /* PERSISTENCE_ERROR_H */

