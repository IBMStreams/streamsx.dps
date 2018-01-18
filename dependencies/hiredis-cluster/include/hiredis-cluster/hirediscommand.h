/*
 * Copyright (c) 2015, Dmitrii Shinkevich <shinmail at gmail dot com>
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __libredisCluster__command__
#define __libredisCluster__command__

#include <iostream>
#include "cluster.h"
#include "hiredisprocess.h"

extern "C"
{
#include "hiredis/hiredis.h"
}

namespace RedisCluster
{
    using std::string;
    class HiredisCommand : public NonCopyable
    {
        enum CommandType
        {
            SDS,
            FORMATTED_STRING
        };
        
    public:

        /*
        // Senthil commented out the following block of code on Apr/09/2015 to avoid a
        // compiler error at the timeout default value assignment.
        // I added a new block below without that timeout line. Moved the timeout line to inside the function.
        static Cluster<redisContext>::ptr_t createCluster(const char* host,
                                                          int port,
                                                          void* data = NULL,
                                                          Cluster<redisContext>::pt2RedisConnectFunc conn = ConnectFunction,
                                                          Cluster<redisContext>::pt2RedisFreeFunc free = redisFree,
                                                          const struct timeval &timeout = { 3, 0 })
         */

        static Cluster<redisContext>::ptr_t createCluster(const char* host,
                                                          int port,
                                                          void* data = NULL,
                                                          Cluster<redisContext>::pt2RedisConnectFunc conn = ConnectFunction,
                                                          Cluster<redisContext>::pt2RedisFreeFunc free = redisFree
                                                          )
        {
            Cluster<redisContext>::ptr_t cluster(NULL);
            redisReply *reply;
            // Senthil moved this line from the function definition above to here on Apr/09/2015.
            // This was done to avoid a compiler error.
            const struct timeval timeout = { 3, 0 };

            redisContext *con = redisConnectWithTimeout( host, port, timeout );
            if( con == NULL || con->err )
                throw ConnectionFailedException();
            
            reply = static_cast<redisReply*>( redisCommand( con, Cluster<redisContext>::CmdInit() ) );
            HiredisProcess::checkCritical( reply, true );

            cluster = new Cluster<redisContext>( reply, conn, free, data );
            
            freeReplyObject( reply );
            redisFree( con );
            return cluster;
        }

        // Senthil added this new overloaded method on Oct/02/2017 to avoid the original 
        // createCluster method above from crashing whenever the Redis cluster is 
        // configured with the requirepass configuration parameter in the redis.conf file.
        // When password authentication is configured for a Redis cluster, then the
        // very first Redis command issued to a cluster must be the Redis AUTH command.
        // The original method above doesn't do that. Hence, it fails when calling the
        // redisCommand method with a command "cluster slots". This overloaded method
        // has a new method argument to receive the password string from the caller.
        // Then, this method will also call the Redis AUTH command as the
        // the very first thing to avoid the crash.
        static Cluster<redisContext>::ptr_t createCluster(const char* host,
                                                          int port,
                                                          string password,
                                                          void* data = NULL,
                                                          Cluster<redisContext>::pt2RedisConnectFunc conn = ConnectFunction,
                                                          Cluster<redisContext>::pt2RedisFreeFunc free = redisFree
                                                          )
        {
            Cluster<redisContext>::ptr_t cluster(NULL);
            redisReply *reply;
            // Senthil moved this line from the function definition above to here on Apr/09/2015.
            // This was done to avoid a compiler error.
            const struct timeval timeout = { 3, 0 };

            redisContext *con = redisConnectWithTimeout( host, port, timeout );
            if( con == NULL || con->err )
                throw ConnectionFailedException();
            
            // Senthil added the following lines of code on Oct/02/2017 to avoid a crash when
            // the Redis cluster is configured with password authentication. In that case,
            // the very first Redis command executed must be the Redis AUTH command.
            if (password.length() > 0) {
               string authCommand = "auth " + password;
               // std::cout << "Calling the redis auth command for " << string(host) << ":" << port << "-->" << authCommand << std::endl;
               reply = static_cast<redisReply*>( redisCommand( con, authCommand.c_str() ) );
               HiredisProcess::checkCritical( reply, true );
               // std::cout << "Returned from the redis auth command." << std::endl;
               freeReplyObject(reply);
            }

            reply = static_cast<redisReply*>( redisCommand( con, Cluster<redisContext>::CmdInit() ) );
            HiredisProcess::checkCritical( reply, true );
            // std::cout << "Returned from the redis cluster slots command." << std::endl;

            cluster = new Cluster<redisContext>( reply, conn, free, data );
            
            freeReplyObject( reply );
            redisFree( con );
            // std::cout << "All is well with the overloaded createCluster method." << std::endl;
            return cluster;
        }
        
        // Senthil changed the following line on Apr/09/2015. Following typename syntax is not supported in the
        // non C++11 compliant gcc compiler we use for Streams as of this date.
        // static inline void* Command( typename Cluster<redisContext>::ptr_t cluster_p,
        static inline void* Command(Cluster<redisContext>::ptr_t cluster_p,
                                   string key,
                                   int argc,
                                   const char ** argv,
                                   const size_t *argvlen )
        {
            return HiredisCommand( cluster_p, key, argc, argv, argvlen ).process();
        }

        // Senthil added the following overloaded method on Oct/02/2017 to accept a redis cluster password as a method argument.
        static inline void* Command(Cluster<redisContext>::ptr_t cluster_p,
                                   string password,
                                   string key,
                                   int argc,
                                   const char ** argv,
                                   const size_t *argvlen )
        {
            return HiredisCommand( cluster_p, password, key, argc, argv, argvlen ).process();
        }


        
        // static inline void* Command( typename Cluster<redisContext>::ptr_t cluster_p,
	static inline void* Command(Cluster<redisContext>::ptr_t cluster_p,
                                   string key,
                                   const char *format, ...)
        {
            va_list ap;
            va_start( ap, format );
            return HiredisCommand( cluster_p, key, format, ap ).process();
            va_end(ap);
        }
        
        // Senthil added the following overloaded method on Oct/02/2017 to accept a redis cluster password as a method argument.
	static inline void* Command(Cluster<redisContext>::ptr_t cluster_p,
                                   string password,
                                   string key,
                                   const char *format, ...)
        {
            va_list ap;
            va_start( ap, format );
            return HiredisCommand( cluster_p, password, key, format, ap ).process();
            va_end(ap);
        }
        
    protected:
        
	// HiredisCommand( typename Cluster<redisContext>::ptr_t cluster_p,
        HiredisCommand( Cluster<redisContext>::ptr_t cluster_p,
                       string key,
                       int argc,
                       const char ** argv,
                       const size_t *argvlen ) :
        cluster_p_( cluster_p ),
        key_( key ),
        type_( SDS )
        {
            if( cluster_p == NULL )
                throw InvalidArgument();
            
            len_ = redisFormatSdsCommandArgv(&cmd_, argc, argv, argvlen);
        }

        // Senthil added the following overloaded method on Oct/02/2017 to accept a redis cluster password as a method argument.
        HiredisCommand( Cluster<redisContext>::ptr_t cluster_p,
                       string password,
                       string key,
                       int argc,
                       const char ** argv,
                       const size_t *argvlen ) :
        cluster_p_( cluster_p ),
        password_( password ),
        key_( key ),
        type_( SDS )
        {
            if( cluster_p == NULL )
                throw InvalidArgument();
            
            len_ = redisFormatSdsCommandArgv(&cmd_, argc, argv, argvlen);
        }

        
        // HiredisCommand( typename Cluster<redisContext>::ptr_t cluster_p,
        HiredisCommand( Cluster<redisContext>::ptr_t cluster_p,
                       string key,
                       const char *format, va_list ap ) :
        cluster_p_( cluster_p ),
        key_( key ),
        type_( FORMATTED_STRING )
        {
            if( cluster_p == NULL )
                throw InvalidArgument();

            len_ = redisvFormatCommand(&cmd_, format, ap);
        }

        // Senthil added the following overloaded method on Oct/02/2017 to accept a redis cluster password as a method argument.
        HiredisCommand( Cluster<redisContext>::ptr_t cluster_p,
                       string password,
                       string key,
                       const char *format, va_list ap ) :
        cluster_p_( cluster_p ),
        password_( password ),
        key_( key ),
        type_( FORMATTED_STRING )
        {
            if( cluster_p == NULL )
                throw InvalidArgument();

            len_ = redisvFormatCommand(&cmd_, format, ap);
        }
        
        ~HiredisCommand()
        {
            if( type_ == SDS )
            {
                sdsfree( (sds)cmd_ );
            }
            else
            {
                free( cmd_ );
            }
        }
        
        redisReply* processHiredisCommand( redisContext *con )
        {
            // Aug/18/2017. Senthil made this subtle but an important change to avoid a segfault.
            // Following variable declaration was originally uninitialized. Because of that,
            // it was randomly pointing to an illegal memory address whenever the redisGetReply API below
            // returns without doing any logic at a time when a redis cluster node goes down suddenly.
            // Until the redis cluster takes a few seconds to stabilize with an updated 
            // map of new masters and slaves, that API will return immediately without 
            // assigning anything to the uninitialized variable named 'redisReply *reply;'.
            // Under such conditions, if that uninitialized variable gets accessed by the calling
            // function, that will surely cause a SEGV error. To avoid that from happening, I made a 
            // very simple change to initialize this local variable to NULL.
            //
            // Please note that the NULL pointer situation during the auto reconfiguration of the
            // redis-cluster will make this redis-cluster wrapper layer to throw an exception
            // (e-g: DisconnectedException). Applications using the redis-cluster wrapper should be
            // prepared to catch that exception and reconnect to the reconfigured redis-cluster to
            // continue with an uninterrupted operation.
            redisReply* reply = NULL;

            // Senthil added the following if block on Oct/02/2017 to avoid an error when
            // the Redis cluster is configured with password authentication. In that case,
            // the very first Redis command executed on a new connection must be the Redis AUTH command.
            if (password_.length() > 0) {
               string authCommand = "auth " + password_;
               // std::cout << "Calling the redis auth command '" << authCommand << 
               //   "' before the redis command '" << string(cmd_, len_) << "'" << std::endl;
               reply = static_cast<redisReply*>( redisCommand( con, authCommand.c_str() ) );
               HiredisProcess::checkCritical( reply, true );
               // std::cout << "Returned from the redis auth command." << std::endl;
               freeReplyObject(reply);
               reply = NULL;
            }

            redisAppendFormattedCommand( con, cmd_, len_ );
            redisGetReply( con, (void**)&reply );
            return reply;
        }
        
        redisReply* asking( redisContext *con  )
        {
            return static_cast<redisReply*>( redisCommand( con, "ASKING" ) );
        }
        
        void* process()
        {
            redisReply *reply;
            redisContext *con = cluster_p_->getConnection( key_ );
            string host, port;
            
            reply = processHiredisCommand( con );
            HiredisProcess::checkCritical( reply, false );
            HiredisProcess::processState state = HiredisProcess::processResult( reply, host, port);
            
            switch ( state ) {
                case HiredisProcess::ASK:
                    
                    freeReplyObject( reply );
                    con = cluster_p_->createNewConnection( host, port );
                    
                    if( con != NULL && con->err == 0 )
                    {
                        reply = asking( con );
                        HiredisProcess::checkCritical( reply, true, "asking error" );
                    
                        freeReplyObject( reply );
                        reply = processHiredisCommand( con );
                        HiredisProcess::checkCritical( reply, false );
                    
                        redisFree( con );
                    }
                    else if( con == NULL )
                    {
                        throw LogicError("Can't connect while resolving asking state");
                    }
                    else
                    {
                        throw LogicError( con->errstr );
                        redisFree( con );
                    }
                    
                    break;
                    
                case HiredisProcess::MOVED:
                    
                    freeReplyObject( reply );
                    con = cluster_p_->createNewConnection( host, port );
                    
                    if( con != NULL && con->err == 0 )
                    {
                        reply = processHiredisCommand( con );
                        redisFree( con );
                        cluster_p_->moved();
                    }
                    else if( con == NULL )
                    {
                        throw LogicError("Can't connect while resolving asking state");
                    }
                    else
                    {
                        throw LogicError( con->errstr );
                        redisFree( con );
                    }
                    
                    break;
                case HiredisProcess::READY:
                    break;
                default:
                    throw LogicError( "error in state processing" );
                    break;
            }
            return reply;
        }
        
        static redisContext* ConnectFunction( const char* host, int port, void * )
        {
            return redisConnect( host, port);
        }
        
        // typename Cluster<redisContext>::ptr_t cluster_p_;
        Cluster<redisContext>::ptr_t cluster_p_;
        // Senthil added this new member variable on Oct/02/2017 to store a redis cluster password.
        string password_;
        string key_;
        char *cmd_;
        int len_;
        CommandType type_;
    };
}

#endif /* defined(__libredisCluster__command__) */
