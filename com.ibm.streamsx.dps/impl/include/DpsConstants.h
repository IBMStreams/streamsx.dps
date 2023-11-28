/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2023
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
#ifndef DPS_CONSTANTS_H_
#define DPS_CONSTANTS_H_
/*
============================================================
Define all the constants used in the glue code required to
interface with many different back-end in-memory stores.
============================================================
*/
#define DPS_AND_DL_GUID_KEY											"dps_and_dl_guid"
#define DPS_LOCK_TOKEN												"dps_lock"
#define DL_LOCK_TOKEN												"dl_lock"
#define GENERIC_LOCK_TOKEN											"generic_lock"
#define DPS_TTL_STORE_TOKEN											"dps_ttl_kv_global_store" // Used in Cassandra, HBase, Mongo, Couchbase, Aerospike.
#define DPS_DL_META_DATA_DB											"dps_dl_meta_data"  // Used in Cloudant, Mongo, Couchbase
#define DPS_DL_META_DATA_SET										"dps_dl_meta_data"  // Used in Aerospike
#define DPS_STORE_ID_TRACKER_SET									"dps_store_id_tracker" // Used in Aerospike
#define DPS_STORE_NAME_TYPE											"0"
#define DPS_STORE_INFO_TYPE											"1" // Used in memcached
#define DPS_STORE_CONTENTS_HASH_TYPE								                "1" // Used in Redis
#define DPS_STORE_CATALOG_TYPE										        "2" // Used only in memcached
#define DPS_STORE_DATA_ITEM_TYPE									        "3"
#define DPS_STORE_ORDERED_KEYS_SET_TYPE                                                                         "101" // Used in Redis
#define DPS_STORE_LOCK_TYPE											"4"
#define DL_LOCK_NAME_TYPE											"5"
#define DL_LOCK_INFO_TYPE											"6"
#define DL_LOCK_TYPE												"7"
#define GENERAL_PURPOSE_LOCK_TYPE									"501"
#define REDIS_SERVER_PORT											6379
#define AEROSPIKE_SERVER_PORT										3000
#define REDIS_STORE_ID_TO_STORE_NAME_KEY							"dps_name_of_this_store"
#define CASSANDRA_STORE_ID_TO_STORE_NAME_KEY						"dps_name_of_this_store"
#define CLOUDANT_STORE_ID_TO_STORE_NAME_KEY							"dps_name_of_this_store"
#define HBASE_STORE_ID_TO_STORE_NAME_KEY							"dps_name_of_this_store"
#define MONGO_STORE_ID_TO_STORE_NAME_KEY							"dps_name_of_this_store"
#define COUCHBASE_STORE_ID_TO_STORE_NAME_KEY						"dps_name_of_this_store"
#define AEROSPIKE_STORE_ID_TO_STORE_NAME_KEY						"dps_name_of_this_store"
#define REDIS_SPL_TYPE_NAME_OF_KEY									"dps_spl_type_name_of_key"
#define CASSANDRA_SPL_TYPE_NAME_OF_KEY								"dps_spl_type_name_of_key"
#define CLOUDANT_SPL_TYPE_NAME_OF_KEY								"dps_spl_type_name_of_key"
#define HBASE_SPL_TYPE_NAME_OF_KEY									"dps_spl_type_name_of_key"
#define MONGO_SPL_TYPE_NAME_OF_KEY									"dps_spl_type_name_of_key"
#define COUCHBASE_SPL_TYPE_NAME_OF_KEY								"dps_spl_type_name_of_key"
#define AEROSPIKE_SPL_TYPE_NAME_OF_KEY								"dps_spl_type_name_of_key"
#define REDIS_SPL_TYPE_NAME_OF_VALUE								"dps_spl_type_name_of_value"
#define CASSANDRA_SPL_TYPE_NAME_OF_VALUE							"dps_spl_type_name_of_value"
#define CLOUDANT_SPL_TYPE_NAME_OF_VALUE								"dps_spl_type_name_of_value"
#define HBASE_SPL_TYPE_NAME_OF_VALUE								"dps_spl_type_name_of_value"
#define MONGO_SPL_TYPE_NAME_OF_VALUE								"dps_spl_type_name_of_value"
#define COUCHBASE_SPL_TYPE_NAME_OF_VALUE							"dps_spl_type_name_of_value"
#define AEROSPIKE_SPL_TYPE_NAME_OF_VALUE							"dps_spl_type_name_of_value"
#define HBASE_MAX_TTL_VALUE											"788400000" // Number of seconds in a 25 year period.
#define MEMCACHED_STORE_INFO_DELIMITER								"_^^_" // Used only in memcached
#define REDIS_EXISTS_CMD											"exists "
#define REDIS_SETNX_CMD												"setnx "
#define REDIS_INCR_CMD												"incr "
#define REDIS_SET_CMD												"set "
#define REDIS_DEL_CMD												"del "
#define REDIS_GET_CMD												"get "
#define REDIS_APPEND_CMD											"append "
#define REDIS_EXPIRE_CMD											"expire "
#define REDIS_PSETEX_CMD											"psetex "  // Set expiration in partial seconds i.e. millisecs
#define REDIS_SETX_CMD												"setex "   // Set expiration in seconds
#define REDIS_SADD_CMD												"sadd "
#define REDIS_SREM_CMD												"srem "
#define REDIS_SDIFFSTORE_CMD										"sdiffstore "
#define REDIS_SCARD_CMD												"scard "
#define REDIS_HSET_CMD												"hset "
#define REDIS_HEXISTS_CMD											"hexists "
#define REDIS_HGET_CMD												"hget "
#define REDIS_HDEL_CMD												"hdel "
#define REDIS_HLEN_CMD												"hlen "
#define REDIS_HKEYS_CMD												"hkeys "
#define REDIS_AUTH_CMD                                                                                          "auth "
#define REDIS_ZADD_CMD                                                                                          "zadd "
#define REDIS_ZREM_CMD                                                                                          "zrem "
#define REDIS_ZRANGE_CMD                                                                                        "zrange "
#define REDIS_HMSET_CMD                                                                                         "hmset "
#define REDIS_HMGET_CMD                                                                                         "hmget "
#define REDIS_ZMSCORE_CMD                                                                                       "zmscore "
#define REDIS_NX_OPTION                                                                                         "NX"
#define REDIS_EX_OPTION                                                                                         "EX"
#define REDIS_PX_OPTION                                                                                         "PX"    
#define CASSANDRA_DPS_KEYSPACE										"com_ibm_streamsx_dps"
#define CASSANDRA_DPS_MAIN_TABLE									"t1"
#define HBASE_DPS_MAIN_TABLE										"dps_t1"
#define MONGO_DPS_DB_NAME											"ibm_dps" // Used in Mongo
#define AEROSPIKE_DPS_NAMESPACE										"ibm_dps" // Used in Aerospike
#define DPS_TOKEN													"dps"
#define CASSANDRA_CONCURRENT_ERROR_MSG								"Column family ID mismatch"
#define MEMCACHED_NO_SQL_DB_NAME									"memcached"
#define REDIS_NO_SQL_DB_NAME										"redis"
#define CASSANDRA_NO_SQL_DB_NAME									"cassandra"
#define CLOUDANT_NO_SQL_DB_NAME										"cloudant"
#define HBASE_NO_SQL_DB_NAME										"hbase"
#define MONGO_NO_SQL_DB_NAME										"mongo"
#define COUCHBASE_NO_SQL_DB_NAME									"couchbase"
#define AEROSPIKE_NO_SQL_DB_NAME									"aerospike"
#define REDIS_CLUSTER_NO_SQL_DB_NAME								"redis-cluster"
#define REDIS_CLUSTER_PLUS_PLUS_NO_SQL_DB_NAME                                                  "redis-cluster-plus-plus"
#define HTTP_GET													"GET"
#define HTTP_PUT													"PUT"
#define HTTP_POST													"POST"
#define HTTP_DELETE													"DELETE"
#define HTTP_COPY													"COPY"
#define HTTP_HEAD													"HEAD"
#define COUCHBASE_WEB_ADMIN_PORT									"8091"
#define COUCHBASE_DESIGN_DOC_VIEW_PORT								"8092"
#define COUCHBASE_RAM_BUCKET_QUOTA_IN_MB							"100"  // In Megabytes per bucket per machine.
#define COUCHBASE_THREAD_COUNT_PER_BUCKET							"3"
#define COUCHBASE_META_DATA_BUCKET_QUOTA_IN_MB						"100"
#define COUCHBASE_TTL_BUCKET_QUOTA_IN_MB							"200"

// This constant tells how many bytes of "comma separated based64 encoded key data" a catalog segment can hold.
// memcached has a default max size for a data item to be 1MB. During the server start-up, it can be
// configured to go upto 128MB. Redis supports a maximum data item length of 512MB.
// In our case, let us stick to the 1MB max limit to get reasonable speed while parsing/manipulating a long string..
#define DPS_MAX_CATALOG_SEGMENT_SIZE								512*1024 // Keep it within 1000 KB.
// 5 seconds TTL for a store lock? It is too much. Hopefully, we can release the lock a lot sooner than 5 seconds.
#define DPS_AND_DL_GET_LOCK_TTL										5 // In seconds
#define DPS_AND_DL_GET_LOCK_SLEEP_TIME								200 // In Microseconds
#define DPS_AND_DL_GET_LOCK_MAX_RETRY_CNT							10000
#define CLOUDANT_DB_CREATED											201
#define CLOUDANT_DB_EXISTS											412
#define CLOUDANT_DB_DELETED											200
#define CLOUDANT_DB_NOT_FOUND										404
#define CLOUDANT_DOC_CREATED										201
#define CLOUDANT_DOC_ACCEPTED_FOR_WRITING							202
#define CLOUDANT_DOC_EXISTS											409
#define CLOUDANT_DOC_RETRIEVED										200
#define CLOUDANT_DOC_NOT_FOUND										404
#define CLOUDANT_DOC_FIELD_NOT_FOUND								4041 // We want it to be a very high number since it is our custom error code.
#define CLOUDANT_REV_FIELD_NOT_FOUND								4042 // We want it to be a very high number since it is our custom error code.
#define HBASE_REST_OK												200
#define HBASE_TABLE_CREATION_OK										201
#define HBASE_REST_NOT_FOUND										404
#define HBASE_REST_ERROR											500
#define HBASE_CELL_VALUE_NOT_FOUND									4041 // We want it to be a very high number since it is our custom error code.
#define HBASE_COLUMN_KEY_NOT_FOUND									4042 // We want it to be a very high number since it is our custom error code.
#define MONGO_DUPLICATE_KEY_FOUND									11000
#define MONGO_MAX_TTL_VALUE											788400000 //  Number of seconds in a 25 year period.
#define COUCHBASE_MAX_TTL_VALUE										788400000 //  Number of seconds in a 25 year period.
#define AEROSPIKE_MAX_TTL_VALUE										788400000 //  Number of seconds in a 25 year period.
#define COUCHBASE_BUCKET_PROXY_BASE_PORT							11215 // We will add a unique number to this base port for every bucket.
#define COUCHBASE_REST_OK											200
#define COUCHBASE_BUCKET_DESIGNDOC_CREATED_OK						201
#define COUCHBASE_REST_ACCEPTED										202
#define COUCHBASE_DOC_FIELD_NOT_FOUND								4041 // We want it to be a very high number since it is our custom error code.

#define DPS_CLOUDANT_DB_LEVEL_COMMAND								1   // Used in Cloudant
#define DPS_CLOUDANT_DOC_LEVEL_COMMAND								2   // Used in Cloudant
#define DPS_RUN_DATA_STORE_COMMAND_ERROR							99
#define DPS_NO_ERROR												0
#define DPS_AND_DL_GET_LOCK_BACKOFF_DELAY_MOD_FACTOR				100
#define DPS_INITIALIZE_ERROR										101
#define DPS_CONNECTION_ERROR										102
#define DPS_GUID_CREATION_ERROR										103
#define DPS_STORE_NAME_CREATION_ERROR								104
#define DPS_STORE_INFO_CREATION_ERROR								105 // Used in memcached
#define DPS_STORE_HASH_METADATA1_CREATION_ERROR						105 // Used in Redis
#define DPS_STORE_CATALOG_CREATION_ERROR							106
#define DPS_DATA_ITEM_WRITE_ERROR									107
#define DPS_DATA_ITEM_READ_ERROR									108
#define DPS_STORE_EXISTS											109
#define DPS_STORE_DOES_NOT_EXIST									110
#define DPS_STORE_CATALOG_READ_ERROR								111
#define DPS_STORE_CATALOG_WRITE_ERROR								112
#define DPS_DATA_ITEM_DELETE_ERROR									113
#define DPS_GET_STORE_ID_ERROR										114
#define DPS_GET_STORE_INFO_ERROR									115 // Used for memcached
#define DPS_GET_STORE_CONTENTS_HASH_ERROR							115 // User for Redis
#define DPS_GET_STORE_LOCK_ERROR									116
#define DPS_GET_STORE_CATALOG_COUNT_ERROR							117
#define DPS_GET_STORE_NAME_ERROR									118
#define DPS_STORE_CATALOG_SEGMENT_CREATION_ERROR					119
#define DPS_STORE_NEW_CATALOG_SEGMENT_UNAVAILABLE_ERROR				120
#define DPS_STORE_INFO_UPDATE_ERROR									121
#define DPS_GET_DATA_ITEM_CATALOG_INDEX_ERROR						122
#define DPS_STORE_INFO_RESET_ERROR									123
#define DPS_STORE_CATALOG_RESET_ERROR								124
#define DPS_GET_DATA_ITEM_MALLOC_ERROR								125
#define DPS_CATALOG_SEGMENT_INDEX_PARSING_ERROR						126
#define DPS_PUT_DATA_ITEM_MALLOC_ERROR								127
#define DPS_DATA_ITEM_FAKE_READ_ERROR								128
#define DPS_STORE_KEY_NOT_FOUND_IN_CATALOG_AFTER_DELETION_ERROR		129
#define DPS_STORE_ITERATION_MALLOC_ERROR							130
#define DPS_STORE_ITERATION_DELETION_ERROR							131
#define DPS_GET_GENERIC_LOCK_ERROR									132
#define DPS_KEY_EXISTENCE_CHECK_ERROR								133
#define DPS_STORE_KEYS_SET_CREATION_ERROR							134
#define DPS_STORE_KEYS_SET_ADD_KEY_ERROR							135
#define DPS_STORE_KEYS_SET_REMOVE_KEY_ERROR							136
#define DPS_STORE_EXISTENCE_CHECK_ERROR								137
#define DPS_USE_OF_RESERVED_KEY_ERROR								138
#define DPS_STORE_CLEARING_ERROR									139
#define DPS_GET_STORE_SIZE_ERROR									140
#define DPS_GET_STORE_DATA_ITEM_KEYS_ERROR							141
#define DPS_GET_STORE_DATA_ITEM_KEYS_AS_AN_ARRAY_ERROR				142
#define DPS_INVALID_STORE_ID_ERROR									143
#define DPS_STORE_EMPTY_ERROR										144
#define DPS_STORE_HASH_METADATA2_CREATION_ERROR						145 // Used in Redis
#define DPS_STORE_HASH_METADATA3_CREATION_ERROR						146 // Used in Redis
#define DPS_GET_KEY_SPL_TYPE_NAME_ERROR								147
#define DPS_GET_VALUE_SPL_TYPE_NAME_ERROR							148
#define DPS_CASSANDRA_STORE_TABLE_CREATION_ERROR					149 // Used in Cassandra
#define DPS_TOO_MANY_REDIS_SERVERS_CONFIGURED						150
#define DPS_MISSING_CLOUDANT_ACCESS_URL								151 // Used in Cloudant
#define DPS_STORE_DB_CREATION_ERROR									152 // Used in Cloudant
#define DPS_TTL_NOT_SUPPORTED_ERROR									153 // Used in Cloudant
#define DPS_HTTP_REST_API_ERROR										154 // Used in Cloudant
#define DPS_MISSING_HBASE_ACCESS_URL								151 // Used in HBase
#define DPS_STORE_REMOVAL_ERROR										152 // Used in Couchbase, Aerospike
#define DPS_STORE_CLEAR_NULL_KEY_ERROR								153 // Used in Aerospike
#define DPS_STORE_ITERATION_NULL_KEY_ERROR							154 // Used in Aerospike
#define DPS_MAKE_DURABLE_ERROR										155
#define DPS_STORE_FATAL_ERROR										156
#define DPS_STORE_UNKNOWN_STATE_ERROR								157
#define DPS_AUTHENTICATION_ERROR                                                                158
#define DPS_NEGATIVE_KEY_START_POS_ERROR                                                        159
#define DPS_INVALID_NUM_KEYS_NEEDED_ERROR                                                       160
#define DPS_REDIS_REPLY_NULL_ERROR                                                              161
#define DPS_REDIS_REPLY_NIL_ERROR                                                               162
#define DPS_EMPTY_DATA_ITEM_VALUE_FOUND_ERROR                                                   163
#define DPS_BULK_PUT_HSET_ERROR                                                                 164
#define DPS_BULK_PUT_ZADD_ERROR                                                                 165
#define DPS_BULK_GET_HMGET_ERROR                                                                166
#define DPS_BULK_GET_HMGET_NO_REPLY_ARRAY_ERROR                                                 167
#define DPS_BULK_GET_HMGET_MALLOC_ERROR                                                         168
#define DPS_BULK_GET_HMGET_EMPTY_VALUE_ERROR                                                    169
#define DPS_BULK_GET_ZMSCORE_ERROR                                                              170
#define DPS_BULK_GET_ZMSCORE_NO_REPLY_ARRAY_ERROR                                               171
#define DPS_BULK_GET_ZMSCORE_EMPTY_VALUE_ERROR                                                  172
#define DPS_BULK_REMOVE_HDEL_ERROR                                                              173
#define DPS_BULK_REMOVE_ZREM_ERROR                                                              174
#define DPS_BULK_REMOVE_CNT_MISMATCH_ERROR                                                      175
#define DPS_BULK_REMOVE_HDEL_NO_INTEGER_REPLY_ERROR                                             176
#define DPS_BULK_REMOVE_ZREM_NO_INTEGER_REPLY_ERROR                                             177
#define DPS_GET_VALUES_EMPTY_KEYS_LIST_ERROR                                                    178
#define DPS_PUT_KV_PAIRS_KEYS_VALUES_LISTS_NOT_OF_SAME_SIZE_ERROR                               179
#define DPS_HAS_KEYS_EMPTY_KEYS_LIST_ERROR                                                      180
#define DPS_REMOVE_KEYS_EMPTY_KEYS_LIST_ERROR                                                   181

#define DL_CONNECTION_ERROR										501
#define DL_GET_LOCK_ID_ERROR										502
#define DL_GUID_CREATION_ERROR										503
#define DL_LOCK_NAME_CREATION_ERROR									504
#define DL_LOCK_INFO_CREATION_ERROR									505
#define DL_GET_DISTRIBUTED_LOCK_ERROR								506
#define DL_GET_LOCK_INFO_ERROR										507
#define DL_GET_LOCK_NAME_ERROR										508
#define DL_LOCK_INFO_UPDATE_ERROR									509
#define DL_GET_LOCK_ERROR											510
#define DL_LOCK_RELEASE_ERROR										511
#define DL_INVALID_LOCK_ID_ERROR									512
#define DL_GET_LOCK_TIMEOUT_ERROR									513
#define DL_LOCK_NOT_FOUND_ERROR										514
#define DL_LOCK_REMOVAL_ERROR										515
#define AEROSPIKE_GET_STORE_ID_ERROR								516
#define REDIS_PLUS_PLUS_NO_ERROR 0
#define REDIS_PLUS_PLUS_CONNECTION_ERROR 1
#define REDIS_PLUS_PLUS_REPLY_ERROR 2
#define REDIS_PLUS_PLUS_OTHER_ERROR 3

#endif /* DPS_CONSTANTS_H_ */
