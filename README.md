## Distributed Process Store (DPS) ##

## Note from the toolkit author
This toolkit created by Senthil Nathan is a differentiator for key customers. He is now an independent software consultant. To benefit from the compelling features of this asset, for any enhancements, any need for a new powerful toolkit as well as for creating new data streaming solutions, customers can email senthil@moonraytech.com to reach him. Thank you.

## Purpose
The IBMStreams/streamsx.dps GitHub repository is home to the Streams toolkit named
DPS (Distributed Process Store). This Streams toolkit allows a simple way for the
SPL, C++ and Java operators belonging to a single or multiple applications to share
the application specific state information via an external K/V store. It does this
via a collection of APIs that can be called from any part of the SPL, C++ and Java
operator code.

Following are the external NoSQL K/V stores that can be configured to work with the
DPS toolkit for the purpose of sharing application state in a distributed manner.

<ol>
<li>Memcached</li>
<li>Redis           [version 2.x that doesn't have a built-in cluster feature]</li>
<li>Cassandra</li>
<li>IBM Cloudant</li>
<li>HBase</li>
<li>Mongo</li>
<li>Couchbase</li>
<li>Redis-Cluster   [New cluster feature is available in Redis version 3 and above]</li>
<li>Valkey</li>
</ol>

#### February/05/2025: Note about Redis and Valkey. ####
In March/2024, the Redis company switched the licensing for the Redis core code repository from the free of cost BSD license to for-fee proprietary licenses. This is in effect for any Redis version higher than 7.2.5. This prompted a large portion of the user and developer community, led by the Linux Foundation, to fork the Redis code v7.2.5 under the new name Valkey, retaining the BSD license. Valkey looks and works exactly like redis except for its name and its free of cost availability as open source.

DPS toolkit supports redis as before for the free versions up to v7.2.5 and any redis proprietary paid versions higher than v7.2.5. DPS toolkit creator Senthil Nathan completed the testing to ensure that DPS toolkit also supports Valkey v8.0.2 and higher. DPS toolkit users can install Valkey single server or a cluster with TLS or non-TLS and password or no-password options and use the DPS toolkit configuration as before with redis or redis-cluster or redis-cluster-plus-plus as the backend database name. No code changes will be required in the DPS enabled applications for them to work with Valkey.


## DPS toolkit origins ##
This toolkit evolved from the early research work done at the IBM.T.J.Watson Research Center, Yorktown Heights, New York. The links below highlight the origins of this asset.

[Technical Positioning](dps-technical-positioning.pdf)

[Podcast](http://public.dhe.ibm.com/software/dw/podcast/twodw/twodw20140127.mp3)

## Documentation ##

For an API description see the [GitHub pages](http://ibmstreams.github.io/streamsx.dps) for this toolkit.
The documentation is also available locally in the doc folder, after building the toolkit.
For configuration hints and other information, see the [Wiki pages](https://github.com/IBMStreams/streamsx.dps/wiki)

Other things to consider when working with this toolkit:
* [The messages and the NLS for toolkits](https://github.com/IBMStreams/administration/wiki/Messages-and-National-Language-Support-for-toolkits)

## Building the toolkit ##

To build the toolkit perform the following steps:

1. Clone the repository   
   `git clone https://github.com/IBMStreams/streamsx.dps.git`
2. Build the toolkit   
   `make clean all`

Prerequisuites   
you need to have the following rpm packages installed on the build machine:
```
curl
curl-devel
lua
lua-devel
openldap-devel
openssl-devel
cyrus-sasl
cyrus-sasl-devel
```

## Learn more about Streams ##
* [IBM Streams on Github](http://ibmstreams.github.io)
* [Introduction to Streams Quick Start Edition](http://ibmstreams.github.io/streamsx.documentation/docs/4.3/qse-intro/)
* [Streams Getting Started Guide](http://ibmstreams.github.io/streamsx.documentation/docs/4.3/qse-getting-started/)

