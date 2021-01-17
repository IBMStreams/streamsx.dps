## Overview ##

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
</ol>

This toolkit evolved from the early research work done at the IBM.T.J.Watson Research Center, Yorktown Heights, New York. The links below highlight the origins of this asset.

[Technical Article](https://www.ibm.com/developerworks/library/bd-streamsmemcached/)

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

