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
<li>Aerospike</li>
<li>Redis-Cluster   [New cluster feature is available in Redis version 3 and above]</li>
</ol>

There are plenty of details available about the installation, configuration, API description,
built-in example etc. Please refer to the com.ibm.streamsx.dps/doc/dps-usage-tips.txt file
for getting a good start in using this toolkit.  

Things to consider when working with this toolkit:
* [The messages and the NLS for toolkits](https://github.com/IBMStreams/administration/wiki/Messages-and-National-Language-Support-for-toolkits)

To learn more about Streams:
* [IBM Streams on Github](http://ibmstreams.github.io)
* [Introduction to Streams Quick Start Edition](http://ibmstreams.github.io/streamsx.documentation/docs/4.1/qse-intro/)
* [Streams Getting Started Guide](http://ibmstreams.github.io/streamsx.documentation/docs/4.1/qse-getting-started/)
* [StreamsDev](https://developer.ibm.com/streamsdev/)
