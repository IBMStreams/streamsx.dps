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
<li>Redis</li>
<li>Cassandra</li>
<li>IBM Cloudant</li>
<li>HBase</li>
<li>Mongo</li>
<li>Couchbase</li>
<li>Aerospike</li>
</ol>

There are plenty of details available about the installation, configuration, API description,
built-in example etc. Please refer to the com.ibm.streamsx.dps/doc/dps-usage-tips.txt file
for getting a good start in using this toolkit.  
