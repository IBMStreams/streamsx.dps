/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2014
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface file where all the available dps store methods are declared.
// A store implementation class will provide concrete logic for these methods.
package com.ibm.streamsx.dps;

import java.nio.ByteBuffer;
import java.util.Iterator;

/***
 * 
 * Represents a user created distributed store.
 * Methods are available for adding and removing elements from the store.
 * <pre><code>
StoreFactory sf = DistributedStores.getStoreFactory();
Store store = null;
try {
   //specify the SPL types for the keys and values in the store
   String keyType = "rstring";
   String valueType = "int32";
   store = sf.createOrGetStore("Java Test Store1", keyType, valueType);
} catch (StoreFactoryException sfe) {
 throw sfe;
  //or use	sfe.getErrorCode() and  sfe.getErrorMessage()) for more info
}


//once ready to access the store,
//get a lock for the store, may have previously been created
LockFactory lf = DistributedLocks.getLockFactory(); 
Lock myLock = lf.createOrGetLock("Lock_For_Test_Store1");

// Acquire the lock
try {
 	myLock.acquireLock();
} catch (LockException le) {
    throw le;
}

//perform store operations
store.put("IBM", 39);
store.put("Lenovo", 50);
//release the lock  when finished
myLock.releaseLock(); 
	</code></pre>
 * Iterating over the elements in the store is possible using the <code>iterator</code> method,
 * or in a simple "foreach" statement as follows: 
	  <pre>
	   <code>
for (KeyValuePair kv: store) {
  System.out.println("'" + kv.getKey() + "' =&gt; '" + kv.getValue() + "'");
}
		</code></pre>	

 *
 */
public interface Store extends Iterable<KeyValuePair> {
	public long getId();
	/**Insert a key/value pair into the store. The types of your key and value must be the same ones that were provided at the time of creating the store.
	 *  If you don't follow that rule, your store operations will produce undesired results.
	 *  The types of <code>key</code> and <code>value</code> must be the same as the types specified when the store was created.
	 *  This method does not perform any internal safety checks.
	 *
	 * 
	 * <pre>
	 * <code>
long tickerId = 46267353;
String tickerSymbol = "IBM";

try {
   store.put(tickerSymbol, tickerId);
} catch (StoreException se) {
   throw se;
}
</code>
	 * </pre>
	 * @param <T1>  the key to store with the value
	 * @param <T2> value associated with the key
	 * @return true on success, false otherwise
	 * @throws StoreException on failure or error.
	 */
	public <T1, T2> boolean put(T1 key, T2 value) throws StoreException;
	/** Same function as put, but slower due to overhead arising from internal safety checks.
	 *  * @param <T1> the key to store with the value
	 * @param <T2> value associated with the key
	 * @return true on success, false otherwise
	 * @throws StoreException on failure or error.
	 * */
	public <T1, T2> boolean putSafe(T1 key, T2 value) throws StoreException;
	/**Get the value associated with the given key.
	 * This method performas faster than getSafe because it does not do any internal safety checks.
	 * @param <T1> the key to look up
	 * @return the saved value associated with the key
	 * @throws StoreException if an error occurs
	 */
	public <T1> Object get(T1 key) throws StoreException;
	/**Similar to <code>get</code> but with higher overhead arising from internal safety checks.
	 * * @param <T1> the key to look up
	 * @return the saved value associated with the key
	 * @throws StoreException if an error occurs*/
	public <T1> Object getSafe(T1 key) throws StoreException;
	/**Removes the given key from the store.
	 * @param <T1>  the key to remove
	 * @return true if the key was successfully removed
	 * @throws StoreException if an error occurs.
	 */
	public <T1> boolean remove(T1 key) throws StoreException;
	/***Check if the store contains a value for the given key
	 * 
	 * @param <T1>  the key to look up
	 * @return whether or not if the store has a value for the key
	 * @throws StoreException if an error occurs.
	 */
	public <T1> boolean has(T1 key) throws StoreException;
	/**
	 * Deletes all the key-value pairs in this store.
	 * @throws StoreException if an error occurs.
	 */
	public void clear() throws StoreException;
	/**@return the total number of K/V pairs stored in this store
	 * @throws StoreException if an error occurs clearing the store.
	 */
	public long size() throws StoreException;
	/**Serialize the contents of this store into a {@link ByteBuffer}.
	 * The resulting ByteBuffer can then be used to recreate all the K/V pairs into another store.
	 * This is a useful technique for copying an entire store into a different store.
	 * See <code>deserialize()</code> for a complete example
	 * @return a <code>ByteBuffer</code> containing the serialized contents of the store.
	 * @throws StoreException if an error occurs during  serialization
	 */
	public ByteBuffer serialize() throws StoreException;
/**
 * Deserializes a {@link ByteBuffer} containing one or more serialized key-value pairs into this store, thereby populating it with those pairs. 
 * 
 * @param data {@link ByteBuffer} containing the serialized data 
 * @throws StoreException if an error occurs.
 * <pre><code>
//create a store 
Store topBrandsStore = sf.createOrGetStore("ABC", "int32", "ustring");
// Add few data items.
topBrandsStore.put(3, "Thinkpad");
topBrandsStore.put(4, "Watson");
// Serialize this entire store now.
ByteBuffer serializedStore = null;

try {
   serializedStore = topBrandsStore.serialize();
} catch (StoreException se) {
   System.out.println("Problem in serializing the store ");
      throw se;
}

// Deserialize the byte buffer containing the serialized store contents and populate a brand new store.
// Remove the top brands store to demonstrate that deserialization is independent of the original store
sf.removeStore(topBrandsStore);
topBrandsStore = null;
//create a new store
Store newStore = sf.createOrGetStore("XYZ", "int32", "ustring");
System.out.println("Size of the 'XYZ' store before deserialize: " + newStore.size());
// We are going to populate this empty store by deserializing the serialized content into this new store.
try {
   newStore.deserialize(serializedStore);
   System.out.println("Successfully deserialized into the store 'XYZ'");
} catch (StoreException se) {
   System.out.println("Problem in deserializing the byte buffer into the store 'XYZ'. Error code = " +
      se.getErrorCode() + ", Error msg = " + se.getErrorMessage());
   throw se;        	
}

System.out.println("Size of the 'XYZ' store after deserialize: " + newStore.size());
</code></pre>
 * 
 * 
 */
	public void deserialize(ByteBuffer data) throws StoreException;
	/**@return  store name provided at store creation*/
	public String getStoreName();
	/**@return the SPL type for the keys in the store**/
	public String getKeySplTypeName();
	/**@return  the SPL type for each value in the store**/
	public String getValueSplTypeName();
	/**
	 * @return a {@link StoreIterator}, which is an iterator over this store's contents, where each
	 * key and value is represented by a {@link KeyValuePair}.  
	 * The returned iterator is a subclass of {@link Iterator}
	 * Note that this is one way to iterate over the store's contents,
	  Iteration could also be done using a "foreach" statement.
	 */
	public StoreIterator iterator();
}
