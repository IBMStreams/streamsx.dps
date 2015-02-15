/*
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2011, 2013
# US Government Users Restricted Rights - Use, duplication or
# disclosure restricted by GSA ADP Schedule Contract with
# IBM Corp.
*/
// This is an interface that will be implemented by the StoreIteratorImpl class.
package com.ibm.streamsx.dps;

import java.util.Iterator;
import com.ibm.streamsx.dps.KeyValuePair;

public interface StoreIterator extends Iterator<KeyValuePair> {
	// This interface doesn't have any methods of its own other than the ones from the Iterator interface.
}
