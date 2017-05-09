/**
 * Main purpose of this Java primitive operator is to explain how to use the dps (Distributed Process Store).
 * In order to use the dps functions in your own Java primitive operators, you can use this example as a
 * reference.
 * This class demonstrates how to 
 * Create a store (StoreFactory.createStore/StoreFactory.createOrGetStore)
 * Find a previously created store (StoreFactory.findStore)
 * Read from a store (StoreFactory.get)
 * Write to a store (StoreFactory.put)
 * See Main.spl for examples on how to iterate over the contents of a store
*/
package application;

import org.apache.log4j.Logger;

import com.ibm.streams.operator.AbstractOperator;
import com.ibm.streams.operator.OperatorContext;
import com.ibm.streams.operator.OutputTuple;
import com.ibm.streams.operator.StreamingData.Punctuation;
import com.ibm.streams.operator.StreamingInput;
import com.ibm.streams.operator.StreamingOutput;
import com.ibm.streams.operator.Tuple;
import com.ibm.streams.operator.model.InputPortSet;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.InputPorts;
import com.ibm.streams.operator.model.Libraries;
import com.ibm.streams.operator.model.OutputPortSet;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;
import com.ibm.streams.operator.model.OutputPorts;
import com.ibm.streams.operator.model.PrimitiveOperator;
import com.ibm.streams.operator.model.SharedLoader;
import com.ibm.streams.operator.types.RString;
import com.ibm.streamsx.dps.DistributedStores;
import com.ibm.streamsx.dps.Store;
import com.ibm.streamsx.dps.StoreException;
import com.ibm.streamsx.dps.StoreFactory;
import com.ibm.streamsx.dps.StoreFactoryException;

@PrimitiveOperator(name="TickerIdGenerator", namespace="application",
description="This operator demonstrates how to use the DPS (Distributed Process Store) API to demonstrates how to: " +
 " * Create a store (StoreFactory.createStore/StoreFactory.createOrGetStore)" +
 " * Find a previously created store (StoreFactory.findStore)" +
 " * Read from a store (StoreFactory.get)"+
 " * Write to a store (StoreFactory.put)" +
 " See Main.spl for examples on how to iterate over the contents of a store")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@OutputPorts({@OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
//Add the DPS toolkit's Java library (dps-helper.jar) to the path of this operator.
//There are 2 ways to do this:
//1. If your application will have access to the Streams install location at runtime, then you can specify the full path to the location of the dps-helper.jar file present inside the DPS toolkit as follows:
@Libraries("@STREAMS_INSTALL@/toolkits/com.ibm.streamsx.dps/impl/java/lib/dps-helper.jar")
//if that path will be accessible at runtime. 
//2. Or, you can copy the dps-helper.jar from <STREAMS_INTSALL>/toolkits/com.ibm.streamsx.dps/impl/java/lib/dps-helper.jar into the lib folder of this application and reference it as follows:
//@Libraries("lib/dps-helper.jar")

//Next, add the following annotation if you are going to fuse this Java operator with other Java operators that will also use
//the DPS APIs. In that case, it is necessary to add the following annotation so that the fused PE will use a shared class loader.
//This annotation is typically effective only when all the fused Java operators have exactly the 
//same entries in their @Libraries annotation (That rule is from Java and not from Streams). 
//If you don't have this annotation, it will give a runtime exception.
//
@SharedLoader(true)
public class TickerIdGenerator extends AbstractOperator {
	// Declare a member variable to hold the instance of the dps store factory object.
	private StoreFactory sf = null;
	// Declare a member variable to hold the instance of the distributed lock factory object.
	
    /**
     * Initialize this operator. Called once before any tuples are processed.
     * @param context OperatorContext for this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
	@Override
	public synchronized void initialize(OperatorContext context)
			throws Exception {
    	// Must call super.initialize(context) to correctly setup an operator.
		super.initialize(context);
		Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " initializing in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );

        
		//First, initialize the store API
		DistributedStores.initialize();

		// Next, get a store factory for creating/getting/finding/removing stores throughout the lifetime of this Java primitive operator.
        sf =DistributedStores.getStoreFactory();
	}

    /**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
    	// This method is commonly used by source operators. 
    	// Operators that process incoming tuples generally do not need this notification. 
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " all ports are ready in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
    }

    /**
     * Process an incoming tuple that arrived on the specified port.
     * <P>
     * Copy the incoming tuple to a new output tuple and submit to the output port. 
     * </P>
     * @param inputStream Port the tuple is arriving on.
     * @param tuple Object representing the incoming tuple.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public final void process(StreamingInput<Tuple> inputStream, Tuple tuple)
            throws Exception {

    	// Create a new tuple for output port 0
        StreamingOutput<OutputTuple> outStream = getOutput(0);
        OutputTuple outTuple = outStream.newTuple();

        // Copy across all matching attributes.
        outTuple.assign(tuple);

        // For each ticker received, query the Tickers_Store for its value 
        //and insert that value in a new store called "TickerId_Store" 
        Store t1s = null;
        try {
        	t1s = sf.findStore("Tickers_Store");
        } catch (StoreFactoryException sfe) {
        	// Unable to get the store.
        	System.out.println("Unable to get the Tickers_Store: Error code = " + 
        		sfe.getErrorCode() + ", Error Msg = " + sfe.getErrorMessage());
        	// Throw the same exception.
        	throw sfe;
        }

        Store t2s = null;
        // Let us create a brand new store called "TickerId_Store".
        try {
        	t2s = sf.createOrGetStore("TickerId_Store", "rstring", "uint64");
        } catch (StoreFactoryException sfe) {
        	// Unable to create a new store.
        	System.out.println("Unable to create the TickerId_Store: Error code = " + 
        		sfe.getErrorCode() + ", Error Msg = " + sfe.getErrorMessage());
        	// Throw the same exception.
        	throw sfe;
        }
        
    	RString tickerSymbol = new RString(tuple.getString("ticker_symbol"));
    	// Look up the company name for a given ticker symbol from the "Tickers_Store".
    	RString companyName = null;
    	try {
    		companyName = (RString)t1s.get(tickerSymbol);
    	} catch (StoreException se) {
    		String msg = "TickerIdGenerator: Unable to get the company name from Tickers_Store for ticker symbol " + 
    			tickerSymbol + ". Error code = " + se.getErrorCode() + ", Error Msg = " + se.getErrorMessage();
    		System.out.println(msg);
    		// Rethrow the same exception.
    		throw se;
    	}
    	
    	// Let us compute a unique hash code for this company name.
    	long tickerId = (long)companyName.hashCode();
    	// Put this away in the TickerId_Store now.
    	try {
    		t2s.put(tickerSymbol, tickerId);
    	} catch (StoreException se) {
    		String msg = "TickerIdGenerator: Unable to put the tickerId " + tickerId + 
				" for the ticker symbol " + tickerSymbol + " in the TickerId_Store. Error code = " + 
				se.getErrorCode() + ", Error Msg = " + se.getErrorMessage();
    		System.out.println(msg);
    		throw se;
    	}
    
        // We finished creating a new TickerId_Store and created/inserted "TickerSymbol => TickerId" entries in it.
        // Set any value to this attribute.
        // Send the success attribute to true to indicate ticker ids were generated successfully.
        outTuple.assign(tuple);
        // Submit new tuple to output port 0
        outStream.submit(outTuple);  
    }
    
    /**
     * Process an incoming punctuation that arrived on the specified port.
     * @param stream Port the punctuation is arriving on.
     * @param mark The punctuation mark
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public void processPunctuation(StreamingInput<Tuple> stream,
    		Punctuation mark) throws Exception {
    	// For window markers, punctuate all output ports 
    	super.processPunctuation(stream, mark);
    	getOutput(0).punctuate(mark);
    	
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        
        // TODO: If needed, close connections or release resources related to any external system or data store.

        // Must call super.shutdown()
        super.shutdown();
    }
    
    // This method provides an utility function for parsing and displaying the JSON results received while
    // executing the native HBase put/get commands in the process method above.
    public void displayHBaseTableRows(String jsonResponse) {
    	// You should properly parse it using a JSON library.
    	// i.e. via your own C++ or Java native functions that can parse JSON in the right way. 
    	// But, as a quick and dirty solution, we are going to use string token 
    	// parsing to pull out the K/V pairs from this JSON result message.
    	// Response will be in this format:
    	// {"Row":[{"key":"X","Cell":[{"column":"A","timestamp":1,"$":"a"}]},{"key":"Y","Cell":[{"column":"B","timestamp":1,"$":"b"}]}]}
    	//
    	int rowIdx1 = 0, rowIdx2 = 0, keyIdx1 = 0, keyIdx2 = 0, valueIdx1 = 0, valueIdx2 = 0;
    	while(true) {
    		// Find the next row.
    		rowIdx1 = jsonResponse.indexOf("{\"key\":", rowIdx2);
    		if (rowIdx1 == -1) {
    			// Row not found.
    			break;
    		} 
    		
    		// Parse the row key name.
    		String rowKey = "";
    		String base64DecodedString = "";
    		String columnKey = "";
    		String columnValue = "";
    		// Find the beginning of the row key name.
    		rowIdx1 = jsonResponse.indexOf(":\"", rowIdx1);
    		if (rowIdx1 == -1) {
    			break;
    		}
    		
    		// Find the end of the row key name.
    		rowIdx2 = jsonResponse.indexOf("\",", rowIdx1);
    		if (rowIdx2 == -1) {
    			break;
    		}
    		
    		// Parse the row key name
    		rowKey = jsonResponse.substring(rowIdx1+2, rowIdx2);
    		try {
    			base64DecodedString = sf.base64Decode(rowKey);
    		} catch (StoreFactoryException sfe) {
    			;
    		}
    		System.out.print(base64DecodedString + "-->");
    		boolean firstColumnKeyFound = false;
    		
    		// We got our row key. Let us now collect all the K/V pairs stored in this row.
    		while(true) {
    			// Locate the key field.
    			keyIdx1 = jsonResponse.indexOf("column\":\"", keyIdx2);
    			if (keyIdx1 == -1) {
    				break;
    			}
    			
    			// Find the end of the key field.
    			keyIdx2 = jsonResponse.indexOf("\",", keyIdx1);
    			if (rowIdx2 == -1) {
    				break;
    			}
    			
    			// Parse the column key name.
    			columnKey = jsonResponse.substring(keyIdx1+9, keyIdx2);
    			try {
    				base64DecodedString = sf.base64Decode(columnKey);
    			} catch (StoreFactoryException sfe) {
    				;
    			}
    			// If there are more than one K/V pair, then dispaly them with comma to separate them.
    			if (firstColumnKeyFound == true) {
    				System.out.print(", ");
    			}
    			// Decoded column key will have the "cf1:" prefix. Let us not display that prefix.
    			System.out.print(base64DecodedString.substring(4) + ":");
    			firstColumnKeyFound = true;
    			
    			// Locate the value field.
    			valueIdx1 = jsonResponse.indexOf("$\":\"", valueIdx2);
    			if (valueIdx1 == -1) {
    				break;
    			}
    			
    			// Find the end of the value field.
    			valueIdx2 = jsonResponse.indexOf("\"}", valueIdx1);
    			if (valueIdx2 == -1) {
    				break;
    			}
    			
    			// Parse the column value name.
    			columnValue = jsonResponse.substring(valueIdx1+4, valueIdx2);
    			try {
    				base64DecodedString = sf.base64Decode(columnValue);
    			} catch (StoreFactoryException sfe) {
    				;
    			}
    			System.out.print(base64DecodedString);
    			
    			// If there are no more K/V pairs beyond this, let us skip this loop.
    			if (jsonResponse.charAt(valueIdx2+2) == ']') {
    				System.out.println("");
    				break;
    			}
    		}
    	} 
    }    
}
