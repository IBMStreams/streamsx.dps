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
import com.ibm.streamsx.dps.StoreFactory;

@PrimitiveOperator(name="DataStoreTester", namespace="application",
description="This operator is a no-op.  It exists for the purpose of demonstrating how to successfully fuse two Java operators that use the DPS toolkit.  Because it is fused with the TickerIdGenerator operator, the SharedLoader annotation is used in both classes. See the operators' source for more information.")
@InputPorts({@InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})

//Add the DPS toolkit's Java library (dps-helper.jar) to the path of this operator.
//There are 2 ways to do this:
//1. If your application will have access to the Streams install location at runtime, then you can specify the full path to the location of the dps-helper.jar file present inside the DPS toolkit as follows:
//@Libraries("@STREAMS_INSTALL@/toolkits/com.ibm.streamsx.dps/impl/java/lib/dps-helper.jar")
//if that path will be accessible at compile time and at runtime. 

//2. Or, you can copy the dps-helper.jar either from the <STREAMS_INTSALL>/toolkits/com.ibm.streamsx.dps/impl/java/lib/dps-helper.jar or from a more recent version of the DPS toolkit directory into the impl/java/lib folder of this application and reference it as follows. You will have to create the lib sub-directory inside impl/java of this application before copying the jar file there. It is done in the Makefile of this example. If you choose this option, then you must ensure that the @Libraries shown above is commented out and the following one is uncommented.
@Libraries("impl/java/lib/dps-helper.jar")

// Add the following annotation if you are going to fuse this Java operator with other Java operators that will also use
// the DPS APIs. In that case, it is necessary to add the following annotation so that the fused PE will use a shared class loader.
// This annotation is typically effective only when all the fused Java operators have exactly the 
// same entries in their @Libraries annotation. 
// If you don't have this annotation, it will give a runtime exception.
//
@SharedLoader(true)
public class DataStoreTester extends AbstractOperator {
	// Declare a member variable to hold the instance of the dps store factory object.
	private StoreFactory sf = null;
	
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
        
        //Call the initialize method before trying to get a store factory
     	DistributedStores.initialize();
        sf = DistributedStores.getStoreFactory();
        // Get the details about the machine where this operator is running.
        // This method returns a String array with 3 elements.
        // Each element will carry the value for machine name, os version, cpu architecture in that order.
        String [] machineDetails = new String[3];
        machineDetails = sf.getDetailsAboutThisMachine();
        // Display the NoSQL DB product name being used for this test run.
        System.out.println("=====================================================");
        System.out.println("Details about this DPS client machine:");
        String dbProductName = sf.getNoSqlDbProductName();

        System.out.println("NoSQL K/V store product name: " + dbProductName);
        System.out.println("Machine name: " + machineDetails[0]);
        System.out.println("OS version: " + machineDetails[1]);
        System.out.println("CPU architecture: " + machineDetails[2]);
        System.out.println("=====================================================");
        sf.putTTL(new RString("name"), new RString("kelly"), 10, "rstring", "rstring");
        RString key = new RString("name");
		if (sf.hasTTL(key, "rstring")){
    		Object value = sf.getTTL(key, "rstring","rstring");
    		//Read the value from the TTL store that we saved earlier
    		System.out.println("Retrieved value " + ((RString)value).getString() + " from the TTL store");
    	} else{
    		System.out.println("The value we saved has expired from TTL store");
    	}
	}
	
	/**
     * Notification that initialization is complete and all input and output ports 
     * are connected and ready to receive and submit tuples.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    @Override
    public synchronized void allPortsReady() throws Exception {
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

    	//no op
        	
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
    }

    /**
     * Shutdown this operator.
     * @throws Exception Operator failure, will cause the enclosing PE to terminate.
     */
    public synchronized void shutdown() throws Exception {
        OperatorContext context = getOperatorContext();
        Logger.getLogger(this.getClass()).trace("Operator " + context.getName() + " shutting down in PE: " + context.getPE().getPEId() + " in Job: " + context.getPE().getJobId() );
        

        // Must call super.shutdown()
        super.shutdown();
    }
}
