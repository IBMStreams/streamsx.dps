package application;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@com.ibm.streams.operator.model.PrimitiveOperator(name="TickerIdGenerator", namespace="application", description="This operator demonstrates how to use the DPS (Distributed Process Store) API to demonstrates how to:  * Create a store (StoreFactory.createStore/StoreFactory.createOrGetStore) * Find a previously created store (StoreFactory.findStore) * Read from a store (StoreFactory.get) * Write to a store (StoreFactory.put) See Main.spl for examples on how to iterate over the contents of a store")
@com.ibm.streams.operator.model.InputPorts(value={@com.ibm.streams.operator.model.InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @com.ibm.streams.operator.model.InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@com.ibm.streams.operator.model.OutputPorts(value={@com.ibm.streams.operator.model.OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @com.ibm.streams.operator.model.OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@com.ibm.streams.operator.model.Libraries(value={"impl/java/lib/dps-helper.jar"})
@com.ibm.streams.operator.model.SharedLoader(value=true)
@com.ibm.streams.operator.internal.model.ShadowClass("application.TickerIdGenerator")
@javax.annotation.Generated("com.ibm.streams.operator.internal.model.processors.ShadowClassGenerator")
public class TickerIdGenerator$StreamsModel extends com.ibm.streams.operator.AbstractOperator
 {
}