package application;
import com.ibm.streams.operator.metrics.Metric.Kind;
import com.ibm.streams.operator.model.InputPortSet.WindowMode;
import com.ibm.streams.operator.model.InputPortSet.WindowPunctuationInputMode;
import com.ibm.streams.operator.model.OutputPortSet.WindowPunctuationOutputMode;

@com.ibm.streams.operator.model.PrimitiveOperator(name="DataStoreTester", namespace="application", description="This operator's purpose is to demonstrate how to successfully fuse two Java operators that use the DPS toolkit.  Because it is fused with the TickerIdGenerator operator, the SharedLoader annotation is used in both classes. See the operators' source for more information.")
@com.ibm.streams.operator.model.InputPorts(value={@com.ibm.streams.operator.model.InputPortSet(description="Port that ingests tuples", cardinality=1, optional=false, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious), @com.ibm.streams.operator.model.InputPortSet(description="Optional input ports", optional=true, windowingMode=WindowMode.NonWindowed, windowPunctuationInputMode=WindowPunctuationInputMode.Oblivious)})
@com.ibm.streams.operator.model.OutputPorts(value={@com.ibm.streams.operator.model.OutputPortSet(description="Port that produces tuples", cardinality=1, optional=false, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating), @com.ibm.streams.operator.model.OutputPortSet(description="Optional output ports", optional=true, windowPunctuationOutputMode=WindowPunctuationOutputMode.Generating)})
@com.ibm.streams.operator.model.Libraries(value={"@STREAMS_INSTALL@/toolkits/com.ibm.streamsx.dps/impl/java/lib/dps-helper.jar"})
@com.ibm.streams.operator.model.SharedLoader(value=true)
@com.ibm.streams.operator.internal.model.ShadowClass("application.DataStoreTester")
@javax.annotation.Generated("com.ibm.streams.operator.internal.model.processors.ShadowClassGenerator")
public class DataStoreTester$StreamsModel extends com.ibm.streams.operator.AbstractOperator
 {
}