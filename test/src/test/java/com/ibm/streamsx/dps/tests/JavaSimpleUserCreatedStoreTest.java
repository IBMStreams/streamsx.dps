package com.ibm.streamsx.dps.tests;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.ibm.streamsx.topology.Topology;

public class JavaSimpleUserCreatedStoreTest extends AbstractDPSTest {

	@Override
	String getTestName() {
		return "javaSimpleUserCreatedStore";
	}
	
	@Override
	protected void compileToolkit(File toolkitPath) throws Exception {
		Topology.TOPOLOGY_LOGGER.info("Compiling toolkit...");
		
		File binDir = new File(toolkitPath, "impl/java/bin");
		if(!binDir.exists())
			binDir.mkdirs();
			
		// need to run javac first
		List<String> commands = new ArrayList<String>();
		commands.add(System.getenv("JAVA_HOME") + "/bin/javac");
		commands.add("-cp");
		commands.add(System.getenv("STREAMS_INSTALL") + "/lib/*" + ":" + "../com.ibm.streamsx.dps/impl/java/lib/*");
		commands.add("-s");
		commands.add(new File(toolkitPath, "impl/java/src/").getAbsolutePath());
		commands.add("-d");
		commands.add(new File(toolkitPath, "impl/java/bin/").getAbsolutePath());
		commands.add(new File(toolkitPath, "impl/java/src/dps/test/java/JavaDPSTestOp.java").getAbsolutePath());
		
		ProcessBuilder pb = new ProcessBuilder(commands);
		pb.redirectError(File.createTempFile("pbtest", ""));
		Process process = pb.start();
		process.waitFor(TIMEOUT, TIME_UNIT);
		
		super.compileToolkit(toolkitPath);
	}
	
	
}
