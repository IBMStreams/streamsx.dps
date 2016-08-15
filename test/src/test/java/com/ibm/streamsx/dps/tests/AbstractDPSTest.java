package com.ibm.streamsx.dps.tests;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.streamsx.topology.TStream;
import com.ibm.streamsx.topology.Topology;
import com.ibm.streamsx.topology.context.ContextProperties;
import com.ibm.streamsx.topology.context.StreamsContext;
import com.ibm.streamsx.topology.context.StreamsContext.Type;
import com.ibm.streamsx.topology.context.StreamsContextFactory;
import com.ibm.streamsx.topology.spl.SPL;
import com.ibm.streamsx.topology.tester.Condition;

public abstract class AbstractDPSTest {

	private static final String EXPECTED_FILENAME = "expected.txt";
	private static final long TIMEOUT = 45;
	private static final TimeUnit TIME_UNIT = TimeUnit.SECONDS;
	
	private Properties props;
	private String[] expected;
	private Logger logger = LoggerFactory.getLogger(AbstractDPSTest.class);
	
	/* Abstracts */
	abstract String getTestName();

	@Before
	public void loadProperties() throws Exception {
		props = new Properties();
		props.load(new FileInputStream(new File("junit.properties")));
	}	
		
	@Test
	public void unitTest() throws Exception {
		Topology t = createTopology(getTestName());
		addTopologyFileDependencies(t);
		
		// launch the test application
		Map<String, Object> params = new HashMap<>();
		SPL.invokeOperator(t, getTestName(), getMainComp(), null, null, params);

		// collect results and validate
		TStream<String> stringStream = t.subscribe("test.results", String.class);
		Map<String, Object> config = getJobConfiguation();
		StreamsContext<?> context = StreamsContextFactory.getStreamsContext(Type.DISTRIBUTED_TESTER);
		Condition<List<String>> contents = t.getTester().completeAndTestStringOutput(context, config, stringStream, TIMEOUT, TIME_UNIT, expected);
		printContents(contents.getResult());

		Assert.assertTrue(contents.valid());			
	}
	
	/*
	 * By default, no parameters are set. Sub-classes should
	 * override this method to add submission time params
	 */
	protected Map<String, Object> getSubmissionTimeParams() {
		Map<String, Object> submissionParams = new HashMap<String, Object>();
		return submissionParams;
	}
	
	protected Map<String, Object> getJobConfiguation() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ContextProperties.SUBMISSION_PARAMS, getSubmissionTimeParams());
		return config;
	}
	
	protected String getMainComp() {
		return "application::dpstest";
	}
	
	protected void addTopologyFileDependencies(Topology t) throws Exception {
		t.addFileDependency(createConfigFile("no-sql-kv-store-servers.cfg", true), "etc");
	}
	
	private Topology createTopology(String name) throws Exception {
		File testToolkit = new File("spl/" + name);
		
		Topology t = new Topology(name);
		SPL.addToolkit(t, new File(props.getProperty("dps.toolkit.location")));
		SPL.addToolkit(t, testToolkit);
		compileToolkit(testToolkit);
		expected = Files.readAllLines(new File(testToolkit, "/" + EXPECTED_FILENAME).toPath()).toArray(new String[0]);
		
		return t;
	}

	private void compileToolkit(File toolkitPath) throws Exception {
		List<String> command = new ArrayList<>();
		command.add(System.getenv("STREAMS_INSTALL") + "/bin/spl-make-toolkit");
		command.add("-i");
		command.add(toolkitPath.getAbsolutePath());

		System.out.println("Indexing " + getTestName() + " toolkit...");
		ProcessBuilder pb = new ProcessBuilder(command);
		Process proc = pb.start();
		boolean waitFor = proc.waitFor(TIMEOUT, TIME_UNIT);
		if(waitFor == true) {
			System.out.println("Toolkit indexing finished!");
		} else {
			System.out.println("Error indexing toolkit!");
		}
	}
	
	private void printContents(List<String> contents) {
		System.out.println("**Contents**:");
		contents.forEach(System.out::println);
	}
	
	protected String createConfigFile(String filename, boolean addServerInfo) throws Exception {
		File f = new File(System.getProperty("java.io.tmpdir"), filename);
		if(f.exists())
			f.delete();
		f.createNewFile();
		f.deleteOnExit();
		
		FileWriter fw = new FileWriter(f);
		fw.append(props.getProperty("server.type"));
		if(addServerInfo) {
			fw.append("\n");
			fw.append(props.getProperty("server.address"));			
		}
		fw.flush();
		fw.close();
		
		return f.getAbsolutePath();
	}
}
