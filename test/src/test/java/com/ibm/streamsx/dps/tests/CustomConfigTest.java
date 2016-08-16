package com.ibm.streamsx.dps.tests;

import java.util.Map;

import com.ibm.streamsx.topology.Topology;

public class CustomConfigTest extends AbstractDPSTest {

	private String configFilePath;
	
	@Override
	String getTestName() {
		return "customConfig";
	}
	
	@Override
	protected void addTopologyFileDependencies(Topology t) throws Exception {
		// create the no-sql-kv-store-servers.cfg with just the server type in it (i.e. redis)
		t.addFileDependency(createConfigFile("no-sql-kv-store-servers.cfg", false), "etc");
		
		// create the custom config file outside of the app bundle 
		configFilePath = createConfigFile("my_dps_server.cfg", true);
	}
	
	@Override
	protected Map<String, Object> getSubmissionTimeParams() {
		Map<String, Object> params = super.getSubmissionTimeParams();
		params.put("dps.config", configFilePath);
		
		return params;
	}
}
