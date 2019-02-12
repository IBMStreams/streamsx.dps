import unittest
from time import sleep

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

from streamsx.topology.schema import *

import subprocess
import streamsx.rest as sr


import streamsx.spl.op as op
import streamsx.spl.toolkit as tk
import urllib3
import os



class DpsTests(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        # toolkit from this repository is used
        self.dpsToolkit = '../../com.ibm.streamsx.dps/'
        try:
            # Optionally accepts DPS_CFG environment variable with settings for Compose for Redis service
            self.dps_cfg = os.environ['DPS_CFG']
        except KeyError:
            self.dps_cfg = 'etc/no-sql-kv-store-servers.cfg'
        print("Config file: "+str(self.dps_cfg))

    def setUp(self):
        # Sets self.test_ctxtype and self.test_config
        Tester.setup_distributed(self)
        # disable ssl verification
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        # requires redis command line tools for server start/stop 
        self.isLocalRedisServer = True
        self.dps_cfg_file = os.path.basename(self.dps_cfg)


    def test_healthy_even_with_failed_connection(self):
        if self.isLocalRedisServer:
            self.shutdown_redis()
            topology, values = self.build_dps_composite_simple_app("test_healthy_even_with_failed_connection")

            # Create tester and assign conditions
            self.tester = Tester(topology)
        
            # Need to generate a dummy 1 tuple since tuple_count doesn't support checking
            # for 0 tuples
            dummyVal = topology.source(['dummy'])
            one_tuple_stream = dummyVal.union({values})

            # Make sure we don't get any tuples since there should be
            # no connection
            # This currently doesn't work because tuple_count doesn't support 0 as a number of tuples
            self.tester.tuple_count(one_tuple_stream, 1, True)

            # Make sure we are healthy even though there is no connection
            self.tester.local_check = self.local_checks
        
            # Submit the application for test
            # If it fails an AssertionError will be raised.
            self.tester.test(self.test_ctxtype, self.test_config)

    def test_dps_composites(self):
        if self.isLocalRedisServer:        
            self.start_redis()
        topology, values = self.build_dps_composite_simple_app("test_dps_composites")

        # Create tester and assign conditions
        self.tester = Tester(topology)

        # Make sure the job gets healthy
        self.tester.local_check = self.local_checks

        # 10 tuples should be returned
        self.tester.tuple_count(values, 10, True)

        # Make sure we get all expected values in order
        self.tester.contents(values, get_expected_values(10), True)

        # Submit the application for test
        # If it fails an AssertionError will be raised.
        self.tester.test(self.test_ctxtype, self.test_config)

    def build_dps_composite_simple_app(self, name):
        # Declare the application to be tested
        topology = Topology(name)
        # Add toolkit and toolkit dependencies
        if self.dpsToolkit is not None:
            tk.add_toolkit(topology, self.dpsToolkit)
        # add file to bundle
        topology.add_file_dependency(self.dps_cfg, 'etc')
        #
        source = topology.source(dps_put_stream(10, 300))
        source = source.map(lambda x: (x['key'], x['value'], x['ttl']),
                            schema='tuple<rstring keyAttributeNotNamedKey, rstring valueAttributeNotNamedValue, uint32 ttlNotNamedTTL>')
        # Put the source stream into the DPS DB
        put_result = op.Map('com.ibm.streamsx.store.distributed::DpsPutTTLWithReconnect',
                           source,
                           StreamSchema('tuple<rstring keyAttributeNotNamedKey>'),
                           params={'configFile': 'etc/'+self.dps_cfg_file,
                                   'outputType': StreamSchema('tuple<rstring keyAttributeNotNamedKey>')},
                           name='DpsPut')
        put_result.params['keyAttribute'] = put_result.attribute('keyAttributeNotNamedKey');
        put_result.params['valueAttribute'] = put_result.attribute('valueAttributeNotNamedValue');
        put_result.params['ttlAttribute'] = put_result.attribute('ttlNotNamedTTL');


        # Now go and get what we put in to the DPS DB
        get_result = op.Map('com.ibm.streamsx.store.distributed::DpsGetTTLWithReconnect',
                           put_result.stream,
                           StreamSchema('tuple<rstring keyAttribute, rstring value>'),
                           params={'configFile': 'etc/'+self.dps_cfg_file,
                                   'outputType': StreamSchema('tuple<rstring keyAttribute, rstring value>')},
                           name='DpsGet')
        get_result.params['keyAttribute'] = put_result.attribute('keyAttributeNotNamedKey');
        get_result_stream = get_result.stream

        get_result_stream.print()
        values = get_result_stream.map(lambda x: x['value'])
        return topology, values

    def tearDown(self):
        if self.isLocalRedisServer:
            print("Shutting down Redis")
            self.shutdown_redis()

    @staticmethod
    def start_redis():
        subprocess.Popen(['redis-server'], shell=False)

    @staticmethod
    def shutdown_redis():
        subprocess.Popen(['redis-cli', 'SHUTDOWN'], shell=False)

    @staticmethod
    def start_streams_service():
        print("Starting Streaming Analytics Service")
        connection = sr.StreamingAnalyticsConnection()
        service = connection.get_streaming_analytics()
        result = service.start_instance()

    def local_checks(self):
        job = self.tester.submission_result.job
        self.assertEqual('healthy', job.health)


class TestCloud(DpsTests):
    """ Test using local toolkit from repo launched in Streaming Analytics Service and connect to Compose for Redis IBM Cloud service """

    @classmethod
    def setUpClass(self):
        self.start_streams_service()
        env_chk = True
        try:
            # Expects DPS_CFG environment variable with settings for Compose for Redis service
            self.dps_cfg = os.environ['DPS_CFG']
        except KeyError:
            env_chk = False
        assert env_chk, "DPS_CFG environment variable must be set"
        print("Config file: "+str(self.dps_cfg))

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        # toolkit from this repository is used
        self.dpsToolkit = '../../com.ibm.streamsx.dps/'

class TestCloudLocal(TestCloud):
    """ Test in Streaming Analytics Service using local installed toolkit """

    @classmethod
    def setUpClass(self):     
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=False)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        # local installed toolkit is used
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.dpsToolkit = self.streams_install+'/toolkits/com.ibm.streamsx.dps'

class TestCloudRemote(TestCloud):
    """ Test in Streaming Analytics Service using remote toolkit and remote build """

    @classmethod
    def setUpClass(self):     
        super().setUpClass()

    def setUp(self):
        Tester.setup_streaming_analytics(self, force_remote_build=True)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        # remote toolkit is used
        self.dpsToolkit = None

class TestICP(DpsTests):
    """ Test in ICP env using local toolkit (repo) """

    @classmethod
    def setUpClass(self):
        super().setUpClass()
        env_chk = True
        try:
            print("STREAMS_REST_URL="+str(os.environ['STREAMS_REST_URL']))
        except KeyError:
            env_chk = False
        assert env_chk, "STREAMS_REST_URL environment variable must be set"

    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config = self._service(False)
        # disable ssl verification
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        # toolkit from this repository is used
        self.dpsToolkit = '../../com.ibm.streamsx.dps/'

    def _service(self, remote_build=True):
        streams_rest_url = os.environ['STREAMS_REST_URL']
        streams_service_name = os.environ['STREAMS_SERVICE_NAME']
        streams_user = os.environ['STREAMS_USERNAME']
        streams_password = os.environ['STREAMS_PASSWORD']
        uri_parsed = urlparse(streams_rest_url)
        hostname = uri_parsed.hostname
        r = requests.get('https://'+hostname+':31843/v1/preauth/validateAuth', auth=(streams_user, streams_password), verify=False)
        token = r.json()['accessToken']
        cfg =  {
            'type':'streams',
            'connection_info':{
                'serviceBuildEndpoint':'https://'+hostname+':32085',
                'serviceRestEndpoint': 'https://'+uri_parsed.netloc+'/streams/rest/instances/'+streams_service_name},
            'service_token': token }
        cfg[streamsx.topology.context.ConfigParams.FORCE_REMOTE_BUILD] = remote_build
        return cfg


class TestICPLocal(TestICP):
    """ Test in ICP env using local installed toolkit """

    @classmethod
    def setUpClass(self):     
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config = self._service(False)
        # disable ssl verification
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        # local installed toolkit is used
        self.streams_install = os.environ.get('STREAMS_INSTALL')
        self.dpsToolkit = self.streams_install+'/toolkits/com.ibm.streamsx.dps'

class TestICPRemote(TestICP):
    """ Test in ICP env using remote toolkit (build service) """

    @classmethod
    def setUpClass(self):     
        super().setUpClass()

    def setUp(self):
        Tester.setup_distributed(self)
        self.test_config = self._service(True)
        # disable ssl verification
        self.test_config[streamsx.topology.context.ConfigParams.SSL_VERIFY] = False
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.isLocalRedisServer = False
        self.dps_cfg_file = os.path.basename(self.dps_cfg)
        self.dpsToolkit = None

def get_expected_values(size):
    expected_vals = []
    for i in range(size):
        expected_vals.append("val" + str(i))
    return expected_vals


def dps_put_stream(size, ttl):
    dps_put_tuples = []
    for i in range(size):
        dps_put_tuples.append({'key': str(i) , 'value': "val" + str(i), 'ttl': ttl})
    return dps_put_tuples


def dps_put_generator():
    i = 0
    while True:
        sleep(0.5)
        i = i + 1
        yield {'key': str(i) , 'value': "val" + str(i), 'ttl': 300}

