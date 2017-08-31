import unittest
from time import sleep

from streamsx.topology.topology import *
from streamsx.topology.tester import Tester

from streamsx.topology.schema import *

import subprocess

import streamsx.spl.op as op
import streamsx.spl.toolkit as tk

import os

cwd = os.getcwd()

dpsToolkit = '/home/streamsadmin/git/streamsx.dps/com.ibm.streamsx.dps/'

class DpsPutAndGetTTLTests(unittest.TestCase):

    def setUp(self):
        # Sets self.test_ctxtype and self.test_config
        Tester.setup_distributed(self)


    def test_healthy_even_with_failed_connection(self):
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
        tk.add_toolkit(topology, dpsToolkit)
        #
        source = topology.source(dps_put_stream(10, 300))
        source = source.map(lambda x: (x['key'], x['value'], x['ttl']),
                            schema='tuple<rstring keyAttributeNotNamedKey, rstring valueAttributeNotNamedValue, uint32 ttlNotNamedTTL>')
        # Put the source stream into the DPS DB
        put_result = op.Map('com.ibm.streamsx.store.distributed::DpsPutTTLWithReconnect',
                           source,
                           StreamSchema('tuple<rstring keyAttributeNotNamedKey>'),
                           params={'configFile': cwd + '/etc/no-sql-kv-store-servers.cfg',
                                   'outputType': StreamSchema('tuple<rstring keyAttributeNotNamedKey>')},
                           name='DpsPut')
        put_result.params['keyAttribute'] = put_result.attribute('keyAttributeNotNamedKey');
        put_result.params['valueAttribute'] = put_result.attribute('valueAttributeNotNamedValue');
        put_result.params['ttlAttribute'] = put_result.attribute('ttlNotNamedTTL');


        # Now go and get what we put in to the DPS DB
        get_result = op.Map('com.ibm.streamsx.store.distributed::DpsGetTTLWithReconnect',
                           put_result.stream,
                           StreamSchema('tuple<rstring keyAttribute, rstring value>'),
                           params={'configFile': cwd + '/etc/no-sql-kv-store-servers.cfg',
                                   'outputType': StreamSchema('tuple<rstring keyAttribute, rstring value>')},
                           name='DpsGet')
        get_result.params['keyAttribute'] = put_result.attribute('keyAttributeNotNamedKey');
        get_result_stream = get_result.stream

        get_result_stream.print()
        values = get_result_stream.map(lambda x: x['value'])
        return topology, values

    def tearDown(self):
        print("Shutting down Redis")
        self.shutdown_redis()

    @staticmethod
    def start_redis():
        subprocess.Popen(['redis-server'], shell=False)

    @staticmethod
    def shutdown_redis():
        subprocess.Popen(['redis-cli', 'SHUTDOWN'], shell=False)

    def local_checks(self):
        job = self.tester.submission_result.job
        self.assertEqual('healthy', job.health)


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

