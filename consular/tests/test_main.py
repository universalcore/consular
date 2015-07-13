import json

from twisted.trial.unittest import TestCase
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks, DeferredQueue, Deferred
from twisted.web.client import HTTPConnectionPool

from consular.main import Consular

import treq


class ConsularTest(TestCase):

    timeout = 1

    def setUp(self):
        self.consular = Consular(
            'http://localhost:8500',
            'http://localhost:8080',
        )

        # spin up a site so we can test it, pretty sure Klein has better
        # ways of doing this but they're not documented anywhere.
        self.site = Site(self.consular.app.resource())
        self.listener = reactor.listenTCP(0, self.site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        self.addCleanup(self.listener.loseConnection)

        # cleanup stuff for treq's global http request pool
        self.pool = HTTPConnectionPool(reactor, persistent=False)
        self.addCleanup(self.pool.closeCachedConnections)

        # We use this to mock requests going to Consul
        self.consul_requests = DeferredQueue()

        def mock_consul_request(method, path, data=None):
            d = Deferred()
            self.consul_requests.put({
                'method': method,
                'path': path,
                'data': data,
                'deferred': d,
            })
            return d

        self.patch(self.consular, 'consul_request', mock_consul_request)

    def request(self, method, path, data=None):
        return treq.request(
            method, 'http://localhost:%s%s' % (
                self.listener_port,
                path
                ),
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)

    def tearDown(self):
        pass

    @inlineCallbacks
    def test_service(self):
        response = yield self.request('GET', '/')
        self.assertEqual(response.code, 200)
        self.assertEqual((yield response.json()), [])

    @inlineCallbacks
    def test_handle_unknown_event(self):
        response = yield self.request('POST', '/events', {'eventType': 'Foo'})
        data = yield response.json()
        self.assertEqual(data, {
            'error': 'Event type Foo not supported.'
        })

    @inlineCallbacks
    def test_handle_unspecified_event(self):
        response = yield self.request('POST', '/events', {})
        data = yield response.json()
        self.assertEqual(data, {
            'error': 'Event type None not supported.'
        })

    @inlineCallbacks
    def test_TASK_STAGING(self):
        response = yield self.request('POST', '/events', {
            "eventType": "status_update_event",
            "timestamp": "2014-03-01T23:29:30.158Z",
            "slaveId": "20140909-054127-177048842-5050-1494-0",
            "taskId": "my-app_0-1396592784349",
            "taskStatus": "TASK_STAGING",
            "appId": "/my-app",
            "host": "slave-1234.acme.org",
            "ports": [31372],
            "version": "2014-04-04T06:26:23.051Z"
        })
        self.assertEqual((yield response.json()), {
            'status': 'ok'
        })

    @inlineCallbacks
    def test_TASK_RUNNING(self):
        d = self.request('POST', '/events', {
            "eventType": "status_update_event",
            "timestamp": "2014-03-01T23:29:30.158Z",
            "slaveId": "20140909-054127-177048842-5050-1494-0",
            "taskId": "my-app_0-1396592784349",
            "taskStatus": "TASK_RUNNING",
            "appId": "/my-app",
            "host": "slave-1234.acme.org",
            "ports": [31372],
            "version": "2014-04-04T06:26:23.051Z"
        })
        request = yield self.consul_requests.get()
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(request['path'], '/v1/agent/service/register')
        self.assertEqual(request['data'], {
            'Name': 'my-app',
            'Address': 'slave-1234.acme.org',
            'Port': 31372,
        })
        request['deferred'].callback('ok')
        response = yield d
        self.assertEqual((yield response.json()), {
            'status': 'ok'
        })

    @inlineCallbacks
    def test_TASK_KILLED(self):
        d = self.request('POST', '/events', {
            "eventType": "status_update_event",
            "timestamp": "2014-03-01T23:29:30.158Z",
            "slaveId": "20140909-054127-177048842-5050-1494-0",
            "taskId": "my-app_0-1396592784349",
            "taskStatus": "TASK_KILLED",
            "appId": "/my-app",
            "host": "slave-1234.acme.org",
            "ports": [31372],
            "version": "2014-04-04T06:26:23.051Z"
        })
        request = yield self.consul_requests.get()
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(
            request['path'], '/v1/agent/service/deregister/my-app')
        request['deferred'].callback('ok')
        response = yield d
        self.assertEqual((yield response.json()), {
            'status': 'ok'
        })
