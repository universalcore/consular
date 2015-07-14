import json
from urllib import urlencode

from twisted.trial.unittest import TestCase
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.defer import (
    inlineCallbacks, DeferredQueue, Deferred, succeed)
from twisted.web.client import HTTPConnectionPool
from twisted.python import log

from consular.main import Consular

import treq


class FakeResponse(object):

    def __init__(self, code, headers, content=None):
        self.code = code
        self.headers = headers
        self._content = content

    def content(self):
        return succeed(self._content)

    def json(self):
        d = self.content()
        d.addCallback(lambda content: json.loads(content))
        return d


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

        # We use this to mock requests going to Consul & Marathon
        self.consul_requests = DeferredQueue()
        self.marathon_requests = DeferredQueue()

        def mock_requests(queue):
            def mock_it(method, path, data=None):
                d = Deferred()
                queue.put({
                    'method': method,
                    'path': path,
                    'data': data,
                    'deferred': d,
                })
                return d
            return mock_it

        self.patch(self.consular, 'consul_request',
                   mock_requests(self.consul_requests))
        self.patch(self.consular, 'marathon_request',
                   mock_requests(self.marathon_requests))

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

        # We should get the app info for the event
        marathon_app_request = yield self.marathon_requests.get()
        self.assertEqual(marathon_app_request['method'], 'GET')
        self.assertEqual(marathon_app_request['path'],
                         '/v2/apps/my-app')
        marathon_app_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'app': {
                    'id': '/my-app',
                }
            })))

        # Then we collect the tasks for the app
        marathon_tasks_request = yield self.marathon_requests.get()
        self.assertEqual(marathon_tasks_request['method'], 'GET')
        self.assertEqual(marathon_tasks_request['path'],
                         '/v2/apps/my-app/tasks')
        marathon_tasks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'tasks': [{
                    'id': 'my-app_0-1396592784349',
                    'host': 'slave-1234.acme.org',
                    'ports': [31372],
                }]
            })))

        request = yield self.consul_requests.get()
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(request['path'], '/v1/agent/service/register')
        self.assertEqual(request['data'], {
            'Name': 'my-app',
            'ID': 'my-app_0-1396592784349',
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
            request['path'],
            '/v1/agent/service/deregister/my-app_0-1396592784349')
        request['deferred'].callback('ok')
        response = yield d
        self.assertEqual((yield response.json()), {
            'status': 'ok'
        })

    @inlineCallbacks
    def test_register_with_marathon(self):
        d = self.consular.register_marathon_event_callback(
            'http://localhost:7000/events?registration=the-uuid')
        d.addErrback(log.err)
        list_callbacks_request = yield self.marathon_requests.get()
        list_callbacks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'callbackUrls': []})))

        create_callback_request = yield self.marathon_requests.get()
        self.assertEqual(
            create_callback_request['path'],
            '/v2/eventSubscriptions?%s' % (urlencode({
                'callbackUrl': ('http://localhost:7000/'
                                'events?registration=the-uuid')
            }),))

        self.assertEqual(create_callback_request['method'], 'POST')
        create_callback_request['deferred'].callback(FakeResponse(200, []))
        response = yield d
        self.assertEqual(response, True)

    @inlineCallbacks
    def test_already_registered_with_marathon(self):
        d = self.consular.register_marathon_event_callback(
            'http://localhost:7000/events?registration=the-uuid')
        list_callbacks_request = yield self.marathon_requests.get()
        list_callbacks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'callbackUrls': [
                    'http://localhost:7000/events?registration=the-uuid'
                ]
            })))

        response = yield d
        self.assertEqual(response, True)

    @inlineCallbacks
    def test_sync_app_task(self):
        app = {'id': '/my-app'}
        task = {'id': 'my-task-id', 'host': '0.0.0.0', 'ports': [1234]}
        d = self.consular.sync_app_task(app, task)
        consul_request = yield self.consul_requests.get()
        self.assertEqual(consul_request['path'], '/v1/agent/service/register')
        self.assertEqual(consul_request['data'], {
            'Name': 'my-app',
            'ID': 'my-task-id',
            'Address': '0.0.0.0',
            'Port': 1234,
        })
        self.assertEqual(consul_request['method'], 'PUT')
        consul_request['deferred'].callback('')
        yield d

    @inlineCallbacks
    def test_sync_app_labels(self):
        app = {
            'id': '/my-app',
            'labels': {'foo': 'bar'}
        }
        d = self.consular.sync_app_labels(app)
        consul_request = yield self.consul_requests.get()
        self.assertEqual(consul_request['method'], 'PUT')
        self.assertEqual(consul_request['path'], '/v1/kv/consular/my-app/foo')
        self.assertEqual(consul_request['data'], 'bar')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d

    @inlineCallbacks
    def test_sync_app(self):
        app = {
            'id': '/my-app',
        }
        d = self.consular.sync_app(app)
        marathon_request = yield self.marathon_requests.get()
        self.assertEqual(marathon_request['path'], '/v2/apps/my-app/tasks')
        self.assertEqual(marathon_request['method'], 'GET')
        marathon_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'tasks': []})))
        yield d

    @inlineCallbacks
    def test_sync_apps(self):
        d = self.consular.sync_apps(purge=False)
        marathon_request = yield self.marathon_requests.get()
        self.assertEqual(marathon_request['path'], '/v2/apps')
        self.assertEqual(marathon_request['method'], 'GET')
        marathon_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'apps': []})))
        yield d

    @inlineCallbacks
    def test_purge_dead_services(self):
        d = self.consular.purge_dead_services()
        consul_request = yield self.consul_requests.get()

        # Expecting a request to list of all services in Consul,
        # returning 2
        self.assertEqual(consul_request['path'], '/v1/agent/services')
        self.assertEqual(consul_request['method'], 'GET')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "testingapp.someid1": {
                    "ID": "testingapp.someid1",
                    "Service": "testingapp",
                    "Tags": None,
                    "Address": "machine-1",
                    "Port": 8102
                },
                "testingapp.someid2": {
                    "ID": "testingapp.someid2",
                    "Service": "testingapp",
                    "Tags": None,
                    "Address": "machine-2",
                    "Port": 8103
                }
            }))
        )

        # Expecting a request for the tasks for a given app, returning
        # 1 less than Consul thinks exists.
        testingapp_request = yield self.marathon_requests.get()
        self.assertEqual(testingapp_request['path'],
                         '/v2/apps/testingapp/tasks')
        self.assertEqual(testingapp_request['method'], 'GET')
        testingapp_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "tasks": [{
                    "appId": "/testingapp",
                    "id": "testingapp.someid2",
                    "host": "machine-2",
                    "ports": [8103],
                    "startedAt": "2015-07-14T14:54:31.934Z",
                    "stagedAt": "2015-07-14T14:54:31.544Z",
                    "version": "2015-07-14T13:07:32.095Z"
                }]
            }))
        )

        # Expecting a service registering in Consul as a result for one
        # of these services
        deregister_request = yield self.consul_requests.get()
        self.assertEqual(deregister_request['path'],
                         '/v1/agent/service/deregister/testingapp.someid1')
        self.assertEqual(deregister_request['method'], 'PUT')
        deregister_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d
