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


class DummyConsularException(Exception):
    pass


class ConsularTest(TestCase):

    timeout = 1

    def setUp(self):
        self.consular = Consular(
            'http://localhost:8500',
            'http://localhost:8080',
            False,
            'test'
        )
        self.consular.debug = True

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
        self.requests = DeferredQueue()

        def mock_requests(method, url, headers, data, pool, timeout):
            d = Deferred()
            self.requests.put({
                'method': method,
                'url': url,
                'data': data,
                'deferred': d,
            })
            return d

        self.patch(self.consular, 'requester', mock_requests)

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

    def test_reg_id_tag(self):
        """ Consular's registration ID tag is properly formed. """
        self.assertEqual(self.consular.reg_id_tag(), 'consular-reg-id=test')

    def test_app_id_tag(self):
        """ Consular's application ID tag is properly formed. """
        self.assertEqual(self.consular.app_id_tag('test'),
                         'consular-app-id=test')

    def test_get_app_id_from_tags(self):
        """ The app ID is successfully parsed from the Consul tags. """
        tags = [
            'randomstuff',
            'consular-reg-id=test',
            'consular-app-id=/my-app',
        ]
        self.assertEqual(self.consular.get_app_id_from_tags(tags), '/my-app')

    def test_get_app_id_from_tags_not_found(self):
        """
        None is returned when the app ID cannot be found in the Consul tags.
        """
        tags = [
            'randomstuff',
            'consular-reg-id=test',
        ]
        self.assertEqual(self.consular.get_app_id_from_tags(tags), None)

    def test_get_app_id_from_tags_multiple(self):
        """
        An exception is raised when multiple app IDs are found in the Consul
        tags.
        """
        tags = [
            'randomstuff',
            'consular-reg-id=test',
            'consular-app-id=/my-app',
            'consular-app-id=/my-app2',
        ]
        exception = self.assertRaises(RuntimeError,
                                      self.consular.get_app_id_from_tags, tags)
        self.assertEqual(str(exception),
                         'Multiple (2) Consular tags found for key '
                         '"consular-app-id=": [\'consular-app-id=/my-app\', '
                         '\'consular-app-id=/my-app2\']')

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
        marathon_app_request = yield self.requests.get()
        self.assertEqual(marathon_app_request['method'], 'GET')
        self.assertEqual(marathon_app_request['url'],
                         'http://localhost:8080/v2/apps/my-app')
        marathon_app_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'app': {
                    'id': '/my-app',
                }
            })))

        # Check if any existing labels stored in Consul
        consul_kv_request = yield self.requests.get()
        self.assertEqual(consul_kv_request['method'], 'GET')
        self.assertEqual(consul_kv_request['url'],
                         'http://localhost:8500/v1/kv/consular/my-app?keys=')
        consul_kv_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([])))

        # Then we collect the tasks for the app
        marathon_tasks_request = yield self.requests.get()
        self.assertEqual(marathon_tasks_request['method'], 'GET')
        self.assertEqual(marathon_tasks_request['url'],
                         'http://localhost:8080/v2/apps/my-app/tasks')
        marathon_tasks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'tasks': [{
                    'id': 'my-app_0-1396592784349',
                    'host': 'slave-1234.acme.org',
                    'ports': [31372],
                }]
            })))

        request = yield self.requests.get()
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(
            request['url'],
            'http://slave-1234.acme.org:8500/v1/agent/service/register')
        self.assertEqual(request['data'], json.dumps({
            'Name': 'my-app',
            'ID': 'my-app_0-1396592784349',
            'Address': 'slave-1234.acme.org',
            'Port': 31372,
            'Tags': [
                'consular-reg-id=test',
                'consular-app-id=/my-app',
            ],
        }))
        request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
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
        request = yield self.requests.get()
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(
            request['url'],
            ('http://slave-1234.acme.org:8500'
             '/v1/agent/service/deregister/my-app_0-1396592784349'))
        request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        response = yield d
        self.assertEqual((yield response.json()), {
            'status': 'ok'
        })

    @inlineCallbacks
    def test_register_with_marathon(self):
        d = self.consular.register_marathon_event_callback(
            'http://localhost:7000/events?registration=the-uuid')
        d.addErrback(log.err)
        list_callbacks_request = yield self.requests.get()
        list_callbacks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'callbackUrls': []})))

        create_callback_request = yield self.requests.get()
        self.assertEqual(
            create_callback_request['url'],
            'http://localhost:8080/v2/eventSubscriptions?%s' % (urlencode({
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
        list_callbacks_request = yield self.requests.get()
        list_callbacks_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                'callbackUrls': [
                    'http://localhost:7000/events?registration=the-uuid'
                ]
            })))

        response = yield d
        self.assertEqual(response, True)

    @inlineCallbacks
    def test_register_with_marathon_unexpected_response(self):
        """
        When registering a Marathon event callback Consular checks if an event
        callback already exists for itself. When we get the existing callbacks,
        Consular should inform the user of any errors returned by Marathon.
        """
        d = self.consular.register_marathon_event_callback(
            'http://localhost:7000/events?registration=the-uuid')
        list_callbacks_request = yield self.requests.get()
        list_callbacks_request['deferred'].callback(
            FakeResponse(400, [], json.dumps({
                'message':
                'http event callback system is not running on this Marathon ' +
                'instance. Please re-start this instance with ' +
                '"--event_subscriber http_callback".'})))

        failure = self.failureResultOf(d, RuntimeError)
        self.assertEqual(
            failure.getErrorMessage(),
            'Unable to get existing event callbacks from Marathon: ' +
            '\'{u\\\'message\\\': u\\\'http event callback system is not ' +
            'running on this Marathon instance. Please re-start this ' +
            'instance with "--event_subscriber http_callback".\\\'}\'')

    @inlineCallbacks
    def test_sync_app_task(self):
        app = {'id': '/my-app'}
        task = {'id': 'my-task-id', 'host': '0.0.0.0', 'ports': [1234]}
        d = self.consular.sync_app_task(app, task)
        consul_request = yield self.requests.get()
        self.assertEqual(
            consul_request['url'],
            'http://0.0.0.0:8500/v1/agent/service/register')
        self.assertEqual(consul_request['data'], json.dumps({
            'Name': 'my-app',
            'ID': 'my-task-id',
            'Address': '0.0.0.0',
            'Port': 1234,
            'Tags': [
                'consular-reg-id=test',
                'consular-app-id=/my-app',
            ],
        }))
        self.assertEqual(consul_request['method'], 'PUT')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d

    @inlineCallbacks
    def test_sync_app_task_grouped(self):
        """
        When syncing an app in a group with a task, Consul is updated with a
        service entry for the task.
        """
        app = {'id': '/my-group/my-app'}
        task = {'id': 'my-task-id', 'host': '0.0.0.0', 'ports': [1234]}
        d = self.consular.sync_app_task(app, task)
        consul_request = yield self.requests.get()
        self.assertEqual(
            consul_request['url'],
            'http://0.0.0.0:8500/v1/agent/service/register')
        self.assertEqual(consul_request['data'], json.dumps({
            'Name': 'my-group-my-app',
            'ID': 'my-task-id',
            'Address': '0.0.0.0',
            'Port': 1234,
            'Tags': [
                'consular-reg-id=test',
                'consular-app-id=/my-group/my-app',
            ],
        }))
        self.assertEqual(consul_request['method'], 'PUT')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d

    @inlineCallbacks
    def test_sync_app_labels(self):
        app = {
            'id': '/my-app',
            'labels': {'foo': 'bar'}
        }
        d = self.consular.sync_app_labels(app)
        consul_request = yield self.requests.get()
        self.assertEqual(consul_request['method'], 'PUT')
        self.assertEqual(consul_request['url'],
                         'http://localhost:8500/v1/kv/consular/my-app/foo')
        self.assertEqual(consul_request['data'], '"bar"')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))

        consul_request = yield self.requests.get()
        self.assertEqual(consul_request['method'], 'GET')
        self.assertEqual(consul_request['url'],
                         'http://localhost:8500/v1/kv/consular/my-app?keys=')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([])))

        yield d

    @inlineCallbacks
    def test_sync_app_labels_cleanup(self):
        """
        When Consular syncs app labels, and labels are found in Consul which
        aren't present in the Marathon app definition, those labels are deleted
        from Consul.
        """
        app = {
            'id': '/my-app',
            'labels': {'foo': 'bar'}
        }
        d = self.consular.sync_app_labels(app)
        put_request = yield self.requests.get()
        self.assertEqual(put_request['method'], 'PUT')
        self.assertEqual(put_request['url'],
                         'http://localhost:8500/v1/kv/consular/my-app/foo')
        self.assertEqual(put_request['data'], '"bar"')
        put_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))

        get_request = yield self.requests.get()
        self.assertEqual(get_request['method'], 'GET')
        self.assertEqual(get_request['url'],
                         'http://localhost:8500/v1/kv/consular/my-app?keys=')
        consul_labels = [
            'consular/my-app/foo',
            'consular/my-app/oldfoo',
            'consular/my-app/misplaced/foo',
        ]
        get_request['deferred'].callback(
            FakeResponse(200, [], json.dumps(consul_labels)))

        delete_request1 = yield self.requests.get()
        self.assertEqual(delete_request1['method'], 'DELETE')
        self.assertEqual(delete_request1['url'],
                         'http://localhost:8500/v1/kv/consular/my-app/oldfoo')
        delete_request1['deferred'].callback(
            FakeResponse(200, [], json.dumps(True)))

        delete_request2 = yield self.requests.get()
        self.assertEqual(delete_request2['method'], 'DELETE')
        self.assertEqual(
            delete_request2['url'],
            'http://localhost:8500/v1/kv/consular/my-app/misplaced/foo')
        delete_request2['deferred'].callback(
            FakeResponse(200, [], json.dumps(True)))

        yield d

    @inlineCallbacks
    def test_sync_app(self):
        app = {
            'id': '/my-app',
        }
        d = self.consular.sync_app(app)

        # First Consular syncs app labels...
        # There are no labels in this definition so Consular doesn't push any
        # labels to Consul, it just tries to clean up any existing labels.
        consul_request = yield self.requests.get()
        self.assertEqual(consul_request['method'], 'GET')
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/kv/consular/my-app?keys=')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([])))

        # Next Consular syncs app tasks...
        # It fetches a list of tasks for an app and gets an empty list so
        # nothing is to be done.
        marathon_request = yield self.requests.get()
        self.assertEqual(
            marathon_request['url'],
            'http://localhost:8080/v2/apps/my-app/tasks')
        self.assertEqual(marathon_request['method'], 'GET')
        marathon_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'tasks': []})))
        yield d

    @inlineCallbacks
    def test_sync_apps(self):
        d = self.consular.sync_apps(purge=False)
        marathon_request = yield self.requests.get()
        self.assertEqual(marathon_request['url'],
                         'http://localhost:8080/v2/apps')
        self.assertEqual(marathon_request['method'], 'GET')
        marathon_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({'apps': []})))
        yield d

    @inlineCallbacks
    def test_purge_dead_services(self):
        d = self.consular.purge_dead_services()
        consul_request = yield self.requests.get()
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/catalog/nodes')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([{
                'Node': 'consul-node',
                'Address': '1.2.3.4',
            }]))
        )
        agent_request = yield self.requests.get()
        # Expecting a request to list of all services in Consul,
        # returning 2
        self.assertEqual(
            agent_request['url'],
            'http://1.2.3.4:8500/v1/agent/services')
        self.assertEqual(agent_request['method'], 'GET')
        agent_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "testinggroup-someid1": {
                    "ID": "taskid1",
                    "Service": "testingapp",
                    "Tags": None,
                    "Address": "machine-1",
                    "Port": 8102,
                    "Tags": [
                        "consular-reg-id=test",
                        "consular-app-id=/testinggroup/someid1",
                    ],
                },
                "testinggroup-someid1": {
                    "ID": "taskid2",
                    "Service": "testingapp",
                    "Tags": None,
                    "Address": "machine-2",
                    "Port": 8103,
                    "Tags": [
                        "consular-reg-id=test",
                        "consular-app-id=/testinggroup/someid1",
                    ],
                }
            }))
        )

        # Expecting a request for the tasks for a given app, returning
        # 1 less than Consul thinks exists.
        testingapp_request = yield self.requests.get()
        self.assertEqual(testingapp_request['url'],
                         'http://localhost:8080/v2/apps/testinggroup/someid1/'
                         'tasks')
        self.assertEqual(testingapp_request['method'], 'GET')
        testingapp_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "tasks": [{
                    "appId": "/testinggroup/someid1",
                    "id": "taskid2",
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
        deregister_request = yield self.requests.get()
        self.assertEqual(
            deregister_request['url'],
            ('http://1.2.3.4:8500/v1/agent/service/deregister/'
             'testinggroup-someid1'))
        self.assertEqual(deregister_request['method'], 'PUT')
        deregister_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d

    @inlineCallbacks
    def test_purge_old_services(self):
        """
        Services previously registered with Consul by Consular but that no
        longer exist in Marathon should be purged if a registration ID is set.
        """
        d = self.consular.purge_dead_services()
        consul_request = yield self.requests.get()
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/catalog/nodes')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([{
                'Node': 'consul-node',
                'Address': '1.2.3.4',
            }]))
        )
        agent_request = yield self.requests.get()
        # Expecting a request to list of all services in Consul, returning 3
        # services - one tagged with our registration ID, one tagged with a
        # different registration ID, and one with no tags.
        self.assertEqual(
            agent_request['url'],
            'http://1.2.3.4:8500/v1/agent/services')
        self.assertEqual(agent_request['method'], 'GET')
        agent_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "testingapp.someid1": {
                    "ID": "testingapp.someid1",
                    "Service": "testingapp",
                    "Tags": [
                        "consular-reg-id=test",
                        "consular-app-id=/testingapp",
                    ],
                    "Address": "machine-1",
                    "Port": 8102
                },
                "testingapp.someid2": {
                    "ID": "testingapp.someid2",
                    "Service": "testingapp",
                    "Tags": [
                        "consular-reg-id=blah",
                        "consular-app-id=/testingapp",
                    ],
                    "Address": "machine-2",
                    "Port": 8103
                },
                "testingapp.someid3": {
                    "ID": "testingapp.someid2",
                    "Service": "testingapp",
                    "Tags": None,
                    "Address": "machine-2",
                    "Port": 8104
                }
            }))
        )

        # Expecting a request for the tasks for a given app, returning no tasks
        testingapp_request = yield self.requests.get()
        self.assertEqual(testingapp_request['url'],
                         'http://localhost:8080/v2/apps/testingapp/tasks')
        self.assertEqual(testingapp_request['method'], 'GET')
        testingapp_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({}))
        )

        # Expecting a service deregistering in Consul as a result. Only the
        # task with the correct tag is returned.
        deregister_request = yield self.requests.get()
        self.assertEqual(
            deregister_request['url'],
            ('http://1.2.3.4:8500/v1/agent/service/deregister/'
             'testingapp.someid1'))
        self.assertEqual(deregister_request['method'], 'PUT')
        deregister_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))
        yield d

    @inlineCallbacks
    def test_purge_old_service_no_app_id(self):
        """
        Services previously registered with Consul by Consular but without an
        app ID tagged (for some reason) should not be purged.
        """
        d = self.consular.purge_dead_services()
        consul_request = yield self.requests.get()
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/catalog/nodes')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([{
                'Node': 'consul-node',
                'Address': '1.2.3.4',
            }]))
        )
        agent_request = yield self.requests.get()
        self.assertEqual(
            agent_request['url'],
            'http://1.2.3.4:8500/v1/agent/services')
        self.assertEqual(agent_request['method'], 'GET')
        agent_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({
                "testingapp.someid1": {
                    "ID": "testingapp.someid1",
                    "Service": "testingapp",
                    "Tags": [
                        "consular-reg-id=test",
                    ],
                    "Address": "machine-1",
                    "Port": 8102
                }
            }))
        )

        # Expecting no action to be taken as there is no app ID.
        yield d

    @inlineCallbacks
    def test_purge_dead_app_labels(self):
        """
        Services previously registered with Consul by Consular but that no
        longer exist in Marathon should have their labels removed from the k/v
        store.
        """
        d = self.consular.purge_dead_app_labels([{
            'id': 'my-app'
        }])
        consul_request = yield self.requests.get()
        self.assertEqual(consul_request['method'], 'GET')
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/kv/consular/?keys=&separator=%2F')
        # Return one existing app and one non-existing app
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps([
                'consular/my-app',
                'consular/my-app2',
            ]))
        )

        # Consular should delete the app that doesn't exist
        consul_request = yield self.requests.get()
        self.assertEqual(consul_request['method'], 'DELETE')
        self.assertEqual(
            consul_request['url'],
            'http://localhost:8500/v1/kv/consular/my-app2?recurse')
        consul_request['deferred'].callback(
            FakeResponse(200, [], json.dumps({})))

        yield d

    @inlineCallbacks
    def test_fallback_to_main_consul(self):
        self.consular.enable_fallback = True
        self.consular.register_service(
            'http://foo:8500', '/app_id', 'service_id', 'foo', 1234)
        request = yield self.requests.get()
        self.assertEqual(
            request['url'],
            'http://foo:8500/v1/agent/service/register')
        request['deferred'].errback(
            DummyConsularException('Something terrible'))
        [exc] = self.flushLoggedErrors(DummyConsularException)
        self.assertEqual(str(exc.value), 'Something terrible')

        fallback_request = yield self.requests.get()
        self.assertEqual(
            fallback_request['url'],
            'http://localhost:8500/v1/agent/service/register')
        self.assertEqual(fallback_request['data'], json.dumps({
            'Name': 'app_id',
            'ID': 'service_id',
            'Address': 'foo',
            'Port': 1234,
            'Tags': [
                'consular-reg-id=test',
                'consular-app-id=/app_id',
            ],
        }))
