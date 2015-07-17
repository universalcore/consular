import json

from urllib import quote, urlencode
from twisted.internet import reactor
from twisted.web import client
# Twisted'de fault HTTP11 client factory is way too verbose
client._HTTP11ClientFactory.noisy = False
from twisted.internet.defer import (
    succeed, inlineCallbacks, returnValue, gatherResults)
from twisted.python import log


import treq
from klein import Klein


def get_appid(app_id_string):
    return app_id_string.rsplit('/', 1)[1]


class Consular(object):

    app = Klein()
    debug = False

    def __init__(self, consul_endpoint, marathon_endpoint):
        self.consul_endpoint = consul_endpoint
        self.marathon_endpoint = marathon_endpoint
        self.pool = client.HTTPConnectionPool(reactor, persistent=False)
        self.event_dispatch = {
            'status_update_event': self.handle_status_update_event,
        }

    def get_marathon_event_callbacks(self):
        d = self.marathon_request('GET', '/v2/eventSubscriptions')
        d.addErrback(log.err)
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: data['callbackUrls'])
        return d

    def create_marathon_event_callback(self, url):
        d = self.marathon_request(
            'POST', '/v2/eventSubscriptions?%s' % urlencode({
                'callbackUrl': url,
            }))
        d.addErrback(log.err)
        d.addCallback(lambda response: response.code == 200)
        return d

    @inlineCallbacks
    def register_marathon_event_callback(self, events_url):
        existing_callbacks = yield self.get_marathon_event_callbacks()
        already_registered = any(
            [events_url == url for url in existing_callbacks])
        if already_registered:
            log.msg('Consular event callback already registered.')
            returnValue(True)

        registered = yield self.create_marathon_event_callback(events_url)
        if registered:
            log.msg('Consular event callback registered.')
        else:
            log.err('Consular event callback registration failed.')
        returnValue(registered)

    def log_http_response(self, response, method, path, data):
        log.msg('%s %s with %s returned: %s' % (
            method, path, data, response.code))
        return response

    def marathon_request(self, method, path, data=None):
        d = treq.request(
            method, ('%s%s' % (self.marathon_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)
        if self.debug:
            d.addCallback(self.log_http_response, method, path, data)
        return d

    def consul_request(self, method, path, data=None):
        d = treq.request(
            method, ('%s%s' % (self.consul_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)
        if self.debug:
            d.addCallback(self.log_http_response, method, path, data)
        return d

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        return json.dumps([])

    @app.route('/events')
    def events(self, request):
        request.setHeader('Content-Type', 'application/json')
        event = json.load(request.content)
        handler = self.event_dispatch.get(
            event.get('eventType'), self.handle_unknown_event)
        return handler(request, event)

    def handle_status_update_event(self, request, event):
        dispatch = {
            'TASK_STAGING': self.noop,
            'TASK_STARTING': self.noop,
            'TASK_RUNNING': self.update_task_running,
            'TASK_FINISHED': self.update_task_killed,
            'TASK_FAILED': self.update_task_killed,
            'TASK_KILLED': self.update_task_killed,
            'TASK_LOST': self.update_task_killed,
        }
        handler = dispatch.get(event['taskStatus'])
        return handler(request, event)

    def noop(self, request, event):
        return succeed(json.dumps({
            'status': 'ok'
        }))

    def update_task_running(self, request, event):
        # NOTE: Marathon sends a list of ports, I don't know yet when & if
        #       there are multiple values in that list.
        d = self.get_app(event['appId'])
        d.addCallback(lambda app: self.sync_app(app))
        d.addCallback(lambda _: json.dumps({'status': 'ok'}))
        return d

    def update_task_killed(self, request, event):
        d = self.deregister_service(event['taskId'])
        d.addCallback(lambda _: json.dumps({'status': 'ok'}))
        return d

    def handle_unknown_event(self, request, event):
        event_type = event.get('eventType')
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(400)  # bad request
        log.msg('Not handling event type: %s' % (event_type,))
        return json.dumps({
            'error': 'Event type %s not supported.' % (event_type,)
        })

    def register_service(self, name, id, address, port):
        log.msg('Registering %s.' % (name,))
        return self.consul_request('PUT', '/v1/agent/service/register', {
            'Name': name,
            'ID': id,
            'Address': address,
            'Port': port,
        })

    def deregister_service(self, service_id):
        log.msg('Deregistering %s.' % (service_id,))
        return self.consul_request('PUT', '/v1/agent/service/deregister/%s' % (
            service_id,))

    def sync_apps(self, purge=False):
        d = self.marathon_request('GET', '/v2/apps')
        d.addCallback(lambda response: response.json())
        d.addCallback(
            lambda data: gatherResults(
                [self.sync_app(app) for app in data['apps']]))
        if purge:
            d.addCallback(lambda _: self.purge_dead_services())
        return d

    def get_app(self, app_id):
        d = self.marathon_request('GET', '/v2/apps%s' % (app_id,))
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: data['app'])
        return d

    def sync_app(self, app):
        return gatherResults([
            self.sync_app_labels(app),
            self.sync_app_tasks(app),
            ])

    def sync_app_labels(self, app):
        labels = app.get('labels', {})
        return gatherResults([
            self.consul_request(
                'PUT', '/v1/kv/consular/%s/%s' % (
                    quote(get_appid(app['id'])), quote(key)), value)
            for key, value in labels.items()
        ])

    def sync_app_tasks(self, app):
        d = self.marathon_request('GET', '/v2/apps%(id)s/tasks' % app)
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: gatherResults(
            self.sync_app_task(app, task) for task in data['tasks']))
        return d

    def sync_app_task(self, app, task):
        return self.register_service(
            get_appid(app['id']), task['id'], task['host'], task['ports'][0])

    @inlineCallbacks
    def purge_dead_services(self):
        response = yield self.consul_request('GET', '/v1/agent/services')
        data = yield response.json()

        # collect the task ids for the service name
        services = {}
        for service_id, service in data.items():
            services.setdefault(service['Service'], set([])).add(service_id)

        for app_id, task_ids in services.items():
            yield self.purge_service_if_dead(app_id, task_ids)

    @inlineCallbacks
    def purge_service_if_dead(self, app_id, consul_task_ids):
        response = yield self.marathon_request(
            'GET', '/v2/apps/%s/tasks' % (app_id,))
        data = yield response.json()
        if 'tasks' not in data:
            log.msg(('App %s does not look like a Marathon application, '
                     'skipping') % (str(app_id),))
            return

        marathon_task_ids = set([task['id'] for task in data['tasks']])
        tasks_to_be_purged = consul_task_ids - marathon_task_ids
        for task_id in tasks_to_be_purged:
            yield self.deregister_service(task_id)
