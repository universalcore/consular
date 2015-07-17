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

    def marathon_request(self, method, path, data=None):
        return treq.request(
            method, ('%s%s' % (self.marathon_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)

    def consul_request(self, method, path, data=None):
        return treq.request(
            method, ('%s%s' % (self.consul_endpoint, path)).encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool)

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
        d = self.deregister_service(event['host'], event['taskId'])
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

    def register_service(self, node_id, name, id, address, port):
        log.msg('Registering %s.' % (name,))
        return self.consul_request(
            'PUT', '/v1/catalog/service/register', {
                'Node': node_id,
                'Service': {
                    'Service': name,
                    'ID': id,
                    'Address': address,
                    'Port': port,
                }
            })

    def deregister_service(self, node_id, service_id):
        log.msg('Deregistering %s.' % (service_id,))
        return self.consul_request(
            'PUT', '/v1/catalog/deregister', {
                'Node': node_id,
                'ServiceID': service_id,
            })

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
            task['host'],
            get_appid(app['id']), task['id'], task['host'], task['ports'][0])

    @inlineCallbacks
    def purge_dead_services(self):
        service_names_response = yield self.consul_request(
            'GET', '/v1/catalog/services')
        service_names_data = yield service_names_response.json()

        # collect the task ids for the service name
        for service_name in service_names_data.keys():
            services_response = yield self.consul_request(
                'GET', '/v1/catalog/service/%s' % (service_name,))
            services = yield services_response.json()
            yield self.purge_service_if_dead(service_name, services)

    @inlineCallbacks
    def purge_service_if_dead(self, service_name, services):
        response = yield self.marathon_request(
            'GET', '/v2/apps/%s/tasks' % (service_name,))
        data = yield response.json()
        if 'tasks' not in data:
            log.msg(('App %s does not look like a Marathon application, '
                     'skipping') % (str(service_name),))
            return

        marathon_task_ids = set([task['id'] for task in data['tasks']])
        for service in services:
            if service['ServiceID'] not in marathon_task_ids:
                yield self.deregister_service(
                    service['Node'], service['ServiceID'])
