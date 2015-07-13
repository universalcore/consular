import json

from twisted.internet import reactor
from twisted.web.client import HTTPConnectionPool

import treq
from klein import Klein


class Consular(object):

    app = Klein()

    def __init__(self, consul_endpoint, marathon_endpoint):
        self.consul_endpoint = consul_endpoint
        self.marathon_endpoint = marathon_endpoint
        self.pool = HTTPConnectionPool(reactor, persistent=False)
        self.event_dispatch = {
            'status_update_event': self.handle_status_update_event,
        }
        self.unknown_event_handler = self.handle_unknown_event

    def consul_request(self, method, path, data):
        return treq.request(
            method, '%s%s' % (self.consul_endpoint, path),
            headers={
                'Content-Type': 'application/json',
            },
            data=json.dumps(data),
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
        # NOTE: Marathon sends a list of ports, I don't know yet when & if
        #       there are multiple values in that list.
        d = self.consul_request('PUT', '/v1/agent/service/register', {
            "Name": event['appId'].rsplit('/', 1)[1],
            "Address": event['host'],
            "Port": event['ports'][0],
        })
        d.addCallback(lambda _: json.dumps({'status': 'ok'}))
        return d

    def handle_unknown_event(self, request, event):
        request.setHeader('Content-Type', 'application/json')
        request.setResponseCode(400)  # bad request
        return json.dumps({
            'error': 'Event type %s not supported.' % (event.get('eventType'),)
        })
