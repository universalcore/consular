import json

from klein import Klein


class Consular(object):

    app = Klein()

    def __init__(self, consul_endpoint, marathon_endpoint):
        self.consul_endpoint = consul_endpoint
        self.marathon_endpoint = marathon_endpoint

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        return json.dumps([])
