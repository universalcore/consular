import json

from twisted.internet.defer import inlineCallbacks, DeferredQueue
from twisted.trial.unittest import TestCase
from twisted.web.server import NOT_DONE_YET

from txfake import FakeHttpServer
from txfake.fake_connection import wait0

from consular.clients import HTTPError, JsonClient


class JsonClientTestBase(TestCase):
    def setUp(self):
        self.client = self.get_client()
        self.requests = DeferredQueue()
        self.fake_server = FakeHttpServer(self.handle_request)

        self.client.agent = self.fake_server.get_agent()

    def handle_request(self, request):
        self.requests.put(request)
        return NOT_DONE_YET

    def get_client(self):
        """To be implemented by subclass"""
        raise NotImplementedError()

    def write_json_response(self, request, json_data, response_code=200,
                            headers={'Content-Type': 'application/json'}):
        request.setResponseCode(response_code)
        for name, value in headers.items():
            request.setHeader(name, value)
        request.write(json.dumps(json_data))
        request.finish()

    def uri(self, path):
        return '%s%s' % (self.client.endpoint, path,)


class JsonClientTest(JsonClientTestBase):

    def get_client(self):
        return JsonClient('http://localhost:8000')

    @inlineCallbacks
    def test_request(self):
        """
        When a request is made, it should be made with the correct method,
        address and headers, and should contain an empty body. The response
        should be returned.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))
        self.assertEqual(request.getHeader('content-type'), 'application/json')
        self.assertEqual(request.getHeader('accept'), 'application/json')
        self.assertEqual(request.content.read(), '')

        request.setResponseCode(200)
        request.write('hi\n')
        request.finish()

        response = yield d
        text = yield response.text()
        self.assertEqual(text, 'hi\n')

    @inlineCallbacks
    def test_request_json_data(self):
        """
        When a request is made with the json_data parameter set, that data
        should be sent as JSON.
        """
        d = self.client.request('GET', '/hello', json_data={'test': 'hello'})

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))
        self.assertEqual(json.load(request.content), {'test': 'hello'})

        request.setResponseCode(200)
        request.finish()

        yield d

    @inlineCallbacks
    def test_request_endpoint(self):
        """
        When a request is made with the endpoint parameter set, that parameter
        should be used as the endpoint.
        """
        d = self.client.request('GET', '/hello',
                                endpoint='http://localhost:9000')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, 'http://localhost:9000/hello')

        request.setResponseCode(200)
        request.finish()

        yield d

    @inlineCallbacks
    def test_get_json(self):
        """
        When the get_json method is called, a GET request should be made and
        the response should be deserialized from JSON.
        """
        d = self.client.get_json('/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(200)
        request.write(json.dumps({'test': 'hello'}))
        request.finish()

        res = yield d
        self.assertEqual(res, {'test': 'hello'})

    @inlineCallbacks
    def test_client_error_response(self):
        """
        When a request is made and a 4xx response code is returned, a HTTPError
        should be raised to indicate a client error.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(403)
        request.write('Unauthorized\n')
        request.finish()

        yield wait0()
        failure = self.failureResultOf(d, HTTPError)
        self.assertEqual(
            failure.getErrorMessage(),
            '403 Client Error for url: %s' % self.uri('/hello'))

    @inlineCallbacks
    def test_server_error_response(self):
        """
        When a request is made and a 5xx response code is returned, a HTTPError
        should be raised to indicate a server error.
        """
        d = self.client.request('GET', '/hello')

        request = yield self.requests.get()
        self.assertEqual(request.method, 'GET')
        self.assertEqual(request.uri, self.uri('/hello'))

        request.setResponseCode(502)
        request.write('Bad gateway\n')
        request.finish()

        yield wait0()
        failure = self.failureResultOf(d, HTTPError)
        self.assertEqual(
            failure.getErrorMessage(),
            '502 Server Error for url: %s' % self.uri('/hello'))
