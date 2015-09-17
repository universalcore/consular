from urllib import quote, urlencode
import json

from twisted.internet import reactor
from twisted.python import log
from twisted.web import client

# Twisted's default HTTP11 client factory is way too verbose
client._HTTP11ClientFactory.noisy = False

import treq


class JsonClient(object):
    debug = False
    clock = reactor
    timeout = 5
    requester = lambda self, *a, **kw: treq.request(*a, **kw)

    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.pool = client.HTTPConnectionPool(self.clock, persistent=False)

    def _log_http_response(self, response, method, path, data):
        log.msg('%s %s with %s returned: %s' % (
            method, path, data, response.code))
        return response

    def _log_http_error(self, failure, url):
        log.err(failure, 'Error performing request to %s' % (url,))
        return failure

    def request(self, method, path, endpoint=None, json_data=None, **kwargs):
        """
        Perform a request. A number of basic defaults are set on the request
        that make using a JSON API easier.

        :param: method:
            The HTTP method to use (example is `GET`).
        :param: path:
            The URL path. This is appended to the endpoint and should start
            with a '/' (example is `/v2/apps`).
        :param: endpoint:
            The URL endpoint to use. The default value is the endpoint this
            client was created with (`self.endpoint`) (example is
            `http://localhost:8080`)
        :param: json_data:
            A python data structure that will be converted to a JSON string
            using `json.dumps` and used as the request body.
        :param: kwargs:
            Any other parameters that will be passed to `treq.request`, for
            example headers or parameters.
        """
        url = ('%s%s' % (endpoint or self.endpoint, path)).encode('utf-8')

        data = json.dumps(json_data) if json_data else None
        requester_kwargs = {
            'headers': {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            'data': data,
            'pool': self.pool,
            'timeout': self.timeout
        }
        requester_kwargs.update(kwargs)

        d = self.requester(method, url, **requester_kwargs)

        if self.debug:
            d.addCallback(self._log_http_response, method, url, data)

        return d.addErrback(self._log_http_error, url)

    def get_json(self, path, **kwargs):
        """
        Perform a GET request to the given path and return the JSON response.
        """
        d = self.request('GET', path, **kwargs)
        return d.addCallback(lambda response: response.json())


class MarathonClient(JsonClient):

    def _basic_get_request(self, path, field, raise_error=True):
        d = self.get_json(path)
        return d.addCallback(self._get_json_field, field, raise_error)

    def _get_json_field(self, response_json, field_name, raise_error=True):
        if field_name not in response_json:
            if raise_error:
                raise KeyError('Unable to get value for "%s" from Marathon '
                               'response: "%s"' % (
                                   field_name, str(response_json),))
            else:
                return None

        return response_json[field_name]

    def get_event_subscriptions(self):
        return self._basic_get_request(
            '/v2/eventSubscriptions', 'callbackUrls')

    def post_event_subscription(self, callback_url):
        d = self.request(
            'POST', '/v2/eventSubscriptions?%s' % urlencode({
                'callbackUrl': callback_url,
            }))
        return d.addCallback(lambda response: response.code == 200)

    def get_apps(self):
        return self._basic_get_request('/v2/apps', 'apps')

    def get_app(self, app_id):
        return self._basic_get_request('/v2/apps%s' % (app_id,), 'app')

    def get_app_tasks(self, app_id, raise_error=True):
        return self._basic_get_request(
            '/v2/apps%s/tasks' % (app_id,), 'tasks', raise_error)


class ConsulClient(JsonClient):

    fallback_timeout = 2

    def __init__(self, endpoint, enable_fallback=False):
        super(ConsulClient, self).__init__(endpoint)
        self.endpoint = endpoint
        self.enable_fallback = enable_fallback

    def register_agent_service(self, agent_endpoint, registration):
        d = self.request('PUT', '/v1/agent/service/register',
                         endpoint=agent_endpoint, json_data=registration)

        if self.enable_fallback:
            d.addErrback(self.register_agent_service_fallback, registration)

        return d

    def register_agent_service_fallback(self, failure, registration):
        log.msg('Falling back for %s at %s.' % (
            registration['Name'], self.endpoint))
        return self.request(
            'PUT', '/v1/agent/service/register',  json_data=registration,
            timeout=self.fallback_timeout)

    def deregister_agent_service(self, agent_endpoint, service_id):
        return self.request('PUT', '/v1/agent/service/deregister/%s' % (
            service_id,), endpoint=agent_endpoint)

    def put_kv(self, key, value):
        return self.request(
            'PUT', '/v1/kv/%s' % (quote(key),), json_data=value)

    def get_kv_keys(self, keys_path, separator=None):
        params = {'keys': ''}
        if separator:
            params['separator'] = separator
        return self.get_json('/v1/kv/%s?%s' % (quote(keys_path),
                                               urlencode(params)))

    def delete_kv_keys(self, key, recurse=False):
        return self.request('DELETE', '/v1/kv/%s%s' % (
            quote(key), '?recurse' if recurse else '',))

    def get_catalog_nodes(self):
        return self.get_json('/v1/catalog/nodes')

    def get_agent_services(self, agent_endpoint):
        return self.get_json('/v1/agent/services', endpoint=agent_endpoint)
