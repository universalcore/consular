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

    def __init__(self):
        self.pool = client.HTTPConnectionPool(self.clock, persistent=False)

    def _log_http_response(self, response, method, path, data):
        log.msg('%s %s with %s returned: %s' % (
            method, path, data, response.code))
        return response

    def _log_http_error(self, failure, url):
        log.err(failure, 'Error performing request to %s' % (url,))
        return failure

    def request(self, method, url, data=None, timeout=None):
        d = self.requester(
            method,
            url.encode('utf-8'),
            headers={
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            },
            data=(json.dumps(data) if data is not None else None),
            pool=self.pool,
            timeout=timeout or self.timeout)

        if self.debug:
            d.addCallback(self._log_http_response, method, url, data)

        return d.addErrback(self._log_http_error, url)

    @classmethod
    def response_json(cls, response):
        return response.json()

    @classmethod
    def response_ok(cls, response):
        return response.code == 200


class MarathonClient(JsonClient):

    def __init__(self, endpoint):
        super(MarathonClient, self).__init__()
        self.endpoint = endpoint

    def marathon_request(self, method, path, data=None):
        return self.request(method, '%s%s' % (self.endpoint, path), data)

    def _basic_get_request(self, path, field, raise_error=True):
        d = self.marathon_request('GET', path)
        d.addCallback(JsonClient.response_json)
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
        d = self.marathon_request(
            'POST', '/v2/eventSubscriptions?%s' % urlencode({
                'callbackUrl': callback_url,
            }))
        return d.addCallback(JsonClient.response_ok)

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
        super(ConsulClient, self).__init__()
        self.endpoint = endpoint
        self.enable_fallback = enable_fallback

    def consul_request(self, method, path, endpoint=None, data=None,
                       timeout=None):
        if not endpoint:
            endpoint = self.endpoint
        if not timeout:
            timeout = self.timeout

        return self.request(
            method, '%s%s' % (endpoint, path,), data=data, timeout=timeout)

    def register_agent_service(self, agent_endpoint, registration):
        d = self.consul_request('PUT', '/v1/agent/service/register',
                                endpoint=agent_endpoint, data=registration)

        if self.enable_fallback:
            d.addErrback(self.register_agent_service_fallback, registration)

        return d

    def register_agent_service_fallback(self, failure, registration):
        log.msg('Falling back for %s at %s.' % (
            registration['Name'], self.endpoint))
        return self.consul_request(
            'PUT', '/v1/agent/service/register',  data=registration,
            timeout=self.fallback_timeout)

    def deregister_agent_service(self, agent_endpoint, service_id):
        return self.consul_request('PUT', '/v1/agent/service/deregister/%s' % (
            service_id,), endpoint=agent_endpoint)

    def put_kv(self, key, value):
        return self.consul_request(
            'PUT', '/v1/kv/%s' % (quote(key),), data=value)

    def get_kv_keys(self, keys_path, separator=None):
        params = {'keys': ''}
        if separator:
            params['separator'] = separator
        d = self.consul_request('GET', '/v1/kv/%s?%s' % (
            quote(keys_path), urlencode(params)))
        return d.addCallback(JsonClient.response_json)

    def delete_kv_keys(self, key, recurse=False):
        return self.consul_request('DELETE', '/v1/kv/%s%s' % (
            quote(key), '?recurse' if recurse else '',))

    def get_catalog_nodes(self):
        d = self.consul_request('GET', '/v1/catalog/nodes')
        return d.addCallback(JsonClient.response_json)

    def get_agent_services(self, agent_endpoint):
        d = self.consul_request(
            'GET', '/v1/agent/services', endpoint=agent_endpoint)
        return d.addCallback(JsonClient.response_json)
