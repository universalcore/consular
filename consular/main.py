import json

from urllib import quote, urlencode
from twisted.internet import reactor
from twisted.web import client, server
# Twisted's fault HTTP11 client factory is way too verbose
client._HTTP11ClientFactory.noisy = False
from twisted.internet.defer import (
    succeed, inlineCallbacks, returnValue, gatherResults)
from twisted.python import log


import treq
from klein import Klein


def get_app_name(app_id):
    """
    Get the app name from the marathon app ID. Separators in the ID ('/') are
    replaced with '-'s while the leading separator is removed.
    """
    return app_id.lstrip('/').replace('/', '-')


def get_agent_endpoint(host):
    return 'http://%s:8500' % (host,)


class ConsularSite(server.Site):

    debug = False

    def log(self, request):
        if self.debug:
            server.Site.log(self, request)


class Consular(object):
    """
    :param str consul_endpoint:
        The HTTP endpoint for Consul (often http://example.org:8500).
    :param str marathon_endpoint:
        The HTTP endpoint for Marathon (often http://example.org:8080).
    :param bool enable_fallback:
        Fallback to the main Consul endpoint for registrations if unable
        to reach Consul running on the machine running a specific Marathon
        task.
    :param str registration_id:
        A unique parameter for this Consul server. It is used for house-keeping
        purposes such as purging tasks that are no longer running in Marathon.
    """

    app = Klein()
    debug = False
    clock = reactor
    timeout = 5
    fallback_timeout = 2
    requester = lambda self, *a, **kw: treq.request(*a, **kw)

    def __init__(self, consul_endpoint, marathon_endpoint, enable_fallback,
                 registration_id):
        self.consul_endpoint = consul_endpoint
        self.marathon_endpoint = marathon_endpoint
        self.pool = client.HTTPConnectionPool(self.clock, persistent=False)
        self.enable_fallback = enable_fallback
        self.registration_id = registration_id
        self.event_dispatch = {
            'status_update_event': self.handle_status_update_event,
        }

    def run(self, host, port):
        """
        Starts the HTTP server.

        :param str host:
            The host to bind to (example is ``localhost``)
        :param int port:
            The port to listen on (example is ``7000``)
        """
        site = ConsularSite(self.app.resource())
        site.debug = self.debug
        self.clock.listenTCP(port, site, interface=host)

    def get_marathon_event_callbacks(self):
        d = self.marathon_request('GET', '/v2/eventSubscriptions')
        d.addErrback(log.err)
        d.addCallback(lambda response: response.json())
        d.addCallback(self.get_marathon_event_callbacks_from_json)
        return d

    def get_marathon_event_callbacks_from_json(self, json):
        # NOTE:
        # Marathon may return a bad response when we get the existing event
        # callbacks. A common cause for this is that Marathon is not properly
        # configured. Raise an exception with information from Marathon if this
        # is the case, else return the callback URLs from the JSON response.
        if 'callbackUrls' not in json:
            raise RuntimeError('Unable to get existing event callbacks from ' +
                               'Marathon: %r' % (str(json),))

        return json['callbackUrls']

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
        """
        Register Consular with Marathon to receive HTTP event callbacks.
        To use this ensure that `Marathon is configured`_ to send HTTP event
        callbacks for state changes in tasks.

        :param str events_url:
            The HTTP endpoint to register with Marathon for event callbacks.

        .. _`Marathon is configured`:
            https://mesosphere.github.io/marathon/docs/event-bus.html
            #configuration
        """
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

    def log_http_error(self, failure, url):
        log.err(failure, 'Error performing request to %s' % (url,))
        return failure

    def marathon_request(self, method, path, data=None):
        return self._request(
            method, '%s%s' % (self.marathon_endpoint, path), data)

    def consul_request(self, method, url, data=None):
        return self._request(method, url, data, timeout=self.fallback_timeout)

    def _request(self, method, url, data, timeout=None):
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
            d.addCallback(self.log_http_response, method, url, data)

        d.addErrback(self.log_http_error, url)
        return d

    @app.route('/')
    def index(self, request):
        request.setHeader('Content-Type', 'application/json')
        return json.dumps([])

    @app.route('/events')
    def events(self, request):
        """
        Listens to incoming events from Marathon on ``/events``.

        :param klein.app.KleinRequest request:
            The Klein HTTP request
        """
        request.setHeader('Content-Type', 'application/json')
        event = json.load(request.content)
        handler = self.event_dispatch.get(
            event.get('eventType'), self.handle_unknown_event)
        return handler(request, event)

    def handle_status_update_event(self, request, event):
        """
        Handles status updates from Marathon.

        The various task stages are handled as follows:

        TASK_STAGING: ignored
        TASK_STARTING: ignored
        TASK_RUNNING: task data updated on Consul
        TASK_FINISHED: task data removed from Consul
        TASK_FAILED: task data removed from Consul
        TASK_KILLED: task data removed from Consul
        TASK_LOST: task data removed from Consul

        :param klein.app.KleinRequest request:
            The Klein HTTP request
        :param dict event:
            The Marathon event
        """
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
        d = self.deregister_service(
            get_agent_endpoint(event['host']),
            get_app_name(event['appId']),
            event['taskId'])
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

    def reg_id_tag(self):
        """ Get the registration ID tag for this instance of Consular. """
        return self._consular_tag('reg-id', self.registration_id)

    def app_id_tag(self, app_id):
        """ Get the app ID tag for the given app ID. """
        return self._consular_tag('app-id', app_id)

    def _consular_tag(self, tag_name, value):
        return self._consular_tag_key(tag_name) + value

    def get_app_id_from_tags(self, tags):
        """
        Get the app ID from the app ID tag in the given tags, or None if the
        tag could not be found.
        """
        return self._find_consular_tag(tags, 'app-id')

    def _find_consular_tag(self, tags, tag_name):
        pseudo_key = self._consular_tag_key(tag_name)
        matches = [tag for tag in tags if tag.startswith(pseudo_key)]

        if not matches:
            return None
        if len(matches) > 1:
            raise RuntimeError('Multiple (%d) Consular tags found for key '
                               '"%s": %s'
                               % (len(matches), pseudo_key, matches,))

        return matches[0].lstrip(pseudo_key)

    def _consular_tag_key(self, tag_name):
        return 'consular-%s=' % (tag_name,)

    def _create_service_registration(self, app_id, service_id, address, port):
        """
        Create the request body for registering a service with Consul.
        """
        registration = {
            'Name': get_app_name(app_id),
            'ID': service_id,
            'Address': address,
            'Port': port,
            'Tags': [
                self.reg_id_tag(),
                self.app_id_tag(app_id),
            ]
        }
        return registration

    def register_service(self, agent_endpoint,
                         app_id, service_id, address, port):
        """
        Register a task in Marathon as a service in Consul

        :param str agent_endpoint:
            The HTTP endpoint of where Consul on the Mesos worker machine
            can be accessed.
        :param str app_id:
            Marathon's App-id for the task.
        :param str service_id:
            The service-id to register it as in Consul.
        :param str address:
            The host address of the machine the task is running on.
        :param int port:
            The port number the task can be accessed on on the host machine.
        """
        log.msg('Registering %s at %s with %s at %s:%s.' % (
            app_id, agent_endpoint, service_id, address, port))
        registration = self._create_service_registration(app_id, service_id,
                                                         address, port)

        d = self.consul_request(
            'PUT',
            '%s/v1/agent/service/register' % (agent_endpoint,),
            registration)
        if self.enable_fallback:
            d.addErrback(self.register_service_fallback, registration)
        return d

    def register_service_fallback(self, failure, registration):
        log.msg('Falling back for %s at %s.' % (
            registration['Name'], self.consul_endpoint))
        return self.consul_request(
            'PUT',
            '%s/v1/agent/service/register' % (self.consul_endpoint,),
            registration)

    def deregister_service(self, agent_endpoint, app_id, service_id):
        """
        Deregister a service from Consul

        :param str agent_endpoint:
            The HTTP endpoint of where Consul on the Mesos worker machine
            can be accessed.
        :param str app_id:
            Marathon's App-id for the task.
        :param str service_id:
            The service-id to register it as in Consul.
        """
        log.msg('Deregistering %s at %s with %s' % (
            app_id, agent_endpoint, service_id,))
        return self.consul_request(
            'PUT', '%s/v1/agent/service/deregister/%s' % (
                agent_endpoint, service_id,))

    def sync_apps(self, purge=False):
        """
        Ensure all the apps in Marathon are registered as services
        in Consul.

        Set ``purge`` to ``True`` if you automatically want services in Consul
        that aren't registered in Marathon to be purged. Consular only purges
        services that have been registered with the same ``registration-id``.

        :param bool purge:
            To purge or not to purge.
        """
        d = self.get_marathon_apps()
        return d.addCallback(self.sync_and_purge_apps, purge)

    def get_marathon_apps(self):
        """ Get a list of running apps from the Marathon API. """
        d = self.marathon_request('GET', '/v2/apps')
        d.addCallback(lambda response: response.json())
        return d.addCallback(lambda data: data['apps'])

    def sync_and_purge_apps(self, apps, purge=False):
        deferreds = [gatherResults([self.sync_app(app) for app in apps])]
        if purge:
            deferreds.append(self.purge_dead_apps(apps))
        return gatherResults(deferreds)

    def get_app(self, app_id):
        d = self.marathon_request('GET', '/v2/apps%s' % (app_id,))
        d = d.addCallback(lambda response: response.json())
        return d.addCallback(lambda data: data['app'])

    def sync_app(self, app):
        return gatherResults([
            self.sync_app_labels(app),
            self.sync_app_tasks(app),
        ])

    def purge_dead_apps(self, apps):
        return gatherResults([
            self.purge_dead_services(),
            self.purge_dead_app_labels(apps)
        ])

    def sync_app_labels(self, app):
        """
        Sync the app labels for the given app by pushing its labels to the
        Consul k/v store and cleaning any labels there that are no longer
        present.

        :param: app:
            The app JSON as return by the Marathon HTTP API.
        """
        # NOTE: KV requests can go straight to the consul registry
        #       we're already connected to, they're not local to the agents.
        app_name = get_app_name(app['id'])
        labels = app.get('labels', {})
        return gatherResults([
            self.put_consul_app_labels(app_name, labels),
            self.clean_consul_app_labels(app_name, labels)
        ])

    def put_consul_app_labels(self, app_name, labels):
        """
        Store the given set of labels under the given app name in the Consul
        k/v store.
        """
        return self.put_consul_kvs({'consular/%s/%s' % (app_name, key,): value
                                    for key, value in labels.items()})

    def put_consul_kvs(self, key_values):
        """ Store the given key/value set in the Consul k/v store. """
        return gatherResults([self.put_consul_kv(key, value)
                              for key, value in key_values.items()])

    def put_consul_kv(self, key, value):
        """ Store the given value at the given key in the Consul k/v store. """
        return self.consul_request('PUT', '%s/v1/kv/%s' % (
            self.consul_endpoint, quote(key),), value)

    def clean_consul_app_labels(self, app_name, labels):
        """
        Delete app labels stored in the Consul k/v store under the given app
        name that aren't present in the given set of labels.
        """
        # Get the existing labels from Consul
        d = self.get_consul_app_keys(app_name)

        # Filter out the Marathon labels
        d.addCallback(self._filter_marathon_labels, labels)

        # Delete the non-existant keys
        return d.addCallback(self.delete_consul_kv_keys)

    def get_consul_app_keys(self, app_name):
        """ Get the Consul k/v keys for the app with the given name. """
        return self.get_consul_kv_keys('consular/%s' % (app_name,))

    def get_consul_consular_keys(self):
        """
        Get the next level of Consul k/v keys at 'consular/', i.e. will
        return 'consular/my-app' but not 'consular/my-app/my-label'.
        """
        return self.get_consul_kv_keys('consular/', separator='/')

    def get_consul_kv_keys(self, key_path, separator=None):
        """ Get the Consul k/v keys present at the given key path. """
        params = {'keys': ''}
        if separator:
            params['separator'] = separator
        d = self.consul_request('GET', '%s/v1/kv/%s?%s' % (
            self.consul_endpoint, quote(key_path), urlencode(params)))
        return d.addCallback(lambda response: response.json())

    def delete_consul_kv_keys(self, keys, recurse=False):
        """ Delete a sequence of Consul k/v keys. """
        return gatherResults([self.delete_consul_kv_key(key, recurse)
                              for key in keys])

    def delete_consul_kv_key(self, key, recurse=False):
        """ Delete the Consul k/v entry associated with the given key. """
        return self.consul_request('DELETE', '%s/v1/kv/%s%s' % (
            self.consul_endpoint, quote(key),
            '?recurse' if recurse else '',))

    def _filter_marathon_labels(self, consul_keys, marathon_labels):
        """
        Takes a list of Consul keys and removes those with keys not found in
        the given dict of Marathon labels.

        :param: consul_keys:
            The list of Consul keys as returned by the Consul API.
        :param: marathon_labels:
            The dict of Marathon labels as returned by the Marathon API.
        """
        label_key_set = set(marathon_labels.keys())
        return [key for key in consul_keys
                if (self._consul_key_to_marathon_label_key(key)
                    not in label_key_set)]

    def _consul_key_to_marathon_label_key(self, consul_key):
        """
        Trims the 'consular/<app_name>/' from the front of the key path to get
        the Marathon label key.
        """
        return consul_key.split('/', 2)[-1]

    def sync_app_tasks(self, app):
        d = self.marathon_request('GET', '/v2/apps%(id)s/tasks' % app)
        d.addCallback(lambda response: response.json())
        return d.addCallback(lambda data: gatherResults(
            self.sync_app_task(app, task) for task in data['tasks']))

    def sync_app_task(self, app, task):
        return self.register_service(
            get_agent_endpoint(task['host']), app['id'], task['id'],
            task['host'], task['ports'][0])

    def purge_dead_app_labels(self, apps):
        """
        Delete any keys stored in the Consul k/v store that belong to apps that
        no longer exist.

        :param: apps:
            The list of apps as returned by the Marathon API.
        """
        # Get the existing keys
        d = self.get_consul_consular_keys()

        # Filter the present apps out
        d.addCallback(self._filter_marathon_apps, apps)

        # Delete the remaining keys
        return d.addCallback(self.delete_consul_kv_keys, recurse=True)

    def _filter_marathon_apps(self, consul_keys, marathon_apps):
        """
        Takes a list of Consul keys and removes those with keys not found in
        the given list of Marathon apps.

        :param: consul_keys:
            The list of Consul keys as returned by the Consul API.
        :param: marathon_apps:
            The list of apps as returned by the Marathon API.
        """
        app_name_set = set([get_app_name(app['id']) for app in marathon_apps])
        return [key for key in consul_keys
                if (self._consul_key_to_marathon_app_name(key)
                    not in app_name_set)]

    def _consul_key_to_marathon_app_name(self, consul_key):
        """
        Trims the 'consular/' from the front of the key path to get the
        Marathon app name.
        """
        return consul_key.split('/', 1)[-1]

    def purge_dead_services(self):
        d = self.consul_request(
            'GET', '%s/v1/catalog/nodes' % (self.consul_endpoint,))
        d.addCallback(lambda response: response.json())
        d.addCallback(lambda data: gatherResults([
            self.purge_dead_agent_services(
                get_agent_endpoint(node['Address'])) for node in data
        ]))
        return d

    @inlineCallbacks
    def purge_dead_agent_services(self, agent_endpoint):
        response = yield self.consul_request(
            'GET', '%s/v1/agent/services' % (agent_endpoint,))
        data = yield response.json()

        # collect the task ids for the service name
        services = {}
        for service_id, service in data.items():
            # Check the service for a tag that matches our registration ID
            tags = service['Tags']
            if tags and self.reg_id_tag() in tags:
                app_id = self.get_app_id_from_tags(tags)
                if app_id:
                    services.setdefault(app_id, set()).add(service_id)
                else:
                    log.msg('Service "%s" does not have an app ID in its '
                            'tags, it cannot be purged.'
                            % (service['Service'],))
            elif self.debug:
                log.msg('Service "%s" is not tagged with our registration ID, '
                        'not touching it.' % (service['Service'],))

        for app_id, task_ids in services.items():
            yield self.purge_service_if_dead(agent_endpoint, app_id, task_ids)

    @inlineCallbacks
    def purge_service_if_dead(self, agent_endpoint, app_id, consul_task_ids):
        response = yield self.marathon_request(
            'GET', '/v2/apps%s/tasks' % (app_id,))
        data = yield response.json()
        tasks_to_be_purged = set(consul_task_ids)
        if 'tasks' in data:
            marathon_task_ids = set([task['id'] for task in data['tasks']])
            tasks_to_be_purged -= marathon_task_ids

        for task_id in tasks_to_be_purged:
            yield self.deregister_service(agent_endpoint, app_id, task_id)
