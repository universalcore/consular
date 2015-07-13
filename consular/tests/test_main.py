from twisted.trial.unittest import TestCase
from twisted.web.server import Site
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.web.client import HTTPConnectionPool

from consular.main import Consular

import treq


class ConsularTest(TestCase):

    def setUp(self):
        self.consular = Consular(
            'http://localhost:8500',
            'http://localhost:8080',
        )
        self.site = Site(self.consular.app.resource())
        self.listener = reactor.listenTCP(0, self.site, interface='localhost')
        self.listener_port = self.listener.getHost().port
        self.addCleanup(self.listener.loseConnection)
        self.pool = HTTPConnectionPool(reactor, persistent=False)
        self.addCleanup(self.pool.closeCachedConnections)

    def request(self, method, path):
        return treq.request(method, 'http://localhost:%s%s' % (
            self.listener_port,
            path
        ), pool=self.pool)

    def tearDown(self):
        pass

    @inlineCallbacks
    def test_service(self):
        response = yield self.request('GET', '/')
        self.assertEqual(response.code, 200)
        self.assertEqual((yield response.json()), [])
