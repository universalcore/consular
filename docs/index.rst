Consular
========

Consular is a micro-service that relays information between Marathon_ and
Consul_. It registers itself for HTTP event callbacks with Marathon_ and makes
the appropriate API calls to register applications that Marathon_ runs as
services in Consul_. Registration of applications happens in the same way.

Marathon_ is always considered the source of truth.

If Marathon application definitions contain labels_ (application metadata)
they will be added to the Consul_ key/value store. This can be especially
useful when Consul_ is combined with `Consul Template`_ to automatically
generate configuration files for proxies such as HAProxy_ and Nginx_.

.. image:: https://travis-ci.org/universalcore/consular.svg?branch=develop
    :target: https://travis-ci.org/universalcore/consular
    :alt: Continuous Integration

.. image:: https://coveralls.io/repos/universalcore/consular/badge.png?branch=develop
    :target: https://coveralls.io/r/universalcore/consular?branch=develop
    :alt: Code Coverage

.. image:: https://readthedocs.org/projects/consular/badge/?version=latest
    :target: https://consular.readthedocs.org
    :alt: Consular Documentation

.. image:: https://badge.fury.io/py/consular.svg
    :target: https://pypi.python.org/pypi/consular
    :alt: Pypi Package

Usage
~~~~~

::

    $ pip install consular
    $ consular --help


Installing for local dev
~~~~~~~~~~~~~~~~~~~~~~~~

::

    $ git clone https://github.com/universalcore/consular.git
    $ cd consular
    $ virtualenv ve
    $ source ve/bin/activate
    (ve)$ pip install -e .
    (ve)$ pip install -r requirements-dev.txt

Running tests
~~~~~~~~~~~~~

::

    $ source ve/bin/activate
    (ve)$ py.test consular

.. _Marathon: http://mesosphere.github.io/marathon/
.. _Consul: http://consul.io/
.. _labels: https://mesosphere.github.io/marathon/docs/rest-api.html#labels-object-of-string-values
.. _HAProxy: http://www.haproxy.org/
.. _Nginx: http://nginx.org/
.. _`Consul Template`: https://github.com/hashicorp/consul-template
