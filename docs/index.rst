Consular
========

Consular is a micro-service that relays information between Marathon_ and
Consul_. It registers itself for HTTP event callbacks with Marathon_ and makes
the appropriate API calls to register applications that Marathon_ runs as
services in Consul_. De-registrations of applications happens in the same way.

Marathon_ is always considered the source of truth.

If Marathon application definitions contain labels_ (application metadata)
they will be added to the Consul_ key/value store. This can be especially
useful when Consul_ is combined with `Consul Template`_ to automatically
generate configuration files for proxies such as HAProxy_ and Nginx_.

.. image:: http://www.websequencediagrams.com/cgi-bin/cdraw?lz=dGl0bGUgQ29uc3VsYXIsAAMHICYgTWFyYXRob24KCgACCCAtPgAgCTogTm90aWZpY2F0aW9uIG9mXG5uZXcgYXBwbAANBwoATAgALQo6IFJlZ2lzdHIAJw5zZXJ2aWNlABwVQWRkIEEASwogbWV0YWRhdGFcbihsYWJlbHMpIHRvAIE4B1xuSy9WIHN0b3IARAgAgSgKLVRlbXBsYXRlAIEmE2NoYW5nZXMAgSoHACEJIC0-IExvYWQtQmFsYW5jZXI6IEdlbmVyYXRlIG5ld1xubG9hZC1iABYHIGNvbmZpZwAiI1JlbG9hZAApB3UAggYG&s=napkin
    :target: http://www.websequencediagrams.com/?lz=dGl0bGUgQ29uc3VsYXIsAAMHICYgTWFyYXRob24KCgACCCAtPgAgCTogTm90aWZpY2F0aW9uIG9mXG5uZXcgYXBwbAANBwoATAgALQo6IFJlZ2lzdHIAJw5zZXJ2aWNlABwVQWRkIEEASwogbWV0YWRhdGFcbihsYWJlbHMpIHRvAIE4B1xuSy9WIHN0b3IARAgAgSgKLVRlbXBsYXRlAIEmE2NoYW5nZXMAgSoHACEJIC0-IExvYWQtQmFsYW5jZXI6IEdlbmVyYXRlIG5ld1xubG9hZC1iABYHIGNvbmZpZwAiI1JlbG9hZAApB3UAggYG&s=napkin
    :alt: consular sequence diagram

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


Consular Class Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. automodule:: consular.main
    :members:
