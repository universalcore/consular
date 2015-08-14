Consular
========

Receive events from Marathon_, update Consul_ with the relevant information
about services & tasks.

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


.. _Marathon: http://mesosphere.github.io/marathon/
.. _Consul: http://consul.io/
