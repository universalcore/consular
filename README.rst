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

Deprecated
~~~~~~~~~~
This project is no longer maintained. Instead, we recommend you use marathon-lb_ for load-balancing or marathon-consul_ for syncing state between Marathon and Consul.

Usage
~~~~~

::

    $ pip install consular
    $ consular --help


.. _Marathon: http://mesosphere.github.io/marathon/
.. _Consul: http://consul.io/
.. _marathon-lb: https://github.com/mesosphere/marathon-lb
.. _marathon-consul: https://github.com/allegro/marathon-consul
