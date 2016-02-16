import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

with open(os.path.join(here, 'VERSION')) as f:
    version = f.read().strip()

setup(name='consular',
      version=version,
      description='Consular',
      long_description=README,
      classifiers=[
          "Programming Language :: Python",
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
      ],
      author='Simon de Haan',
      author_email='simon@praekeltfoundation.org',
      url='http://github.com/universalcore/consular',
      license='BSD',
      keywords='marathon,consul,mesos',
      packages=find_packages(exclude=['docs']),
      include_package_data=True,
      zip_safe=False,
      install_requires=[
        'click',
        'Klein',
        'treq',
        'Twisted',
      ],
      entry_points={
          'console_scripts': ['consular = consular.cli:main'],
      })
