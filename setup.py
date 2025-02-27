#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-exchangeratesapi',
      version='0.1.1',
      description='Singer.io tap for extracting currency exchange rate data from the exchangeratesapi.io API',
      author='Stitch',
      url='http://github.com/singer-io/tap-exchangeratesapi',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_exchangeratesapi'],
      install_requires=['singer-python==5.12.2',
                        'backoff==1.8.0',
                        'requests==2.27.1',
                        'pandas==1.3.5'],
      extras_require={
          'dev': [
              'ipdb==0.11'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-exchangeratesapi=tap_exchangeratesapi:main
      ''',
      packages=['tap_exchangeratesapi'],
      include_package_data=True
)
