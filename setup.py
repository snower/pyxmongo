#!/usr/bin/env python
from setuptools import setup


setup(
    name='pyxmongo',
    version='0.0.1',
    packages=['pyxmongo'],
    package_data={
        '': ['README.md'],
    },
    install_requires=['pymongo>=2.6.3'],
    author='snower',
    author_email='sujian199@gmail.com',
    url='http://github.com/snower/pyxmongo',
    license='MIT',
    description='pymongo分库分表client',
    long_description='pymongo分库分表client'
)
