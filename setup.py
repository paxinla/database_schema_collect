#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import

import os
from setuptools import setup, find_packages

ROOTDIR = os.path.abspath(os.path.dirname(__file__))

def requirements():
    pip_reqf = os.path.join(os.path.abspath(os.path.dirname(__file__)), "requirements.txt")
    reqs = []
    with open(pip_reqf, "rb") as f:
        for each_line in f:
            if each_line.startswith("-f"):
                continue
            reqs.append(each_line.strip())
    return reqs


setup(
    name='database_schema_collect',
    version='0.1.dev0',
    description='A little tool for managing metadata of tables.',
    long_description=open('ReadMe.md').read(),
    author="paxinla",
    license="MIT",
    packages=find_packages(),
    package_data={"database_schema_collect": ["config.ini"]},
    install_requires=requirements(),
    keywords='metadata postgresql hive',
    classifiers=[
           'Development Status :: 3 - Alpha',
           'Intended Audience :: Developers',
           'Intended Audience :: System Administrators',
           'Topic :: System :: Systems Administration',
           'License :: OSI Approved :: MIT License',
           'Programming Language :: Python :: 2',
           'Programming Language :: Python :: 2.7'
    ],
    entry_points={
        'console_scripts': [
            'database_schema_collect=database_schema_collect.run:main',
        ],
    }
)
