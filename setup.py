#!/usr/bin/env python
from setuptools import setup, find_packages

with open('README.rst', encoding='utf-8') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst', encoding='utf-8') as history_file:
    history = history_file.read()

requirements = [
    'aiohttp',
    'lxml',
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='micasa',
    version='0.1.0',
    description="micasa house hunting",
    long_description=readme + '\n\n' + history,
    author="Hugo P. Castilho",
    author_email='hcastilho@gmail.com',
    url='https://github.com/hcastilho/micasa',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    include_package_data=True,
    install_requires=requirements,
    license="ISCL",
    zip_safe=False,
    keywords='micasa',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: ISC License (ISCL)',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
