# !/usr/bin/env python

import setuptools


setuptools.setup(
    name='konfug',
    py_modules=['konfug'],
    version='0.0.3',
    description='The configuration source for all your projects',
    author='Sergio Pulgarin',
    license='BSD',
    author_email='serpulga@gmail.com',
    url="https://github.com/serpulga/konfug",
    keywords=['konfug', 'datastore', 'configuration'],
    python_requires='>=3.6',
    install_requires=[
        "google-cloud-secret-manager==2.8.0",
        "google-cloud-datastore==2.4.0"
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Software Development',
    ],
)
