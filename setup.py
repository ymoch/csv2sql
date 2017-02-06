#!/usr/bin/env python

"""Setting up program for csv2sql."""

import os
import setuptools

import csv2sql.meta


base_dir_path = os.path.dirname(__file__)
readme_file_path = os.path.join(base_dir_path, 'README.rst')
requirements_file_path = os.path.join(base_dir_path, 'requirements.txt')

long_description = open(readme_file_path).read()
requirements = open(requirements_file_path).readlines()
packages = setuptools.find_packages()

setuptools.setup(
    name='csv2sql',
    description='Convert CSV data into SQL.',
    long_description=long_description,
    version=csv2sql.meta.__version__,
    author=csv2sql.meta.__author__,
    author_email=csv2sql.meta.__author_email__,
    url='https://github.com/ymoch/csv2sql',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: SQL',
        'Topic :: Software Development :: Code Generators',
        'Topic :: Utilities',
    ],

    install_requires=requirements,
    packages=packages,
    entry_points={
        'console_scripts': [
            'csv2sql=csv2sql.main:main',
        ],
    },

    test_suite='nose.collector',
    tests_require=['nose', 'nose_parameterized', 'mock'],
)
