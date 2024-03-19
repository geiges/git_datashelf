#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 10:57:13 2024

@author: andreasgeiges
"""

from __future__ import print_function


from setuptools import setup

def main():
    packages = [
        'git_datashelf',

        ]
    pack_dir = {
        'git_datashelf': 'git_datashelf',
#	     'data': 'git_datashelf/data'
    }
    package_data = {
        'git_datashelf': 
            [
                'data/*',
                'data/SANDBOX_datashelf/*',
                'data/SANDBOX_datashelf/mappings/*',
            ]
        }
    with open('README.md', encoding='utf-8') as f:
        long_description = f.read()
#    packages = find_packages()
    setup_kwargs = {
        "name": "git_datashelf",
        "description": 'The Python Datamanagement system using git',
        "long_description" : long_description,
        "long_description_content_type" :"text/markdown",
        "author": 'Andreas Geiges',
        "author_email": 'a.geiges@gmail.com',
        "url": 'https://github.com/geiges/git_datashelf',
        "packages": packages,
        "package_dir": pack_dir,
        "package_data" : package_data,
        "use_scm_version": {'write_to': 'git_datashelf/version.py'},
        "setup_requires": ['setuptools_scm'],
        "install_requires": [
            "appdirs",
            "pandas",
            "gitpython",
            "tqdm",
            "openpyxl",
            "tabulate",        
            "deprecated",],
        }
    rtn = setup(**setup_kwargs)

if __name__ == "__main__":
    main()

