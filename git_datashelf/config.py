#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 09:51:20 2024

@author: andreasgeiges
"""
import os

DEBUG = True
MODULE_PATH = os.path.dirname(__file__)

SOURCE_META_FIELDS = [
    'SOURCE_ID',
    'collected_by',
    'date',
    'source_url',
    'licence',
    'git_commit_hash',
    'tag'
]

INVENTORY_FIELDS = [
    'variable',
    'entity',
    'category',
    'pathway',
    'scenario',
    'model',
    'source',
    'source_name',
    'source_year',
    'unit',
]