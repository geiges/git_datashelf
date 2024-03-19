#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 11:37:04 2024

@author: andreasgeiges
"""
import shutil
import tempfile
import os

from git_datashelf import create_empty_datashelf

def test_create_empty_datashelf():
    
    tmp_folder = tempfile.gettempdir()
    tmp_shelf = os.path.join(tmp_folder, 'datashelf')
    create_empty_datashelf(tmp_shelf)
    
    shutil.rmtree(tmp_shelf)
    pass