#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 11:17:04 2024

@author: andreasgeiges
"""
import shutil
from pathlib import Path
import os
import pandas as pd
import git

from . import config


def create_empty_datashelf(pathToDataself, force_new=False):
    """
    User funciton to create a empty datashelf

    Parameters
    ----------
    pathToDataself : TYPE
        DESCRIPTION.
    force_new : TYPE, optional
        DESCRIPTION. The default is False.

    Returns
    -------
    None.

    """
    from . import config

    _create_empty_datashelf(
        pathToDataself,
        config.MODULE_PATH,
        config.SOURCE_META_FIELDS,
        config.INVENTORY_FIELDS,
        force_new=force_new,
    )
    
def _create_empty_datashelf(
    pathToDataself, MODULE_PATH, SOURCE_META_FIELDS, INVENTORY_FIELDS, force_new=False
):
    """
    Private function to create empty datashelf without propper
    init of config
    """


    path = Path(pathToDataself)

    if force_new:
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)

    # add subfolders database
    Path(os.path.join(pathToDataself, 'database')).mkdir()
    #Path(os.path.join(pathToDataself, 'mappings')).mkdir()
    Path(os.path.join(pathToDataself, 'rawdata')).mkdir()

    # # create mappings
    # os.makedirs(os.path.join(pathToDataself, 'mappings'), exist_ok=True)
    # shutil.copyfile(
    #     os.path.join(MODULE_PATH, 'data/regions.csv'),
    #     os.path.join(pathToDataself, 'mappings/regions.csv'),
    # )
    # shutil.copyfile(
    #     os.path.join(MODULE_PATH, 'data/continent.csv'),
    #     os.path.join(pathToDataself, 'mappings/continent.csv'),
    # )
    # shutil.copyfile(
    #     os.path.join(MODULE_PATH, 'data/country_codes.csv'),
    #     os.path.join(pathToDataself, 'mappings/country_codes.csv'),
    # )

    sourcesDf = pd.DataFrame(columns=SOURCE_META_FIELDS)
    filePath = os.path.join(pathToDataself, 'sources.csv')
    sourcesDf.to_csv(filePath,  index=False)

    inventoryDf = pd.DataFrame(columns=INVENTORY_FIELDS)
    filePath = os.path.join(pathToDataself, 'inventory.csv')
    inventoryDf.to_csv(filePath)
    git.Repo.init(pathToDataself)