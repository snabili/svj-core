#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path as osp
import os, logging

from .logger import setup_logger, setup_subprocess_logger, set_log_file
logger = setup_logger()
subprocess_logger = setup_subprocess_logger()


from . import utils

def tarball(outfile=None, dry=False):
    """ Wrapper function to create a tarball of svj.core """
    return utils.tarball(__file__, outfile=outfile, dry=dry)

import condor.jobfiles
import condor.submitters
