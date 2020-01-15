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

# Path to do any temporary running on
RUNDIR = '/tmp/svj'
BATCH_MODE = False
if 'SVJ_BATCH_MODE' in os.environ:
    RUNDIR = osp.join(os.environ['_CONDOR_SCRATCH_DIR'], 'svj')
    BATCH_MODE = True

from . import seutils
import condor.jobfiles
import condor.submitters
from cmssw_tarball import CMSSWTarball