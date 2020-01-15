#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os.path as osp
import logging, subprocess, os, shutil, re, pprint, csv
import svj.core

logger = logging.getLogger('root')
DEFAULT_MGM = 'root://cmseos.fnal.gov'

def split_mgm(filename):
    if not filename.startswith('root://'):
        raise ValueError(
            'Cannot split mgm; passed filename: {0}'
            .format(filename)
            )
    elif not '/store' in filename:
        raise ValueError(
            'No substring \'/store\' in filename {0}'
            .format(filename)
            )
    i = filename.index('/store')
    mgm = filename[:i]
    lfn = filename[i:]
    return mgm, lfn

def _safe_split_mgm(path, mgm=None):
    """
    Returns the mgm and lfn that the user most likely intended to
    if path starts with 'root://', the mgm is taken from the path
    if mgm is passed, it is used as is
    if mgm is None and path has no mgm, the class var is taken
    """
    if path.startswith('root://'):
        _mgm, lfn = split_mgm(path)
        if not(mgm is None) and not _mgm == mgm:
            raise ValueError(
                'Conflicting mgms determined from path and passed argument: '
                'From path {0}: {1}, from argument: {2}'
                .format(path, _mgm, mgm)
                )
        mgm = _mgm
    elif mgm is None:
        mgm = DEFAULT_MGM
        lfn = path
    else:
        lfn = path
    # Some checks
    if not mgm.rstrip('/') == DEFAULT_MGM.rstrip('/'):
        logger.warning(
            'Using mgm {0}, which is not the default mgm {1}'
            .format(mgm, DEFAULT_MGM)
            )
    if not lfn.startswith('/store'):
        raise ValueError(
            'LFN {0} does not start with \'/store\'; something is wrong'
            .format(lfn)
            )
    return mgm, lfn

def _join_mgm_lfn(mgm, lfn):
    """
    Joins mgm and lfn, ensures correct formatting
    """
    if not mgm.endswith('/'): mgm += '/'
    return mgm + lfn

def create_directory(directory):
    """
    Creates a directory on the SE
    Does not check if directory already exists
    """
    mgm, directory = _safe_split_mgm(directory)
    logger.warning('Creating directory on SE: {0}'.format(_join_mgm_lfn(mgm, directory)))
    cmd = [ 'xrdfs', mgm, 'mkdir', '-p', directory ]
    svj.core.utils.run_command(cmd)

def is_directory(directory):
    """
    Returns a boolean indicating whether the directory exists
    """
    mgm, directory = _safe_split_mgm(directory)
    cmd = [ 'xrdfs', mgm, 'stat', '-q', 'IsDir', directory ]
    status = (subprocess.check_output(cmd) == 0)
    if not status:
        logger.info('Directory {0} does not exist'.format(_join_mgm_lfn(mgm, directory)))
    return status

def copy_to_se(src, dst, create_parent_directory=True):
    """
    Copies a file `src` to the storage element
    """
    mgm, dst = _safe_split_mgm(dst)
    dst = _join_mgm_lfn(mgm, dst)
    if create_parent_directory:
        parent_directory = osp.dirname(dst)
        create_directory(parent_directory)
    logger.warning('Copying {0} to {1}'.format(src, dst))
    cmd = [ 'xrdcp', '-s', src, dst ]
    svj.core.utils.run_command(cmd)

def format(src, mgm=None):
    """
    Formats a path to ensure it is a path on the SE
    """
    mgm, lfn = _safe_split_mgm(src, mgm=mgm)
    return _join_mgm_lfn(mgm, lfn)

def list_directory(directory):
    """
    Lists all files and directories in a directory on the se
    """
    mgm, directory = _safe_split_mgm(directory)
    contents = svj.core.utils.run_command([ 'xrdfs', mgm, 'ls', directory ])
    return [ l.strip() for l in contents if not len(l.strip()) == 0 ]

def list_root_files(directory):
    """
    Lists all root files in a directory on the se
    """
    contents = list_directory(directory)
    root_files = [ f for f in contents if f.endswith('.root') ]
    root_files.sort()
    return root_files
