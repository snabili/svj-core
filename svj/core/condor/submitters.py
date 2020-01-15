#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import os.path as osp
import logging, os, collections, shutil
from time import strftime
import svj.core
logger = logging.getLogger('root')


class Submitter(object):
    """docstring for Submitter"""
    def __init__(self):
        super(Submitter, self).__init__()
        self.needs_modules = []
        self.module_tarballs = {}
        self._iscalled_create_module_tarballs = False

    def submit(self, dry=False):
        svj.core.utils.check_proxy()

    def add_module(self, module):
        self.needs_modules.append(module)

    def create_module_tarballs(self, dry=False):
        if self._iscalled_create_module_tarballs:
            logger.error(
                'create_module_tarballs called twice, should not happen. '
                'Not recreating the module tarballs.'
                )
            return
        self._iscalled_create_module_tarballs = True
        for module in self.needs_modules:
            logger.info('Creating tarball for %s', module)
            if dry:
                self.module_tarballs[module] = 'tarball_{0}.tar'.format(module.__name__)
            else:
                self.module_tarballs[module] = svj.core.utils.tarball(module)


class PySubmitter(Submitter):
    """
    Sets up to run a python file
    """
    def __init__(self, python_file):
        super(PySubmitter, self).__init__()
        self.python_file = osp.abspath(python_file)
        self.python_file_basename = osp.basename(self.python_file)
        self.rundir = osp.join(
            os.getcwd(),
            self.python_file_basename.replace('.py', '') + strftime('_%Y%m%d_%H%M%S')
            )

        self.tarball = None
        self.seed = 1001
        self.n_jobs = 1
        self.n_events = 20

        self.preprocessing = svj.core.utils.read_preprocessing_directives(self.python_file)
        self.preprocessing_override('n_jobs', int)
        self.preprocessing_override('n_events', int)
        self.preprocessing_override('seed', int)
        if self.preprocessing_override('tarball'):
            svj.genprod.SVJ_TARBALL = self.tarball

        self.sh_file = osp.join(self.rundir, self.python_file_basename.replace('.py', '.sh'))
        self.jdl_file = osp.join(self.rundir, self.python_file_basename.replace('.py', '.jdl'))


    def preprocessing_override(self, key, type=str):
        """
        If key exists in the preprocessing, set it as an attribute to this instance.
        Returns True if a key was set, False if not.
        """
        if key in self.preprocessing:
            value = type(self.preprocessing[key])
            setattr(self, key, value)
            logger.info(
                'Setting %s %s based on preprocessing directive in %s',
                key, value, self.python_file
                )
            return True
        return False


    def submit(self, dry=False):
        super(PySubmitter, self).submit(dry=dry)
        # Setup the rundir
        svj.core.utils.create_directory(self.rundir, must_not_exist=True, dry=dry)
        with svj.core.utils.switchdir(self.rundir, dry=dry):
            # Copy the python file
            svj.core.utils.copy_file(self.python_file, self.python_file_basename, dry=dry)
            # Create the code tarballs
            self.create_module_tarballs(dry=dry)
            # Create also a small script to delete the output and logs
            svj.core.condor.jobfiles.SHClean().to_file('clean.sh', dry=dry)

        for module, code_tarball in self.module_tarballs.items():
            # Make sure the .sh will install the code tarball
            self.sh.add_code_tarball(code_tarball)
            # Make sure the .jdl will transfer the code tarball
            self.jdl.transfer_input_files.append(code_tarball)


class PyCMSSWSubmitter(PySubmitter):
    """docstring for PyCMSSWSubmitter"""
    def __init__(self, python_file, cmssw_tarball=None):
        super(PyCMSSWSubmitter, self).__init__(python_file)

        self.preprocessing_override('cmssw_tarball')
        if not(cmssw_tarball is None):
            self.cmssw_tarball = cmssw_tarball
        if self.cmssw_tarball is None:
            raise ValueError('Specify the CMSSW tarball either in the python file or to the submitter.')
        self.cmssw_tarball = osp.abspath(self.cmssw_tarball)

        self.jdl = svj.core.condor.jobfiles.JDLPythonFile(self.sh_file, self.python_file)
        self.sh = svj.core.condor.jobfiles.SHPython(self.python_file)
        self.add_module(svj.core)

    def submit(self, dry=False):
        super(PyCMSSWSubmitter, self).submit(dry=dry)
        if self.n_jobs > 1:
            self.jdl.queue = 'queue {0}'.format(self.n_jobs)
        with svj.core.utils.switchdir(self.rundir, dry=dry):
            # Copy the CMSSW tarball and make sure it's transferred
            svj.core.utils.copy_file(self.cmssw_tarball, osp.basename(self.cmssw_tarball), dry=dry)
            self.jdl.transfer_input_files.append(osp.basename(self.cmssw_tarball))

            # Generate .sh and .jdl files
            self.sh.to_file(self.sh_file, dry=dry)
            self.jdl.to_file(self.jdl_file, dry=dry)

            # Submit the job
            submit_jdl(self.jdl_file, dry=dry)


class ProductionSubmitter(PySubmitter):
    """docstring for ProductionSubmitter"""
    def __init__(self, python_file, tarball=None, n_jobs=None):
        super(ProductionSubmitter, self).__init__(python_file)
        if not(tarball is None): self.tarball = tarball
        if not(n_jobs is None): self.n_jobs = n_jobs

        self.sh = svj.core.condor.jobfiles.SHPython(self.python_file)
        self.jdl = svj.core.condor.jobfiles.JDLProduction(self.sh_file, self.python_file, self.n_jobs)

        self.add_module(svj.core)
        self.add_module(svj.genprod)


    def submit(self, dry=False):
        super(ProductionSubmitter, self).submit(dry=dry)

        with svj.core.utils.switchdir(self.rundir, dry=dry):
            # Generate .sh and .jdl files
            self.sh.to_file(self.sh_file, dry=dry)
            if self.tarball:
                self.jdl.transfer_input_files.append(self.tarball)
            self.jdl.to_file(self.jdl_file, dry=dry)

            # Submit the job
            submit_jdl(self.jdl_file, dry=dry)


def submit_jdl(jdl_file, dry=False):
    try:
        from cjm import TodoList
        logger.info('Found installation of cjm')
        if not dry: TodoList().submit(jdl_file)
    except ImportError:
        logger.info('Submitting using plain condor_submit')
        cmd = ['condor_submit', jdl_file]
        svj.core.utils.run_command(jdl_file, dry=dry, shell=True)
