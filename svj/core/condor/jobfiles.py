#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function
import os.path as osp
import logging, os, collections
from time import strftime
import svj.core
logger = logging.getLogger('root')


class JobFileBase(object):
    """Base class for files related to condor jobs"""

    def __init__(self):
        super(JobFileBase, self).__init__()

    def configure(self):
        """
        Optionally set class variables herex
        """
        pass

    def parse(self):
        """
        Should return a str with the contents to be placed in a file    
        """
        raise NotImplementedError('Should be subclassed')

    def to_file(self, file, dry=False):
        self.configure()
        parsed = self.parse()
        logger.info('Writing to {0}'.format(file))
        if not dry:
            with open(file, 'w') as f:
                f.write(parsed)


class JDLBase(JobFileBase):
    """docstring for JDLBase"""

    def __init__(self, sh_file):
        super(JDLBase, self).__init__()
        self.environment = {
            'CONDOR_CLUSTER_NUMBER' : '$(Cluster)',
            'CONDOR_PROCESS_ID' : '$(Process)',
            'USER' : os.environ['USER'],
            'CLUSTER_SUBMISSION_TIMESTAMP' : strftime('%Y%m%d_%H%M%S'),
            'CLUSTER_SUBMISSION_TIMESTAMP_SHORT' : strftime('%Y-%m-%d'),
            'CLUSTER_SUBMISSION_TIMESTAMP_VERBOSE' : strftime('%b %d %H:%M:%S (%Y)'),
            }
        self.sh_file = sh_file
        self.options = collections.OrderedDict()
        self.options['universe'] = 'vanilla'
        self.options['environment'] = self.environment
        self.queue = 'queue'
        self.environment['SVJ_BATCH_MODE'] = 'lpc'
        self.transfer_input_files = []

    def configure(self):
        self.options['executable'] = osp.basename(self.sh_file)

    def parse(self):
        jdl = []
        for key, value in self.options.iteritems():
            if key == 'environment':
                jdl.append(
                    'environment = "{0}"'.format(
                        ' '.join(
                            [ '{0}=\'{1}\''.format(key, value) for key, value in self.environment.iteritems() ]
                            )
                        ))
            else:
                jdl.append('{0} = {1}'.format(key, value))
        jdl.append(self.queue)
        jdl = '\n'.join(jdl)
        logger.info('Parsed the following jdl file:\n{0}'.format(jdl))
        return jdl


class JDLPythonFile(JDLBase):
    """
    Base JDL file where a python file is called in the job
    """
    def __init__(self, sh_file, python_file):
        super(JDLPythonFile, self).__init__(sh_file)
        self.python_file = osp.abspath(python_file)
        self.transfer_input_files.append(osp.basename(self.python_file)) # Use the copied version

    def configure(self):
        super(JDLPythonFile, self).configure()
        if len(self.transfer_input_files) > 0:
            self.options['transfer_input_files'] = ','.join(
                [ f for f in self.transfer_input_files if not f.startswith('root:') ]
                )
        self.options['on_exit_hold'] = '(ExitBySignal == True) || (ExitCode != 0)' # Hold job on failure
        # Set the logging files
        python_basename = osp.basename(self.python_file).replace('.py', '')
        self.options['output'] = '{0}_$(Cluster)_$(Process).stdout'.format(python_basename)
        self.options['error']  = '{0}_$(Cluster)_$(Process).stderr'.format(python_basename)
        self.options['log']    = '{0}_$(Cluster)_$(Process).log'.format(python_basename)


class JDLProduction(JDLPythonFile):
    """
    Base JDL file for production jobs
    """

    starting_seed = 1001
    default_scram_arch = 'slc7_amd64_gcc493'

    def __init__(self, sh_file, python_file, n_jobs):
        super(JDLProduction, self).__init__(sh_file, python_file)
        self.n_jobs = n_jobs

    def configure(self):
        super(JDLProduction, self).configure()
        self.environment['SCRAM_ARCH'] = self.default_scram_arch
        self.options['should_transfer_files'] = 'YES'  # May not be needed if staging out to SE!
        self.options['when_to_transfer_output'] = 'ON_EXIT'
        self.options['transfer_output_files'] = 'output'  # Should match with what is defined in svj.genprod.SVJ_OUTPUT_DIR
        # Queue one job per seed
        seeds = [ str(self.starting_seed + i) for i in range(self.n_jobs) ]
        self.queue = 'queue 1 arguments in {0}'.format(', '.join(seeds))



class SHBase(JobFileBase):
    """docstring for SHBase"""
    def __init__(self):
        super(SHBase, self).__init__()
        self.lines = []

    def parse(self):
        parsed = '\n'.join(self.lines)
        logger.info('Parsed the following sh file:\n{0}'.format(parsed))
        return parsed

class SHClean(SHBase):
    """docstring for SHClean"""
    def __init__(self):
        super(SHClean, self).__init__()
        self.lines.extend([
            'rm *.stdout    > /dev/null 2>& 1'
            'rm *.stderr    > /dev/null 2>& 1'
            'rm *.log       > /dev/null 2>& 1'
            'rm docker_stderror > /dev/null 2>& 1'
            ])


class SHPython(SHBase):
    """
    """
    def __init__(self, python_file):
        super(SHPython, self).__init__()
        self.python_file = python_file
        self.code_tarballs = []

    def add_code_tarball(self, code_tarball):
        self.code_tarballs.append(code_tarball)

    def echo(self, text):
        self.lines.append('echo "{0}"'.format(text))

    def install_code_tarballs(self):
        def code_tarball_iterator(code_tarballs):
            for tarball in code_tarballs:
                tarball = osp.basename(tarball)
                name = tarball.split('.')[0]
                yield tarball, name

        sh = []
        # First untar all tarballs
        for tarball, name in code_tarball_iterator(self.code_tarballs):
            sh.extend([
                'mkdir {0}'.format(name),
                'tar xf {0} -C {1}'.format(tarball, name),
                ])
        # Source the env script
        sh.append('source svj-core/env.sh')
        # Add the package paths
        for tarball, name in code_tarball_iterator(self.code_tarballs):
            sh.extend([
                'export PATH="${{PWD}}/svj/{0}/bin:${{PATH}}"'.format(name),
                'export PYTHONPATH="${{PWD}}/{0}:${{PYTHONPATH}}"'.format(name),
                ])
        return sh

    def configure(self):
        self.lines.append('#!/bin/bash')
        self.lines.append('set -e')
        self.echo('##### HOST DETAILS #####')
        self.echo('hostname: $(hostname)')
        self.echo('date:     $(date)')
        self.echo('pwd:      $(pwd)')
        self.lines.append('export SVJ_SEED=$1')
        self.echo('seed:     ${SVJ_SEED}')
        if len(self.code_tarballs) > 0:
            self.echo('Installing code tarballs')
            self.lines.extend(self.install_code_tarballs())
        self.lines.append('mkdir output')
        self.echo('ls -al:')
        self.lines.append('ls -al')
        self.echo('Starting python {0}'.format(osp.basename(self.python_file)))
        self.lines.append('python {0}'.format(osp.basename(self.python_file)))

