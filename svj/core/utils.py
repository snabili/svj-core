#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import os.path as osp
import logging, subprocess, os, shutil, re, pprint, csv, glob, math

logger = logging.getLogger('root')
subprocess_logger = logging.getLogger('subprocess')


class switchdir(object):
    """
    Temporarily changes the working directory
    """
    def __init__(self, newdir, dry=False):
        super(switchdir, self).__init__()
        self.newdir = newdir
        self._backdir = os.getcwd()
        self.dry = dry
        
    def __enter__(self):
        logger.info('chdir to {0}'.format(self.newdir))
        if not self.dry: os.chdir(self.newdir)

    def __exit__(self, type, value, traceback):
        logger.info('chdir back to {0}'.format(self._backdir))
        if not self.dry: os.chdir(self._backdir)


def run_command(cmd, env=None, dry=False, shell=False):
    logger.warning('Issuing command: {0}'.format(' '.join(cmd)))
    if dry: return

    if shell:
        cmd = ' '.join(cmd)

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        universal_newlines=True,
        shell=shell
        )

    output = []
    for stdout_line in iter(process.stdout.readline, ""):
        subprocess_logger.info(stdout_line.rstrip('\n'))
        output.append(stdout_line)
    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if returncode == 0:
        logger.info('Command exited with status 0 - all good')
    else:
        logger.error('Exit status {0} for command: {1}'.format(returncode, cmd))
        raise subprocess.CalledProcessError(cmd, returncode)
    return output


def run_multiple_commands(cmds, env=None, dry=False):
    logger.info('Sending cmds:\n{0}'.format(pprint.pformat(cmds)))
    if dry:
        logger.info('Dry mode - not running command')
        return

    process = subprocess.Popen(
        'bash',
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        # universal_newlines=True,
        bufsize=1,
        close_fds=True
        )

    # Break on first error (stdin will still be written but execution will be stopped)
    process.stdin.write('set -e\n')
    process.stdin.flush()

    for cmd in cmds:
        if not(type(cmd) is str):
            cmd = ' '.join(cmd)
        if not(cmd.endswith('\n')):
            cmd += '\n'
        # logger.warning('Sending cmd \'{0}\''.format(cmd.replace('\n', '\\n')))
        process.stdin.write(cmd)
        process.stdin.flush()
    process.stdin.close()

    process.stdout.flush()
    for line in iter(process.stdout.readline, ""):
        if len(line) == 0: break
        # print(colored('[subprocess] ', 'red') + line, end='')
        subprocess_logger.info(line.rstrip('\n'))

    process.stdout.close()
    process.wait()
    returncode = process.returncode

    if (returncode == 0):
        logger.info('Command exited with status 0 - all good')
    else:
        raise subprocess.CalledProcessError(cmd, returncode)


def create_directory(dir, force=False, dry=False, must_not_exist=False):
    newly_created = False

    def create():
        logger.warning('Creating {0}'.format(dir))
        if not dry: os.makedirs(dir)

    def delete():
        logger.warning('Removing dir {0}'.format(dir))
        if not dry: shutil.rmtree(dir)

    if osp.isdir(dir):
        if force:
            delete()
            create()
            newly_created = True
        elif must_not_exist:
            raise OSError('{0} already exist but must not exist'.format(dir))
        else:
            logger.info('Already exists: {0}'.format(dir))
    else:
        create()
        newly_created = True

    return newly_created


def make_inode_unique(file):
    if not osp.exists(file): return file
    file += '_{i_attempt}'
    i_attempt = 1
    while osp.exists(file.format(i_attempt=i_attempt)):
        i_attempt += 1
        if i_attempt == 999:
            raise ValueError('Problem making unique directory/file (999 attempts): {0}'.format(file))
    return file.format(i_attempt=i_attempt)


def check_proxy():
    # cmd = 'voms-proxy-info -exists -valid 168:00' # Check if there is an existing proxy for a full week
    try:
        proxy_valid = subprocess.check_output(['grid-proxy-info', '-exists', '-valid', '168:00']) == 0
        logger.info('Found a valid proxy')
    except subprocess.CalledProcessError:
        logger.error(
            'Grid proxy is not valid for at least 1 week. Renew it using:\n'
            'voms-proxy-init -voms cms -valid 192:00'
            )
        raise


def check_scram_arch():
    """
    Checks whether the scram_arch is slc6
    """
    scram_arch = os.environ['SCRAM_ARCH']
    if not scram_arch.startswith('slc6'):
        logger.warning(
            'Detected SCRAM_ARCH = {0}; '
            'There might be incompatibility issues later on by not '
            'using slc6!!'
            .format(scram_arch)
            )


def decomment(open_file):
    """
    Yields lines one by one, stripping everything after '#'
    """
    for line in open_file:
        line = line.split('#')[0].strip()
        if line: yield line


def setup_cmssw(workdir, version, arch):
    """
    Generic function to set up CMSSW in workdir
    """

    if osp.isdir(osp.join(workdir, version)):
        logger.info('{0} already exists, skipping'.format(version))
        return
    logger.info('Setting up {0} {1} in {2}'.format(version, arch, workdir))
    cmds = [
        'cd {0}'.format(workdir),
        'shopt -s expand_aliases',
        'source /cvmfs/cms.cern.ch/cmsset_default.sh',
        'export SCRAM_ARCH={0}'.format(arch),
        'cmsrel {0}'.format(version),
        'cd {0}/src'.format(version),
        'cmsenv',
        'scram b',
        ]
    run_multiple_commands(cmds)
    logger.info('Done setting up {0} {1} in {2}'.format(version, arch, workdir))


def compile_cmssw_src(cmssw_src, arch, clean_env=False):
    """
    Generic function to (re)compile a CMSSW setup
    """
    if not osp.abspath(cmssw_src).endswith('src'):
        raise ValueError('cmssw_src {0} does not end with "src"'.format(cmssw_src))

    logger.info('Compiling {0} with scram arch {1}'.format(cmssw_src, arch))
    cmds = [
        'shopt -s expand_aliases',
        'source /cvmfs/cms.cern.ch/cmsset_default.sh',
        'export SCRAM_ARCH={0}'.format(arch),
        'cd {0}'.format(cmssw_src),
        'cmsenv',
        'scram b',
        ]

    if clean_env:
        env = os.environ.copy()
        for var in [
            'ROOTSYS',
            'PATH',
            'LD_LIBRARY_PATH',
            'DYLD_LIBRARY_PATH',
            'SHLIB_PATH',
            'LIBPATH',
            'PYTHONPATH',
            'MANPATH',
            'CMAKE_PREFIX_PATH',
            'JUPYTER_PATH',
            ]:
            del env[var]

    run_multiple_commands(cmds, env=env if clean_env else None)
    logger.info('Done compiling {0} with scram arch {1}'.format(cmssw_src, arch))


def compile_cmssw(workdir, version, arch):
    """
    As compile_cmssw_src but takes separated arguments
    """
    compile_cmssw_src(osp.join(workdir, version))


def remove_file(file, dry=False):
    """
    Removes a file only if it exists, and logs
    """
    if osp.isfile(file):
        logger.warning('Removing {0}'.format(file))
        if not dry: os.remove(file)
    else:
        logger.info('No file {0} to remove'.format(file))


def copy_file(src, dst, overwrite=False, dry=False):
    """
    Copies a file
    """
    if not(dry) and osp.isfile(dst):
        if overwrite:
            remove_file(dst)
        else:
            raise OSError(
                '{0} already exists, abort copying'.format(dst)
                )
    logger.info('Copying {0} --> {1}'.format(src, dst))
    if not dry: shutil.copyfile(src, dst)


def remove_dir(directory):
    """
    Removes a dir only if it exists, and logs
    """
    if osp.isdir(directory):
        logger.warning('Removing dir {0}'.format(directory))
        shutil.rmtree(directory)
    else:
        logger.info('No directory {0} to remove'.format(directory))


def read_preprocessing_directives(filename):
    """
    Reads '#$' directives form a file
    """
    r = {}
    with open(filename, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            if line.startswith('#$'):
                if not '=' in line:
                    logger.warning(
                        'Preprocessing directive does not contain \'=\'; skipping: \'%s\'',
                        line
                        )
                    continue
                line = line.lstrip('#$')
                key, value = [c.strip() for c in line.split('=', 1)]
                r[key.lower()] = value
                continue
    return r


def is_string(string):
    # Python 2 / 3 compatibility (https://stackoverflow.com/a/22679982/9209944)
    try:
        basestring
    except NameError:
        basestring = str
    return isinstance(string, basestring)


def tarball(module, outfile=None, dry=False):
    """
    Takes a python module or a path to a file of said module, goes to the associated
    top-level git directory, and creates a tarball.
    Will throw subprocess.CalledProcessError if there are uncommitted changes.
    """
    if dry:
        logger.info('Dry mode: Would create tarball')
        return 'path/to/tarball.tar'

    # Input variable may be a path
    if is_string(module):
        # Treat the input variable as a path
        path = module
    else:
        path = module.__file__

    # Make sure path exists and is a directory
    if not osp.exists(path):
        raise OSError('{0} is not a valid path'.format(path))
    elif osp.isfile(path):
        path = osp.dirname(path)

    # Get the top-level git dir
    with switchdir(path):
        toplevel_git_dir = run_command(['git', 'rev-parse', '--show-toplevel'])[0].strip()

    # Fix the output name of the tarball
    if outfile is None:
        outfile = osp.join(os.getcwd(), osp.basename(toplevel_git_dir) + '.tar')

    with switchdir(toplevel_git_dir):
        # Check if there are uncommitted changes
        try:
            run_command(['git', 'diff-index', '--quiet', 'HEAD', '--'])
        except subprocess.CalledProcessError:
            logger.error(
                'Uncommitted changes detected; it is unlikely you want a tarball '
                'with some changes not committed.'
                )
            raise
        # Create the actual tarball of the latest commit
        run_command(['git', 'archive', '-o', outfile, 'HEAD'])
        logger.info('Created tarball {0}'.format(outfile))
        return outfile


def tarball_cmssw(cmssw_path, outdir='.', tag=None, dry=False):
    """
    :param cmssw_path: Path to CMSSW_BASE (i.e. ../src)
    :type cmssw_path: str
    :param outdir: Path to put the tarball. Defaults to cwd.
    :type outdir: str
    :param tag: Optional tag to append to the tarball name (for convenient versioning)
    :type tag: str, optional
    """
    cmssw_path = osp.abspath(cmssw_path)
    outdir = osp.abspath(outdir)
    check_is_cmssw_path(cmssw_path)
    # Determine the final filename with the optional tag
    dst = osp.basename(cmssw_path).strip('/') + ('' if tag is None else '_' + tag) + '.tar.gz'
    dst_abs = osp.abspath(osp.join(outdir, dst))
    if osp.isfile(dst_abs): raise OSError('{0} already exists'.format(dst_abs))
    logger.warning(
        'Tarballing {0} ==> {1}'
        .format(osp.abspath(cmssw_path), dst_abs)
        )
    with switchdir(osp.dirname(cmssw_path)):
        cmd = [
            'tar',
            '--exclude-caches-all',
            '--exclude-vcs',
            '-zcvf',
            dst_abs,
            osp.basename(cmssw_path),
            # '--exclude=src',  # Necessary? Probably do need src... it's anyway tiny, usually
            '--exclude=tmp',
            ]
        run_command(cmd, dry=dry)


def extract_tarball(tarball, outdir='.', dry=False):
    """
    Extracts a tarball to outdir
    """
    tarball = osp.abspath(tarball)
    outdir = osp.abspath(outdir)
    logger.warning(
        'Extracting {0} ==> {1}'
        .format(tarball, outdir)
        )
    cmd = [
        'tar', '-xvf', tarball,
        '-C', outdir
        ]
    run_command(cmd, dry=dry)


def extract_tarball_cmssw(tarball, outdir='.', dry=False):
    """
    Extracts a tarball to outdir, and returns the extracted CMSSW dir
    """
    extract_tarball(tarball, outdir, dry)
    # return the CMSSW directory
    if dry: return 'CMSSW_dry'
    return [ d for d in glob.glob(osp.join(outdir, 'CMSSW*')) if not d.endswith('.gz')][0]


def check_is_cmssw_path(path):
    """
    Checks whether the passed path contains a CMSSW distribution.
    """
    abs_path = osp.abspath(path)
    if not osp.basename(path).startswith('CMSSW'):
        raise ValueError(
            'Expected {0} to start with "CMSSW" (path: {1})'
            .format(osp.basename(path), abs_path)
            )
    if not osp.isdir(path):
        raise OSError(
            '{0} is not a directory (path: {1})'
            .format(path, abs_path)
            )
    if not osp.isdir(osp.join(path, 'src')):
        raise OSError(
            '{0} is not a directory (path: {1})'
            .format(osp.join(path, 'src'), abs_path)
            )

def get_ith_chunk(i, n_chunks, all):
    n_all = len(all)
    n_per_chunk = math.ceil(float(n_all) / n_chunks)
    i_begin = i * n_per_chunk
    i_end = (i+1) * n_per_chunk
    if i_begin >= n_all:
        return []
    elif i_end >= n_all:
        return all[i_begin:]
    else:
        return all[i_begin:i_end]






