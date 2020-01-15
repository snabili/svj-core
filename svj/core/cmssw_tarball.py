import os.path as osp
import svj.core

class CMSSWTarball(object):
    """docstring for CMSSWTarball"""
    def __init__(self, tarball, scram_arch, rundir=None):
        super(CMSSWTarball, self).__init__()
        self.tarball = tarball
        self.scram_arch = scram_arch
        self.rundir = svj.core.RUNDIR if rundir is None else rundir
        self._is_renamed = False

    def extract(self):
        svj.core.utils.create_directory(self.rundir, force=True)
        cmssw_dir = svj.core.utils.extract_tarball_cmssw(self.tarball, outdir=self.rundir)
        self.cmssw_src = osp.abspath(osp.join(cmssw_dir, 'src'))

    def rename_project(self):
        if self._is_renamed: return
        self._is_renamed = True
        svj.core.logger.info('Renaming project %s', self.cmssw_src)
        with svj.core.utils.switchdir(self.cmssw_src):
            cmds = [
                'shopt -s expand_aliases',
                'source /cvmfs/cms.cern.ch/cmsset_default.sh',
                'export SCRAM_ARCH={0}'.format(self.scram_arch),
                'scram b ProjectRename',
                'cmsenv',
                ]
            svj.core.utils.run_multiple_commands(cmds)

    def run_command_cmssw_env(self, cmd):
        if not self._is_renamed: self.rename_project()
        with svj.core.utils.switchdir(self.cmssw_src):
            cmds = [
                'shopt -s expand_aliases',
                'source /cvmfs/cms.cern.ch/cmsset_default.sh',
                'export SCRAM_ARCH={0}'.format(self.scram_arch),
                'scram b ProjectRename',
                'cmsenv',
                cmd
                ]
            svj.core.utils.run_multiple_commands(cmds)
