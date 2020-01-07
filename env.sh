#!/usr/bin/env bash

setup() {
    export VO_CMS_SW_DIR=/cvmfs/cms.cern.ch/
    source /cvmfs/cms.cern.ch/cmsset_default.sh
    source "${gccsetup}"
    source "${rootsetup}"
    export PATH="${pipdir}/bin:${pythondir}/bin:${PATH}"
    export PYTHONPATH="${pipdir}/lib/python2.7/site-packages/:${PYTHONPATH}"
    echo "Setup for architecture ${SCRAM_ARCH}"
    alias pip="${pipdir}/bin/pip"  # To avoid any local pip installations
    }

if [[ "$SCRAM_ARCH" == "slc6"* ]]; then
    pythondir=/cvmfs/sft.cern.ch/lcg/releases/Python/2.7.15-c333c/x86_64-slc6-gcc62-opt
    pipdir=/cvmfs/sft.cern.ch/lcg/releases/pip/9.0.1-e2f3e/x86_64-slc6-gcc62-opt
    gccsetup=/cvmfs/sft.cern.ch/lcg/contrib/gcc/6.2/x86_64-slc6/setup.sh
    rootsetup=/cvmfs/sft.cern.ch/lcg/releases/LCG_88/ROOT/6.08.06/x86_64-slc6-gcc62-opt/bin/thisroot.sh
    setup
elif [[ "$SCRAM_ARCH" == "slc7"* ]]; then
    pythondir=/cvmfs/sft.cern.ch/lcg/releases/Python/2.7.15-c333c/x86_64-centos7-gcc7-opt
    pipdir=/cvmfs/sft.cern.ch/lcg/releases/pip/9.0.1-e2f3e/x86_64-centos7-gcc7-opt
    gccsetup=/cvmfs/sft.cern.ch/lcg/contrib/gcc/7/x86_64-centos7/setup.sh
    rootsetup=/cvmfs/sft.cern.ch/lcg/releases/LCG_95/ROOT/6.16.00/x86_64-centos7-gcc7-opt/bin/thisroot.sh
    setup
else
    echo "No known setup for arch ${SCRAM_ARCH}. Either use a supported architecture (slc6*, slc7*) or review the environment setup in $0"
fi

