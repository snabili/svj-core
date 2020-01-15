from setuptools import setup, find_packages

setup(
    name          = 'svj_core',
    version       = '0.1',
    license       = 'BSD 3-Clause License',
    description   = 'Core package for boosted semi-visible jet analysis',
    url           = 'https://github.com/tklijnsma/svj-core.git',
    download_url  = 'https://github.com/tklijnsma/svj-core/archive/v0_1.tar.gz',
    author        = 'Thomas Klijnsma',
    author_email  = 'tklijnsm@gmail.com',
    packages      = find_packages(),
    zip_safe      = False,
    scripts       = [
        'svj/bin/svj-pyjob-cmssw',
        ],
    )
