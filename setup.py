from setuptools import setup

setup(
    name          = 'cjm',
    version       = '0.1',
    license       = 'BSD 3-Clause License',
    description   = 'Description text',
    url           = 'https://github.com/tklijnsma/cjm.git',
    download_url  = 'https://github.com/tklijnsma/cjm/archive/v0_1.tar.gz',
    author        = 'Thomas Klijnsma',
    author_email  = 'tklijnsm@gmail.com',
    packages      = ['cjm'],
    zip_safe      = False,
    tests_require = ['nose'],
    test_suite    = 'nose.collector',
    scripts       = [
        'bin/cjm-ls'
        ],
    )
