from __future__ import print_function, absolute_import, division, unicode_literals

import os

from setuptools import find_packages, setup

_HERE = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(_HERE, 'README.md'), 'r') as f:
    long_desc = f.read()

setup(
    name='hyp3proclib',
    use_scm_version=True,
    description='HyP3 process communication and IO library plugin',
    long_description=long_desc,
    long_description_content_type='text/markdown',

    url='https://github.com/asfadmin/hyp3-proc-lib',

    author='ASF APD/Tools Team',
    author_email='uaf-asf-apd@alaska.edu',

    license='BSD-3-Clause',
    include_package_data=True,

    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Topic :: Software Development :: Libraries',
        ],

    install_requires=[
        'boto3',
        'hyp3lib~=1.0',
        'importlib_metadata',
        'pillow',
        'psycopg2',
        'requests',
        'six',
    ],

    extras_require={
        'develop': [
            'pytest',
            'pytest-cov',
            'pytest-console-scripts',
        ]
    },

    packages=find_packages(),

    # entry_points={'console_scripts': [
    #     ]
    # },

    zip_safe=False,
)
