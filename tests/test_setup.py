from __future__ import print_function, absolute_import, division, unicode_literals

import os
import pytest

import hyp3proclib

_HERE = os.path.dirname(__file__)


def test_setup_cli():
    with pytest.raises(SystemExit):
        _ = hyp3proclib.setup('test_setup_cli', cli_args=['-h'])


def test_setup_cli_version():
    with pytest.raises(SystemExit):
        _ = hyp3proclib.setup(
            'test_setup_cli', cli_args=['--version'], sci_version='0.1.2'
        )


def test_setup_cfg():
    truth = 'abracadabra'

    hyp3proclib.default_config_file = os.path.join(_HERE, 'data', 'proc.cfg')
    cfg = hyp3proclib.setup('test_setup_cli', cli_args=[], airgap=True)

    assert cfg['oracle-pass'] == truth
