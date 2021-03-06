# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [PEP 440](https://www.python.org/dev/peps/pep-0440/) 
and uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v1.0.2](https://github.com/asfadmin/hyp3-proc-lib/compare/v1.0.1...v1.0.2)

### Changed
* Whitelist warning messages from `hyp3lib.get_orb` for `hyp3lib` >= v1.4.1

## [v1.0.1](https://github.com/asfadmin/hyp3-proc-lib/compare/v1.0.0...v1.0.1)

### Fixed
* Will not overwrite `default_rtc_resolution` as determined in code with value 
  from depreciated HyP3 database config table

## [v1.0.0](https://github.com/asfadmin/hyp3-proc-lib/compare/v0.0.0...v1.0.0)

This is a fork of the old HyP3 `cloud-prog/processing` library with substantial changes
 
### Removed
 * Unused `hyp3proclib.flush_print` function -- use `print( , flush=True)` instead
 * Any official python 2 support (Note: this version will *likely* still work with python 2, but future versions are
  not expected to)
 
### Added
* A packaging and testing structure -- now `pip` installable and testing is done via `pytest` and `tox` 
* `hyp3proclib.setup`  accepts new keyword arguments:
  * `cli_args` -- programmatically specify arguments to the argparse parser
  *  `airgap` -- Flag to prevent `setup` from connecting to the remote databases (for testing)
  * `sci_version` -- allow adding the science code's version to the CLI (codes using this library)
* `hyp3proclib.proc_base.Processor` class  initializer will pass specified `cli_args`, `sci_version` to
 `hyp3proclib.setup` (see above)

### Changed
* All files have the `from __future__ import print_function, absolute_import, division, unicode_literals` 
 imports added to make python 2 behave more like python 3 (NOTE: Python 2 is not longer officially supported, but it
  *should* work for this version)
* all of `cloud-proc/processing/proc_lib` is now contained in the `hyp3proclib` package
* `cloud-prog/processing/proc_base` module has been moved inside the `hyp3proclib` package
* `hyp3proclib.process` now accepts the specific script to run, instead of a config key to locate the script
    * accordingly, these fields in `proc.cfg` have been depreciated:
      ```ini
      procS1StackGAMMA = /usr/local/hyp3-insar-gamma/src/procS1StackGAMMA.py
      procSentinelIntf = /usr/local/hyp3-insar-s1tbx/src/procSentinelIntf.py
      processAlosPair = /usr/local/hyp3-insar-gamma-alos/src/processAlosPair.py
      procS1StackISCE = /usr/local/hyp3-insar-isce/src/procAllS1StackISCE.py
      procS1GMT5SAR = /usr/local/hyp3-insar-gmt5sar/src/procS1GMT5SAR.py
      procS1StackGIANT = /usr/local/apd_insar/src/procS1StackGIANT.py
      run_rtc = /usr/local/gap_rtc/src/run_rtc.pl
      run_rtc_ts = /usr/local/hyp3-giant/src/procS1StackRTC.py
      run_insar_ts = /usr/local/hyp3-giant/src/procS1StackGIANT.py
      run_new_rtc = /usr/local/hyp3-rtc-gamma/src/rtc_sentinel.py
      run_geocode = /usr/local/hyp3-geocode/src/geocode_sentinel.py
      run_rtc_snap = python -u /usr/local/hyp3-rtc-snap/src/procSentinelRTC-3.py
      run_rtc_legacy = /usr/local/hyp3-rtc-gamma-legacy/src/rtc_legacy.py
      glacierTrackingMVP = python /usr/local/Glacier-Tracking/glacierTrackingMVP.py
      run_tcd = /usr/local/hyp3-change-detection/tcd/src/run_tcd.py
      run_niyi = /usr/local/hyp3-change-detection/niyi/src/ChangeDetection_Niyi.py
      run_niyi_prep = /usr/local/hyp3-change-detection/niyi/src/prep_images_ChangeDetection_Niyi.py
      ```  
* `proc.cfg` is now expected to be in a `${HOME}/.hyp3` directory by default
  * some limited (experimental) support for specifying a different config file has been added
* `proc.cfg` is no longer loaded upon import, but is loaded into a `hyp3proclib.default_cfg` global default variable if
 the `hyp3proclib.config.init_config` function is called. The `load_all_general_config`, `is_config`, and `get_config` 
 methods in `hyp3proclib.config` will initialize the config file if `hyp3proclib.default_cfg` is `None`. 
* lock files and logs will now be `${HOME}/.hyp3/lock` and `${HOME}/.hyp3/log` directories, respectively
* `hyp3proclib.setup`  now uses argparse to setup a CLI for the science codes and a `--version` option was added that
 will display the versions being used of the science code, `hyp3lib`, and `hyp3proclib`
 
