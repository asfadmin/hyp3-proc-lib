# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://scm.asf.alaska.edu/hyp3/hyp3-proc-lib/compare/v0.0.0...develop) -- likely v0.0.1

This is a fork of the old HyP3 `cloud-prog/processing` library with substantial changes
 
### Removed
 * python < 3.8 is no longer supported and likely won't work 
 * Removed unused `hyp3proclib.flush_print` function -- use `print( , flush=True)` instead.
 
### Added
* A packaging and testing structure -- now `pip` installable and testing is done via `pytest` and `tox` 
* A `hyp3proclib.file_system.mkdir_p` method to make a directory and any needed parents (like `mkdir -p`)
* `hyp3proclib.setup`  accepts new keyword arguments:
  * `cli_args` -- programmatically specify arguments to the argparse parser
  *  `airgap` -- Flag to prevent `setup` from connecting to the remote databases (for testing)
  * `sci_version` -- allow adding the science code's version to the CLI (codes using this library)
* `hyp3proclib.proc_base.Processor` class  initializer will pass specified `cli_args`, `sci_version` to
 `hyp3proclib.setup` (see above)

### Changed
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
 
