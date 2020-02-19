#!/usr/bin/env python

from __future__ import division
from __future__ import print_function

import argparse
import boto3
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import zipfile
import signal
import time
import PIL
from six.moves.urllib.request import urlopen, Request
from PIL import Image
import mimetypes
from zipfile import ZipFile

from hyp3lib import __version__ as _hyp3lib_version
from hyp3lib.draw_polygon_on_raster import draw_polygon_from_shape_on_raster
from hyp3lib.subset_geotiff_shape import subset_geotiff_shape
from hyp3lib.asf_geometry import get_latlon_extent

from hyp3proclib.config import get_config, is_config, load_all_general_config, is_yes
from hyp3proclib.db import get_db_connection, query_database, get_db_config
from hyp3proclib.emailer import notify_user, notify_user_failure
from hyp3proclib.logger import log, setup_logger
from hyp3proclib.file_system import setup_workdir, cleanup_lockfile, cleanup_workdir, check_stop  # noqa: F401
from hyp3proclib.file_system import mkdir_p
from hyp3proclib.instance_tracking import add_instance_record, update_instance_record
from hyp3proclib.process_ids import get_process_id_dict

# FIXME: Python 3.8+ this should be `from importlib.metadata...`
from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    # package is not installed!
    # Install in editable/develop mode via (from the top of this repo):
    #    pip install --user .
    # Or, to just get the version number use:
    #    python setup.py --version
    pass

# FIXME: really should refactor to eliminate package globals
# Package globals
default_cfg = None
# FIXME: add ability to specify as argparse options
default_lock_dir = os.path.join(os.path.expanduser('~'), '.hyp3', 'lock')
default_log_dir = os.path.join(os.path.expanduser('~'), '.hyp3', 'log')
default_config_file = os.path.join(os.path.expanduser('~'), '.hyp3',  'proc.cfg')


def signal_handler(signum, frame):
    # this ugly line creates a lookup table between signal numbers and their "nice" names
    signum_to_names = dict((getattr(signal, n), n) for n in dir(
        signal) if n.startswith('SIG') and '_' not in n)
    log.critical("Received a {0}; bailing out.".format(
        signum_to_names[signum]))
    sys.exit(1)


def setup(name, cli_args=None, airgap=False, sci_version='Unknown'):
    # FIXME: Add description
    parser = argparse.ArgumentParser(prog=name)
    parser.add_argument(
        "-v", "--verbose", action="store_true",
        help="Print debug message (data will still be processed)",
    )
    parser.add_argument(
        "-d", "--debug",
        help="Specify a previously used workdir.  Do not process, and use the results from that workdir.",
    )
    parser.add_argument(
        "-k", "--keep", action="store_true",
        help="Do not clean up work directory after processing",
    )
    parser.add_argument(
        "--queue-id",
        help="Process a specific queue item.",
    )
    parser.add_argument(
        "-n", "--num", type=int, default=1,
        help="Process the specified number of products, default 1",
    )
    parser.add_argument(
        "--retry", action="store_true",
        help="Process jobs in RETRY status",
    )
    parser.add_argument(
        "--spot", action="store_true",
        help="Select only jobs where 0 < s.priority <= 10. To be used for spot instances",
    )
    parser.add_argument(
        "--on-prem", action="store_true",
        help="Select any job with any s.priority. To be used for on-premise.",
    )
    # FIXME: This should use the choices keyword argument to allow only RTC or InSAR,
    #        if help message is true.
    parser.add_argument(
        "--input", type=str, dest="input_type", default='RTC',
        help="For generic processors (time series), select input type, RTC or InSAR.",
    )
    parser.add_argument(
        '--version', action='store_true',
        help="Show the HyP3 plugin and libraries version numbers and exit",
    )

    args = parser.parse_args(cli_args)

    # NOTE: argparse doesn't provide a way to use newlines in the version string
    #       without also using raw formatting for the rest of the help message,
    #       so we'll do it ourselves.
    if args.version is True:
        print(
            '{name} v{sci_version}\n' \
            'hyp3lib v{_hyp3lib_version}\n' \
            'hyp3proclib v{_hyp3proclib_version}'.format(
                name=name, sci_version=sci_version,
                _hyp3lib_version=_hyp3lib_version,
                _hyp3proclib_version=__version__,
            )
        )
        sys.exit()

    cfg = dict()
    load_all_general_config(cfg)

    if 'lock_dir' not in cfg:
        cfg['lock_dir'] = default_lock_dir
    mkdir_p(cfg['lock_dir'])
    if 'notify_fail' not in cfg:
        cfg['notify_fail'] = False
    if 'write_log_file' not in cfg:
        cfg['write_log_file'] = True
    mkdir_p(default_log_dir)
    if 'workdir' not in cfg:
        cfg['workdir'] = '/tmp'
    if 'default_rtc_resolution' not in cfg:
        cfg['default_rtc_resolution'] = '30m'

    # Update proc name in case of generic wrapper
    if name == 'generic_ts':
        cfg['input_type'] = args.input_type
        if args.input_type.lower() == 'insar':
            name = 'stack_ts_insar'
        else:
            name = 'stack_ts_rtc'
        log.debug(
            "Initializing process as proc-type {0} and with process key {1}".format(args.input_type.lower(), name)
        )
    cfg['proc_name'] = name

    cfg["keep"] = args.keep
    cfg["queue_id"] = args.queue_id
    cfg['allow_non_sentinel'] = (name == 'rtc_gamma')  # TODO: Move to the DB?
    cfg['attachment'] = None
    cfg['lag'] = ''
    cfg['Legacy'] = False
    cfg['log'] = ''
    cfg['notify_retries'] = 2
    cfg['num_to_process'] = args.num
    cfg['on_prem'] = args.on_prem
    cfg['original_workdir'] = cfg['workdir']
    cfg['PALSAR'] = False
    cfg['retry'] = args.retry
    cfg['skip_processing'] = False
    cfg['spot'] = args.spot
    cfg['user_workdir'] = False

    cfg['aws_region'] = get_config('aws', 'region')
    cfg['aws_secret_access_key'] = get_config('aws', 'secret_access_key')
    cfg['aws_access_key_id'] = get_config('aws', 'access_key_id')
    cfg['bucket'] = get_config('aws', 'bucket')
    cfg['browse_bucket'] = get_config('aws', 'browse_bucket')

    cfg['gtm5sar_config'] = get_config('gmt5sar', 'config_file')

    cfg['local_host'] = get_config('local', 'host')
    cfg['local_folder'] = get_config('local', 'folder')
    cfg['local_tiffs_only'] = is_yes(get_config('local', 'tiffs_only'))
    cfg['local_by_sub'] = is_yes(get_config('local', 'by_sub'))

    cfg['oracle-dbsid'] = get_config('oracle', 'dbsid', '')
    cfg['oracle-user'] = get_config('oracle', 'user', '')
    cfg['oracle-pass'] = get_config('oracle', 'pass', '')

    if not airgap:
        with get_db_connection('hyp3-db') as conn:
            cfg['product_hash_type'] = get_db_config(conn, "product_hash_type")
            cfg['bucket_lifecycle'] = get_db_config(conn, "bucket_lifecycle")
            cfg['hyp3_product_url'] = get_db_config(conn, "hyp3_product_url")
            cfg['hyp3-data-url'] = get_db_config(conn, "hyp3-data-url")
            cfg['hyp3-browse-url'] = get_db_config(conn, "hyp3-browse-url")
            cfg['default_rtc_resolution'] = get_db_config(conn, 'default_rtc_resolution')
            cfg['from_esa'] = is_yes(get_db_config(conn, 'download_from_esa'))
            jwl = get_db_config(conn, "jers_whitelist")
            if jwl is None:
                jwl = []
            else:
                jwl = [int(x) for x in jwl.split(',') if x.strip().isdigit()]
            cfg['jers_whitelist'] = jwl

        cfg['process_ids'] = get_process_id_dict()
   
    if is_config('general', 'verbose'):
        args.verbose = True
    setup_logger(cfg, args.verbose)

    if args.debug:
        if not os.path.isdir(args.debug):
            log.critical("Workdir not found: " + args.debug)
            sys.exit(1)
        else:
            cfg['workdir'] = args.debug
            cfg['user_workdir'] = True

    # install signal handling for SIGTERM, SIGQUIT, and SIGHUP
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGHUP, signal_handler)

    return cfg


def get_looks(dir_):
    for subdir, dirs, files in os.walk(dir_):
        for file in files:
            filepath = os.path.join(subdir, file)
            if "README" in filepath or "ESA_citation" in filepath:
                continue
            elif filepath.endswith(".txt"):
                log.info('Metadata: ' + filepath)
                try:
                    d = dict(line.split(':', 1) for line in open(filepath))
                    if 'range looks' in d and 'azimuth looks' in d:
                        return '-' + d['range looks'].strip() + 'x' + d['azimuth looks'].strip()
                    elif 'Range looks' in d and 'Azimuth looks' in d:
                        return '-' + d['Range looks'].strip() + 'x' + d['Azimuth looks'].strip()
                    else:
                        log.warning(
                            'range_looks/azimuth_looks not found in ' + filepath)
                        return ''
                except Exception:
                    log.warning(
                            'An error occurred trying to get the number of looks from ' + filepath)
                    return ''

    log.warning('Could not find metadata info in ' + str(dir_))
    return ''


def user_ok_for_jers(cfg, user_id):
    return user_id in cfg['jers_whitelist']


def find_phase_png(dir_):
    for subdir, dirs, files in os.walk(dir_):
        for file in files:
            filepath = os.path.join(subdir, file)
            if filepath.endswith("color_phase.png"):
                log.info('Browse image: ' + filepath)
                return filepath

    return None


def obscure_pwd(cfg, cmd):
    p = get_config('hyp3-db', 'pass')
    return cmd.replace(p, '***')


def contains_allowable_error(s):
    # These probably should be in a config file
    l = [
        'Error per GCP',
        'no offsets found above correlation threshold',
        'cannot open Sentinel-1 OPOD orbit data file',
        'Setting maximum error to be',
        'Settimg maximum error to be',
        'Root mean squared error',
        'error threshold for geocoding along track',
        'range, azimuth error thresholds',
        'final range offset poly. coeff. errors',
        'final azimuth offset poly. coeff. errors',
        'raise FileException(error)',
        'S3RegionRedirector.redirect_from_error'
    ]

    return any([i in s for i in l])


def execute(cfg, cmd, expected=None):
    print_cmd = obscure_pwd(cfg, cmd)

    log.debug('Running command: ' + print_cmd)
    rcmd = cmd + ' 2>&1'

    pipe = subprocess.Popen(rcmd, shell=True, stdout=subprocess.PIPE)
    output = pipe.communicate()[0]
    return_val = pipe.returncode
    log.debug('subprocess return value was ' + str(return_val))

    # Sometimes processes have weird output, leading to this
    output = output.decode('iso8859-1')

    cfg['log'] += "cmd: " + print_cmd + "\n\n" + output

    print_warnings = False
    for line in output.split('\n'):
        if '** Error: **' in line:
            print_warnings = True
        if (print_warnings):
            log.warn('Proc: ' + line)
        else:
            if len(line.rstrip()) > 0 and line[0:7] != "Process":
                log.debug('Proc: ' + line)
        if '** End of error **' in line:
            print_warnings = False

    log.debug('Finished: ' + print_cmd)

    if return_val != 0:
        log.debug('Nonzero return value!')

        # This error we always miss for some reason
        if 'ERROR: Failed to find a DEM' in output:
            raise Exception('get_dem.pl: ERROR: Failed to find a DEM')
        # get_dem.py has a little different verbiage
        if 'ERROR: Unable to find a DEM' in output:
            raise Exception('get_dem.py: ERROR: Failed to find a DEM')

        tool = cmd.split(' ')[0]
        last = 'Nonzero return value: ' + str(return_val)
        next_line = False
        for line in output.split('\n'):
            last = line
            if next_line:
                raise Exception(tool + ': ' + line)
            elif '** Error: *****' in line:  # MapReady style error
                next_line = True
            # Certain lines contain the word but aren't errors
            elif contains_allowable_error(line):
                pass
            elif 'ERROR' in line.upper() or 'Exception: ' in line:
                raise Exception(tool + ': ' + line)
        # No error line found, die with last line
        raise Exception(tool + ': ' + last)

    if expected is not None:
        log.debug('Checking for expected output: ' + expected)
        if os.path.isfile(expected):
            log.debug('Found: ' + expected)
        else:
            log.warn('Expected output file not found: ' + expected)
            raise Exception("Expected output file not found: " + expected)

    return output


def get_hash(file_path, algorithm):
    if algorithm == "md5":
        hasher = hashlib.md5()
    if algorithm == "sha512":
        hasher = hashlib.sha512()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(hasher.block_size), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def remove_from_s3(product_path, cfg, bucket):
    log.info("Removing: {0} from bucket {1}".format(product_path, bucket))


def upload_to_s3(product_path, cfg, bucket, is_public=False):
    if bucket is None:
        log.info('No bucket, skipping upload of: ' + str(product_path))
        return None
    if product_path is None:
        log.info('Nothing to upload.')
        return None

    log.info("Uploading product: " + product_path)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=cfg["aws_access_key_id"],
        aws_secret_access_key=cfg["aws_secret_access_key"],
        region_name=cfg["aws_region"],
    )
    # s3_client.create_bucket(Bucket=bucket,
    #        CreateBucketConfiguration={ 'LocationConstraint': cfg['aws_region'] } )
    fsize = os.stat(product_path).st_size
    log.debug('File size: {0}'.format(fsize))
    tsize = 50 * 1024 * 1024
    tconfig = boto3.s3.transfer.TransferConfig(
        multipart_threshold=tsize, multipart_chunksize=tsize)
    s3_transfer = boto3.s3.transfer.S3Transfer(s3_client, config=tconfig)

    tries = 0
    while True:
        try:
            log.debug("Uploading {0} to bucket {1}".format(
                product_path, bucket))
            key = os.path.basename(product_path)
            mime_type = mimetypes.guess_type(key, strict=True)[0]
            if mime_type is None:
                log.error(
                    "Mime type '{0}' for '{1}' not valid.".format(mime_type, key))

            if is_public:
                acl = 'public-read'
            else:
                acl = 'bucket-owner-full-control'

            key = key.replace("[", "_").replace("]", "_").replace(",","_").replace(":", "-")
            log.debug("Attempting to upload to s3 with parameters:")
            log.debug("Filename: {0}".format(product_path))
            log.debug("Bucket: {0}".format(bucket))
            log.debug("key: {0}".format(key))
            log.debug("Content Type: {0}".format(mime_type))
            log.debug("ACL: {0}".format(acl))

            s3_transfer.upload_file(
                filename=product_path, bucket=bucket,
                key=key, extra_args={'ContentType': mime_type, 'ACL': acl}
            )

            log.debug("Generate url")
            if is_public:
                product_url = cfg['hyp3-browse-url'] + \
                    os.path.basename(product_path)
                log.debug("Browse URL: " + product_url)
            else:
                product_url = cfg['hyp3-data-url'] + \
                    os.path.basename(product_path)
                log.debug("Data URL: " + product_url)

            log.info('Upload complete')
            break
        except Exception as e:  # boto3.exceptions.S3UploadFailedError:
            log.exception("Failed to upload product")
            print(e)
            tries += 1
            if tries < 3:
                log.info('Retrying in 30 seconds...')
                time.sleep(30)
                continue
            else:
                failure(cfg, "An error occurred while uploading the product or browse image.")
                return None

    return product_url


def reproject_image(cfg, in_image, out_image, epsg):
    if 'png' not in out_image:
        raise Exception("JPG not yet implemented for Mercator browses")

    try:
        tmpTiff = out_image.replace('.png', '.tif')
        execute(cfg, "gdalwarp -t_srs EPSG:{0} {1} {2}".format(
            epsg, in_image, tmpTiff), expected=tmpTiff)
        execute(
            cfg, "gdal_translate -of PNG {0} {1}".format(tmpTiff, out_image), expected=out_image)
        return True
    except Exception:
        log.exception('Geocoded browse generation failed.')
        return False


def find_browses(cfg, dir_):
    geo = None
    for subdir, dirs, files in os.walk(dir_):
        for file in files:
            if 'unw_phase' not in file:
                filepath = os.path.join(subdir, file)
                if filepath.endswith('.png') or filepath.endswith('.jpg'):
                    type_ = 'LOW-RES'
                    if 'large' in filepath:
                        type_ = 'HIGH-RES'
                        geo = filepath
                    elif 'thumb' in filepath:
                        type_ = 'THUMBNAIL'
                    add_browse(cfg, type_, filepath)

                    if type_ != 'THUMBNAIL' and geo is None:
                        geo = filepath

    if geo is not None:
        epsg = 3857

        # Don't create this in the output directory
        base = os.path.basename(geo)
        new_geo = os.path.join(cfg['workdir'], base)

    if geo.endswith('.png'):
        geo_merc_xml = new_geo.replace(
            '.png', '_epsg' + str(epsg) + '.png.aux.xml')

    elif geo.endswith('.jpg'):
        geo_merc = geo.replace('.jpg', '_epsg' + str(epsg) + '.jpg')
        geo_merc_xml = geo.replace(
            '.jpg', '_epsg' + str(epsg) + '.jpg.aux.xml')

        log.info("Trying to create a mercator browse")

        log.info("Reprojecting {0} -> {1}".format(geo, geo_merc))
        ok = reproject_image(cfg, geo, geo_merc, epsg)

        if ok and os.path.isfile(geo_merc):
            add_browse(cfg, 'GEO-IMAGE', geo_merc)

            lat_min, lat_max, lon_min, lon_max = get_latlon_extent(geo)

            cfg['browse_lat_min'] = lat_min
            cfg['browse_lat_max'] = lat_max
            cfg['browse_lon_min'] = lon_min
            cfg['browse_lon_max'] = lon_max
            cfg['browse_epsg'] = epsg

    if os.path.isfile(geo_merc_xml):
        add_browse(cfg, 'GEO-XML', geo_merc_xml)
    else:
        log.warning("Geo-XML not found: " + geo_merc_xml)


def add_browse(cfg, type_, path):
    k = 'browse_images'
    if k not in cfg:
        cfg[k] = dict()

    l = [path, ]
    if type_ in cfg[k]:
        if path not in cfg[k][type_]:
            cfg[k][type_].extend(l)
            log.debug('Added another browse of type {0}: {1} (now have {2})'.format(
                type_, path, len(cfg[k][type_])))
    else:
        cfg[k][type_] = l
        log.debug('Added browse of type {0}: {1} (now have {2})'.format(
            type_, path, len(cfg[k][type_])))


def add_thumbnail(cfg):
    bd = cfg['browse_images']
    if 'THUMBNAIL' in bd:
        log.debug('Already have a thumbnail: ' + str(bd['THUMBNAIL']))
        return

    f = None
    if 'LOW-RES' in bd:
        f = bd['LOW-RES'][0]
    elif 'HIGH-RES' in bd:
        f = bd['HIGH-RES'][0]
    elif 'FULL-RES' in bd:
        f = bd['FULL-RES'][0]

    if f is None:
        log.info('Could not find an image to generate a thumbnail from')
        return

    thumb_path = os.path.splitext(f)[0] + ".thumb.png"
    s = int(get_config('general', 'thumbnail_size', 200))
    size = (s, s)

    im = Image.open(f)
    im.thumbnail(size, Image.ANTIALIAS)
    im.save(thumb_path, "PNG")

    log.info('Thumbnail: ' + thumb_path)
    bd['THUMBNAIL'] = [thumb_path, ]


def clear_browse(cfg, conn, product_id):
    params = {'product_id': product_id}
    res = query_database(
        conn, 'select id, type, name, url from browse where product_id = %(product_id)s', params)
    for rec in res:
        id_ = int(rec[0])
        type_ = rec[1]
        name = rec[2]
        url = rec[3]

        log.info('Existing record: {0}, {1}, {2}, {3}'.format(
            id_, type_, name, url))
        if url is not None:
            log.info('Removing: ' + str(url))
            remove_from_s3(url, cfg, cfg['browse_bucket'])

    query_database(
        conn, 'delete from browse where product_id = %(product_id)s', params, commit=True)


def insert_browse(cfg, conn):
    bd = cfg['browse_images']
    add_thumbnail(cfg)

    product_id = cfg['product_id']
    clear_browse(cfg, conn, product_id)

    lat_min = cfg.get('browse_lat_min')
    lat_max = cfg.get('browse_lat_max')
    lon_min = cfg.get('browse_lon_min')
    lon_max = cfg.get('browse_lon_max')
    epsg = cfg.get('browse_epsg')
    if lat_min is None or lat_max is None or lon_min is None or lon_max is None or epsg is None:
        lat_min = None
        lat_max = None
        lon_min = None
        lon_max = None
        epsg = None

    for browse_type in bd:
        paths = bd[browse_type]
        for path in paths:
            log.debug("Path: " + str(path))
            if path is None or len(path) == 0:
                continue
        filename = os.path.basename(path)
        log.info('Browse: {0} -> {1}'.format(browse_type, filename))
        log.debug('Path: ' + str(path))

        if cfg['browse_url'] is not None and filename == os.path.basename(cfg['browse_url']):
            log.info('File has already been uploaded to S3')
            url = cfg['browse_url']
        else:
            res = query_database(conn, 'select id, type, url from browse where product_id = %(product_id)s and name = %(name)s',
                                       {'product_id': product_id, 'name': filename})
            if len(res) == 0:
                log.info('Uploading {0} to S3'.format(filename))
                url = upload_to_s3(
                    path, cfg, cfg['browse_bucket'], is_public=True)
                log.info('URL is ' + str(url))
            else:
                existing_type = res[0][1]
                url = res[0][2]
                log.info('Product already uploaded as ' + existing_type)
                log.info('Reusing URL: ' + url)

        log.info('Adding record for {0}: {1}'.format(browse_type, url))

        if browse_type == 'GEO-IMAGE':
            query_database(
                conn,
                '''
                INSERT INTO browse(type, product_id, name, url, lat_min, lat_max, lon_min, lon_max, epsg)
                VALUES (%(browse_type)s, %(product_id)s, %(name)s, %(url)s,
                                 %(lat_min)s, %(lat_max)s, %(lon_min)s, %(lon_max)s, %(epsg)s)
                ''',
                {
                    "browse_type": browse_type,
                    "product_id": product_id,
                    "name": filename,
                    "url": url,
                    "lat_min": lat_min,
                    "lat_max": lat_max,
                    "lon_min": lon_min,
                    "lon_max": lon_max,
                    "epsg": epsg
                },
                commit=True)
        else:
            query_database(
                conn,
                '''
                INSERT INTO browse(type, product_id, name, url)
                VALUES (%(browse_type)s, %(product_id)s, %(name)s, %(url)s)
                ''',
                {
                    "browse_type": browse_type,
                    "product_id": product_id,
                    "name": filename,
                    "url": url,
                },
                commit=True)

        browse_id = int(query_database(conn, 'select id from browse where product_id = %(product_id)s and type = %(browse_type)s',
                                       {"product_id": product_id, "browse_type": browse_type})[0][0])

        log.debug('Browse ID: {0} for {1}'.format(browse_id, filename))


def ssh_mkdir(host, folder):
    log.debug('Creating remote directory: {0}'.format(folder))
    os.system('ssh {0} mkdir -p {1}'.format(host, folder))


def scp_file(host, local_file, remote_path):
    log.info('SCP: {0} to {1}:{2}'.format(local_file, host, remote_path))
    ssh_mkdir(host, remote_path)
    os.system('scp {0} {1}:{2}'.format(local_file, host, remote_path))
    # verify?


def cp_file(local_file, dest_path):
    if not os.path.isdir(dest_path):
        log.info('Creating directory {0}'.format(dest_path))
        try:
            os.makedirs(dest_path)
        except OSError as e:
            if e.errno == 'EEXIST': #errno.EEXIST:
                pass
            else:
                raise

    log.info('Copying {0} to {1}'.format(local_file, dest_path))
    shutil.copy(local_file, dest_path)


def is_rtc_tiff(filename):
    l = filename.lower()
    return l.endswith('_hh.tif') or l.endswith('_hv.tif') or \
        l.endswith('_vv.tif') or l.endswith('_vh.tif')


def is_sacd_tiff(filename):
    l = filename.lower()
    return l.endswith('thresh.tif')


def is_iso_xml(fileName):
    l = fileName.lower()
    return l.endswith('.iso.xml')


def want_this_file(cfg, fileName):
    if is_rtc_tiff(fileName) and cfg['proc_id'] == 1:
        return True
    if is_sacd_tiff(fileName) and cfg['proc_id'] == 3:
        return True
    if is_iso_xml(fileName) and cfg['proc_id'] == 1:
        return True
    return False


def stage_product_locally(product_path, cfg):
    if not cfg['local_folder']:
        log.debug('Not configured to store products locally.')
        return

    if cfg['local_by_sub']:
        dest_path = os.path.join(cfg['local_folder'], cfg['username'], cfg['sub_name'].replace(' ','_'))
    else:
        dest_path = cfg['local_folder']
    log.info('Local storage path: ' + dest_path)

    if cfg['local_tiffs_only']:
        log.debug('Copying RTC GeoTIFFs and ISO XMLs, or SACD threshold GeoTIFFs to local storage')
        d = '/tmp'
        log.debug('Local staging dir: ' + d)
        with ZipFile(product_path, 'r') as zipObj:
            listOfFileNames = zipObj.namelist()
            for fileName in listOfFileNames:
                if want_this_file(cfg, fileName):
                    log.debug('Extracting: ' + fileName + ' to ' + d)
                    zipObj.extract(fileName, d)
                    f = os.path.join(d, fileName)
                    if cfg['local_host']:
                        scp_file(cfg['local_host'], f, dest_path)
                    else:
                        cp_file(f, dest_path)

                    log.debug('Cleaning up: ' + f)
                    path = os.path.join(d, os.path.split(fileName)[0])
                    log.debug('  Deleting file: ' + fileName)
                    os.remove(f)
                    if os.path.isdir(path):
                        log.debug('  Removing path: ' + path)
                        shutil.rmtree(path)
    else:
        if cfg['local_host']:
            scp_file(cfg['local_host'], product_path, dest_path)
        else:
            cp_file(product_path, dest_path)


def upload_product(product_path, cfg, conn, browse_path=None, skip_notify=False):
    """Upload a product to the AWS S3 bucket.

    Takes a product path, the subscription ID for the product, the
    configuration parameters, and a database connection, uploads the
    product to the AWS S3 bucket, and emails the user.

    This function return True if it succeeds and False if it fails.
    """
    sub_id = None
    if cfg['sub_id'] > 0:
        sub_id = cfg['sub_id']
    user_id = cfg['user_id']

    if browse_path is None and 'attachment' in cfg:
        browse_path = cfg['attachment']

    add_browse(cfg, 'LOW-RES', browse_path)

    product_url = upload_to_s3(
        product_path, cfg, cfg['bucket'], is_public=False)
    browse_url = upload_to_s3(
        browse_path, cfg, cfg['browse_bucket'], is_public=True)

    stage_product_locally(product_path, cfg)

    log.debug('Product URL: ' + str(product_url))
    log.debug('Browse URL: ' + str(browse_url))

    cfg['browse_url'] = browse_url
    cfg['product_url'] = product_url

    if('Subscription: ' not in cfg['granule']):
        pathFrame = findPathFrame(cfg['granule'])
    else:
        pathFrame = {"path": None, "frame": None}
    
    cfg['filename'] = os.path.basename(product_path)

    res = query_database(conn, "select id, name, url, browse_url from products where local_queue_id = %(local_queue_id)s",
                         {"local_queue_id": cfg['id']})

    params = {
        "sub_id": sub_id,
        "name": os.path.basename(product_path),
        "url": product_url,
        "browse_url": browse_url,
        "hash": get_hash(product_path, cfg["product_hash_type"]),
        "hash_type": cfg["product_hash_type"],
        "size": os.stat(product_path).st_size,
        "user_id": user_id,
        "process_id": cfg['proc_id'],
        "proc_node_type": cfg["proc_node_type"],
        "local_queue_id": cfg['id'],
        "path": pathFrame['path'],
        "frame": pathFrame['frame']
    }

    product_id = None
    if len(res) == 0:
        log.debug("No product for this job yet")

        query_database(
            conn,
            '''
            INSERT INTO products (subscription_id, name, url, browse_url, hash, hash_type,
                      size, creation_date, user_id, process_id, proc_node_type, local_queue_id,
                                      ok_to_duplicate, path, frame)
            VALUES (%(sub_id)s, %(name)s, %(url)s, %(browse_url)s,
                %(hash)s, %(hash_type)s, %(size)s, current_timestamp, %(user_id)s, %(process_id)s,
                %(proc_node_type)s, %(local_queue_id)s, True, %(path)s, %(frame)s)
            ''',
            params,
            commit=True,
        )

        product_id = int(query_database(conn, "select id from products where local_queue_id = %(local_queue_id)s",
                                        {"local_queue_id": cfg['id']})[0][0])
    else:
        log.info("Already have a product for this job -- updating")
        product_id = int(res[0][0])
        params['id'] = product_id
        log.debug("Existing ID = {0}".format(params['id']))

        existing_name = res[0][1]
        existing_url = res[0][2]
        existing_browse_url = res[0][3]
        log.debug("Existing product: {0} {1}".format(
            existing_name, existing_url))
        log.debug("Existing browse: {0}".format(existing_browse_url))

        if existing_name == params['name'] and existing_url == params['url']:
            log.info("Existing product matches current, will be overwritten")
        else:
            log.info("Removing existing product: {0}".format(existing_url))
            remove_from_s3(existing_url, cfg, cfg['bucket'])

        if existing_browse_url == params['browse_url']:
            log.info("Existing browse matches current, will be overwritten")
        elif existing_browse_url is not None:
            log.info("Removing existing product: {0}".format(
                existing_browse_url))
            remove_from_s3(existing_browse_url, cfg, cfg['browse_bucket'])

        query_database(
            conn,
            '''
            UPDATE products
                SET
                    subscription_id = %(sub_id)s,
                    name = %(name)s,
                    url = %(url)s,
                    browse_url = %(browse_url)s,
                    hash = %(hash)s,
                    hash_type = %(hash_type)s,
                    size = %(size)s,
                    creation_date = current_timestamp,
                    user_id = %(user_id)s,
                    process_id = %(process_id)s,
                    proc_node_type = %(proc_node_type)s,
                    local_queue_id = %(local_queue_id)s,
                    path = %(path)s,
                    frame = %(frame)s
                WHERE id = %(id)s
            ''',
            params,
            commit=True
        )

    log.debug('Product ID is ' + str(product_id))
    if product_id is None or product_id <= 0:
        raise Exception("Invalid product id: " + str(product_id))
    cfg['product_id'] = product_id

    if browse_path is not None:
        insert_browse(cfg, conn)
    update_completed_time(cfg)

    if not skip_notify:
        notify_user(product_url, sub_id, cfg, conn)


def get_top_queue_items(num=1, retry=False, procs=None):
    status = 'QUEUED'
    if retry:
        status = 'RETRY'

    sql = '''
        select u.username, u.priority, lq.priority, p.text_id, s.name
        from local_queue lq
               left join subscriptions s on lq.sub_id = s.id
               join users u on lq.user_id = u.id
               join processes p on lq.process_id = p.id
        where lq.status = '{0}' and
               lq.granule like 'S%%' and
               u.system_access_id > 1 and
               (u.max_granules <= 0 or u.max_granules is null or
                (u.max_granules is not null and u.granules_processed < u.max_granules)) and
               u.priority > 10 and
               p.enabled = True and
               (s.enabled = True or lq.sub_id is null) and
    '''.format(status)

    if procs:
        # TODO: Fix SQL injection vulnerability (use %s and pass in procs as argument)
        sql += '''
               p.text_id in ({0})
        '''.format(','.join(["'" + x + "'" for x in procs]))
    else:
        sql += "p.text_id != 'notify_only'"

    sql += '''
        order by coalesce(s.priority,10) desc, u.priority desc, lq.priority desc
        limit {0} '''.format(num)

    with get_db_connection('hyp3-db') as conn:
        recs = query_database(conn, sql, {})

    n = 0
    items = dict()
    for r in recs:
        n += 1
        log.debug('{0}: {2: <3} {3: <3} {1} -- {4} {5}'.format(n,
                                                               r[0], r[1], r[2], r[3], r[4]))
        if r[3] in items:
            items[r[3]] += 1
        else:
            items[r[3]] = 1

    top = None
    top_n = 0
    for k in items:
        log.debug('{0}: {1}'.format(k, items[k]))
        if top_n < items[k]:
            top = k
            top_n = items[k]

    log.info('Top: ' + str(top))
    return top


def sub_priority_string(sub_priority):
    if sub_priority == 10 or sub_priority is None:
        return "NORMAL"
    elif sub_priority > 10:
        return "EXPIDITED"
    elif sub_priority < 10:
        return "LOW"
    else:
        return "???"


def get_queue_item(cfg, exit=True, make_workdir=True):
    check_stop(cfg)

    sql = '''
        select lq.granule, lq.granule_url, lq.other_granules, lq.other_granule_urls, lq.id,
               s.priority as sub_priority, u.priority as user_priority, lq.priority as item_priority,
               s.name as sub_name, s.id, u.username, u.id, p.name, p.suffix, p.id,
               st_ymin(s.location) as min_lat, st_ymax(s.location) as max_lat,
               st_xmin(s.location) as min_lon, st_xmax(s.location) as max_lon,
               s.crop_to_selection, s.project_id, s.description,
               round((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM request_time))/3600) as age_hours,
               lq.extra_arguments
        from local_queue lq
               left join subscriptions s on lq.sub_id = s.id
               join users u on lq.user_id = u.id
               join processes p on lq.process_id = p.id
        where
    '''

    wanted_status = 'QUEUED'
    found = False

    with get_db_connection('hyp3-db') as conn:
        if cfg["queue_id"] is not None and int(cfg["queue_id"]) > 0:
            log.info("Using specified queue item with id = {0}".format(
                cfg["queue_id"]))
            update_queue_status(conn, cfg, 'QUEUED', queue_id=cfg["queue_id"])

            sql += "lq.id = %(id)s"
            vals = {'id': cfg['queue_id']}

        else:
            if cfg['spot'] is not None:
                log.debug(
                    'For processing using spot instances where 0 < s.priority <= 10. One-times included.')
                sql += '''
                    lq.priority > 0 and
                    (lq.sub_id is null or (lq.sub_id is not null and s.priority > 0 and s.priority <= 10)) and
                    u.system_access_id > 1
                '''

            elif cfg['on_prem'] is not None:
                log.debug(
                    'For processing using on-premises servers. (Any and all queued granules)')
                sql += '''
                    (1 = 1)
                '''

            else:
                # Default is to run in standard EC2
                log.debug(
                    'For processing using scheduled EC2 where 0 < s.priority')
                sql += '''
                    (lq.sub_id is null or s.priority > 0)
                '''

            if cfg['retry']:
                log.info('Looking for status RETRY')
                wanted_status = 'RETRY'

            if not cfg['allow_non_sentinel']:
                sql += '''
                    and lq.granule like 'S%%'
                '''

            sql += '''
                and p.text_id = %(text_id)s
                and lq.status = %(status)s
                and ((p.enabled = True and s.enabled = True) or lq.sub_id is null)
            '''

            vals = {'text_id': cfg['proc_name'], 'status': wanted_status }

            if 'test_mode' in cfg and is_yes(cfg['test_mode']) and 'test_user_id' in cfg:
                sql += '''
                    and u.id = %(test_user_id)s
                '''
                vals['test_user_id'] = cfg['test_user_id']

                log.info('TEST MODE: Only considering jobs for user {0}'.format(cfg['test_user_id']))

            if cfg['proc_name'] != "notify":
                sql += '''
                    and u.system_access_id > 1
                    and (u.max_granules <= 0 or u.max_granules is null or
                        (u.max_granules is not null and u.granules_processed < u.max_granules))
                '''

            sql += '''
                order by coalesce(s.priority,5) desc, u.priority desc, lq.priority desc, AGE_HOURS asc, sub_name desc
                limit 30
            '''

        recs = query_database(conn, sql, vals)

        if len(recs) == 0:
            log.debug('No records found. SQL = ' + sql)

        for r in recs:
            if r and r[0] and len(r[0]) > 0:
                id_ = int(r[4])
                if r[5] is not None:
                    sub_priority = int(r[5])
                else:
                    sub_priority = None
                user_priority = int(r[6])
                item_priority = int(r[7])
                granule = r[0]
                username = r[10]
                log.debug('Trying to grab lock for granule {0} for user {1}.'.format(
                    granule, username))
                log.debug('  Subscription priority={0}, User priority={1}, job priority={2}'.format(
                    sub_priority_string(sub_priority), user_priority, item_priority))

                sql = "update local_queue set status = 'PROCESSING', processed_time = current_timestamp where id = %(id)s and status = %(status)s"
                count = query_database(
                    conn, sql, {'id': id_, 'status': wanted_status}, commit=True)

                if count < 1:
                    log.debug('Failed to obtain lock to process ' + granule)
                    continue
                else:
                    log.info('Obtained processing lock for ' + granule)
                    log.debug('local_queue id is {0}'.format(r[4]))
                    cfg['granule'] = granule
                    cfg['granule_url'] = r[1]
                    cfg['other_granules'] = r[2].split(',') if (
                        r[2] is not None and len(r[2]) > 0) else None
                    cfg['other_granule_urls'] = r[3].split(',') if (
                        r[3] is not None and len(r[3]) > 0) else None

                    cfg['id'] = int(r[4])

                    cfg['user_priority'] = user_priority
                    cfg['item_priority'] = item_priority
                    if r[9] is not None:
                        cfg['sub_name'] = r[8]
                        cfg['sub_id'] = int(r[9])
                        cfg['min_lat'] = float(r[15])
                        cfg['max_lat'] = float(r[16])
                        cfg['min_lon'] = float(r[17])
                        cfg['max_lon'] = float(r[18])
                    else:
                        cfg['sub_name'] = 'One-Time'
                        cfg['sub_id'] = 0
                        cfg['min_lat'] = -90.0
                        cfg['max_lat'] = 90.0
                        cfg['min_lon'] = -180.0
                        cfg['max_lon'] = 180.0
                    cfg['user_id'] = int(r[11])
                    cfg['username'] = username
                    cfg['process_name'] = r[12]
                    cfg['suffix'] = r[13]
                    cfg['proc_id'] = int(r[14])
                    cfg['crop_to_selection'] = False #bool(r[19])
                    if r[20] is None:
                        cfg['project_id'] = -1
                    else:
                        cfg['project_id'] = int(r[20])
                        log.debug('Project ID: ' + str(cfg['project_id']))
                    if r[21] is None:
                        cfg['description'] = ''
                    else:
                        cfg['description'] = r[21]
                    if r[23] is None or len(str(r[23])) <= 2:
                        cfg['extra_arguments'] = dict()
                    else:
                        cfg['extra_arguments'] = json.loads(r[23])
                    found = True
                    break

        if 'granule' in cfg and cfg['granule'] is not None and cfg['proc_name'] != 'notify':
            # if not (cfg['granule'].startswith('S1') or cfg['granule'].startswith('ALPSRP') or ):
            #    raise Exception('Currently HyP3 only supports Sentinel-1 data products.')
            if 'RAW' in cfg['granule']:
                raise Exception(
                    'Currently HyP3 does not support Sentinel-1 RAW data products.')

        if not found:
            log.info('Found nothing to process.')
            if exit:
                cleanup_lockfile(cfg)
                log.info('Exiting')
                sys.exit(0)
            else:
                return False

        cfg['process_start_time'] = datetime.datetime.now()

        if make_workdir:
            setup_workdir(cfg)

        if cfg['proc_name'] != "notify":
            add_instance_record(cfg, conn)

    return found


def findPathFrame(granule):
    url = "https://api.daac.asf.alaska.edu/services/search/param?granule_list={0}&output=json".format(granule)
    req = Request(url, headers={'content-type': 'application/json'})
    response = urlopen(req)
    decoded = response.read().decode('utf8')
    parsed_json = json.loads(decoded)    
    
    if(parsed_json[0]):
        path = parsed_json[0][1]['track']
        frame = parsed_json[0][1]['frameNumber']
        return {'path': path, 'frame': frame}
    else:
        log.debug("Could not locate granule: {0}".format(granule))
        return {'path': None, 'frame': None}


def find_orb(dir_, num=1):
    prec = 0
    res = 0

    for subdir, dirs, files in os.walk(dir_):
        for f in files:
            if f.endswith(".EOF"):
                if 'POEORB' in f:
                    prec += 1
                elif 'RESORB' in f:
                    res += 1

    log.debug("Found {0} precision orbit file(s)".format(prec))
    log.debug("Found {0} restituted orbit file(s)".format(res))

    if prec >= num:
        ret = "POEORB"
    elif res + prec >= num:
        ret = "RESORB"
    else:
        ret = "PREDORB"

    log.info("Orbit quality: " + ret)
    return ret


def build_output_name(granule, workdir, suffix):
    if granule.startswith('S1'):
        return granule + '-' + find_orb(workdir) + suffix.replace('.zip', '')
    else:
        return granule + suffix.replace('.zip', '')


def build_output_name_pair(granule1, granule2, workdir, suffix):
    if granule1.startswith('S1'):
        dt1 = granule1[17:32]
        dt2 = granule2[17:32]
        dt_str = dt1 + '-' + dt2

        delta = (datetime.datetime.strptime(
            dt2[:8], '%Y%m%d') - datetime.datetime.strptime(dt1[:8], '%Y%m%d')).days

        orb_type = find_orb(workdir, num=2)
        interval = "{0}d".format(int(abs(delta)))

        # "AA" or "BB" or "AB" or "BA"
        plats = granule1[2] + granule2[2]

        polar1 = granule1[14:16]
        polar2 = granule2[14:16]
        polar = ""
        if polar1 == polar2:
            polar = polar1 + '-'
        else:
            polar = polar1 + polar2 + '-'

        return 'S1' + plats + '-' + dt_str + '-' + polar + orb_type + '-' + interval + suffix.replace('.zip', '')

    elif granule1.startswith('ALPSRP'):
        return granule1 + '-' + granule2 + suffix.replace('.zip', '')

    else:
        raise Exception('Bad granule name: ' + granule1)


def earlier_granule_first(g1, g2):
    delta = 0
    if g1.startswith('ALPSRP'):
        d1 = g1[6:16]
        d2 = g2[6:16]
        log.debug('Orbit/Frames: {0} {1}'.format(d1, d2))
        delta = int(d2) - int(d1)
    else:
        d1 = g1[17:25]
        d2 = g2[17:25]
        delta = (datetime.datetime.strptime(d2, '%Y%m%d') -
                 datetime.datetime.strptime(d1, '%Y%m%d')).days

    if delta >= 0:
        return g1, g2
    else:
        return g2, g1


def time_since_acquired(sentinel_granule_name):
    if sentinel_granule_name.startswith('S1'):
        a = sentinel_granule_name[17:32].replace('T', ' ')
        d = datetime.datetime.strptime(a, '%Y%m%d %H%M%S')
        s = str(datetime.datetime.utcnow() - d)
        return s.split('.')[0]
    else:
        return 0


def record_metrics(cfg, conn):
    log.info('Populating metrics table')
    log.debug('Granule: ' + str(cfg['granule']))
    log.debug('Process Time: ' + str(cfg['process_time']))
    log.debug('Process Start Time: ' + str(cfg['process_start_time']))
    log.debug('Subscription ID: ' + str(cfg['subscriptions']))
    log.debug('Process ID: ' + str(cfg['processes']))
    log.debug('Original Product Size: ' + str(cfg['original_product_size']))
    log.debug('Final Product Size: ' + str(cfg['final_product_size']))
    log.debug('Success: ' + str(cfg['success']))

    # This only makes sense for Sentinel
    if not cfg['PALSAR']:
        cfg['lag'] = str(time_since_acquired(cfg['granule']))
        log.info("Lag: {0} for sub '{1}' user '{2}' product {3}".format(
                 cfg['lag'], cfg['sub_name'], cfg['username'], cfg['granule']))


def success(conn, cfg):
    update_queue_status(conn, cfg, 'COMPLETE')


def is_permanent_fail(cfg, error_msg):
    # Kludge to handle failures we shouldn't bother to retry
    if 'Failed to find a DEM' in error_msg:
        return True
    elif 'Unable to find a DEM' in error_msg:
        return True
    elif 'no coverage of SAR image by DEM' in error_msg:
        return True
    elif 'User does not have permission' in error_msg:
        return True

    if 'granule' in cfg and 'RAW' in cfg['granule']:
        return True

    return False


def failure(cfg, error_msg):
    with get_db_connection('hyp3-db') as conn:
        if 'id' in cfg and cfg['id'] is not None:
            # TODO: Should this be checking cfg['retry'] is False?
            if cfg['retry'] is True or is_permanent_fail(cfg, error_msg):
                update_queue_status(conn, cfg, 'FAILED', msg=error_msg)
                notify_user_failure(cfg, conn, error_msg)
            else:
                log.info('Marking job for RETRY')
                update_queue_status(conn, cfg, 'RETRY', msg=error_msg)


def update_queue_status(conn, cfg, new_status, msg=None, queue_id=None):
    if queue_id is None:
        queue_id = cfg['id']

    log.debug('Updating status of local_queue id={0} to {1}'.format(
        queue_id, new_status))

    # wow this is the worst
    if new_status == 'COMPLETE':
        log.debug(
            'Updating completed_time for local_queue id={0}'.format(queue_id))
        sql = "update local_queue set status = %(status)s, completed_time = current_timestamp where id = %(id)s"
        query_database(
            conn, sql, {'status': new_status, 'id': queue_id}, commit=True)
    elif msg is None:
        sql = "update local_queue set status = %(status)s where id = %(id)s"
        query_database(
            conn, sql, {'status': new_status, 'id': queue_id}, commit=True)
    elif new_status == 'FAILED':
        log.debug(
            'Updating completed_time for local_queue id={0}'.format(queue_id))
        sql = "update local_queue set status = %(status)s, message = %(msg)s, completed_time = current_timestamp where id = %(id)s"
        query_database(
            conn, sql, {'status': new_status, 'msg': msg, 'id': queue_id}, commit=True)
    else:
        sql = "update local_queue set status = %(status)s, message = %(msg)s where id = %(id)s"
        query_database(
            conn, sql, {'status': new_status, 'msg': msg, 'id': queue_id}, commit=True)
 
    if cfg['proc_name'] != "notify":
        update_instance_record(cfg, conn)

    log.debug('Status updated.')


def zip_dir(path, zip_name):
    if not os.path.isdir(path):
        log.error('zip_dir: Directory does not exist: ' + path)
        return False

    mode = 'w'
    if os.path.isfile(zip_name):
        log.info('zip_name: File already exists, to appending it.')
        mode = 'a'
    else:
        log.info('Creating zip {0} from folder {1}'.format(zip_name, path))
    
    ziph = zipfile.ZipFile(
        zip_name, mode, zipfile.ZIP_DEFLATED, allowZip64=True)
    
    for root, dirs, files in os.walk(path):
        for f in files:
            pathToRead = os.path.join(root, f)
            archivePath = os.path.relpath(os.path.join(root, f), os.path.join(path, ".."))
            ziph.write(pathToRead, archivePath)
            
    ziph.close()
    return True


def unzip(file, dir_):
    zf = zipfile.ZipFile(file)
    zf.extractall(dir_)


def resize_image(filename, width):
    if filename.endswith('.pdf'):
        return filename
    img = Image.open(filename).convert('RGB')
    if img.size[0] <= width * 2:
        # Don't enlarge an image, or shrink if already pretty small
        return filename

    pct = float(width) / float(img.size[0])
    height = int(float(img.size[1]) * pct)
    img = img.resize((width, height), PIL.Image.ANTIALIAS)
    newname = filename + '.small.jpg'
    img.save(newname, "JPEG")
    return newname


def process(cfg, processor_script, args):
    cmd = " ".join([processor_script] + args)

    log.info('Processing starting at ' + str(datetime.datetime.now()))

    if not cfg['skip_processing']:
        output = execute(cfg, cmd)
    else:
        log.info('Processing skipped!')
        log.debug('Command was ' + cmd)
        output = "(debug mode)"

    cfg["success"] = True
    update_completed_time(cfg)

    cfg["granule_name"] = cfg["granule"]
    cfg["processes"] = [cfg["proc_id"], ]
    cfg["subscriptions"] = [cfg["sub_id"], ]

    if "log" in cfg:
        cfg["log"] += output
    else:
        cfg["log"] = output


def update_completed_time(cfg):
    log.info('Processing completed at ' + str(datetime.datetime.now()))

    elapsed = (datetime.datetime.now() -
               cfg["process_start_time"]).total_seconds()
    log.info('Processing took ' + str(datetime.timedelta(seconds=int(elapsed))))
    cfg["process_time"] = elapsed


def extra_arg_is(cfg, key, val):
    if cfg and 'extra_arguments' in cfg:
        if key in cfg['extra_arguments']:
            return cfg['extra_arguments'][key] == val
    return False


def get_extra_arg(cfg, key, default):
    if cfg and 'extra_arguments' in cfg:
        if key in cfg['extra_arguments']:
            return cfg['extra_arguments'][key]
    return default


def generate_shapefile(conn, cfg, shapefile):
    host = get_config('hyp3-db', 'host')
    dbname = get_config('hyp3-db', 'db')
    user = get_config('hyp3-db', 'user')
    password = get_config('hyp3-db', 'pass')

    log.debug('Generating shapefile')
    cmd = 'ogr2ogr -f "ESRI Shapefile" {0} "PG:host={1} dbname={2} user={3} password={4}" -sql "select location from subscriptions where id={5}"'.format(
        shapefile, host, dbname, user, password, cfg['sub_id'])

    execute(cfg, cmd, expected=shapefile)


def clip_geotiff(conn, cfg, geotiff, shapefile=None):
    in_geotiff = geotiff
    out_geotiff = geotiff.replace('.tif', '.clip.tif')

    if shapefile is None:
        shapefile = os.path.join(cfg['workdir'], 'roi.shp')
        generate_shapefile(conn, cfg, shapefile)

    subset_geotiff_shape(in_geotiff, shapefile, out_geotiff)

    do_rename(out_geotiff, in_geotiff)


def tiff_to_geo_jpg(cfg, tiff, xsize=None):
    jpeg = os.path.splitext(tiff)[0] + '.jpg'
    if xsize is None:
        cmd = 'gdal_translate -of JPEG -co "worldfile=yes" {0} {1}'.format(
            tiff, jpeg)
    else:
        cmd = 'gdal_translate -of JPEG -co "worldfile=yes" -outsize {0} 0 {1} {2}'.format(
            xsize, tiff, jpeg)
    execute(cfg, cmd, expected=jpeg)
    return jpeg


def clip_geo_jpg(conn, cfg, jpeg, shapefile=None):
    in_jpeg = jpeg
    out_jpeg = jpeg.replace('.jpg', '.clip.jpg')

    log.info('Clipping {0} to subscription region of interest'.format(jpeg))

    if shapefile is None:
        shapefile = os.path.join(cfg['workdir'], 'roi.shp')
        generate_shapefile(conn, cfg, shapefile)

    log.debug('Drawing boundary: {0} -> {1}'.format(in_jpeg, out_jpeg))
    draw_polygon_from_shape_on_raster(in_jpeg, shapefile, 'red', out_jpeg)

    do_rename(out_jpeg, in_jpeg)

    if os.path.isfile(out_jpeg.replace('.jpg', '.wld')):
        do_rename(out_jpeg.replace('.jpg', '.wld'),
                  in_jpeg.replace('.jpg', '.wld'))
    if os.path.isfile(out_jpeg.replace('.jpg', '.jpg.aux.xml')):
        do_rename(out_jpeg.replace('.jpg', '.jpg.aux.xml'),
                  in_jpeg.replace('.jpg', '.jpg.aux.xml'))


def clip_geo_png(conn, cfg, png, shapefile=None):
    in_png = png
    out_png = png.replace('.png', '.clip.png')

    log.info('Clipping {0} to subscription region of interest'.format(png))

    if shapefile is None:
        shapefile = os.path.join(cfg['workdir'], 'roi.shp')
        generate_shapefile(conn, cfg, shapefile)

    log.debug('Drawing boundary: {0} -> {1}'.format(in_png, out_png))
    draw_polygon_from_shape_on_raster(in_png, shapefile, 'red', out_png)

    do_rename(out_png, in_png)

    if os.path.isfile(out_png.replace('.png', '.wld')):
        do_rename(out_png.replace('.png', '.wld'),
                  in_png.replace('.png', '.wld'))
    if os.path.isfile(out_png.replace('.png', '.png.aux.xml')):
        do_rename(out_png.replace('.png', '.png.aux.xml'),
                  in_png.replace('.png', '.png.aux.xml'))


def do_rename(i, o):
    log.debug('Renaming {0} -> {1}'.format(i, o))
    os.remove(o)
    shutil.move(i, o)


def clip_tiffs_to_roi(cfg, conn, path):
    if 'crop_to_selection' in cfg and cfg['crop_to_selection'] is True:
        log.info('Clipping geotiffs in ' + path)

        shapefile = os.path.join(cfg['workdir'], 'roi.shp')
        generate_shapefile(conn, cfg, shapefile)

        for file in os.listdir(path):
            full = os.path.join(path, file)
            log.debug("Possibly clipping: " + full)
            if file.endswith(".tif"):
                clip_geotiff(conn, cfg, full, shapefile=shapefile)
            elif file.endswith(".jpg"):
                aux_file = full.replace(".jpg", ".jpg.aux.xml")
                if os.path.isfile(aux_file):
                    clip_geo_jpg(conn, cfg, full, shapefile=shapefile)
                else:
                    log.info("No aux file: " + aux_file)
            elif file.endswith(".png") and 'large' not in file:
                aux_file = full.replace(".png", ".png.aux.xml")
                if os.path.isfile(aux_file):
                    clip_geo_png(conn, cfg, full, shapefile=shapefile)
                else:
                    log.info("No aux file: " + aux_file)
        log.debug('Done')
    else:
        log.info('Cropping not enabled for this subscription.')

