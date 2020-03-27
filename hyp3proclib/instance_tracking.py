"""Module for proc_lib instance tracking functions"""

from __future__ import print_function, absolute_import, division, unicode_literals

import socket
from contextlib import contextmanager

import requests

from hyp3proclib.db import get_db_connection, query_database
from hyp3proclib.file_system import lockfile
from hyp3proclib.logger import log


@contextmanager
def manage_instance_and_lockfile(cfg):
    with lockfile(cfg):
        add_instance_to_hyp3_db(cfg)
        yield
        log_instance_shutdown_in_hyp3_db(cfg)


def add_instance_to_hyp3_db(cfg):
    if cfg['proc_node_type'] != 'CLOUD':
        return
    instance = get_instance_info(cfg)
    try:
        with get_db_connection('hyp3-db') as hyp3db_conn:
            sql = 'insert into instances (id, start_time, process_id) values (%(instance_id)s, current_timestamp, %(process_id)s);'
            query_database(conn=hyp3db_conn, query=sql,
                           params=instance, commit=True)
    except Exception as e:
        log.error("Instance %s could not be inserted into instances",
                  instance['instance_id'])
        log.error("Error was: %s", str(e))
    else:
        log.info("Instance %s was inserted into instances",
                 instance['instance_id'])


def update_instance_with_specific_gamma_id(cfg):
    if cfg['proc_node_type'] != 'CLOUD':
        return

    instance = get_instance_info(cfg)
    try:
        with get_db_connection('hyp3-db') as hyp3db_conn:
            sql = 'update instances set process_id = %(process_id)s where id = %(instance_id)s'
            query_database(conn=hyp3db_conn, query=sql,
                           params=instance, commit=True)
    except Exception as e:
        log.error("any_gamma instance %s could not be updated with specific gamma process id, %s",
                  instance['instance_id'], cfg['proc_name'])
        log.error("Error was: %s", str(e))
    else:
        log.info("any_gamma instance %s was update with specific gamma process id, %s",
                 instance['instance_id'], cfg['proc_name'])


def log_instance_shutdown_in_hyp3_db(cfg):
    if cfg['proc_node_type'] != 'CLOUD':
        return

    instance = get_instance_info(cfg)
    try:
        with get_db_connection('hyp3-db') as hyp3db_conn:
            sql = "update instances set shutdown_time = current_timestamp where id = (%(instance_id)s)"
            query_database(hyp3db_conn, sql, instance, commit=True)
    except Exception as e:
        log.error("Instance %s could not be updated with shutdown time",
                  instance["instance_id"])
        log.error("Error was: %s", str(e))
    else:
        log.info("Instance %s was updated with shutdown time",
                 instance["instance_id"])


def get_instance_info(cfg):
    instance = dict()
    instance['instance_id'] = get_instance_id()
    instance['process_id'] = cfg["process_ids"].get(cfg["proc_name"])
    return instance


def add_instance_record(cfg, conn):
    log.debug('Adding instance record')

    instance_record = {}
    if cfg['proc_node_type'] != 'CLOUD':
        instance_record['instance_id'] = socket.gethostname()
        instance_record['instance_type'] = 'on-prem'
    else:
        instance_record['instance_id'] = get_instance_id()
        instance_record['instance_type'] = get_instance_type()
    instance_record['local_queue_id'] = cfg['id']
    cfg['instance_record'] = instance_record

    try:
        instance_record_sql = '''
            insert into instance_records (instance_id, local_queue_id, start_time, instance_type)
            values (%(instance_id)s, %(local_queue_id)s, current_timestamp, %(instance_type)s)
        '''
        query_database(conn=conn, query=instance_record_sql,
                       params=instance_record, commit=True)

    except Exception:
        log.exception("Instance record could not be inserted")
    else:
        log.info("Instance record of instance %s and job %s inserted",
                 instance_record['instance_id'], instance_record['local_queue_id'])


def update_instance_record(cfg, conn):
    if 'instance_record' in cfg:
        instance_record = cfg['instance_record']
        try:
            instance_record_sql = 'update instance_records set end_time=current_timestamp where (instance_id=%(instance_id)s and local_queue_id=%(local_queue_id)s);'
            query_database(conn=conn, query=instance_record_sql,
                           params=instance_record, commit=True)
        except Exception:
            log.exception("Instance record for instance %s and job %s could not be updated with job completion time",
                          instance_record['instance_id'],
                          instance_record['local_queue_id']
                          )
        else:
            log.info("Instance record for instance %s and job %s had end_time updated with job completion time",
                     instance_record['instance_id'], instance_record['local_queue_id'])
    else:
        log.debug('No instance record found to update')


def get_instance_id():
    # 169.254.169.254 is EC2 metadata endpoint
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
    return (requests.get('http://169.254.169.254/latest/meta-data/instance-id').text)


def get_instance_type():
    # 169.254.169.254 is EC2 metadata endpoint
    # https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
    return (requests.get('http://169.254.169.254/latest/meta-data/instance-type').text)


