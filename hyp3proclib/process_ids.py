from __future__ import print_function, absolute_import, division, unicode_literals

from hyp3proclib.db import get_db_connection, query_database


def get_process_id_dict():
    process_id_dict = dict()
    process_names_ids_sql = 'select text_id, id FROM processes'
    process_names_ids = list()

    with get_db_connection('hyp3-db') as hyp3db_conn:
        process_names_ids = query_database(
            conn=hyp3db_conn, query=process_names_ids_sql, returning=True)
        for proc in process_names_ids:
            process_id_dict[proc[0]] = proc[1]

    return process_id_dict
