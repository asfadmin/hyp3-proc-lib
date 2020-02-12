"""Module for proc_lib email functions"""

import datetime
import smtplib
import uuid
try:
    # Python 2 w/ futures and python 3
    from html import escape
except ImportError:
    # python 2 w/o futures
    from cgi import escape

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from hyp3proclib.config import get_config
from hyp3proclib.db import get_db_connection, get_user_info, query_database
from hyp3proclib.logger import log


def queue_email(conn, lqid, to_address, subject, body):
    sql = '''
    insert into email_queue(local_queue_id, status, recipients, subject, message, mime_type)
    values (%(local_queue_id)s, %(status)s, %(recipients)s, %(subject)s, %(message)s, %(mime_type)s)
    '''
    values = {
        "local_queue_id": lqid,
        "status": 'QUEUED',
        "recipients": to_address,
        "subject": subject,
        "message": body,
        "mime_type": 'html'
    }
    query_database(conn, sql, values, commit=True)


def send_queued_emails():
    with get_db_connection('hyp3-db') as conn:
        sql = '''
            select id, local_queue_id, recipients, subject, message, attachment_filename, attachment, mime_type
            from email_queue where status = 'QUEUED'
        '''
        recs = query_database(conn, sql)
        if len(recs) == 0:
            log.info('No emails to send')

        for r in recs:
            if r and r[0] and r[2] and len(r[2]) > 0:
                id_ = int(r[0])
                lqid = None
                if r[1] is not None:
                    lqid = int(r[1])
                to = r[2]
                subject = r[3]
                body = r[4]

                mime_type = "plain"
                if r[7] is not None:
                    mime_type = r[7]
                if mime_type == "text":
                    mime_type = "plain"

                log.info('Emailing ' + to + ' for lqid: ' + str(lqid))
                log.debug('Subject: ' + subject)

                ok, msg = send_email(to, subject, body, mime_type=mime_type)
                if ok:
                    status = 'SENT'
                else:
                    status = 'FAILED'

                log.debug('Updating status to ' + status)
                sql = "update email_queue set status = %(status)s, system_message = %(msg)s, processed_time = current_timestamp where id = %(id)s"
                query_database(conn, sql, {'status': status, 'msg': msg, 'id': id_}, commit=True)


def send_email(
        to_address, subject, body, from_address="no-reply@asf-hyp3", retries=0,
        maximum_retries=0, mime_type="plain"):
    """Send an email and return whether the email was successfully sent.

    We also retry sending the email if something went wrong the first
    time, with the maximum number of retries configurable in the
    arguments. This method only supports sending plain text emails.
    """
    if retries > maximum_retries:
        log.critical(
            "Notification failed permanently (maximum retries reached)",
        )
        return False, None
    if retries == 0:
        log.info("Sending email")
    else:
        log.info("Retrying email")

    smtp = smtplib.SMTP("localhost")

    msg = MIMEMultipart('related')
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = to_address
    msg.preamble = 'This is a multi-part message in MIME format.'

    msgAlt = MIMEMultipart('alternative')
    msg.attach(msgAlt)

    msgText = MIMEText('HyP3 product notification email')
    msgAlt.attach(msgText)

    msgText = MIMEText(body)
    msgText.replace_header('Content-Type', 'text/html')
    msgAlt.attach(msgText)

    log.debug("Sending email from {0} to {1}".format(from_address, to_address))

    bcc_address = []
    bcc = get_config('general', 'bcc', default='')
    if len(bcc) > 0:
        bcc_address += bcc.split(',')
        log.debug("Bcc: " + str(bcc_address))

    try:
        smtp.sendmail(from_address, [to_address] + bcc_address, msg.as_string())
    except smtplib.SMTPException as e:
        msg = str(e)
        log.error("Failed to notify user: " + msg)
        smtp.quit()

        if retries >= maximum_retries:
            log.critical("Notification failed permanently (maximum retries reached)")
            return False, msg

        return send_email(to_address, subject, body, from_address, retries + 1, maximum_retries)

    smtp.quit()
    return True, None


def notify_user_failure(cfg, conn, msg):
    if cfg['notify_fail'] is False:
        log.info('Notifications for failures not turned on.')
        return

    log.debug('Preparing to notify user of processing failure')

    username, email, wants_email, subscription_name, process_name = get_user_info(cfg, conn)

    if wants_email:
        log.debug('Notifying {0}...'.format(username))
        message = "Hi, {0}\n\n".format(username)

        if cfg['sub_id'] > 0:
            message += "Your subscription '{0}' attempted to process a product but failed.\n\n".format(subscription_name)
            subject = "[{0}] Failed processing for subscription '{1}'".format(cfg['subject_prefix'], subscription_name)
        else:
            message += "Your one-time '{0}' processing request failed.\n\n".format(process_name)
            subject = "[{0}] Failed one-time processing for '{1}'".format(cfg['subject_prefix'], process_name)

        # if len(msg.strip())>0:
        #    message += "\n" + "Captured error message:\n" + msg + "\n\n"

        if 'granule_url' in cfg and len(cfg['granule_url']) > 0:
            message += "You can download the original data here:<br>" + cfg['granule_url'] + "<br>"
            if cfg['other_granule_urls'] is not None:
                for url in cfg['other_granule_urls']:
                    message += url + "<br>"

        if "email_text" in cfg and len(cfg["email_text"]) > 0:
            message += "\n" + cfg["email_text"] + "\n\n"
        else:
            message += "\n"

        if cfg['sub_id'] > 0:
            param = str(cfg['sub_id'])
            id_, hashval = create_one_time_hash(conn, 'disable_subscription', cfg['user_id'], param)
            message += "Disable this subscription:\n" \
                "https://api.hyp3.asf.alaska.edu/onetime/disable_subscription?id="+str(id_)+"&key="+hashval+"\n\n"

        # message += "Captured processing info:\n\n" + cfg['log']

        queue_email(conn, cfg['id'], email, subject, message)
    else:
        log.info("Email will not be sent to user {0} due to user preference".format(username))


def notify_user(product_url, queue_id, cfg, conn):
    """Email a user notifying them of a finished product.

    Takes the name of a finished product, download link for the finished
    product, subscription ID for the product, the configuration
    parameters, and a database connection, and emails the user to notify
    them that the product has been finished and provide them with the
    download links for both the finished product and the original
    granule.

    This function return True upon success and False upon failure.
    """
    username, email, wants_email, subscription_name, process_name = get_user_info(cfg, conn)

    if cfg['sub_id'] > 0:
        title = "A new '{0}' product for your subscription '{1}' is ready.".format(process_name, subscription_name)
        subject = "[{0}] New product for subscription '{1}'".format(cfg['subject_prefix'], subscription_name)
    else:
        title = "A new product for your '{0}' one-time processing request has been generated.".format(process_name)
        subject = "[{0}] New {1} product available".format(cfg['subject_prefix'], process_name)

    message = get_email_header(title)

    message += "<p>Hello HyP3-User!"
    message += "<p>" + title + "\n"

    if 'description' in cfg and cfg['description'] and len(cfg['description']) > 0:
        message += "<p>" + escape(cfg['description'], quote=False).replace('\n', '<br>') + "<br>\n"

    if process_name != "Notify Only":
        message += '<p>You can download it here:<br><a href="{0}">{1}</a><br><br>\n'.format(product_url, cfg['filename'])

        if 'browse_url' in cfg and cfg['browse_url'] is not None and len(cfg['browse_url']) > 0:
            message += '<center><a href="{0}"><img src="{1}" width="80%" border="0"/></a></center><br>\n'.format(cfg['browse_url'], cfg['browse_url'])

        if 'final_product_size' in cfg:
            sz = cfg['final_product_size'][0]
            mb = float(sz)/1024.0/1024.0
            message += "<p>Size: %.2f MB<br><br>\n" % mb

        message += "You can find all of your products at the HyP3 website:<br>{0}/products<br>\n".format(cfg['hyp3_product_url'])

        if 'granule_url' in cfg and len(str(cfg['granule_url'])) > 0 and 'Subscription: ' not in str(cfg['granule_url']):
            message += "<p>You can download the original data from the ASF datapool here:<br>" + urlify(cfg['granule_url']) + "<br>\n"
            if 'other_granule_urls' in cfg and cfg['other_granule_urls'] is not None:
                for url in cfg['other_granule_urls']:
                    message += urlify(url) + "<br>\n"

        if 'SLC' in cfg['granule']:
            message += '<p>View this stack in the ASF baseline tool:<br>'
            message += 'http://baseline.asf.alaska.edu/#baseline?granule={0}\n'.format(cfg['granule'])
    else:
        message += "<p>You can download it here:<br>" + urlify(product_url) + "<br>"

    if "email_text" in cfg and len(cfg["email_text"]) > 0:
        message += "<p>" + cfg["email_text"] + "<br>"
    if 'process_time' in cfg:
        message += process_name + " processing time: " + str(datetime.timedelta(seconds=int(cfg['process_time']))) + "<br>\n"

    if cfg['sub_id'] > 0:
        param = str(cfg['sub_id'])
        id_, hashval = create_one_time_hash(conn, 'disable_subscription', cfg['user_id'], param)
        message += "<p>Done with this subscription?  Disable it with this link:<br>" \
                   "https://api.hyp3.asf.alaska.edu/onetime/disable_subscription?id="+str(id_)+"&key="+hashval+"<br><br>\n"

    message += get_email_footer()

    # message += "Hostname: " + socket.gethostname() + "\n"
    if wants_email:
        log.info('Emailing: ' + email)
        queue_email(conn, cfg['id'], email, subject, usr(message, username))
    else:
        log.info("Email will not be sent to user {0} due to user preference".format(username))

        bcc = get_config('general', 'bcc', default='')
        if len(bcc) > 0:
            # We only have to do the first one, the rest will be bcc'ed :)
            addr = bcc.split(',')[0]
            log.debug('Queueing email for BCC user: ' + addr)
            queue_email(conn, cfg['id'], addr, subject, usr(message, username))


def create_one_time_hash(conn, action, user_id, params):
    sql = '''
        update one_time_actions set expires = now() + interval '30' day  where
            user_id=%(user_id)s and action=%(action)s and params=%(params)s
            returning id, hash
    '''
    vals = {'user_id': user_id, 'action': action, 'params': params}
    recs = query_database(conn, sql, vals, commit=True, returning=True)

    if recs and len(recs) > 0 and len(recs[0]) > 0:
        return recs[0][0], recs[0][1]

    hashval = uuid.uuid4().hex
    sql = '''
        insert into one_time_actions(hash,user_id,action,params,expires)
        values (%(hashval)s, %(user_id)s, %(action)s, %(params)s, current_timestamp + interval '30' day)
        returning id
    '''
    vals.update({'hashval': hashval})
    recs = query_database(conn, sql, vals, commit=True, returning=True)

    if not recs[0][0]:
        raise Exception("Error getting id")
    id_ = int(recs[0][0])
    log.debug('One-time hash id: {0} and hash: {1}'.format(id_, hashval))

    return id_, hashval


def usr(message, username):
    return message.replace('HyP3-User', username)


def urlify(url):
    name = url[url.rfind("/")+1:]
    return '<a href="{0}">{1}</a>'.format(url, name)


def get_email_header(title):
    message = """
        <!DOCTYPE html><html><head>
        <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
        <meta name="viewport" content="width=device-width,initial-scale=1.0"/>
        <title>{0}</title>
    """.format(title)

    message += """
        <style type="text/css">.ExternalClass {width:100%;}.ExternalClass,.ExternalClass p,.ExternalClass span,.ExternalClass font,.ExternalClass td,.ExternalClass div {line-height:100%;}table td {border-collapse:collapse;}.granule {display:inline;}.granule-small {display:none;}.granule-medium {display:none;}@media only screen and (max-width:750px) {.granule {display:none;}.granule-medium {display:inline;}.granule-small {display:none;}}@media only screen and (max-width:450px) {.granule {display:none;}.granule-medium {display:none;}.granule-small {display:inline;}}</style>
        </head>
        <body style="width:100% !important;-webkit-text-size-adjust:100%;-ms-text-size-adjust:100%;margin:0;padding:0;margin:0 auto">
        <table cellpadding="0" cellspacing="0" border="0" style="padding:0;width:100% !important;max-width:800px;line-height:100% !important;-webkit-text-size-adjust:none;-ms-text-size-adjust:100%;background-color:#ffffff;margin:0 auto"><!-- Header -->
        <tr>
          <td width="100%" style="border-bottom:1px solid #D4D3D3;" id="header">
            <table width="100%" style="border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;" border="0" cellpadding="0" cellspacing="0" >
              <tr style="padding:0px;">
                <td valign="top" align="left" style="padding:0px;min-height:50px;margin-top:10px" border="0" width="100%" height="100%">
                  <a href="http://hyp3.asf.alaska.edu">
                    <img alt="Alaska Satellite Facility" src="https://www.asf.alaska.edu/wp-content/uploads/2019/06/asf-logo-blue-nav.png" style="display:block;border:none;outline:none;text-decoration:none;-ms-interpolation-mode:bicubic;max-width:100px;" width="100%" border="0"/></a>
                </td>
              </tr>
              <tr>
                <td>
                  <p style="margin:1em 0;color:#999;margin:5px 0px 5px 20px">Hybrid Pluggable Processing Pipeline (HyP3)</p>
                </td>
              </tr>
            </table>
          </td>
          </tr><!-- End Header -->
          <!-- Content -->
          <tr>
            <td width="100%" id="content">
              <center width="100%" height="100%" style="display:grid">
              <table width="95%" style="border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;" border="0" cellpadding="0" cellspacing="0" >
    """

    return message


def get_email_footer():
    return """
            </table>
            </center>
          </td>
        </tr><!-- End Content -->
        <!-- Footer -->
        <tr style="background-color:#eaeaea;background:#eaeaea;margin-top:20px">
          <td width="100%" style="border-top:1px solid #D4D3D3;" id="footer">
            <table width="100%" style="border-collapse:collapse;mso-table-lspace:0pt;mso-table-rspace:0pt;" border="0" cellpadding="0" cellspacing="0" >
              <tr style="padding:0px;" border="0">
                <td valign="bottom" align="left" style="padding:2px" border="0" width="50%">
                  <div style="margin-right:auto;margin-left:auto">
                    <p style="margin:1em 0;color:#777">&nbsp;Thank you for using HyP3!<br>
                      &nbsp;Too many emails?  <a href="http://hyp3.asf.alaska.edu/account">Change your email preferences</a>
                    <br/>
                  </div>
                </td>
                <td valign="bottom" align="right" style="padding:2px" border="0" width="50%">
                  <div style="margin-right:auto;margin-left:auto">
                    <p style="margin:1em 0;color:#777">
                      Try the <a href="https://api.hyp3.asf.alaska.edu">HyP3 API</a>&nbsp;
                  </div>
                </td>
              </tr>
            </table>
          </td>
        </tr><!-- End Footer -->
      </table><!-- End of wrapper table -->
      </body></html>
    """
