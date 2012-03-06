
import os
import sys

import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email import Encoders

import configuration

def send(subject, file_names=[]):
    sendto=configuration.sendto_email_address
    sendfrom=configuration.from_address

    msg = MIMEMultipart()
    msg['To'] = configuration.sendto_email_address
    msg['Subject'] = subject + ' '.join(file_names)

    for file_name in file_names:
        print 'Sending ', file_name
        part = MIMEBase('application', 'octet-stream')
        with open(file_name, 'rb') as f:
            part.set_payload(f.read())
        Encoders.encode_base64(part)

        part.add_header('Content-Disposition', 'attachment; filename=%s' % os.path.basename(file_name))
        msg.attach(part)

    smtp = smtplib.SMTP("localhost")
    smtp.sendmail(sendfrom, sendto, msg.as_string())
    smtp.close()

    
