import subprocess
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEBase import MIMEBase
from email import encoders



class common_function_class:
    def __init__(self):
    
    def send_mail_attachemnt(self,attachement_path,email_sender_id,email_reciever_id,mail_body,mail_subj,server,filename):
    
        msg = MIMEMultipart()
        email_reciever_id1 =[]
        email_reciever_id1 =email_reciever_id
        print "in function email_reciever_id =", email_reciever_id1
        msg['From'] = email_sender_id
        email_sender_id = email_sender_id
        msg['To'] = ", ".join(email_reciever_id1)
        email_reciever_id = email_reciever_id
        msg['Subject'] = mail_subj
        body = mail_body
        path=attachement_path
        file_list=filename
        msg.attach(MIMEText(body, 'plain'))

        for f in file_list:
            file_path = os.path.join(path, f)
            attachment = open(file_path, "rb")
            part = MIMEBase('application', "octet-stream")
            part.set_payload((attachment).read())
            encoders.encode_base64(part)
            part.add_header('Content-Disposition', 'attachment', filename=f)
            msg.attach(part)
        server = smtplib.SMTP('localhost')
        text = msg.as_string()
        server.sendmail(email_sender_id, email_reciever_id,text)
        server.quit()
'
