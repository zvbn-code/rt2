# %% [markdown]
# ## Anleitung Email
# 
# https://realpython.com/python-send-email/#starting-a-secure-smtp-connection

# %%
def log_email(recipients, subject, body_plain, body_html, **files):
    """
    Funktionen zum Senden von Emails

    Variablen 
    Empfänger recipients als Array von E-Mail-Adressen
    Betreff Subject
    Inhalt(Text) body_plain
    Inhalt(html) body_html
    Optional files Array für Dateien als Anhang

    """

    import smtplib, ssl

    from email import encoders
    from email.mime.base import MIMEBase
    from email.header import Header
    from email.mime.multipart import MIMEMultipart
    from email.mime.application import MIMEApplication
    from email.mime.text import MIMEText
    from email.utils import formataddr
    from os.path import basename
    from pathlib import Path

    import os
    from dotenv import load_dotenv

    import datetime as dt

    load_dotenv()
    smtp_server = "smtp.gmail.com"
    port = 465  # For starttls bei Google
    sender_email = "log.zvbn@gmail.com"
    password = os.environ.get('GMAIL_PASSWORD')

    # Create a multipart message and set headers
    message = MIMEMultipart("alternative")
    
    message["From"] = formataddr((str(Header('DRPCA Import Log', 'utf-8')), sender_email))
    message["To"] = ', '.join(recipients)
    message["Subject"] = subject
    #message["Bcc"] = receiver_email  # Recommended for mass emails

    if files:
        for key, value in files.items():
            files = value

            for path in files:
                if Path(path).is_file():    
                    part = MIMEBase('application', "octet-stream")
                    with open(path, 'rb') as file:
                        part.set_payload(file.read())
                    encoders.encode_base64(part)
                    part.add_header('Content-Disposition',
                                    'attachment; filename={}'.format(Path(path).name))
                    message.attach(part)

    # Add body to email
    message.attach(MIMEText(body_plain, "plain"))
    message.attach(MIMEText(body_html, "html"))

    text = message.as_string()

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(sender_email,recipients, text)
