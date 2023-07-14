import credentials
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import datetime
import numpy as np

def email_new(df_list):
    differences = 0
    if len(df_list) > 1: #Then the first record will be the differences df
        differences = np.sum(df_list[1]["IncreasedChanges"])

    message = MIMEMultipart()
    message['Subject'] = f"{differences} New Changes from Today - {datetime.date.today()}"
    message['From'] = credentials.sender
    message['To'] = credentials.recipient

    text_message = "Here are the total changes that have taken place since we've starten scraping:\n"
   
    # Attach the text message as a plain text part
    plain_text = MIMEText(text_message, 'plain')
    message.attach(plain_text)

    # Create a separate MIMEText object for each DataFrame
    html = MIMEText(df_list[0].to_html(index=True), "html")
    message.attach(html)

    #----------------------------------------------------------------------------------------------------

    text_message = f"\n\nThere were {differences} changes from the most recent scrape: \n"

    # Attach the text message as a plain text part
    plain_text = MIMEText(text_message, 'plain')
    message.attach(plain_text)

    if differences > 0:
        # Create a separate MIMEText object for each DataFrame
        html = MIMEText(df_list[1].to_html(index=True), "html")
        message.attach(html)

    #-----------------------------------------------------------------------------------------------------

    text_message = f"\n\nHere are the changes broken down by table: \n"

    # Attach the text message as a plain text part
    plain_text = MIMEText(text_message, 'plain')
    message.attach(plain_text)

    for i, df in enumerate(df_list[2:]):
        # Create a separate MIMEText object for each DataFrame
        html = MIMEText(df.to_html(index=False), "html")
        message.attach(html)

        plain_text = MIMEText("\n\n", 'plain')
        message.attach(plain_text)


    with smtplib.SMTP("smtp.office365.com", 587) as server:
        server.starttls()
        server.login(credentials.sender, credentials.password)
        server.sendmail(credentials.sender, credentials.recipient, message.as_string())