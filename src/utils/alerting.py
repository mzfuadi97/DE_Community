import smtplib
from email.mime.text import MIMEText

def send_alert_email(error_message: str):
    msg = MIMEText(f"ETL pipeline error: {error_message}")
    msg['Subject'] = 'ETL Pipeline Alert'
    msg['From'] = 'no-reply@yourcompany.com'
    msg['To'] = 'admin@yourcompany.com'
    
    try:
        server = smtplib.SMTP('smtp.yourcompany.com')
        server.sendmail(msg['From'], msg['To'], msg.as_string())
        server.quit()
        logging.info("Alert email sent")
    except Exception as e:
        logging.error(f"Error sending alert email: {e}")
