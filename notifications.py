"""
TalentPoint — Email notification system.

Sends branded HTML emails via configurable SMTP.
All recipient addresses come from tenant config, not hardcoded.
"""

import os
import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

import config

logger = logging.getLogger(__name__)


def send_email(to, subject, html_body, cc=None, reply_to=None):
    """
    Send an HTML email.

    Args:
        to: recipient email (string) or list of emails
        subject: email subject line
        html_body: HTML content (will be wrapped in branding template)
        cc: optional CC email(s)
        reply_to: optional reply-to address
    """
    smtp_email = os.environ.get('SMTP_EMAIL', '')
    smtp_password = os.environ.get('SMTP_PASSWORD', '')
    smtp_host = os.environ.get('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))

    if not smtp_email or not smtp_password:
        logger.warning("SMTP not configured — skipping email: %s", subject)
        return False

    from_name = config.org_name()

    # Normalize recipients
    if isinstance(to, str):
        to = [to]
    if isinstance(cc, str):
        cc = [cc]

    # Wrap content in branded template
    full_html = _branded_template(html_body)

    msg = MIMEMultipart('alternative')
    msg['From'] = f"{from_name} <{smtp_email}>"
    msg['To'] = ', '.join(to)
    msg['Subject'] = subject
    if cc:
        msg['Cc'] = ', '.join(cc)
    if reply_to:
        msg['Reply-To'] = reply_to

    msg.attach(MIMEText(full_html, 'html'))

    all_recipients = list(to)
    if cc:
        all_recipients.extend(cc)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_email, smtp_password)
            server.sendmail(smtp_email, all_recipients, msg.as_string())
        logger.info("Email sent: %s -> %s", subject, ', '.join(to))
        return True
    except Exception as e:
        logger.error("Failed to send email '%s': %s", subject, e)
        return False


def _branded_template(body_html):
    """Wrap email body in the TalentPoint branded template."""
    primary = config.primary_color()
    secondary = config.secondary_color()
    org = config.org_name()
    logo = config.logo_url()

    logo_html = f'<img src="{logo}" alt="{org}" style="max-height:40px;">' if logo else f'<strong>{org}</strong>'

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0; padding:0; font-family:'Open Sans',Arial,sans-serif; background-color:#f5f5f5;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f5f5f5;">
    <tr>
      <td align="center" style="padding:20px;">
        <table width="600" cellpadding="0" cellspacing="0" style="background-color:#ffffff; border-radius:8px; overflow:hidden;">
          <!-- Header -->
          <tr>
            <td style="background-color:{secondary}; padding:20px 30px; text-align:center;">
              {logo_html}
            </td>
          </tr>
          <!-- Body -->
          <tr>
            <td style="padding:30px;">
              {body_html}
            </td>
          </tr>
          <!-- Footer -->
          <tr>
            <td style="background-color:{secondary}; padding:15px 30px; text-align:center;">
              <p style="color:#ffffff; font-size:12px; margin:0;">
                {org} &mdash; Powered by TalentPoint
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""
