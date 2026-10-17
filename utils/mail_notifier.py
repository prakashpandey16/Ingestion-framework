import logging
import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from conf.config import config

# Load configuration
CONFIG = config
EMAIL_CONFIG = CONFIG.get("email", {})
SES_CONFIG = CONFIG.get("aws_ses", {})

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# SES Client
def _get_ses_client():
    try:
        return boto3.client(
            "ses",
            region_name=SES_CONFIG["region"],
            aws_access_key_id=SES_CONFIG["access_key_id"],
            aws_secret_access_key=SES_CONFIG["secret_access_key"]
        )
    except Exception as e:
        logger.error(f"[Emailer] Failed to create SES client → {str(e)}")
        raise

# HTML Success Template
def _success_html(job_name, batch_id, rows_read, rows_transformed, rows_loaded):
    return f"""
<!DOCTYPE html>
<html>
<body style="font-family: Arial; color: #222;">
    <h2 style="color: #0A8754;">Ingestion Job Completed Successfully – {job_name} – {datetime.now().date()}</h2>

    <p>Hi Team,</p>
    <p>The ingestion job <b>{job_name}</b> completed successfully.</p>

    <table style="border-collapse: collapse; width: 500px; margin-top: 10px;">
        <tr><td style="border:1px solid #ccc; padding:8px;">Job Name</td><td style="border:1px solid #ccc; padding:8px;">{job_name}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Batch ID</td><td style="border:1px solid #ccc; padding:8px;">{batch_id}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Extracted Rows</td><td style="border:1px solid #ccc; padding:8px;">{rows_read}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Transformed Rows</td><td style="border:1px solid #ccc; padding:8px;">{rows_transformed}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Loaded Rows</td><td style="border:1px solid #ccc; padding:8px;">{rows_loaded}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Status</td><td style="border:1px solid #ccc; padding:8px; color: green;"><b>SUCCESS</b></td></tr>
    </table>

    <p style="margin-top:20px;">Regards,<br><b>dev@bscxpress.com</b></p>
</body>
</html>
"""

# HTML Failure Template
def _failure_html(job_name, batch_id, error_message):
    return f"""
<!DOCTYPE html>
<html>
<body style="font-family: Arial; color: #222;">
    <h2 style="color: #C0392B;">Ingestion Job Failed – {job_name} – {datetime.now().date()}</h2>

    <p>Hi Team,</p>
    <p>The ingestion job <b>{job_name}</b> failed.</p>

    <table style="border-collapse: collapse; width: 500px; margin-top: 10px;">
        <tr><td style="border:1px solid #ccc; padding:8px;">Job Name</td><td style="border:1px solid #ccc; padding:8px;">{job_name}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Batch ID</td><td style="border:1px solid #ccc; padding:8px;">{batch_id}</td></tr>
        <tr><td style="border:1px solid #ccc; padding:8px;">Status</td><td style="border:1px solid #ccc; padding:8px; color: red;"><b>FAILED</b></td></tr>
    </table>

    <p style="margin-top: 20px;"><b>Error Details:</b></p>
    <pre style="background:#f8f8f8; padding:10px; border:1px solid #ccc;">
{error_message}
    </pre>

    <p style="margin-top:20px;">Regards,<br><b>dev@bscxpress.com</b></p>
</body>
</html>
"""

# Email Sender
def _send_email(subject, html_body, recipients):
    ses = _get_ses_client()

    try:
        response = ses.send_email(
            Source=EMAIL_CONFIG["sender_email"],
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject},
                "Body": {"Html": {"Data": html_body}}
            }
        )
        logger.info(f"[Emailer] Email sent successfully → {response['MessageId']}")
        return True

    except ClientError as e:
        logger.error(f"[Emailer] AWS SES error → {str(e)}")
        return False
    except Exception as e:
        logger.error(f"[Emailer] Unexpected error → {str(e)}")
        return False


# Public Functions
def send_success_mail(job_name, batch_id, rows_read, rows_transformed, rows_loaded):
    """Send ingestion success email."""
    subject = f"Ingestion Job Completed Successfully – {job_name} – {datetime.now().date()}"
    html = _success_html(job_name, batch_id, rows_read, rows_transformed, rows_loaded)
    recipients = EMAIL_CONFIG.get("success_recipients", [])

    logger.info("[Emailer] Sending ingestion success email…")
    return _send_email(subject, html, recipients)


def send_failure_mail(job_name, batch_id, error_message):
    """Send ingestion failure email."""
    subject = f"Ingestion Job Failed – {job_name} – {datetime.now().date()}"
    html = _failure_html(job_name, batch_id, error_message)
    recipients = EMAIL_CONFIG.get("failure_recipients", [])

    logger.info("[Emailer] Sending ingestion failure email…")
    return _send_email(subject, html, recipients)

def _batch_summary_html(batch_id, results):
    """
    Generates HTML for a batch summary email.
    results: list of dictionaries containing {job_name, status, rows_read, rows_loaded, error...}
    """
    rows_html = ""
    for res in results:
        status_color = "green" if res.get('status') == 'success' else "red"
        rows_html += f"""
        <tr>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('job_name', 'N/A')}</td>
            <td style="border:1px solid #ccc; padding:8px; color: {status_color}; font-weight: bold;">{res.get('status', 'unknown').upper()}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('source_table', 'N/A')}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('target_table', 'N/A')}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('records_read', 0)}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('inserts', 0)}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('updates', 0)}</td>
            <td style="border:1px solid #ccc; padding:8px;">{res.get('error_message', '')}</td>
        </tr>
        """
    
    return f"""
<!DOCTYPE html>
<html>
<body style="font-family: Arial; color: #222;">
    <h2 style="color: #007bff;">Batch Ingestion Summary – {batch_id} – {datetime.now().date()}</h2>
    <p>Hi Team,</p>
    <p>The batch <b>{batch_id}</b> has completed. Below is the summary of all jobs processed:</p>

    <table style="border-collapse: collapse; width: 100%; margin-top: 10px;">
        <tr style="background-color: #f2f2f2;">
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Job Name</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Status</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Source</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Target</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Read</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Inserts</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Updates</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Remarks/Error</th>
        </tr>
        {rows_html}
    </table>

    <p style="margin-top:20px;">Regards,<br><b>dev@bscxpress.com</b></p>
</body>
</html>
"""

def send_batch_summary_email(batch_id, results):
    """
    Sends a consolidated summary email for a batch execution.
    """
    subject = f"Batch Execution Summary – {batch_id}"
    html = _batch_summary_html(batch_id, results)
    
    # Send to both success/failure lists or a specific summary list. 
    # For now, combining unique recipients from both.
    recipients = list(set(EMAIL_CONFIG.get("success_recipients", []) + EMAIL_CONFIG.get("failure_recipients", [])))
    
    if not recipients:
        logger.warning("[Emailer] No recipients configured for batch summary.")
        return False

    logger.info(f"[Emailer] Sending batch summary email for batch {batch_id}...")
    return _send_email(subject, html, recipients)


def _invoice_summary_html(amazon_id, station, downloaded_invoices):
    """
    Generates HTML for a summary of downloaded invoices grouped by month.
    downloaded_invoices: list of dicts containing {city, invoice, date, filename}
    """
    total_invoices = len(downloaded_invoices)
    
    # Group by month and year
    from collections import defaultdict
    months_count = defaultdict(int)
    
    for inv in downloaded_invoices:
        date_str = inv.get("date", "")
        # Expected format is MM/DD/YYYY based on observed '03/06/2026' being March 6th
        try:
            if "/" in date_str:
                parts = date_str.split("/")
                if len(parts) >= 3:
                    month_num = int(parts[0])
                    year = parts[2]
                    # Handle possible 2-digit years if present
                    if len(year) == 2:
                        year = "20" + year
                    month_name = datetime(int(year), month_num, 1).strftime("%B %Y")
                    months_count[month_name] += 1
                else:
                    months_count["Unknown Date"] += 1
            else:
                months_count["Unknown Date"] += 1
        except Exception:
            months_count["Unknown Date"] += 1
            
    rows_html = ""
    if total_invoices > 0:
        # Sort months so they appear nicely
        for month, count in sorted(months_count.items(), reverse=True):
            rows_html += f"""
            <tr>
                <td style="border:1px solid #ccc; padding:8px;"><b>{month}</b></td>
                <td style="border:1px solid #ccc; padding:8px; text-align: center;">{count}</td>
            </tr>
            """
    else:
        rows_html = """
        <tr>
            <td colspan="2" style="border:1px solid #ccc; padding:8px; text-align: center; color: #666;">
                No invoices were downloaded in this run.
            </td>
        </tr>
        """
    
    return f"""
<!DOCTYPE html>
<html>
<body style="font-family: Arial; color: #222;">
    <h2 style="color: #007bff;">Invoice Download Summary – Batch {amazon_id} ({station}) – {datetime.now().date()}</h2>
    <p>Hi Team,</p>
    <p>The invoice scraping job for <b>Batch {amazon_id} ({station})</b> has completed. Below is the summary of downloaded invoices grouped by month:</p>

    <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-bottom: 20px; border: 1px solid #e9ecef; max-width: 400px;">
        <h3 style="margin-top: 0; color: #495057;">Status Summary</h3>
        <p style="margin-bottom: 5px;"><b>Total Invoices Downloaded:</b> <span style="font-size: 1.1em; color: {'green' if total_invoices > 0 else 'red'};">{total_invoices}</span></p>
    </div>

    <table style="border-collapse: collapse; width: 100%; max-width: 400px; margin-top: 10px;">
        <tr style="background-color: #f2f2f2;">
            <th style="border:1px solid #ccc; padding:8px; text-align: left;">Invoice Month</th>
            <th style="border:1px solid #ccc; padding:8px; text-align: center;">Count</th>
        </tr>
        {rows_html}
    </table>

    <p style="margin-top:20px;">Regards,<br><b>dev@bscxpress.com</b></p>
</body>
</html>
"""

def send_invoice_summary_email(amazon_id, station, downloaded_invoices):
    """
    Sends a consolidated summary email for invoice scraping.
    """
    subject = f"Invoice Download Summary – Batch {amazon_id} ({station})"
    html = _invoice_summary_html(amazon_id, station, downloaded_invoices)
    
    # Send to both success/failure lists or a specific summary list. 
    recipients = list(set(EMAIL_CONFIG.get("success_recipients", []) + EMAIL_CONFIG.get("failure_recipients", [])))
    
    if not recipients:
        logger.warning("[Emailer] No recipients configured for invoice summary.")
        return False

    logger.info(f"[Emailer] Sending invoice summary email for batch {amazon_id}...")
    return _send_email(subject, html, recipients)

