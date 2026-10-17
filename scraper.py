# scraper.py

# Import python modules
import os
import sys
import platform
import boto3
import zipfile
import tempfile
import pandas as pd
from io import BytesIO 
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright, expect
from sqlalchemy.sql import text, select
from urllib.parse import urlparse
import logging


def launch_stealth_browser(playwright, headless=True, storage_state=None):
    """Launch a Playwright Chromium browser with anti-detection (stealth) settings.
    
    Prevents CAPTCHA triggers on Linux headless environments by hiding
    common bot signals (WebDriver flag, HeadlessChrome UA, missing plugins, etc.)
    
    Args:
        playwright: Playwright instance from sync_playwright()
        headless: Run in headless mode (required for Linux servers)
        storage_state: Path to saved session JSON (for session reuse)
    
    Returns:
        tuple: (browser, context, page)
    """
    browser = playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",  # Hides navigator.webdriver
            "--no-sandbox",                                    # Required for Linux root
            "--disable-dev-shm-usage",                         # Prevents /dev/shm crashes on Linux
            "--disable-infobars",                              # Hides "Chrome is being controlled"
            "--disable-extensions",
            "--disable-gpu",                                   # Avoids GPU issues in headless
            "--window-size=1920,1080",
        ]
    )

    # Real-looking user agent (matches recent Chrome on Windows)
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    )

    context_options = {
        "user_agent": user_agent,
        "viewport": {"width": 1920, "height": 1080},
        "screen": {"width": 1920, "height": 1080},
        "locale": "en-US",
        "timezone_id": "Asia/Kolkata",
        "java_script_enabled": True,
    }

    # Reuse saved session if available
    if storage_state and os.path.exists(storage_state):
        context_options["storage_state"] = storage_state
        logging.info(f"Reusing saved session from {storage_state}")

    context = browser.new_context(**context_options)

    # Inject stealth scripts to hide automation signals
    context.add_init_script("""
        // Remove webdriver flag
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        
        // Fake plugins array (empty = bot signal)
        Object.defineProperty(navigator, 'plugins', {
            get: () => [
                {
                    0: {type: "application/x-google-chrome-pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: Plugin},
                    description: "Portable Document Format",
                    filename: "internal-pdf-viewer",
                    length: 1,
                    name: "Chrome PDF Plugin"
                },
                {
                    0: {type: "application/pdf", suffixes: "pdf", description: "Portable Document Format", enabledPlugin: Plugin},
                    description: "Portable Document Format",
                    filename: "internal-pdf-viewer",
                    length: 1,
                    name: "Chrome PDF Viewer"
                },
                {
                    0: {type: "application/x-nacl", suffixes: "", description: "Native Client Executable", enabledPlugin: Plugin},
                    description: "",
                    filename: "internal-nacl-plugin",
                    length: 2,
                    name: "Native Client"
                }
            ]
        });
        
        // Fake languages
        Object.defineProperty(navigator, 'languages', {
            get: () => ['en-US', 'en']
        });
        
        // Hide Chrome automation indicators
        window.chrome = { runtime: {} };
    """)

    page = context.new_page()

    return browser, context, page

def get_table_data(session, table_obj, id=None, filter_active=False):
    """Generic function to fetch all columns from a given Table object in PostgreSQL using SQLAlchemy Core.
    If amazon_id provided, filter by it (assumes column 'amazon_id' exists).
    If filter_active is True, filter by is_active == True (assumes column 'is_active' exists).
    Assumes only one row is returned when amazon_id is provided, returns the dict directly.
    """
    logging.info(id)
    stmt = select(table_obj)
    if filter_active and 'is_active' in table_obj.columns:
        stmt = stmt.where(table_obj.c.is_active == True)
    if id and 'id' in table_obj.columns:
        stmt = stmt.where(table_obj.c.id == id)
    logging.info(stmt)
    try:
        results= session.execute(stmt).fetchall()
        # Get column names from table definition
        columns = table_obj.columns.keys()
        # Convert ResultProxy tuples to dictionaries
        metadata_dicts = []
        for result in results:
            metadata_dict = {column: value for column, value in zip(columns, result)}
            
            metadata_dicts.append(metadata_dict)
        if len(metadata_dicts)>0:
            return metadata_dicts[0]
        else:
            logging.info(f"No rows in {table_obj.name}")

    except Exception as e:
        logging.error(f"Error while extracting rows from {table_obj.name} : {e}")
        raise

def insert_audit(session, amazon_id, station, report_name, status, time_taken):
    """Insert audit log entry"""
    insert_query = """
    INSERT INTO etl_config.audit_log (amazon_id, station, report_name, status, time_taken)
    VALUES (:amazon_id, :station, :report_name, :status, :time_taken)
    """
    params = {
        'amazon_id': amazon_id,
        'station': station,
        'report_name': report_name,
        'status': status,
        'time_taken': time_taken
    }
    try:
        session.execute(text(insert_query), params)
        session.commit()
        logging.info(f"Audit logged for {amazon_id} | {station} | {report_name}: {status}")
    except Exception as e:
        session.rollback()
        logging.error(f"Error inserting audit log: {e}")

# Login
def login_and_save_session(page, context,entry,storage_start):
    """Perform login and save session"""
    try:
        URL_LINK=entry["url_link"]
        user_name=entry["user_name"]
        password=entry["password"]
        page.goto(URL_LINK)

        # Fill credentials
        page.fill("input[name='email']", user_name)
        page.click("input#continue")
        page.fill("input[name='password']", password)
        page.click("input#signInSubmit")

        # Wait until logged in s
        page.wait_for_load_state("networkidle")
        logging.info("Logged in successfully")

        # Save storage_state snapshot
        context.storage_state(path=storage_start)
        logging.info(f"Session saved to {storage_start}")
    except Exception as e:
        logging.error(f"Login failed: {e}")
        raise

def ensure_logged_in_old(browser,entry,storage_state):
    """Check if session is still valid, otherwise re-login"""
    URL_LINK=entry["url_link"]
    # In incognito mode, always create a new page since no pages persist
    page = browser.new_page()
    page.goto(URL_LINK)

    if "signin" in page.url.lower():
        logging.info("Session expired, re-logging in...")
        # In incognito mode, we don't save storage state since that's the point of incognito
        login_without_saving_session(page, entry)
    else:
        logging.info("Session still valid")

    return page

def ensure_logged_in(page, context, entry, storage_state, max_retries=2):
    """Ensure user is logged in using an existing page.
    
    Args:
        page: Playwright page instance
        context: Browser context (needed to save storage_state after re-login)
        entry: Dict with url_link, user_name, password
        storage_state: Path to the storage_state JSON file
        max_retries: Number of login retry attempts (default: 2)
    """

    URL_LINK = entry["url_link"]

    try:
        page.goto(URL_LINK, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_load_state("networkidle", timeout=30000)
    except Exception as e:
        logging.warning(f"Navigation timeout or error: {e}")

    # Check URL path only (not query params which may contain 'signin' in OpenID callbacks)
    if "/ap/signin" in urlparse(page.url.lower()).path:
        logging.info("Session expired, re-logging in...")

        for attempt in range(1, max_retries + 1):
            try:
                login_without_saving_session(page, entry)
                page.wait_for_load_state("networkidle", timeout=30000)

                # Verify login was successful
                if "/ap/signin" not in urlparse(page.url.lower()).path:
                    # Save the new session so next run can reuse it
                    try:
                        context.storage_state(path=storage_state)
                        logging.info(f"Login successful, session saved to {storage_state}")
                    except PermissionError:
                        logging.warning(f"Login successful but could not save session to {storage_state} (permission denied). "
                                        f"Try: sudo rm {storage_state}")
                    except Exception as e:
                        logging.warning(f"Login successful but session save failed: {e}")
                    return
                else:
                    logging.warning(f"Login attempt {attempt} did not succeed, still on sign-in page")

            except Exception as e:
                logging.error(f"Login attempt {attempt}/{max_retries} failed: {e}")

            if attempt < max_retries:
                logging.info(f"Retrying login (attempt {attempt + 1}/{max_retries})...")
                page.wait_for_timeout(2000)  # Brief pause before retry

        logging.error("All login attempts failed")
        logging.info(f"Debug info - Current URL: {page.url}")
        raise Exception("All login attempts failed — possible CAPTCHA or account issue")
    else:
        logging.info("Session still valid")


def solve_if_captcha(page):
    """Check for and solve a CAPTCHA if one is actually visible on the page.
    
    Amazon's sign-in page HTML contains the word 'captcha' in its JavaScript
    even when no CAPTCHA is displayed. We must check for *visible* CAPTCHA
    elements rather than pattern-matching the raw HTML source.
    """
    import logging

    # 1. Check URL — Amazon redirects to a dedicated CAPTCHA page path
    url_has_captcha = "/errors/validateCaptcha" in page.url

    # 2. Check for a visible CAPTCHA image on the page (the real indicator)
    captcha_img = page.locator("img[src*='captcha' i], img[src*='Captcha']").first
    try:
        img_visible = captcha_img.is_visible(timeout=1500)
    except Exception:
        img_visible = False

    if not url_has_captcha and not img_visible:
        # No real CAPTCHA challenge — just continue
        return

    logging.info("Visible CAPTCHA challenge detected, attempting to auto-solve...")

    try:
        from amazoncaptcha import AmazonCaptcha

        if img_visible:
            img_src = captcha_img.get_attribute("src")
            logging.info(f"Solving CAPTCHA image: {img_src}")
            captcha = AmazonCaptcha.fromlink(img_src)
            solution = captcha.solve()
            logging.info(f"CAPTCHA solution: {solution}")

            captcha_input = page.locator(
                "input#captchacharacters, input[name='cvf_captcha_input'], input[name='field-keywords']"
            )
            if captcha_input.is_visible(timeout=2000):
                captcha_input.fill(solution)
                page.wait_for_timeout(500)
                submit_btn = page.locator(
                    "button[type='submit'], input[type='submit'], .a-button-text"
                ).first
                submit_btn.click()
                page.wait_for_load_state("networkidle")
                page.wait_for_timeout(2000)
                logging.info("CAPTCHA submitted successfully")
                return
            else:
                logging.error("CAPTCHA input field not found on page")
        else:
            logging.error("CAPTCHA URL detected but no solvable image found")

    except Exception as e:
        logging.error(f"Failed to auto-solve CAPTCHA: {e}")

    # Save a debug screenshot before raising
    try:
        page.screenshot(path="/tmp/captcha_debug.png")
        logging.info("Saved captcha debug screenshot to /tmp/captcha_debug.png")
    except Exception:
        pass

    raise Exception("CAPTCHA detected and could not be solved")


def login_without_saving_session(page, entry):
    """Perform login with human-like behavior to avoid CAPTCHA detection.
    
    Uses slow typing, random delays, and mouse movements to mimic real user.
    """
    import random
    
    URL_LINK = entry["url_link"]
    user_name = entry["user_name"]
    password = entry["password"]
    
    page.goto(URL_LINK, wait_until="domcontentloaded")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(random.randint(1000, 2000))  # Random pause like a real user
    
    # Check if already on CAPTCHA page before even trying
    solve_if_captcha(page)
    
    # Fill email with human-like typing (type character by character)
    email_input = page.locator("input[name='email']")
    email_input.click()
    page.wait_for_timeout(random.randint(300, 600))
    email_input.fill("")  # Clear first
    page.keyboard.type(user_name, delay=random.randint(50, 150))  # Type slowly
    
    page.wait_for_timeout(random.randint(500, 1000))  # Pause before clicking continue
    page.click("input#continue")
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(random.randint(1000, 2000))  # Wait for password page
    
    # Check for CAPTCHA after email submission
    solve_if_captcha(page)
    
    # Fill password with human-like typing
    password_input = page.locator("input[name='password']")
    password_input.click()
    page.wait_for_timeout(random.randint(300, 600))
    password_input.fill("")  # Clear first
    page.keyboard.type(password, delay=random.randint(50, 150))  # Type slowly
    
    page.wait_for_timeout(random.randint(500, 1000))  # Pause before submitting
    page.click("input#signInSubmit")
    
    # Wait for page to settle
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(2000)  # Extra wait for redirects
    
    # Check for CAPTCHA after login
    solve_if_captcha(page)
    current_url = page.url.lower()
    
    if "/ap/signin" not in urlparse(current_url).path:
        logging.info("Login successful")
    else:
        logging.warning(f"Still on sign-in page after login. URL: {page.url}")

# Week Label
# def get_week_label(date_str: str = None, start_sunday: bool = False, weeks_to_subtract: int = 0) -> str:
#     """
#     Returns week label in format: 'Week 26, Jun 22-Jun 28'.
    
#     Parameters:
#     - date_str: optional, 'YYYY-MM-DD'. If None, uses current date.
#     - start_sunday: if True, week starts on Sunday; otherwise, Monday.

#     Returns:
#     - formatted string with week number and date range
#     """
#     # Use current date if no string is passed
#     if date_str:
#         date_obj = datetime.strptime(date_str, "%Y-%m-%d").date() - timedelta(weeks=weeks_to_subtract)
#     else:
#         date_obj = datetime.today().date() - timedelta(weeks=weeks_to_subtract)

#     if start_sunday:
#         # Sunday = 0, Saturday = 6
#         weekday = (date_obj.weekday()+ 1) % 7
#     else:
#         # Monday = 0, Sunday = 6
#         weekday = date_obj.weekday()

#     start_of_week = date_obj - timedelta(days=weekday)
#     end_of_week = start_of_week + timedelta(days=6)

#     # ISO week number (Monday-based) or adjusted for Sunday-start if needed
#     week_number = start_of_week.isocalendar()[1]
#     # NEW: For Sunday-start, use the ISO week of the next day (e.g., Monday) in the range
#     if start_sunday:
#         week_number = (start_of_week + timedelta(days=1)).isocalendar()[1]
    
#     # Simplified date formatting, removing redundant block
#     # .replace(' 0', ' ') removes the leading zero for single-digit days (e.g., "Oct 09" -> "Oct 9")
#     start_str = start_of_week.strftime("%b %d").replace(' 0', ' ')
#     end_str = end_of_week.strftime("%b %d").replace(' 0', ' ')

#     return f"Week {week_number}, {start_str}-{end_str}"

def get_week_label(date_str: str = None, start_sunday: bool = False, weeks_to_subtract: int = 0) -> tuple:
    """
    Returns (week_label, year_str).
    week_label format: 'Week 26, Jun 22-Jun 28'.
    year_str: The ISO year or fiscal year classification for that week.
    
    If start_sunday=True:
      - Week starts Sunday
      - Sunday belongs to the previous completed week
    """
    # Use current date if not provided
    if date_str:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d").date() - timedelta(weeks=weeks_to_subtract)
    else:
        date_obj = datetime.today().date() - timedelta(weeks=weeks_to_subtract)

    if start_sunday:
        weekday = (date_obj.weekday() + 1) % 7  # Sunday=0
    else:
        weekday = date_obj.weekday()  # Monday=0

    start_of_week = date_obj - timedelta(days=weekday)

    # If it's Sunday, move to the previous week
    if start_sunday and weekday == 0:
        start_of_week -= timedelta(days=7)

    end_of_week = start_of_week + timedelta(days=6)

    # Use Monday's ISO week/year for consistency
    if start_sunday:
        iso_year, week_number, _ = (start_of_week + timedelta(days=1)).isocalendar()
    else:
        iso_year, week_number, _ = start_of_week.isocalendar()

    # Format without leading zeros (cross-platform)
    def format_day(dt):
        return f"{dt.strftime('%b')} {dt.day}"

    start_str = format_day(start_of_week)
    end_str = format_day(end_of_week)

    return f"Week {week_number}, {start_str}-{end_str}", str(iso_year)


def select_week(page, week_label: str, year: str = None):
    # If year is not provided, fallback to current year (unsafe for boundary weeks but backward compatible)
    if not year:
        year = str(datetime.now().year)
        
    def try_select(label: str, year_context: str) -> bool:
        combo.fill(label)
        # Modified locator to scope under the provided year header using XPath
        option_xpath = f"//span[text()='{year_context}']/following::button[@role='option' and @aria-label='{label}'][1]"
        #option = page.locator(f"[role='option']:has-text('{label}')").first
        option = page.locator(f"xpath={option_xpath}")
        page.wait_for_timeout(500)
        try:
            expect(option).to_be_visible(timeout=30000)
            #option.wait_for(state="visible", timeout=30000)
            page.wait_for_timeout(500)  # Short pause for full interactivity
            option.click(force=True)
            #option.click()
            #option.focus()
            
            #page.keyboard.press('Enter')
            return True
        except Exception as e:
            logging.error(f"Failed to select '{label}' under year '{year_context}': {str(e)}")  # Log the actual exception for debugging
            return False
    logging.info(f"Selecting Week: {week_label} (Year: {year})")
    combo = page.locator("input[role='combobox'][placeholder='Select Week:']")
    combo.click()
    page.wait_for_timeout(500)
    if try_select(week_label, year):
        return
        
    # Check for fallback (Sunday-start)
    week_label_sunday, valid_year_sunday = get_week_label(start_sunday=True)
    
    # If the originally requested week was NOT the default Sunday week (because offsets were used), 
    # then this fallback logic might be slightly flawed if we blindly use get_week_label(start_sunday=True) which defaults to offset=0.
    # However, preserving original logic flow:
    
    logging.info(f"Week '{week_label}' not found, trying Sunday-start week '{week_label_sunday}' (Year: {valid_year_sunday})...")
    combo.click(force=True)
    page.wait_for_load_state('networkidle')
    page.wait_for_timeout(500)

    if not try_select(week_label_sunday, valid_year_sunday):
        logging.error(f"Could not select week: {week_label_sunday}")
        raise Exception(f"Could not select week: {week_label_sunday}")

# Stations
def select_station(page, station: str):
    try:
        logging.info(f"Selecting station: {station}")
    
        # Target the combobox input directly
        combo = page.locator("input[role='combobox'][placeholder='Station:']")
        combo.click()
    
        # Fill with station code
        combo.fill(station)
        page.wait_for_timeout(500)
    
        # Hit Enter to confirm
        page.keyboard.press("Enter")
        page.wait_for_timeout(500)
    except Exception as e:
        logging.error(f"Could not select station '{station}'.")
        logging.error(f"Error details: {e}")
        raise

# Navigation
def navigate_to_reports(page, station="WRDD", week_label="Week 35", year=None):
    """Navigate to Supplementary Performance Reports with station + week filters"""
    logging.info("Navigating to Supplementary Performance Reports...")

    header = page.wait_for_selector(
        "h1:has-text('Supplementary Performance Reports')", timeout=30000
    )
    header.scroll_into_view_if_needed()

    # Select station
    select_station(page, station)

    # Wait for table to reload after station change
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)  # Extra wait for station data to load

    # Select week
    select_week(page, week_label, year)

    # Wait for reports table to load after week selection
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(5000)  # Extra wait for table content to render

    # Wait for the reports container to be visible
    container = page.locator("div[data-testid='weekly-reports-table']")
    try:
        container.wait_for(state="visible", timeout=30000)
        logging.info("Reports table loaded successfully")
    except Exception as e:
        logging.warning(f"Reports table container not found: {e}")
    
    # Return reports container
    return container

# Download & S3
def process_and_upload_to_s3(file_path, 
                             filename, 
                             s3_bucket, 
                             s3_key_prefix, 
                             report_name,
                             zip_password,
                             s3_client,
                             file_type="zip"
                            ):
    """Process file (zip, excel, csv, pdf) and upload to S3"""
    lower_name = filename.lower()
    # Add timestamp to the filename
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Handle PDF files directly if configured
    if file_type == 'pdf' or lower_name.endswith(".pdf"):
         # For PDF, we likely want to keep the original extension
        ext = filename.split('.')[-1]
        s3_file = f"{report_name.replace(' ', '-').lower()}_{timestamp_str}.{ext}"
        
        with open(file_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_bucket, f"{s3_key_prefix}{s3_file}")
        logging.info(f"Uploaded PDF to s3://{s3_bucket}/{s3_key_prefix}{s3_file}")
        return

    # s3_file = report_name.replace(' ', '-').lower() + '.' + ".".join(filename.split('.')[1:])
    # Default extension fallback for constructed names if needed, though most logic below handles specific extensions
    
    if report_name == "Daily Data":
        logging.info(f"Report '{report_name}' detected. Attempting multi-sheet Excel processing...")
        s3_file_base = report_name.replace(' ', '-').lower()

        try:
            # Try to read the file, assuming it's Excel, even if named .csv
            df = pd.read_excel(file_path, sheet_name="SSD Orders Consolidated View", engine="openpyxl")
            
            # Successful read means it was an Excel file (or compatible)
            csv_buffer = BytesIO()
            df.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            s3_csv_file = f"{s3_file_base}.csv"
            s3_client.upload_fileobj(csv_buffer, s3_bucket, f"{s3_key_prefix}{s3_csv_file}")
            # Removed the incorrect interpolation braces { } causing the SyntaxError
            logging.info(f"SSD Orders Consolidated View' and uploaded as CSV to s3://{s3_bucket}/{s3_key_prefix}{s3_csv_file}")
            return
            
        except ValueError as e:
            # ValueError occurs if the target sheet isn't found OR if the file isn't a valid Excel format.
            if "Worksheet named" in str(e) or "File is not a zip file" in str(e):
                logging.warning(f"Failed Excel read for '{filename}'. Falling back to simple CSV/raw upload.")
                # Fall through to the next conditions to handle it as a standard file.
            else:
                logging.error(f"Critical error during Excel processing: {e}")
                raise

    # For other files, we construct name with timestamp
    ext = filename.split('.')[-1]
    s3_file = f"{report_name.replace(' ', '-').lower()}_{timestamp_str}.{ext}"

    # Standard File Type Handlers (Only runs if not "Daily Data" or Excel read failed)
    if lower_name.endswith(".csv"):
        with open(file_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_bucket, f"{s3_key_prefix}{s3_file}")
        logging.info(f"Uploaded CSV to s3://{s3_bucket}/{s3_key_prefix}{s3_file}")
    elif lower_name.endswith((".xls", ".xlsx")):
        with open(file_path, "rb") as f:
            s3_client.upload_fileobj(f, s3_bucket, f"{s3_key_prefix}{s3_file}")
        logging.info(f"Uploaded Excel file to s3://{s3_bucket}/{s3_key_prefix}{s3_file}")
        
    # elif lower_name.endswith((".xls", ".xlsx")):
    #    df = pd.read_excel(file_path, engine="openpyxl")
    #    csv_buffer = BytesIO()
    #    df.to_csv(csv_buffer, index=False)
    #    csv_buffer.seek(0)
    #    csv_name = f"{filename.rsplit('.',1)[0]}.csv"
    #    s3_client.upload_fileobj(csv_buffer, s3_bucket, f"{s3_key_prefix}{s3_file}")
    #    logging.info(f"Converted Excel and uploaded CSV to s3://{s3_bucket}/{s3_key_prefix}{s3_file}")

    elif lower_name.endswith(".zip"):
        with zipfile.ZipFile(file_path) as z:
            for inner_file in z.namelist():
                with z.open(inner_file, pwd=zip_password) as f:
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(inner_file)[1]) as tmp:
                        tmp.write(f.read())
                        tmp_name = tmp.name  # Save path before closing handle
                # File handle is now closed — safe to read on Linux
                process_and_upload_to_s3(tmp_name, 
                                         inner_file, 
                                         s3_bucket, 
                                         s3_key_prefix, 
                                         report_name,
                                         zip_password,
                                         s3_client,
                                         file_type
                                        )
    else:
        logging.error(f"Unsupported file type: {filename}")
        #raise ValueError(f"Unsupported file type: {filename}")
def download_report_and_upload_s3(page, 
                                 container_locator, 
                                 report_name, 
                                 s3_bucket, 
                                 s3_key_prefix,
                                 zip_password,
                                 s3_client,
                                 file_type="zip"
                                ):
    """Download a report via Playwright and process it for S3"""
    try:
        logging.info(f"Downloading report: {report_name}")
        
        # Log all visible text in the reports container for debugging
        try:
            table_text = container_locator.inner_text(timeout=5000)
            logging.info(f"Reports table content: {table_text[:2000]}")
        except Exception:
            logging.warning("Could not read reports table text")
        
        # Try multiple selectors to find the report download link
        report_btn = None
        selectors = [
            f"a:has-text('{report_name}')",
            f"button:has-text('{report_name}')",
            f"*:has-text('{report_name}') >> a",
            f":has-text('{report_name}')",
        ]
        
        for selector in selectors:
            try:
                locator = container_locator.locator(selector).last
                if locator.is_visible(timeout=3000):
                    report_btn = locator
                    logging.info(f"Found report with selector: {selector}")
                    break
            except Exception:
                continue
        
        if not report_btn:
            # Last resort: try finding it on the full page
            logging.warning("Report not found in container, searching full page...")
            try:
                report_btn = page.locator(f"a:has-text('{report_name}')").last
                if not report_btn.is_visible(timeout=3000):
                    report_btn = page.locator(f":has-text('{report_name}')").last
            except Exception:
                pass
        
        if not report_btn:
            raise Exception(f"Could not find download link for '{report_name}'.")

        with page.expect_download(timeout=120000) as download_info:
            report_btn.click()

        download = download_info.value
        filename = download.suggested_filename  

        # Save to a temporary file to read
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            download.save_as(tmp_file.name)
            process_and_upload_to_s3(tmp_file.name, 
                                     filename, 
                                     s3_bucket, 
                                     s3_key_prefix, 
                                     report_name,
                                     zip_password,
                                     s3_client,
                                     file_type
                                    )

    except Exception as e:
        logging.error(f"Could not download or process report '{report_name}'")
        logging.error(f"Error details: {e}")
        raise

def navigate_to_payments(page):
    """Navigate to the Payments section"""
    try:
        logging.info("Navigating to Payments section...")
        # Adjust selector based on actual nav bar text/structure
        page.click("a:has-text('Payments')")
        page.wait_for_load_state("networkidle")
        page.wait_for_timeout(2000) # stabilizing
    except Exception as e:
        logging.error(f"Failed to navigate to Payments: {e}")
        raise

def process_payments(page, 
                     s3_bucket, 
                     s3_folder, 
                     s3_client,
                     amazon_id,
                     station
                    ):
    """
    1. Navigate to Payments
    2. Iterate through each City in the sidebar
    3. Select 'PENDING ACTION' tab
    4. Find rows with 'REVIEW' button
    5. Click REVIEW -> Download PDF -> Upload S3 -> Go Back
    """
    
    from utils.mail_notifier import send_invoice_summary_email
    downloaded_invoices = []
    
    navigate_to_payments(page)

    # 1. Identify Cities
    # Sidebar items usually in a list. Adjust selector as needed.
    # Based on screenshot: Left sidebar with city names.
    # We'll look for a container that likely holds these.
    # Assuming standard structure: div with class or role containing list items.
    # A generic approach: get all text from the sidebar area or finding specific elements.
    
    # Wait for sidebar to populate
    # page.wait_for_selector("div#earning-sidebar", timeout=10000) 
    # Use a broader wait to ensure page load, then look for cities
    page.wait_for_load_state("networkidle")
    page.wait_for_timeout(3000)
    
    # Assuming the sidebar items are distinguishable elements
    city_items = page.locator("div[class*='Sidebar'] div[role='button'], div[class*='sidebar'] div[role='button'], ul li[role='button']")
    count = city_items.count()
    
    if count == 0:
        # Fallback simplistic selector if specific roles aren't used
        # Look for the container first
        logging.info("Trying fallback selector for cities...")
        city_items = page.locator("div:has-text('AHMEDABAD') >> xpath=..").locator("div")
        count = city_items.count()

    logging.info(f"Found {count} potential city/station items.")

    for i in range(count):
        try:
            # Re-locate elements every loop to avoid stale handles
            items = page.locator("div[class*='Sidebar'] div[role='button'], div[class*='sidebar'] div[role='button'], ul li[role='button']")
            if items.count() == 0:
                 items = page.locator("div:has-text('AHMEDABAD') >> xpath=..").locator("div")
            
            city_element = items.nth(i)
            city_name = city_element.inner_text().strip()
            
            # Exclude non-city status items (top tabs)
            excluded_items = [
                "COMPLETED", "TIPS", "PENDING DSP REIMBURSEMENTS OR CHARGEBACKS",
                "PENDING ACTION", "UNDER REVIEW", "READY FOR PAYMENT", "HISTORY"
            ]
            if not city_name or city_name == "" or city_name in excluded_items:
                logging.info(f"Skipping excluded/empty item: {city_name}")
                continue
                
            logging.info(f"Processing City: {city_name}")
            # Use force=True because the list container might technically 'intercept' the pointer event
            city_element.click(force=True)
            
            # Wait for the page to fully reload after city switch.
            # Cities with more data (e.g. Bangalore) take significantly longer.
            page.wait_for_load_state("networkidle", timeout=60000)
            page.wait_for_timeout(2000)
            
            # Wait for any loading spinner/overlay to disappear (common in React/SPA apps)
            spinner_selectors = [
                "[class*='spinner']", "[class*='Spinner']",
                "[class*='loading']", "[class*='Loading']",
                "[class*='loader']", "[class*='Loader']",
                "[role='progressbar']",
            ]
            for spinner_sel in spinner_selectors:
                try:
                    spinner = page.locator(spinner_sel).first
                    if spinner.is_visible(timeout=500):
                        logging.info(f"Waiting for spinner ({spinner_sel}) to disappear...")
                        spinner.wait_for(state="hidden", timeout=30000)
                        page.wait_for_timeout(1000)
                except Exception:
                    pass
            
            # 2. Select 'PENDING ACTION' Tab (Optional)
            try:
                # Some UIs might default to a different tab, so we still try to ensure we are on Pending Action
                tab = page.locator("div[role='tab']:has-text('PENDING ACTION'), li:has-text('PENDING ACTION')")
                if not tab.first.is_visible():
                     tab = page.locator("text='PENDING ACTION'")
                
                if tab.count() > 0 and tab.first.is_visible():
                    tab.first.click(force=True)
                    page.wait_for_load_state("networkidle", timeout=30000)
                    page.wait_for_timeout(2000)
                else:
                    logging.info(f"PENDING ACTION tab not explicitly clickable. Proceeding with current view.")
            except Exception as e:
                logging.info(f"Tab selection skipped: {e}")

            # 3. Process Rows
            # finding rows with 'REVIEW' button
            # We specifically look for the "Action" column having "REVIEW"
            
            # Wait for the table content to fully stabilize after city switch.
            # Use a polling approach: wait until the REVIEW button count is stable
            # for two consecutive checks (prevents reading stale data from previous city).
            review_buttons = page.locator("button:has-text('REVIEW')")
            try:
                review_buttons.first.wait_for(state="visible", timeout=15000)
            except:
                logging.info(f"No 'REVIEW' buttons found after waiting for {city_name}.")
            
            # Stabilization check: ensure the count doesn't change between two polls
            # This catches cases where the DOM is still being replaced (old city -> new city)
            prev_count = review_buttons.count()
            page.wait_for_timeout(2000)
            row_count = review_buttons.count()
            
            # If counts differ, the page was still updating — wait more and re-check
            if prev_count != row_count:
                logging.info(f"Row count changed ({prev_count} -> {row_count}), page still loading. Waiting longer...")
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(3000)
                row_count = review_buttons.count()
            
            if row_count == 0:
                logging.info(f"No pending actions for {city_name}.")
                continue
                
            logging.info(f"Found {row_count} pending invoices in {city_name}.")
            
            # We must iterate by index, but RE-FETCH the list every time because going 'Back' invalidates the DOM handles.
            for j in range(row_count):
                logging.info(f"Processing Invoice {j+1} of {row_count} for {city_name}")
                
                # Re-locate the button list to avoid Stale Element Reference
                btns = page.locator("button:has-text('REVIEW')")
                if btns.count() <= j:
                    logging.warning("Number of rows changed unexpectedly. Stopping this city.")
                    break 
                    
                btn = btns.nth(j)
                
                # Capture Invoice Number and Date directly via JS to avoid Playwright xpath bubbling issues
                try:
                    row_data = btn.evaluate("""
                        (element) => {
                            // Find the closest row element (tr or role=row)
                            let row = element.closest('tr') || element.closest('[role="row"]');
                            
                            // If no row found, ascend manually up to 4 levels
                            if (!row) {
                                let curr = element;
                                for (let i = 0; i < 4; i++) {
                                    curr = curr.parentElement;
                                    if (curr && (curr.tagName === 'TR' || curr.getAttribute('role') === 'row')) {
                                        row = curr;
                                        break;
                                    }
                                }
                            }
                            
                            // Last resort fallback
                            if (!row) row = element.parentElement.parentElement.parentElement;
                            
                            let dateStr = 'unknown_date';
                            let invStr = 'unknown_inv';
                            
                            if (row) {
                                // Extract innerText safely 
                                let text = row.innerText || '';
                                let separators = text.includes('\\t') ? '\\t' : '\\n';
                                let parts = text.split(separators);
                                
                                // If splits didn't work for some reason, use standard spaces
                                if (parts.length < 2) parts = text.split(/[ \\t\\n]+/);
                                
                                // Look for date pattern (e.g. 03/06/2026 or 03/06/26)
                                for (let p of parts) {
                                    let cleanDate = p.trim();
                                    if (cleanDate.includes('/') && cleanDate.length >= 8 && cleanDate.length <= 10) {
                                        dateStr = cleanDate;
                                        break;
                                    }
                                }
                                
                                // Look for invoice pattern
                                for (let p of parts) {
                                    if (p.includes('INV-')) {
                                        let words = p.split(/[ \\t\\n]+/);
                                        for (let w of words) {
                                            if (w.includes('INV-')) {
                                                invStr = w.trim();
                                                break;
                                            }
                                        }
                                        break;
                                    }
                                }
                            }
                            
                            return { inv: invStr, date: dateStr };
                        }
                    """)
                    
                    invoice_number = row_data.get('inv', 'unknown_inv')
                    invoice_date = row_data.get('date', 'unknown_date')
                    
                except Exception as e:
                    invoice_number = "unknown_inv"
                    invoice_date = "unknown_date"
                    logging.warning(f"Failed to parse row via JavaScript: {e}")
                
                logging.info(f"Opening review for {invoice_number} (Date: {invoice_date})...")
                
                # CRITICAL: Get the current blob URL BEFORE clicking (to detect when new one loads)
                previous_blob_url = page.evaluate("""
                    () => {
                        const obj = document.querySelector('object[data^="blob:"], embed[src^="blob:"], iframe[src^="blob:"]');
                        return obj ? (obj.getAttribute('data') || obj.getAttribute('src')) : null;
                    }
                """)
                logging.info(f"Previous blob URL: {previous_blob_url[:50] if previous_blob_url else 'None'}")
                
                # Click the REVIEW button to open the modal/viewer
                btn.click(force=True)
                
                try:
                    import os
                    import base64
                    
                    # Create downloads folder in project directory if it doesn't exist
                    project_dir = os.path.dirname(os.path.abspath(__file__))
                    downloads_dir = os.path.join(project_dir, "downloads")
                    os.makedirs(downloads_dir, exist_ok=True)
                    
                    # Wait for a NEW blob URL to appear (different from previous)
                    logging.info("Waiting for new PDF blob URL to load...")
                    max_wait_seconds = 20
                    poll_interval_ms = 500
                    waited = 0
                    blob_url = None
                    
                    while waited < max_wait_seconds * 1000:
                        page.wait_for_timeout(poll_interval_ms)
                        waited += poll_interval_ms
                        
                        # Get the blob URL from any PDF object in the DOM
                        current_blob_url = page.evaluate("""
                            () => {
                                const obj = document.querySelector('object[data^="blob:"], embed[src^="blob:"], iframe[src^="blob:"]');
                                return obj ? (obj.getAttribute('data') || obj.getAttribute('src')) : null;
                            }
                        """)
                        
                        # Accept if: new URL exists AND (no previous OR different from previous)
                        if current_blob_url and (previous_blob_url is None or current_blob_url != previous_blob_url):
                            blob_url = current_blob_url
                            logging.info(f"New blob URL detected after {waited}ms: {blob_url[:60]}...")
                            break
                    
                    if not blob_url:
                        logging.error(f"Timeout waiting for new blob URL after {max_wait_seconds}s. Same URL still present.")
                        page.screenshot(path=os.path.join(downloads_dir, f"timeout_{invoice_number}.png"))
                        raise Exception("New blob URL did not appear")
                    
                    # Additional wait for PDF to fully render
                    page.wait_for_timeout(1000)
                    
                    # Fetch the blob URL content via JavaScript
                    logging.info("Fetching PDF content from blob URL...")
                    pdf_base64 = page.evaluate("""
                        async (blobUrl) => {
                            try {
                                const response = await fetch(blobUrl);
                                const blob = await response.blob();
                                return new Promise((resolve, reject) => {
                                    const reader = new FileReader();
                                    reader.onloadend = () => {
                                        const base64 = reader.result.split(',')[1];
                                        resolve(base64);
                                    };
                                    reader.onerror = reject;
                                    reader.readAsDataURL(blob);
                                });
                            } catch (e) {
                                console.error('Blob fetch error:', e);
                                return null;
                            }
                        }
                    """, blob_url)
                    
                    if pdf_base64:
                        pdf_data = base64.b64decode(pdf_base64)
                        logging.info(f"Successfully fetched PDF ({len(pdf_data)} bytes)")
                        
                        # Save to downloads folder (No timestamp as requested)
                        local_filename = f"{city_name}_{invoice_number}.pdf"
                        # Clean up any potential bad characters in filename
                        local_filename = local_filename.replace("/", "-").replace("\\", "-")
                        local_path = os.path.join(downloads_dir, local_filename)
                        
                        with open(local_path, "wb") as f:
                            f.write(pdf_data)
                        logging.info(f"Saved to: {local_path}")
                        
                        # Upload to S3
                        s3_key = f"{s3_folder}/{local_filename}"
                        s3_client.put_object(Body=pdf_data, Bucket=s3_bucket, Key=s3_key)
                        logging.info(f"Uploaded: s3://{s3_bucket}/{s3_key}")
                        
                        # Delete local file after successful upload
                        os.remove(local_path)
                        logging.info(f"Deleted local file: {local_path}")
                        
                        # Add to summary list
                        downloaded_invoices.append({
                            "city": city_name,
                            "invoice": invoice_number,
                            "date": invoice_date,
                            "filename": local_filename
                        })
                        
                        # CRITICAL: Revoke the blob URL to invalidate it 
                        # This forces the website to create a new blob for the next invoice
                        logging.info("Revoking blob URL to force fresh PDF for next invoice...")
                        page.evaluate("""
                            (blobUrl) => {
                                URL.revokeObjectURL(blobUrl);
                            }
                        """, blob_url)
                        
                    else:
                        logging.error("Failed to fetch blob content")
                        page.screenshot(path=os.path.join(downloads_dir, f"fetch_error_{invoice_number}.png"))
                        
                except Exception as dl_err:
                    logging.error(f"Failed to download PDF for {invoice_number}: {dl_err}")
                    import traceback
                    logging.error(traceback.format_exc())
                
                # Go Back Strategy - Close the PDF viewer modal
                logging.info("Attempting to close PDF viewer modal...")
                
                modal_closed = False
                
                # APPROACH 1: Click the X button in the top-right corner using JavaScript
                # This targets the visible X button shown in the screenshot
                logging.info("Looking for X button in top-right corner...")
                try:
                    clicked = page.evaluate("""
                        () => {
                            // Find elements that might be the X close button
                            const candidates = document.querySelectorAll('*');
                            for (const el of candidates) {
                                const text = el.textContent.trim();
                                const tagName = el.tagName.toLowerCase();
                                // Look for single X character elements
                                if ((text === 'X' || text === 'x' || text === '×' || text === '✕' || text === '✖') 
                                    && (tagName === 'span' || tagName === 'button' || tagName === 'div' || tagName === 'i' || tagName === 'a')) {
                                    const rect = el.getBoundingClientRect();
                                    // Only click if it's in the top-right area (X > 70% of page width, Y < 100px)
                                    if (rect.right > window.innerWidth * 0.7 && rect.top < 100 && rect.width > 0) {
                                        console.log('Found X button at:', rect.right, rect.top);
                                        el.click();
                                        return true;
                                    }
                                }
                            }
                            return false;
                        }
                    """)
                    if clicked:
                        logging.info("Clicked X button via JavaScript")
                        page.wait_for_timeout(1500)
                        if page.locator("button:has-text('REVIEW')").first.is_visible():
                            modal_closed = True
                            logging.info("Modal closed via X button")
                except Exception as e:
                    logging.warning(f"JS click failed: {e}")
                
                # APPROACH 2: Press Escape key multiple times
                if not modal_closed:
                    for _ in range(3):
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(500)
                    
                    page.wait_for_timeout(1000)
                    
                    # Check if modal is closed (REVIEW button should be visible)
                    if page.locator("button:has-text('REVIEW')").first.is_visible():
                        logging.info("Modal closed via Escape key")
                        modal_closed = True
                
                # APPROACH 2: Click outside the modal (on the darkened overlay)
                if not modal_closed:
                    try:
                        overlay_selectors = [
                            ".modal-backdrop",
                            ".overlay",
                            "div[class*='backdrop']",
                            "div[class*='Overlay']",
                        ]
                        for sel in overlay_selectors:
                            if page.locator(sel).first.is_visible():
                                logging.info(f"Clicking overlay: {sel}")
                                page.locator(sel).first.click(force=True)
                                page.wait_for_timeout(1000)
                                if page.locator("button:has-text('REVIEW')").first.is_visible():
                                    modal_closed = True
                                    break
                    except Exception:
                        pass
                
                # APPROACH 3: Find and click close/back button
                if not modal_closed:
                    close_selectors = [
                        "button[aria-label='Close']",
                        "button[aria-label='close']",
                        "[class*='close']",
                        "[class*='Close']",
                        "button:has-text('×')",
                        "button:has-text('X')",
                        "button:has-text('x')",
                        "i.fa-times",
                        "i.fa-close",
                        "svg[class*='close']",
                        "mat-icon:has-text('close')",
                        "span[class*='close']",
                        # Back button options
                        "button:has-text('Back')",
                        "button:has-text('BACK')",
                        "a:has-text('Back')",
                    ]
                    
                    for sel in close_selectors:
                        try:
                            btn = page.locator(sel).first
                            if btn.is_visible(timeout=500):
                                logging.info(f"Clicking close button: {sel}")
                                btn.click(force=True)
                                page.wait_for_timeout(1000)
                                if page.locator("button:has-text('REVIEW')").first.is_visible():
                                    modal_closed = True
                                    logging.info("Modal closed via close button")
                                    break
                        except Exception:
                            continue
                
                # APPROACH 4: Use browser back button
                if not modal_closed:
                    logging.warning("Trying browser back button...")
                    page.go_back()
                    page.wait_for_timeout(2000)
                    
                    # Check if we are on the invoices list OR Payments page
                    if page.locator("button:has-text('REVIEW')").first.is_visible():
                        modal_closed = True
                        logging.info("Modal closed via browser back")
                
                # APPROACH 5: Force navigate to Payments section
                if not modal_closed:
                    logging.warning("All close attempts failed. Navigating to Payments section...")
                    try:
                        payments_nav = page.locator("a:has-text('Payments'), span:has-text('Payments'), div:has-text('Payments')").first
                        if payments_nav.is_visible():
                            payments_nav.click(force=True)
                            page.wait_for_timeout(2000)
                            modal_closed = True
                    except Exception:
                        pass
                
                # APPROACH 6: Re-click the current city to reset
                if not modal_closed:
                    logging.warning("Forcing city re-click...")
                    city_element = page.locator("div[class*='Sidebar'] div[role='button'], div[class*='sidebar'] div[role='button'], ul li[role='button']").nth(i)
                    city_element.click(force=True)
                    page.wait_for_timeout(2000)
                
                # Wait for page to stabilize
                page.wait_for_timeout(1000)
                logging.info("Close attempt completed. Proceeding to next invoice...")

        except Exception as e:
            logging.error(f"Error processing city {i}: {e}")
            # Try to recover navigation for next city
            navigate_to_payments(page)
            continue
            
    # After all cities are processed, send the summary email
    logging.info(f"Finished processing all cities. Generating summary for {len(downloaded_invoices)} invoices.")
    send_invoice_summary_email(amazon_id, station, downloaded_invoices)
