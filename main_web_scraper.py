import sys
import logging
import os
from datetime import datetime
from sqlalchemy.orm import sessionmaker

# Playwright and local imports
from playwright.sync_api import sync_playwright
from Extract.extract_processor import get_s3_client
from conf.config import engine, amazon_config, webscraper_details
from scraper import (
    get_table_data, 
    ensure_logged_in, 
    get_week_label, 
    navigate_to_reports, 
    download_report_and_upload_s3, 
    insert_audit,
    launch_stealth_browser
)

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO, 
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # 1. Initialize Database Session
    SessionFactory = sessionmaker(bind=engine)
    session = SessionFactory()
    
    # 2. Extract Arguments and Database Config
    try:
        amazon_id_arg = sys.argv[1] if len(sys.argv) > 1 else None
        if not amazon_id_arg:
            raise ValueError("No Amazon ID provided as a command line argument.")

        # Fetch configurations from DB
        amazon_entry = get_table_data(session, amazon_config, id=amazon_id_arg, filter_active=True)
        if not amazon_entry:
            raise ValueError(f"No active configuration found for ID: {amazon_id_arg}")

        logger.info(f"Loaded config: {amazon_entry}")

        webscraper_details_id = amazon_entry["webscraper_details_id"]
        web_scraper_entry = get_table_data(session, webscraper_details, id=webscraper_details_id)
        
        # Mapping variables for easier access
        user_data_dir = amazon_entry["user_data_dir"]
        # Use per-account session file to prevent different accounts from overwriting each other
        storage_state = f"/tmp/amazon_session_{webscraper_details_id}.json"
        amazon_id = amazon_entry["id"]
        station = amazon_entry["station"]
        report_name = amazon_entry["report_name"]
        s3_bucket = amazon_entry["s3_bucket"]
        s3_folder = amazon_entry["s3_folder"]
        source_db_id = amazon_entry["source_db_id"]
        zip_password = web_scraper_entry["zip_password"].encode("utf-8")
        weeks_offset = amazon_entry.get("weeks_offset", 0) or 0
        file_type = amazon_entry.get("file_type", "zip")
        download_method = amazon_entry.get("download_method", "report")

    except Exception as e:
        logger.error(f"Initialization Error: {e}")
        session.close()
        # Exit code 1 ensures Airflow marks this as a failure immediately
        sys.exit(1)

    # 3. Start Web Scraping Process
    start_time = datetime.now()
    status = "success"
    context = None

    with sync_playwright() as p:
        try:
            # Use stealth browser to avoid CAPTCHA detection on Linux
            browser, context, page = launch_stealth_browser(
                p, 
                headless=True, 
                storage_state=storage_state
            )

            # Ensure session is valid (passes context so session can be saved after re-login)
            ensure_logged_in(page, context, web_scraper_entry, storage_state)
            
            # Determine target week
            print(f"Weeks Offset: {weeks_offset}")
            week_label, week_year = get_week_label(weeks_to_subtract=weeks_offset, start_sunday=True)
            
            logger.info(f"Processing {amazon_id} | {station} | {report_name} for week {week_label} (Year: {week_year})")
            
            # Navigation
            if download_method == 'invoice':
                logging.info(f"Download method is '{download_method}' - Skipping standard navigation")
                container_locator = None 
            else:
                container_locator = navigate_to_reports(
                    page, station=station, week_label=week_label, year=week_year
                )
            
            # S3 Client and File Handling
            s3_client = get_s3_client(source_db_id)

            if download_method == 'invoice':
                 # New logic for Payments
                 from scraper import process_payments
                 process_payments(
                    page,
                    s3_bucket,
                    s3_folder,
                    s3_client,
                    amazon_id,
                    station
                 )
            else:
                download_report_and_upload_s3(
                    page, 
                    container_locator, 
                    report_name, 
                    s3_bucket, 
                    s3_folder,
                    zip_password,
                    s3_client,
                    file_type
                )

            page.wait_for_timeout(500)
            browser.close()

        except Exception as e:
            status = "failed"
            logger.error(f"Error processing {amazon_id} | {station} | {report_name}: {e}")
        
        finally:
            if context:
                context.close()
            # 4. Audit Trail and Cleanup
            end_time = datetime.now()
            time_taken = (end_time - start_time).total_seconds()
            
            # Record result in the database (even if failed)
            try:
                insert_audit(session, amazon_id, station, report_name, status, time_taken)
            except Exception as audit_err:
                logger.error(f"Failed to insert audit log: {audit_err}")
            
            # Close resources
            session.close()
            logger.info(f"Process finished with status: {status}. Time taken: {time_taken}s")

            if status == "failed":
                logger.error("Process failed, exiting with code 1.")
                sys.exit(1)
            else:
                sys.exit(0)
