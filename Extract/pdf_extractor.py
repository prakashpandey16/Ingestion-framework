import pdfplumber
import re
import os


def derive_type_from_description(description):
    """Derive a clean 'type' value from an invoice description by stripping
    temporal references (month names, year suffixes, date patterns),
    currency/numeric noise (INR amounts), and lowercasing.

    Examples:
        'CP Payout Dec-25'                     -> 'cp payout'
        'BuyBack Reimbursement March'           -> 'buyback reimbursement'
        'variablepayINR25.5'                    -> 'variablepay'
        'Paysquare Reimbursement Sep-25 Payroll'-> 'paysquare reimbursement payroll'
        'BGC Reimbursement for Nov-25'          -> 'bgc reimbursement'
        'Forward Leg inspection Jan and Feb\\'26' -> 'forward leg inspection'
    """
    if not description:
        return ""

    text = str(description).strip()

    # 0. Pre-processing: Insert spaces before embedded month names and 'for'
    #    when glued to other words (e.g. "BuybackforNov'25" -> "Buyback for Nov'25")
    #    Also handles "BlocksJan'26" -> "Blocks Jan'26"
    month_names = r'(?:January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)'
    # Insert space before month name when preceded by a letter
    text = re.sub(r'(?<=[a-zA-Z])(' + month_names + r')', r' \1', text, flags=re.IGNORECASE)
    # Insert space before 'for' when glued to preceding word (e.g. "Buybackfor" -> "Buyback for")
    text = re.sub(r'(?<=[a-zA-Z])(for)(?=[A-Z\s])', r' \1 ', text, flags=re.IGNORECASE)

    # 1. Remove "INR" followed by any digits/dots (e.g. INR25.5, INR6, INR25.525Jan28Feb)
    text = re.sub(r'INR[\d.]+', '', text)

    # 2. Remove embedded month+number fragments left after INR removal
    #    e.g. "Jan28Feb" or "25Jan28Feb" -> ""
    #    Pattern: optional digits + month + optional digits + optional month
    text = re.sub(
        r'\d*(?:' + month_names + r')\d*(?:' + month_names + r')?\d*',
        '', text, flags=re.IGNORECASE
    )

    # 3. Remove date patterns with month abbreviations and year:
    #    Dec-25, DEC-2025, Jan'26, Feb'26, Nov-25, Oct'25, Mar'26, etc.
    text = re.sub(
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)['\u2019'`\-]\s*\d{2,4}",
        '', text, flags=re.IGNORECASE
    )

    # 4. Remove month-year patterns like "Dec-2025", "DEC 2025"
    text = re.sub(
        r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[\s\-]*\d{4}",
        '', text, flags=re.IGNORECASE
    )

    # 5. Remove standalone full/abbreviated month names (case-insensitive, whole word)
    #    Must come AFTER the combined month-year patterns above
    months_word_pattern = (
        r'\b(?:January|February|March|April|May|June|July|August|September|October|November|December'
        r'|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\b'
    )
    text = re.sub(months_word_pattern, '', text, flags=re.IGNORECASE)

    # 6. Remove standalone numbers and numeric suffixes (e.g. "25", "28", "2025")
    text = re.sub(r'\b\d+\b', '', text)

    # 7. Remove dangling connectors "for" and "and" that were linking to dates
    text = re.sub(r'\bfor\s*$', '', text, flags=re.IGNORECASE)
    text = re.sub(r'^\s*for\b', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\bfor\b(?=\s*$)', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\band\b', '', text, flags=re.IGNORECASE)

    # 8. Remove orphaned punctuation that was part of date patterns
    text = re.sub(r"['\u2019'`\-]+(?=\s|$)", '', text)
    text = re.sub(r"(?:^|\s)['\u2019'`\-]+", ' ', text)

    # 9. Remove "for" if it's at the end after cleanup
    text = re.sub(r'\bfor\s*$', '', text.strip(), flags=re.IGNORECASE)

    # 10. Normalize whitespace and lowercase
    text = re.sub(r'\s+', ' ', text).strip().lower()

    return text


def clean_numeric(value):
    """Clean a raw string value for numeric DB columns.
    
    Handles:
      - Commas: '11,604.34' -> '11604.34'
      - 'INR' prefix: 'INR 500' -> '500'
      - '%' suffix: '18%' -> '18'
      - Spaced negatives: '- 11604.34' -> '-11604.34'
      - Newlines in cells: '9\\n+9' -> '18' (CGST+SGST split)
      - Empty/whitespace-only strings -> None (NULL in DB)
    """
    if value is None:
        return None
    # Strip newlines first (PDF cells can span multiple lines)
    text = str(value).replace("\n", "").replace("\r", "")
    text = text.replace(",", "").replace("INR", "").replace("%", "").strip()
    if not text:
        return None
    # Fix spaced negatives: '- 510.00' -> '-510.00'
    text = re.sub(r'^-\s+', '-', text)
    # Handle additive expressions like '9+9' (CGST+SGST split tax rates)
    if '+' in text:
        try:
            parts = [float(p.strip()) for p in text.split('+') if p.strip()]
            text = str(sum(parts))
            # Clean up .0 for whole numbers (e.g. 18.0 -> 18)
            if text.endswith('.0'):
                text = text[:-2]
        except ValueError:
            pass  # Not a valid additive expression, keep as-is
    return text


def extract_invoice_data(pdf_path):
    extracted_data = []
    
    with pdfplumber.open(pdf_path) as pdf:
        page = pdf.pages[0]
        text = page.extract_text()
        
        # --- Header Extraction ---
        # Invoice Number
        # Looks for "Invoice Number - INV-..."
        invoice_match = re.search(r"Invoice Number\s*-\s*([A-Z0-9-]+)", text)
        invoice_number = invoice_match.group(1).strip() if invoice_match else None
        
        # Station
        # Text layout: "Station:\nAmazon ... Ltd IXZA"
        # We look for "Station:" then maybe some text/newlines, then a 4 letter code at specific position or end of line.
        # Heuristic: Look for "Station:" and then find the first 3-4 letter uppercase code that looks like a station code in the subsequent text.
        # Or specifically typical station codes are 3-4 letters.
        # Let's try matching the line logic seen in debug.
        # "Station:" is followed by "Amazon ... IXZA" on the next line.
        # We can try to grab the first word that looks like a station code after "Station:" and some context.
        # Actually, simpler: The station code `IXZA` is at the end of the line following `Station:`.
        station_match = re.search(r"Station:\s*\n.*?\s([A-Z]{3,4})$", text, re.MULTILINE)
        station_code = station_match.group(1).strip() if station_match else None
        
        # Fallback for Station if regex extraction fails (sometimes layout varies)
        if not station_code:
            # Try searching just for the pattern "Station:\s*([A-Z0-9]+)" in case it is on same line
            s_match = re.search(r"Station:\s*([A-Z0-9]+)", text)
            if s_match:
                station_code = s_match.group(1)

        # Month
        # "For Services made during:\n... 01-Dec-2025 to 31-Dec-2025"
        # Use DOTALL to match across lines
        date_range_match = re.search(r"For Services made during:.*?to\s*(\d{2}-([A-Za-z]{3}-\d{4}))", text, re.DOTALL)
        month_str = ""
        if date_range_match:
            full_date = date_range_match.group(1) # 31-Dec-2025
            parts = full_date.split('-')
            if len(parts) == 3:
                month_str = f"{parts[1]}-{parts[2][-2:]}" # Dec-25
        
        # --- Table Extraction ---
        tables = page.extract_tables()
        
        for table in tables:
            if not table: continue
            
            current_section = None # "taxable" or "commercial"
            
            for row in table:
                # Filter None to check content preserving order
                clean_row = [x for x in row if x]
                if not clean_row: continue
                
                clean_row_str = [str(x) for x in clean_row]
                clean_row_lower = [x.lower() for x in clean_row_str]
                first_col_str = str(clean_row_str[0])
                
                # --- State Changes ---
                
                # Check for Taxable Transactions headers
                # Must contain Description and Base Amount. Must NOT contain "Date" (to avoid Transaction Details table)
                if "description" in clean_row_lower and "base amount" in clean_row_lower and "date" not in clean_row_lower:
                    current_section = "taxable"
                    continue # Skip header
                
                # Check for Commercial Transactions headers
                # Screenshot shows: Description | Qty | Rate | Amount. 
                # It does NOT have Base Amount.
                elif "description" in clean_row_lower and "rate" in clean_row_lower and "amount" in clean_row_lower:
                    if "base amount" not in clean_row_lower and "date" not in clean_row_lower:
                        current_section = "commercial"
                        continue # Skip header
                
                # Check for "Transaction Details" header (to ensure we don't treat it as taxable/commercial mismatch)
                elif "date" in clean_row_lower and "description" in clean_row_lower:
                    current_section = None # Ignore this section
                    continue

                # Check for Totals/Summary sections which end the current data block
                # Use word-boundary matching for "total" to avoid false positives on
                # descriptions like "totalpayoutxpt" where "total" is a prefix, not a word.
                first_lower = first_col_str.lower()
                is_total_row = (
                    "sub total" in first_lower
                    or re.match(r'\btotal\b', first_lower)          # "Total Amount ...", "Total GST ..."
                    or first_lower.startswith("summary of")
                    or first_lower.startswith("total payment amount")
                )
                if is_total_row:
                    current_section = None
                    continue

                # --- Data Extraction ---
                
                row_dict = {}
                
                if current_section == "taxable":
                    # Expecting 7 columns: Description, Qty, Rate, Base Amount, Tax Amount, Tax Rate, Amount
                    if len(clean_row) >= 7:
                        desc_value = clean_row[0]
                        row_dict = {
                            "Month": month_str,
                            "Station Code": station_code,
                            "Description": desc_value,
                            "Type": derive_type_from_description(desc_value),
                            "Qty": clean_numeric(clean_row[1]),
                            "Rate": clean_numeric(clean_row[2]),
                            "Base Amount": clean_numeric(clean_row[3]),
                            "Tax Amount": clean_numeric(clean_row[4]),
                            "Tax Rate": clean_numeric(clean_row[5]),
                            "Amount": clean_numeric(clean_row[6]),
                            "Invoice Number": invoice_number
                        }
                        
                elif current_section == "commercial":
                    # Expecting 4 columns: Description, Qty, Rate, Amount
                    if len(clean_row) >= 4:
                        # Commercial rows: Description, Qty, Rate, Amount
                        # Sometimes clean_row might have 4 items.
                        # Position 3 is Amount.
                        amount_val = clean_row[-1]
                        if len(clean_row) == 4:
                            amount_val = clean_row[3]

                        desc_value = clean_row[0]
                        row_dict = {
                            "Month": month_str,
                            "Station Code": station_code,
                            "Description": desc_value,
                            "Type": derive_type_from_description(desc_value),
                            "Qty": clean_numeric(clean_row[1]),
                            "Rate": clean_numeric(clean_row[2]),
                            "Base Amount": None,
                            "Tax Amount": None,
                            "Tax Rate": None,
                            "Amount": clean_numeric(amount_val),
                            "Invoice Number": invoice_number
                        }
                
                if row_dict:
                    extracted_data.append(row_dict)

    return extracted_data

def extract_performance_data(pdf_path):
    extracted_data = []
    filename = os.path.basename(pdf_path)
    current_zone = "Unknown"
    
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # extract_words gives us precise coordinates
            # Use tighter y_tolerance to prevent merging characters from different lines if they are somewhat overlapping (less likely here but safe)
            words = page.extract_words(x_tolerance=3, y_tolerance=2)
            
            # Sort by top (y) then x
            words.sort(key=lambda w: (int(w['top']), w['x0']))
            
            # Group into rows
            rows = []
            if not words: continue
            
            current_row = [words[0]]
            current_top = words[0]['top']
            
            for w in words[1:]:
                # Check if same line (allow small tolerance)
                # The line spacing is approx 4.5 units. We must use a threshold smaller than that.
                # 2.0 works well to group words on same line but separate new lines.
                if abs(w['top'] - current_top) < 2:
                    current_row.append(w)
                else:
                    rows.append(current_row)
                    current_row = [w]
                    current_top = w['top']
            rows.append(current_row)
            
            # Scan rows to find "Zone" headers and Data rows
            # Also collect all X-coordinates for data columns to determine grid
            
            data_rows_raw = [] 
            
            # We first verify if a row is a data row (starts with number)
            # and collect its words.
            
            for row in rows:
                # Pre-process row to split merged percentage words
                # Example: word '94.24%100.00%' -> '94.24%' at x0, '100.00%' at x0+width/2
                new_row = []
                for w in row:
                    text = w['text']
                    # Regex to find multiple percentages in one string
                    # e.g. "94.24%100.00%"
                    split_matches = [m for m in re.finditer(r"(\d+(?:\.\d+)?%)", text)]
                    
                    if len(split_matches) > 1 and len("".join(m.group(0) for m in split_matches)) == len(text):
                        # Calculate roughly where each part should be
                        total_width = w['x1'] - w['x0']
                        total_len = len(text)
                        char_width = total_width / total_len if total_len > 0 else 0
                        
                        current_offset = 0
                        for match in split_matches:
                            part_text = match.group(0)
                            part_len = len(part_text)
                            
                            # Create a new word dict resembling pdfplumber output
                            part_x0 = w['x0'] + (current_offset * char_width)
                            part_x1 = part_x0 + (part_len * char_width)
                            
                            new_word = {
                                'text': part_text,
                                'x0': part_x0,
                                'x1': part_x1,
                                'top': w['top'],
                                'bottom': w['bottom']
                            }
                            new_row.append(new_word)
                            current_offset += part_len
                    else:
                        new_row.append(w)
                
                row = new_row
                text_line = " ".join([w['text'] for w in row])
                
                # Zone Detection
                if "North Zone" in text_line:
                    current_zone = "North Zone"
                elif "South Zone" in text_line:
                    current_zone = "South Zone"
                elif "East Zone" in text_line:
                    current_zone = "East Zone"
                elif "West Zone" in text_line:
                    current_zone = "West Zone"

                # Check for Data Row: Starts with digit followed by station code
                # Pattern: "1 KNLF ..."
                # We check the first token
                if row and row[0]['text'].isdigit():
                    # Check if second token looks like station text (not number)
                    if len(row) > 1 and not re.match(r"^\d+(\.\d+)?%?$", row[1]['text']):
                         data_rows_raw.append((current_zone, row))

            if not data_rows_raw:
                continue

            # --- Column Detection ---
            # We need to find the "canonical" X positions for the metric columns.
            # We'll look at all words in data_rows_raw starting from the 4th token (after S.No, Station, City)
            # actually usually Station/City takes horizontal space.
            # Metric columns start after the City.
            
            # Let's collect all x0 for words that look like metrics (percentages or 0.00)
            # metric_x_coords = []
            # for _, row_words in data_rows_raw:
            #     for w in row_words:
            #         # Heuristic: Metric columns usually contain digits
            #         if re.search(r"\d", w['text']):
            #             # Exclude S.No (usually first item, x < 60)
            #             if w['x0'] > 110: # Based on debug output, metrics start around 120
            #                 metric_x_coords.append(w['x0'])
            
            # # Cluster X coords to find columns
            # # Simple clustering: sort and group if diff < threshold
            # metric_x_coords.sort()
            # columns = []
            # if metric_x_coords:
            #     current_cluster = [metric_x_coords[0]]
            #     for x in metric_x_coords[1:]:
            #         if x - current_cluster[-1] < 10: # 10 units tolerance for same column
            #             current_cluster.append(x)
            #         else:
            #             columns.append(sum(current_cluster)/len(current_cluster))
            #             current_cluster = [x]
            #     columns.append(sum(current_cluster)/len(current_cluster))
            
            # We use hardcoded X-centroids derived from debug analysis to ensure robust mapping,
            # especially when some columns are empty or missing in specific rows.
            # These correspond to the 21 metric columns.
            
            columns = [
                126, 146, 167, 186, 206, 226, 245, 265, 287, 311, 
                335, 358, 381, 403, 425, 446, 465, 483, 502, 519, 536
            ]
            
            # (Dynamic clustering logic removed in favor of hardcoded centroids)
 

            # --- Header Extraction ---
            # User provided a screenshot of the specific headers for this report.
            # We will use these hardcoded headers to ensure accuracy.
            
            hardcoded_metric_headers = [
                "AFN Premium LMC DEA", "AFN Standard LMC DEA", "MFN Premium LMC DEA", "MFN Standard LMC DEA",
                "AFN Premium DoT", "AFN Standard DoT", "MFN Premium DoT", "MFN Standard DoT",
                "DOT Premium (AFN+MFN)", "DOT Standard (AFN+MFN)", "Premium (AFN+MFN) DDS", 
                "Standard (AFN+MFN) DDS", "Good Scan Non-Del", "Good Scan Not Picked", 
                "Slot Adherence for SMD 2.0", "LM CPS", "LM RCPS", "Open COD (>7 Days)", 
                "%DNR within 48 hrs", "DSR", "PSR"
            ]
            
            # Map detected columns to these headers
            # We assume the columns detected correspond 1:1 to these headers in order.
            # If we detect fewer columns, we'll map what we have.
            # If we detect more, we'll truncate or map to Extra_X.
            
            column_names = hardcoded_metric_headers
            
            # Now extract data aligned to these columns
            for zone, row in data_rows_raw: # Iterate through the already processed data_rows
                
                # 1. Parse Metadata (S.No, Station, City)
                # These are the first few tokens.
                # S.No is row[0]
                s_no = row[0]['text']
                
                # Station/City words are between S.No and the first metric column (x > 110)
                name_words = [w['text'] for w in row[1:] if w['x0'] < 110]
                
                station = ""
                city = ""
                
                if name_words:
                    # Heuristic same as before
                    # Join and check
                    full_name = "".join(name_words)
                    if len(name_words) == 1:
                        if len(full_name) > 4: # Merged
                            station = full_name[:4]
                            city = full_name[4:]
                        else:
                            station = full_name
                            city = ""
                    else:
                        station = name_words[0]
                        city = " ".join(name_words[1:])
                
                row_data = {
                    "Zone": zone,
                    # S.No excluded as requested
                    "Station": station,
                    "City": city
                }
                
                # 2. Map Metrics to Columns
                # We look for words that fall into the column buckets
                
                for i, col_x in enumerate(columns):
                    # Find word in this row near col_x
                    found_word = None
                    col_name = column_names[i]
                    
                    for w in row:
                        if abs(w['x0'] - col_x) < 10: # Tolerance
                            text = w['text']
                            found_word = text
                            break
                    
                    # Fill value or empty
                    val = found_word if found_word else ""
                    row_data[col_name] = val
                
                extracted_data.append(row_data)

    return extracted_data


def extract_airtel_bill_data(pdf_path):
    """
    Extract structured data from Airtel Fixedline and Broadband Services PDF bills.
    
    Parses Page 1 for:
      - Billing name, address, pincode (left panel)
      - Fixedline number, Broadband ID, Relationship number, Bill number,
        Bill date, Bill period, Pay by date, Credit limit, Security deposit,
        GST/UIN number, Alternate mobile no (right panel)
      - Account summary (previous balance, payments, this month's charges, amount due)
      - Charge breakdown (rentals, taxes, total)
    
    Returns a list containing a single dict with all extracted fields.
    """
    extracted = {}
    filename = os.path.basename(pdf_path)
    extracted['source_file_name'] = filename

    with pdfplumber.open(pdf_path) as pdf:
        if not pdf.pages:
            return []

        page = pdf.pages[0]
        text = page.extract_text() or ""

        # ── Right Panel: Key-Value pairs ──────────────────────────────
        patterns = {
            'fixedline_number':     r"Fixedline\s+number\s+([\d]+)",
            'broadband_id':         r"Broadband\s+ID\s+([\w_]+)",
            'relationship_number':  r"Relationship\s+number\s+([\d]+)",
            'bill_number':          r"Bill\s+number\s+([A-Za-z0-9]+)",
            'bill_date':            r"Bill\s+date\s+([\d]{2}-[A-Za-z]{3}-[\d]{4})",
            'pay_by_date':          r"Pay\s+by\s+date\s+(.+?)(?:\n|$)",
            'credit_limit':         r"Credit\s+limit\s+([\d,]+\.?\d*)",
            'security_deposit':     r"Security\s+deposit\s+([\d,]+\.?\d*)",
            'gst_uin_number':       r"GST\s+No/UIN\s+No\s+([A-Z0-9]+)",
            'alternate_mobile_no':  r"Alternate\s+mobile\s+no\s+([\d]+)",
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = match.group(1).strip()
                # Clean numeric fields
                if field in ('credit_limit', 'security_deposit'):
                    value = value.replace(',', '')
                extracted[field] = value
            else:
                extracted[field] = None

        # Bill period (two dates)
        period_match = re.search(
            r"Bill\s+period\s+([\d]{2}-[A-Za-z]{3}-[\d]{4})\s+to\s+([\d]{2}-[A-Za-z]{3}-[\d]{4})",
            text, re.IGNORECASE
        )
        if period_match:
            extracted['bill_period_start'] = period_match.group(1).strip()
            extracted['bill_period_end'] = period_match.group(2).strip()
        else:
            extracted['bill_period_start'] = None
            extracted['bill_period_end'] = None

        # ── Left Panel: Billing Name & Address ────────────────────────
        # The billing name and address sit inside the left box.
        # Strategy: extract text between known boundaries.
        # Name is typically the first line(s) before address keywords.
        # Address follows, ending at the pincode line.

        # Try to find the block between the header area and the barcode/landmark area
        left_block_match = re.search(
            r"Tax\s+Invoice\s*\n(.*?)(?:Landmark|Ship\s+To\s+State)",
            text, re.DOTALL | re.IGNORECASE
        )
        if left_block_match:
            block = left_block_match.group(1).strip()
            lines = [l.strip() for l in block.split('\n') if l.strip()]

            # Pincode: find the 6-digit number
            pincode = None
            pincode_line_idx = None
            for i, line in enumerate(lines):
                pin_match = re.search(r'\b(\d{6})\b', line)
                if pin_match:
                    pincode = pin_match.group(1)
                    pincode_line_idx = i
                    break

            extracted['pincode'] = pincode

            # Billing name: typically the first 1-2 lines (company name)
            # Heuristic: lines before the first line containing common address words
            address_keywords = ['floor', 'road', 'street', 'nh ', 'near', 'ground', 'block',
                                'plot', 'sector', 'lane', 'nagar', 'colony', 'home']
            name_lines = []
            address_start_idx = 0
            for i, line in enumerate(lines):
                if any(kw in line.lower() for kw in address_keywords):
                    address_start_idx = i
                    break
                name_lines.append(line)
            else:
                # If no address keyword found, first line is name, rest is address
                address_start_idx = 1 if len(lines) > 1 else 0

            # If no name lines captured via keywords, use first line
            if not name_lines and lines:
                name_lines = [lines[0]]
                address_start_idx = 1

            extracted['billing_name'] = ' '.join(name_lines).strip() if name_lines else None

            # Address: from address_start_idx to pincode line (inclusive)
            end_idx = (pincode_line_idx + 1) if pincode_line_idx is not None else len(lines)
            address_lines = lines[address_start_idx:end_idx]
            extracted['billing_address'] = ' '.join(address_lines).strip() if address_lines else None
        else:
            extracted['billing_name'] = None
            extracted['billing_address'] = None
            extracted['pincode'] = None

        # State code & Place of supply (from barcode area)
        state_code_match = re.search(r"Ship\s+To\s+State\s+Code\s*:\s*(\d+)", text, re.IGNORECASE)
        extracted['state_code'] = state_code_match.group(1).strip() if state_code_match else None

        place_match = re.search(r"Place\s+of\s+Supply\s*:\s*(.+?)(?:\n|$)", text, re.IGNORECASE)
        extracted['place_of_supply'] = place_match.group(1).strip() if place_match else None

        # ── Account Summary ───────────────────────────────────────────
        prev_bal_match = re.search(r"Previous\s+balance\s+([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['previous_balance'] = prev_bal_match.group(1).replace(',', '') if prev_bal_match else None

        payments_match = re.search(r"Payments\s+[-]?\s*([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['payments'] = payments_match.group(1).replace(',', '') if payments_match else None

        month_charges_match = re.search(r"This\s+month.?s?\s+charges\s+[+]?\s*([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['this_month_charges'] = month_charges_match.group(1).replace(',', '') if month_charges_match else None

        # Amount due — value typically follows "=" sign
        amount_due_match = re.search(r"=\s*([\d,]+\.?\d*)", text)
        extracted['amount_due'] = amount_due_match.group(1).replace(',', '') if amount_due_match else None

        # Due date (same as pay_by_date, from the red "immediately" text area)
        extracted['due_date'] = extracted.get('pay_by_date')

        # ── Charge Breakdown ──────────────────────────────────────────
        rentals_match = re.search(r"Rentals\s+([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['rentals'] = rentals_match.group(1).replace(',', '') if rentals_match else None

        taxes_match = re.search(r"Taxes\s+([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['taxes'] = taxes_match.group(1).replace(',', '') if taxes_match else None

        total_match = re.search(r"Total\s*\(.*?\)\s+([\d,]+\.?\d*)", text, re.IGNORECASE)
        extracted['total_charges'] = total_match.group(1).replace(',', '') if total_match else None

        # ── Page 2: Rental From/To Dates ──────────────────────────────
        extracted['rental_from_date'] = None
        extracted['rental_to_date'] = None

        if len(pdf.pages) > 1:
            page2 = pdf.pages[1]
            text2 = page2.extract_text() or ""

            # Look for date patterns (dd/mm/yyyy) in the Rentals detail section
            # Pattern: "Contract Based ARP..." followed by from_date and to_date
            rental_dates_match = re.search(
                r"(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})",
                text2
            )
            if rental_dates_match:
                extracted['rental_from_date'] = rental_dates_match.group(1)
                extracted['rental_to_date'] = rental_dates_match.group(2)

    return [extracted]
