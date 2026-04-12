import asyncio
import json
import csv
import os
import requests
import datetime
from bs4 import BeautifulSoup
from dbfread import DBF
from playwright.async_api import async_playwright
import traceback

CLERK_PORTAL_URL = "https://www.cclerk.hctx.net/Applications/WebSearch/RP.aspx"
PROPERTY_APPRAISER_BULK_DATA_URL = "[paste URL here]"
LOOKBACK_DAYS = 7

LEAD_TYPES = [
    "LIS PENDENS", "NOTICE OF FORECLOSURE", "TAX DEED", "JUDGMENT", 
    "CERTIFIED JUDGMENT", "DOMESTIC JUDGMENT", "CORP TAX LIEN", "IRS LIEN", 
    "FEDERAL LIEN", "LIEN", "MECHANIC LIEN", "HOA LIEN", "MEDICAID LIEN", 
    "PROBATE DOCUMENTS", "NOTICE OF COMMENCEMENT", "RELEASE LIS PENDENS"
]

def generate_owner_variants(owner_name):
    """
    Build owner name lookup with variants:
    "FIRST LAST", "LAST FIRST", "LAST, FIRST"
    """
    parts = owner_name.split()
    if len(parts) >= 2:
        first = parts[0]
        last = parts[-1]
        return [
            f"{first} {last}".upper(),
            f"{last} {first}".upper(),
            f"{last}, {first}".upper()
        ]
    return [owner_name.upper()]

def download_appraiser_data():
    """
    Download the bulk parcel DBF file from property appraiser.
    Handles potentially 3 retries and possible __doPostBack for ASP.NET downloads.
    """
    print("Downloading property appraiser DBF data...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
    })
    
    for attempt in range(3):
        try:
            if PROPERTY_APPRAISER_BULK_DATA_URL == "[paste URL here]":
                print("Skipping DBF download as URL is a placeholder. Please update the URL.")
                return None
                
            resp = session.get(PROPERTY_APPRAISER_BULK_DATA_URL)
            if resp.status_code == 200:
                # Example of handling __doPostBack:
                # If it's an ASP.NET page requiring a form post to download:
                if "___VIEWSTATE" in resp.text:
                    soup = BeautifulSoup(resp.text, 'lxml')
                    viewstate = soup.find('input', {'name': '__VIEWSTATE'})
                    eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})
                    if viewstate and eventvalidation:
                        post_data = {
                            '__EVENTTARGET': 'ctl00$MainContent$btnDownload', # Example IDs
                            '__EVENTARGUMENT': '',
                            '__VIEWSTATE': viewstate['value'],
                            '__EVENTVALIDATION': eventvalidation['value'],
                        }
                        file_resp = session.post(PROPERTY_APPRAISER_BULK_DATA_URL, data=post_data)
                        content_to_write = file_resp.content
                    else:
                        content_to_write = resp.content
                else:
                    content_to_write = resp.content

                dbf_path = "appraiser_data.dbf"
                with open(dbf_path, "wb") as f:
                    f.write(content_to_write)
                print(f"Downloaded DBF successfully on attempt {attempt+1}")
                return dbf_path
            else:
                print(f"Failed to download DBF. Status Code: {resp.status_code}")
                
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                print("Failed to download DBF data after 3 attempts.")
                return None

def build_owner_lookup(dbf_path):
    """
    Read DBF and build an owner dictionary based on name variants.
    """
    lookup = {}
    if not dbf_path or not os.path.exists(dbf_path):
        return lookup
    print("Building owner lookup from DBF...")
    try:
        table = DBF(dbf_path, load=True)
        for record in table:
            owner = str(record.get('OWNER') or record.get('OWN1', ''))
            site_addr = str(record.get('SITE_ADDR') or record.get('SITEADDR', ''))
            site_city = str(record.get('SITE_CITY', ''))
            site_zip = str(record.get('SITE_ZIP', ''))
            mail_addr = str(record.get('ADDR_1') or record.get('MAILADR1', ''))
            mail_city = str(record.get('CITY') or record.get('MAILCITY', ''))
            state = str(record.get('STATE', ''))
            mail_zip = str(record.get('ZIP') or record.get('MAILZIP', ''))
            
            variants = generate_owner_variants(owner)
            for v in variants:
                lookup[v] = {
                    "prop_address": site_addr,
                    "prop_city": site_city,
                    "prop_state": "TX",
                    "prop_zip": site_zip,
                    "mail_address": mail_addr,
                    "mail_city": mail_city,
                    "mail_state": state,
                    "mail_zip": mail_zip
                }
    except Exception as e:
        print(f"Error reading DBF: {e}")
    return lookup

def calculate_score(record):
    """
    SELLER SCORE (0-100): Base 30, +10/flag, +20 LP+FC combo, +15 amount>$100k, 
    +10 amount>$50k, +5 new this week, +5 has address. 
    Flags: "Lis pendens", "Pre-foreclosure", "Judgment lien", "Tax lien", 
    "Mechanic lien", "Probate / estate", "LLC / corp owner", "New this week".
    """
    score = 30
    flags = []
    
    doc_type = (record.get('doc_type') or '').upper()
    amount_str = str(record.get('amount', '0'))
    
    if "LIS PENDENS" in doc_type:
        flags.append("Lis pendens")
    if "FORECLOSURE" in doc_type:
        flags.append("Pre-foreclosure")
    if "JUDGMENT" in doc_type:
        flags.append("Judgment lien")
    if "TAX DEED" in doc_type or "TAX LIEN" in doc_type:
        flags.append("Tax lien")
    if "MECHANIC" in doc_type:
        flags.append("Mechanic lien")
    if "PROBATE" in doc_type:
        flags.append("Probate / estate")
        
    owner = (record.get('owner') or '').upper()
    if "LLC" in owner or "INC" in owner or "CORP" in owner:
        flags.append("LLC / corp owner")

    filed_date = record.get('filed', '')
    if filed_date:
        try:
            dt = datetime.datetime.strptime(filed_date, "%m/%d/%Y")
            if (datetime.datetime.now() - dt).days <= 7:
                flags.append("New this week")
                score += 5
        except:
            pass

    score += 10 * len(flags)
    
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
        
    try:
        amt = float(amount_str.replace('$', '').replace(',', '').strip())
        if amt > 100000:
            score += 15
        elif amt > 50000:
            score += 10
    except:
        pass
        
    if record.get('prop_address'):
        score += 5
        
    score = min(score, 100)
    record['flags'] = flags
    record['score'] = score
    record['amount'] = amount_str
    
    return record

async def scrape_clerk_portal():
    results = []
    print("Starting Playwright to scrape Harris County Clerk portal...")
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context()
            page = await context.new_page()
            
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(days=LOOKBACK_DAYS)
            
            print(f"Navigating to {CLERK_PORTAL_URL}")
            await page.goto(CLERK_PORTAL_URL, wait_until="networkidle")
            
            # Dismiss Disclaimers if open
            try:
                accept_btn = page.locator("input[value='Accept'], button:has-text('Accept')")
                if await accept_btn.count() > 0:
                    await accept_btn.first.click()
                    await page.wait_for_load_state("networkidle")
            except:
                pass
            
            # Harris County uses Date From and To input fields.
            # Using partial matches common for ASP.NET WebForms. 
            print(f"Setting date: {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}")
            try:
                await page.fill('#ctl00_ContentPlaceHolder1_txtFrom', start_date.strftime('%m/%d/%Y'))
                await page.fill('#ctl00_ContentPlaceHolder1_txtTo', end_date.strftime('%m/%d/%Y'))
            except Exception as e:
                print(f"Warning: Could not set date range fields: {e}")
                
            # Submit Search
            try:
                await page.click('#ctl00_ContentPlaceHolder1_btnSearch')
                await page.wait_for_load_state("networkidle")
            except Exception as e:
                print(f"Warning: Could not compile search click: {e}")

            # Parse Table
            try:
                rows = await page.locator("table tr").all()
                print(f"Found {len(rows)} potential records on page.")
                for row in rows:
                    cells = await row.locator("td").all_inner_texts()
                    if len(cells) >= 6:
                        doc_num = cells[0].strip()
                        doc_type = cells[1].strip()
                        filed_date = cells[2].strip()
                        grantor = cells[3].strip()
                        grantee = cells[4].strip()
                        amount = cells[5].strip() if len(cells) > 5 else "0"
                        
                        legal_desc = ""
                        if len(cells) > 6:
                            legal_desc = cells[6].strip()
                        
                        # Match only requested Lead types if doc_type partially matches
                        if any(lt in doc_type.upper() for lt in LEAD_TYPES):
                            results.append({
                                "doc_num": doc_num,
                                "doc_type": doc_type,
                                "filed": filed_date,
                                "owner": grantor,
                                "grantee": grantee,
                                "amount": amount,
                                "legal": legal_desc,
                                "clerk_url": f"https://www.cclerk.hctx.net/Applications/WebSearch/details.aspx?doc_num={doc_num}"
                            })
            except Exception as e:
                print(f"Warning: Could not parse results table: {e}")

            await browser.close()
    except Exception as e:
        print(f"Playwright error: {e}")
        traceback.print_exc()

    return results

def save_exports(records):
    os.makedirs("data", exist_ok=True)
    os.makedirs("dashboard", exist_ok=True)

    json_payload = {
        "fetched_at": datetime.datetime.now().isoformat(),
        "source": "Harris County Clerk",
        "date_range": f"{LOOKBACK_DAYS} days",
        "total": len(records),
        "with_address": sum(1 for r in records if r.get('prop_address')),
        "records": records
    }

    print("Saving records.json...")
    with open("data/records.json", "w") as f:
        json.dump(json_payload, f, indent=2)
    with open("dashboard/records.json", "w") as f:
        json.dump(json_payload, f, indent=2)

    print("Saving ghl_export.csv...")
    csv_headers = [
        "First Name", "Last Name", "Mailing Address", "Mailing City", 
        "Mailing State", "Mailing Zip", "Property Address", "Property City", 
        "Property State", "Property Zip", "Lead Type", "Document Type", 
        "Date Filed", "Document Number", "Amount/Debt Owed", "Seller Score", 
        "Motivated Seller Flags", "Source", "Public Records URL"
    ]
    with open("data/ghl_export.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(csv_headers)
        for r in records:
            owner_parts = r.get("owner", "").split()
            first_name = owner_parts[0] if len(owner_parts) > 0 else ""
            last_name = owner_parts[-1] if len(owner_parts) > 1 else ""
            flags_str = ", ".join(r.get("flags", []))
            
            writer.writerow([
                first_name, last_name, r.get("mail_address", ""), r.get("mail_city", ""),
                r.get("mail_state", ""), r.get("mail_zip", ""), r.get("prop_address", ""),
                r.get("prop_city", ""), r.get("prop_state", ""), r.get("prop_zip", ""),
                r.get("cat_label", r.get("doc_type", "")), r.get("doc_type", ""),
                r.get("filed", ""), r.get("doc_num", ""), r.get("amount", ""),
                r.get("score", ""), flags_str, "Harris County Clerk", r.get("clerk_url", "")
            ])

async def main():
    dbf_path = download_appraiser_data()
    owner_lookup = build_owner_lookup(dbf_path)
    
    clerk_records = await scrape_clerk_portal()
    
    processed_records = []
    for rec in clerk_records:
        # Categorize
        doc_upper = rec.get("doc_type", "").upper()
        cat = "OTHER"
        for lt in LEAD_TYPES:
            if lt in doc_upper:
                cat = lt
                break
        rec["cat"] = cat
        rec["cat_label"] = cat
        
        # Enrich from Data String 
        owner = rec.get("owner", "").upper()
        if owner in owner_lookup:
            rec.update(owner_lookup[owner])
        else:
            rec.update({
                "prop_address": "", "prop_city": "", "prop_state": "", "prop_zip": "",
                "mail_address": "", "mail_city": "", "mail_state": "", "mail_zip": ""
            })
            
        rec = calculate_score(rec)
        processed_records.append(rec)
        
    save_exports(processed_records)
    print(f"Successfully processed and exported {len(processed_records)} records.")

if __name__ == "__main__":
    asyncio.run(main())
