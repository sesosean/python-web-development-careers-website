import gspread
import requests
import openai
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
import time
from datetime import datetime, timedelta, timezone
import logging

# Set up logging
log_filename = f"automate_blog_post_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(filename=log_filename, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger().addHandler(logging.StreamHandler())

# Load environment variables
load_dotenv()

# API Keys
SERVICE_ACCOUNT_FILE = os.getenv("SERVICE_ACCOUNT_FILE")
POP_API_KEY = os.getenv("POP_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
SHEET_URL = os.getenv("SHEET_URL")

# Google API Setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/documents"
]
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
client = gspread.authorize(creds)
sheet = client.open_by_url(SHEET_URL).sheet1

# Fetch next keyword
rows = sheet.get_all_records()
keyword, row_number = None, None

for index, row in enumerate(rows, start=2):
    if not row["Status"] or row["Status"].lower() == "pending":
        keyword, row_number = row["Keyword"], index
        break

if not keyword:
    logging.info("No available keywords. Exiting.")
    exit()

logging.info(f"📌 Selected Keyword: {keyword}")

# Run POP Report
pop_payload = {
    "apiKey": POP_API_KEY,
    "keyword": keyword,
    "locationName": "United Kingdom",
    "targetUrl": "https://example.com",
    "targetLanguage": "english"
}

logging.info("🚀 Initiating Page Optimizer Pro (POP) report generation...")
response = requests.post("https://app.pageoptimizer.pro/api/expose/get-terms/", json=pop_payload)

try:
    pop_data = response.json()
    if pop_data.get("status") != "SUCCESS":
        logging.error(f"❌ POP API Error: {pop_data.get('msg')}")
        exit()
except requests.exceptions.JSONDecodeError:
    logging.error("❌ POP API response is invalid. Exiting.")
    exit()

task_id = pop_data.get("taskId")
start_time = datetime.now(timezone.utc)

# Check task status with improved timeout handling
logging.info("🔄 Checking POP task status...")

timeout_minutes = 20  # Allow up to 20 minutes for processing
timeout_time = start_time + timedelta(minutes=timeout_minutes)

task_complete = False
previous_status = None
unknown_count = 0  # Counter for repeated "Unknown, Progress: 0%" responses
max_unknown_retries = 8  # Allow up to 8 retries for "Unknown"
last_valid_progress = 0  # Track highest progress seen

while not task_complete:
    # Check if timeout is reached
    if datetime.now(timezone.utc) > timeout_time:
        logging.error("❗ Timeout reached! Exiting POP status check.")
        exit()

    # Fetch task status
    task_response = requests.get(f"https://app.pageoptimizer.pro/api/task/{task_id}/results/")
    try:
        task_data = task_response.json()
    except requests.exceptions.JSONDecodeError:
        logging.error("❌ Invalid JSON response from POP API.")
        exit()

    # Log the full response for debugging purposes
    logging.debug(f"🔍 Full POP API response: {task_response.text}")

    # Extract status and progress
    current_status = task_data.get("msg", "Unknown")
    progress = task_data.get("value", 0)

    # Save highest valid progress
    if progress > last_valid_progress:
        last_valid_progress = progress

    # Handle repeated "Unknown" status
    if current_status == "Unknown" and progress == 0:
        unknown_count += 1

        # If progress was already high, extend retries
        if last_valid_progress >= 50 and unknown_count < max_unknown_retries:
            logging.warning(f"⚠️ Unknown response received, but last progress was {last_valid_progress}%. Extending wait...")
            time.sleep(60)  # Wait 60 seconds before retrying
            continue  # Skip the rest of this loop iteration

        # If max retries reached, force one last check before exiting
        if unknown_count >= max_unknown_retries:
            logging.error("❗ POP API status is stuck at 'Unknown' after multiple retries. Checking one last time before exiting.")
            final_response = requests.get(f"https://app.pageoptimizer.pro/api/task/{task_id}/results/")
            try:
                final_data = final_response.json()
                if final_data.get("status") == "SUCCESS" and final_data.get("value", 0) == 100:
                    task_complete = True
                    pop_score = final_data.get("pageScore", "N/A")
                    optimized_content = final_data.get("cleanedContentBrief", {}).get("content", "")
                    logging.info(f"✅ POP Report Ready! Score: {pop_score}")
                    break  # Exit loop successfully
                else:
                    logging.error("❌ Final check failed. Exiting.")
                    logging.debug(f"Final POP API response: {final_response.text}")
                    exit()
            except requests.exceptions.JSONDecodeError:
                logging.error("❌ Invalid JSON response from POP API on final check. Exiting.")
                exit()

        logging.warning(f"⚠️ Received 'Unknown, Progress: 0%'. Retrying ({unknown_count}/{max_unknown_retries})...")
        time.sleep(30)  # Wait longer before retrying
        continue  # Skip the rest of this loop iteration

    # Reset "Unknown" counter if we get a valid status update
    unknown_count = 0  

    # Log status only when it changes
    if current_status != previous_status:
        logging.info(f"Task Status: {current_status}, Progress: {progress}%")
        previous_status = current_status

    # Check if task is complete
    if task_data.get("status") == "SUCCESS" and progress == 100:
        task_complete = True
        pop_score = task_data.get("pageScore", "N/A")
        optimized_content = task_data.get("cleanedContentBrief", {}).get("content", "")
        logging.info(f"✅ POP Report Ready! Score: {pop_score}")
    else:
        time.sleep(20)  # Reduce API load with a longer delay

# Proceed to OpenAI content generation
logging.info("✍️ Generating content with OpenAI...")
openai.api_key = OPENAI_API_KEY

response = openai.ChatCompletion.create(
    model="gpt-4",
    messages=[
        {"role": "system", "content": "You are a British skincare expert."},
        {"role": "user", "content": f"Write an engaging, informative blog post about '{keyword}' with at least 80% Page Optimizer Pro (POP) optimization. Ensure it follows Google's content guidelines and is written in British English."}
    ]
)

blog_content = response.choices[0].message["content"]
logging.info("✅ Content generated successfully!")

# Save content to Google Docs
logging.info("📄 Saving content to Google Docs...")
docs_service = build("docs", "v1", credentials=creds)
doc = docs_service.documents().create(body={"title": keyword}).execute()
doc_id = doc["documentId"]
docs_service.documents().batchUpdate(documentId=doc_id, body={"requests": [{"insertText": {"location": {"index": 1}, "text": blog_content}}]}).execute()

doc_url = f"https://docs.google.com/document/d/{doc_id}"
logging.info(f"📄 Google Doc Created: {doc_url}")

# Update Google Sheet
sheet.update(f"C{row_number}", "Ready for review")
sheet.update(f"D{row_number}", doc_url)
sheet.update(f"E{row_number}", pop_score)

logging.info("✅ Google Sheet updated!")
logging.info(f"🎉 Blog post for '{keyword}' is ready in Google Docs!")