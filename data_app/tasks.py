from celery import shared_task
import pymongo
import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials



@shared_task
def update_mongo_data():
    """Fetch latest data and update MongoDB collections."""
    
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["local"] 

    collections = [
        "amazon_sales",
        "cloudwarehouse_charts",
        "expense_IIGF",
        "internation_sale_report",
        "march_2021",
        "may_2022",
        "sale_report"
    ]

    for collection_name in collections:
        collection = db[collection_name]
        collection.update_many({}, {"$set": {"last_updated": datetime.datetime.utcnow()}}, upsert=True)

    return "MongoDB data updated successfully"


@shared_task
def update_google_sheets():
    """Update business KPIs in Google Sheets."""
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

   
    sheet = client.open("Business KPIs").sheet1

    
    sheet.append_row(["MongoDB Data Updated", "Success", str(datetime.datetime.utcnow())])

    return "Google Sheets updated successfully"

