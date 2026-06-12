import os
import re

DIR = r"D:\Users\yeshk\Documents\ait_platform\templates\program_budget"

files = [
    "account_detail.html",
    "account_edit.html",
    "account_new.html",
    "billing.html",
    "dashboard.html",
    "group_types.html",
    "help.html",
    "import.html",
    "ledger.html",
    "ledger_edit.html",
    "report_by_group.html",
    "report_income_expense.html",
]

for filename in files:
    filepath = os.path.join(DIR, filename)
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # We want to find the first <h1> block and the immediately following <div class="flex..."> or <a> Back button
    # Because regex parsing HTML is fragile, we'll manually use replace_file_content in the agent if this script is too complex.
    
    # Actually, a simple approach:
    # Look for:
    # <h1 class="...">Title</h1>
    # <div class="flex items-center ..."> ... </div>
    
    # We can use a regex to match the first <h1> tag and the first <div> or <a> tag that looks like a button container
    
    # Let's write a robust regex to find the header section.
    # We'll match from <!-- Header --> to the end of the action div if possible, but some templates don't have <!-- Header -->
    
    # For now, let's not risk corrupting the templates with a bad regex.
