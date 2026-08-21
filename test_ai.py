import urllib.request
import json

try:
    url = "https://ait.mathwithhands.com/upload_business_card"
    req = urllib.request.Request(url, method="POST")
    req.add_header("Content-Type", "multipart/form-data")
    with urllib.request.urlopen(req) as response:
        print(response.read().decode('utf-8'))
except urllib.error.HTTPError as e:
    print(f"HTTP Error: {e.code} - {e.reason}")
    print(e.read().decode('utf-8'))
except Exception as e:
    print(f"Error: {e}")
