import urllib.request
try:
    req = urllib.request.Request("https://ait-platform.onrender.com")
    html = urllib.request.urlopen(req).read().decode('utf-8')
    print("Fetched site.")
except Exception as e:
    print(e)
