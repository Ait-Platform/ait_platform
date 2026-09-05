import urllib.request
try:
    response = urllib.request.urlopen("https://ait.mathwithhands.com/sace/reading/simulator")
    html = response.read().decode('utf-8')
    print(html)
except Exception as e:
    print("Error:", e)
