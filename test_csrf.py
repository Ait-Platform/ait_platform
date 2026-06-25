from wsgi import app
with app.test_client() as client:
    resp = client.get('/forgot')
    print("GET /forgot Status:", resp.status_code)
    html = resp.data.decode('utf-8')
    import re
    token = re.search(r'name="csrf_token" value="([^"]+)"', html)
    print("CSRF Token in HTML:", token.group(1) if token else "Not found")
    
    # Now try to post
    resp2 = client.post('/forgot', data={'email': 'test@test.com', 'csrf_token': token.group(1) if token else ''})
    print("POST /forgot Status:", resp2.status_code)
    if resp2.status_code == 400:
        print("Error content:", resp2.data.decode('utf-8')[:200])
