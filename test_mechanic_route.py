from app import create_app
app = create_app()
app.config['TESTING'] = True

with app.test_client() as client:
    response = client.get('/mechanic/price')
    print(f'Status Code: {response.status_code}')
    if response.status_code == 200:
        print('Successfully rendered price page locally!')
    else:
        print('Error rendering price page!')
        print(response.data.decode('utf-8'))
