import os, traceback
from flask import Flask, send_from_directory

app = Flask(__name__)

@app.route('/uploads/<path:filename>')
def serve(filename):
    print(filename)
    return send_from_directory('app/static/uploads', filename)

if __name__ == '__main__':
    client = app.test_client()
    res = client.get('/uploads/cfi/test.mp4')
    print(res.status_code)
