import urllib.request
import time
import multiprocessing

def run_app():
    from app import create_app
    app = create_app()
    app.run(port=5000, use_reloader=False)

if __name__ == '__main__':
    p = multiprocessing.Process(target=run_app)
    p.start()
    time.sleep(2)
    
    try:
        urllib.request.urlopen('http://127.0.0.1:5000/auto_login')
        from app import create_app
        app = create_app()
        with app.app_context():
            u = __import__('app').models.billing.BilSectionalUnit.query.filter_by(property_id=8).first()
            tenant_id = u.tenants[0].id
        
        req = urllib.request.Request(f'http://127.0.0.1:5000/billing/metsoa/{tenant_id}/2026-05')
        html = urllib.request.urlopen(req).read().decode('utf-8')
        
        # Save HTML to a file so we can view it
        with open('debug_metsoa.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("Wrote debug_metsoa.html")
    except Exception as e:
        print("Error:", e)
    finally:
        p.terminate()
