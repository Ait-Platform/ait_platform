from bs4 import BeautifulSoup
import js2py

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')
scripts = soup.find_all('script')
for idx, script in enumerate(scripts):
    js_code = script.string
    if js_code:
        try:
            # We can't really execute it without DOM, but we can compile it
            compile(js_code, f'script_{idx}.js', 'exec')
            print(f"Script {idx} compiled successfully.")
        except Exception as e:
            print(f"Script {idx} error: {e}")
