import sys
from jinja2 import Environment, FileSystemLoader

try:
    env = Environment(loader=FileSystemLoader('templates'))
    template = env.get_template('program_sace/simulator.html')
    # Mock url_for
    env.globals['url_for'] = lambda endpoint, **kwargs: f"/{endpoint}"
    # We might need to mock csrf_token if it's extended
    env.globals['csrf_token'] = lambda: "fake-csrf"
    
    html = template.render()
    print("Rendered OK. Length:", len(html))
    for line in html.split('\n'):
        if 'facilitatorIframe' in line:
            print(line.strip())
except Exception as e:
    print("Error rendering:", e)
