import subprocess

def checkout_blob(revision, path, out_path):
    cmd = ['git', 'show', f'{revision}:{path}']
    result = subprocess.run(cmd, capture_output=True)
    with open(out_path, 'wb') as f:
        f.write(result.stdout)

checkout_blob('6d541b1', 'templates/program_sace/simulator.html', 'templates/program_sace/simulator.html')
checkout_blob('HEAD', 'templates/program_sace/simulator.html', 'templates/program_sace/presentation_ppp.html')
