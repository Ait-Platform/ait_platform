import json
import os

log_file = r'C:\Users\Sanjith\.gemini\antigravity\brain\1271e0e4-bf05-4b78-9e25-90ddb68d514e\.system_generated\logs\transcript.jsonl'

def extract_files():
    found_html = False
    found_routes = False

    with open(log_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if data.get('type') == 'TOOL_RESPONSE':
                    output = str(data.get('output', ''))
                    
                    # Extract manager_dashboard.html
                    if not found_html and 'manager_dashboard.html' in output and '{% extends "layout.html" %}' in output:
                        print("Found dashboard candidate.")
                        lines = output.split('\n')
                        html_lines = []
                        recording = False
                        for l in lines:
                            if '{% extends "layout.html" %}' in l and l.startswith('1:'):
                                recording = True
                            if recording:
                                parts = l.split(': ', 1)
                                if len(parts) == 2 and parts[0].isdigit():
                                    html_lines.append(parts[1])
                                else:
                                    html_lines.append(l)
                        
                        if html_lines:
                            with open('extracted_dashboard.html', 'w', encoding='utf-8') as out:
                                out.write('\n'.join(html_lines))
                            print('Extracted manager_dashboard.html!')
                            found_html = True
                            
                    # Extract routes.py
                    if not found_routes and 'app/program_billing/routes.py' in output and 'from flask import' in output:
                        print("Found routes candidate.")
                        lines = output.split('\n')
                        py_lines = []
                        recording = False
                        for l in lines:
                            if l.startswith('1: ') and 'from flask import' in l:
                                recording = True
                            if recording:
                                parts = l.split(': ', 1)
                                if len(parts) == 2 and parts[0].isdigit():
                                    py_lines.append(parts[1])
                                else:
                                    py_lines.append(l)
                                    
                        if py_lines:
                            with open('extracted_routes.py', 'w', encoding='utf-8') as out:
                                out.write('\n'.join(py_lines))
                            print('Extracted routes.py!')
                            found_routes = True
                            
                if found_html and found_routes:
                    break
            except Exception as e:
                pass

if __name__ == '__main__':
    extract_files()
