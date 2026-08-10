import glob

hero_image_html = '''
        <!-- Chapter Image -->
        {% if hero_image %}
        <div class="mb-8">
            <img
                src="{{ url_for('static', filename='images/' + hero_image) }}"
                alt="{{ chapter.title }}"
                class="w-full rounded-xl shadow border"
            >
        </div>
        {% endif %}
'''

for file_path in glob.glob('templates/subject_home/chapter*_practical.html'):
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
        
    if 'hero_image' in content:
        continue # Already updated
        
    objective_idx = content.find('<!-- Objective -->')
    if objective_idx != -1:
        content = content[:objective_idx] + hero_image_html + content[objective_idx:]
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f'Updated {file_path}')
    else:
        # If no objective block, just put it after the header block
        header_end = content.find('</div>', content.find('<!-- Header -->')) + 6
        content = content[:header_end] + "\n" + hero_image_html + content[header_end:]
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f'Updated {file_path} (after Header)')
