import re

with open('templates/program_mechanic/job_cards_list.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Extract the array population
population_regex = r"(\s*{% set pending = \[\] %}.*?{% endfor %}\s*)"
match = re.search(population_regex, content, re.DOTALL)
if match:
    pop_block = match.group(1)
    # Remove it from its current position
    content = content.replace(pop_block, "\n")
    
    # 2. Extract the Tabs Navigation
    tabs_regex = r"(\s*<!-- Tabs Navigation \(Styled as Buttons\) -->.*?</div>\s*<div id=\"tab-content-pending\")"
    tmatch = re.search(tabs_regex, content, re.DOTALL)
    if tmatch:
        tabs_block = tmatch.group(1).replace('<div id="tab-content-pending"', '')
        # Remove it from current position
        content = content.replace(tabs_block, "\n")
        
        # 3. Find where to insert both (below the back button row)
        insert_marker = r'(<div class="flex items-center justify-between border-b border-slate-100 px-6 pb-4">.*?</div>)'
        
        insertion = pop_block + tabs_block
        
        content = re.sub(insert_marker, r'\1\n<div class="px-6">\n' + insertion + '\n</div>\n', content, flags=re.DOTALL)
        
        with open('templates/program_mechanic/job_cards_list.html', 'w', encoding='utf-8') as f:
            f.write(content)
        print("Moved tabs successfully.")
    else:
        print("Tabs block not found.")
else:
    print("Population block not found.")
