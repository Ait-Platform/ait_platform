import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix 1: Clean up the leftover paragraph and early closing div from app-view-1
bad_leftover = '''                        <p class="text-slate-600">Please direct your attention to the projector screen.</p>
                    </div>'''
content = content.replace(bad_leftover, '')

# Fix 2: Move App View 10 and 11 to before the script tag
# First, extract App View 10 and 11
pattern = re.compile(r'<!-- App View 10: Final Assessment -->.*?<!-- App View 11: Results / Certificate Status -->.*?</div>\s*</div>', re.DOTALL)
match = pattern.search(content)

if match:
    extracted_views = match.group(0)
    # Remove from current location (bottom of file)
    content = content.replace(extracted_views, '')
    
    # Wait, the extracted_views might have matched too far. Let's be precise.
