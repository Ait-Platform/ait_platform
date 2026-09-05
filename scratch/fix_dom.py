import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Extract app-view-10 and app-view-11
pattern_10_11 = re.compile(r'<!-- App View 10: Final Assessment -->.*?<!-- Floating SACE Guide Button -->', re.DOTALL)
match = pattern_10_11.search(content)

extracted_views = ""
if match:
    # Everything from <!-- App View 10... up to just before <!-- Floating...
    extracted_views = match.group(0).replace('<!-- Floating SACE Guide Button -->', '')
    # Remove it from its current bad location (leaving the Floating SACE guide button)
    content = content.replace(match.group(0), '<!-- Floating SACE Guide Button -->')

# 2. Fix the app-view-1 bad leftover HTML
bad_html = '''                        <div class="inline-flex items-center bg-indigo-50 text-indigo-700 px-4 py-2 rounded-full text-sm font-bold border border-indigo-100 shadow-sm">
                            <i class="fas fa-sync-alt fa-spin mr-2"></i> Waiting for next activity...
                        </div>
                    </div>
                        <p class="text-slate-600">Please direct your attention to the projector screen.</p>
                    </div>'''

good_html = '''                        <div class="inline-flex items-center bg-indigo-50 text-indigo-700 px-4 py-2 rounded-full text-sm font-bold border border-indigo-100 shadow-sm">
                            <i class="fas fa-sync-alt fa-spin mr-2"></i> Waiting for next activity...
                        </div>
                    </div>'''
content = content.replace(bad_html, good_html)

# 3. Inject app-view-10 and 11 inside the main container before the script block
# The main container ends right before <script>
pattern_end_container = re.compile(r'    </div>\s*<script>')
match_end = pattern_end_container.search(content)
if match_end:
    insert_pos = match_end.start()
    content = content[:insert_pos] + extracted_views + content[insert_pos:]

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
