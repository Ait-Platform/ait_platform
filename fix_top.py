import os, re

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for i in range(1, 7):
    p = os.path.join(d, f'chapter_{i}.html')
    if not os.path.exists(p): continue
    
    with open(p, 'r', encoding='utf-8') as f:
        c = f.read()
    
    # 1. Remove the old bottom button block
    bottom_btn_pattern = re.compile(r'\s*<div class="mt-10 border-t border-gray-100 pt-6 text-right">[\s\S]*?</form>\s*</div>')
    c = bottom_btn_pattern.sub('', c)
    
    # 2. Extract the chapter title text
    title_match = re.search(r'<h1 class="text-3xl font-bold text-blue-800">(.*?)</h1>', c)
    if not title_match:
        title_match = re.search(r'<h1 class="text-2xl font-bold text-blue-800">(.*?)</h1>', c)
        
    title_text = title_match.group(1) if title_match else f"Chapter {i}"
    
    # 3. Build the new top row
    new_top = f"""  <!-- Top Row: Title, Back, & Finished -->
  <div class="flex flex-row justify-between items-center mb-6 pb-4 border-b border-gray-100">
    <div class="flex-1 text-left">
      <h1 class="text-2xl font-bold text-blue-800">{title_text}</h1>
    </div>
    
    <div class="flex-1 text-center">
      <a href="{{{{ url_for('home_bp.learner_dashboard') }}}}" class="text-sm font-bold text-gray-500 hover:text-blue-600 transition-colors">
        ~ Back to Dashboard
      </a>
    </div>

    <div class="flex-1 text-right">
      <form action="{{{{ url_for('home_bp.advance_chapter', chapter_id={i}) }}}}" method="POST" class="m-0 inline-block">
        <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}"/>
        <button type="submit" class="inline-flex items-center px-5 py-2 border border-transparent text-sm font-semibold rounded-lg shadow-sm text-white bg-emerald-600 hover:bg-emerald-700 transition-colors">
          Mark Finished
        </button>
      </form>
    </div>
  </div>"""

    # 4. Replace the old top row
    old_top_pattern = re.compile(r'<!-- Top Row: Title & Back Button -->[\s\S]*?</div>\s*</div>')
    
    # The regex needs to carefully match the top row. The existing block is:
    # <!-- Top Row: Title & Back Button -->
    # <div class="flex flex-row justify-between items-center mb-6 pb-4 border-b border-gray-100">
    #   <h1 ...>...</h1>
    #   <a ...>...</a>
    # </div>
    
    old_top_pattern2 = re.compile(r'<!-- Top Row: Title & Back Button -->\s*<div class="flex flex-row justify-between items-center mb-6 pb-4 border-b border-gray-100">\s*<h1 class="text-3xl font-bold text-blue-800">.*?</h1>\s*<a href="\{\{ url_for\(\'home_bp\.learner_dashboard\'\) \}\}" class="text-sm font-medium text-gray-500 hover:text-blue-600 transition-colors">\s*Back\s*</a>\s*</div>', re.DOTALL)

    if old_top_pattern2.search(c):
        c = old_top_pattern2.sub(new_top, c)
    else:
        print(f"Warning: Could not match top row in chapter_{i}.html")
        continue

    with open(p, 'w', encoding='utf-8') as f:
        f.write(c)
    print(f'Updated chapter_{i}.html')
