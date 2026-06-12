import os

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'
for i in range(1, 7):
    p = os.path.join(d, f'chapter_{i}.html')
    if os.path.exists(p):
        with open(p, 'r', encoding='utf-8') as f:
            c = f.read()
        
        btn = f"""
    <div class="mt-10 border-t border-gray-100 pt-6 text-right">
      <form action="{{{{ url_for('home_bp.advance_chapter', chapter_id={i}) }}}}" method="POST">
        <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}"/>
        <button type="submit" class="inline-flex items-center px-6 py-3 border border-transparent text-base font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 transition-colors">
          Mark Chapter {i} as Finished
        </button>
      </form>
    </div>
"""
        # Replace only if it hasn't been added yet
        if 'Mark Chapter' not in c:
            c = c.replace('{% else %}', btn + '\n  {% else %}')
            with open(p, 'w', encoding='utf-8') as f:
                f.write(c)
            print(f'Updated chapter_{i}.html')
