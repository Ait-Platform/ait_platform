import os

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'

template = """{{% extends "layout.html" %}}
{{% block title %}}Chapter {chapter_id} | HOME™{{% endblock %}}
{{% block content %}}
<div class="max-w-3xl mx-auto bg-white shadow p-6 rounded-xl mt-6">
  <h1 class="text-2xl font-bold text-blue-700 mb-4">Chapter {chapter_id}</h1>

  {{% if 'home' in session.get('enrolled_subjects', []) %}}
    <p class="text-gray-700 leading-relaxed mb-4">
      Please refer to your HOME™ textbook (hardcopy or digital) to complete this chapter.
    </p>

    <form action="{{{{ url_for('home_bp.advance_chapter', chapter_id={chapter_id}) }}}}" method="POST" class="mt-6">
      <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}"/>
      <button class="px-6 py-2 bg-green-600 text-white rounded hover:bg-green-700 font-semibold">
        {button_text}
      </button>
    </form>

  {{% else %}}
    <div class="p-4 bg-yellow-100 text-yellow-800 border border-yellow-300 rounded">
      ⚠️ This content is restricted.<br>
      You must be enrolled in <strong>HOME™</strong> to begin learning.<br>
      <a href="{{{{ url_for('home_bp.subject_home') }}}}" class="underline text-blue-600 hover:text-blue-800">Visit the School of Mathematics</a> to enroll.
    </div>
  {{% endif %}}
</div>
{{% endblock %}}
"""

for i in range(1, 8):
    file_path = os.path.join(d, f'chapter_{i}.html')
    if i < 6:
        btn_text = f"▶ Continue to Chapter {i+1}"
    elif i == 6:
        btn_text = "▶ Complete Section 1"
    else:
        btn_text = "▶ Continue"
        
    content = template.format(chapter_id=i, button_text=btn_text)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

print("Standardized chapters 1 through 7 with generic textbook placeholder.")
