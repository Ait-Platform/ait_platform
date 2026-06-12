import os

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'

template = """{{% extends "layout.html" %}}
{{% block title %}}Chapter {chapter_id} | HOME™{{% endblock %}}
{{% block content %}}
<div class="max-w-4xl mx-auto bg-white shadow p-8 rounded-xl mt-6">
  <!-- Top Row: Title & Back Button -->
  <div class="flex flex-row justify-between items-center mb-6 pb-4 border-b border-gray-100">
    <h1 class="text-3xl font-bold text-blue-800">Chapter {chapter_id}</h1>
    <a href="{{{{ url_for('home_bp.learner_dashboard') }}}}" class="text-sm font-medium text-gray-500 hover:text-blue-600 transition-colors">
      Back
    </a>
  </div>

  {{% if 'home' in session.get('enrolled_subjects', []) %}}
    <p class="text-gray-700 leading-relaxed mb-4 text-lg">
      Please refer to your HOME™ textbook (hardcopy or digital) to complete this chapter.
    </p>

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

for i in range(2, 8):
    file_path = os.path.join(d, f'chapter_{i}.html')
    content = template.format(chapter_id=i)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

print("Standardized chapters 2 through 7 with new top row back button and removed submit button.")
