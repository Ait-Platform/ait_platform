import os

d = r'D:\Users\yeshk\Documents\ait_platform\templates\school_home'

template = """{{% extends "layout.html" %}}
{{% block title %}}Chapter {chapter_id} | HOME™{{% endblock %}}
{{% block content %}}
<div class="max-w-4xl mx-auto bg-white shadow p-8 rounded-xl mt-6">
  <!-- Top Row: Back Button -->
  <div class="mb-6 pb-4 border-b border-gray-100">
    <a href="{{{{ url_for('home_bp.learner_dashboard') }}}}" class="inline-flex items-center text-sm font-medium text-gray-500 hover:text-blue-600 transition-colors">
      <svg class="w-5 h-5 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7"></path></svg>
      Back to Dashboard
    </a>
  </div>

  <h1 class="text-3xl font-bold text-blue-800 mb-6">Chapter {chapter_id}</h1>

  {{% if 'home' in session.get('enrolled_subjects', []) %}}
    <p class="text-gray-700 leading-relaxed mb-4 text-lg">
      Please refer to your HOME™ textbook (hardcopy or digital) to complete this chapter.
    </p>

    <!-- Complete Button -->
    <form action="{{{{ url_for('home_bp.advance_chapter', chapter_id={chapter_id}) }}}}" method="POST" class="mt-10 flex justify-center border-t border-gray-100 pt-8">
      <input type="hidden" name="csrf_token" value="{{{{ csrf_token() }}}}"/>
      <button class="px-8 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700 font-bold shadow-md transition-colors flex items-center">
        <svg class="w-5 h-5 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M5 13l4 4L19 7"></path></svg>
        Mark Complete & Return to Dashboard
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

for i in range(2, 8):
    file_path = os.path.join(d, f'chapter_{i}.html')
    content = template.format(chapter_id=i)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

print("Standardized chapters 2 through 7 with back button and return to dashboard.")
