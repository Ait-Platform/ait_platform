import re

with open('templates/admin/settings.html', 'r', encoding='utf-8') as f:
    content = f.read()

# We need to extract the form for Module Visibility & Environment
# It starts with <!-- Module Visibility & Environment -->
start_idx = content.find('<!-- Module Visibility & Environment -->')
end_idx = content.find('</form>', start_idx) + len('</form>')

if start_idx != -1 and end_idx != -1:
    modules_form = content[start_idx:end_idx]
    
    # Remove it from settings.html
    new_settings_content = content[:start_idx] + content[end_idx:]
    with open('templates/admin/settings.html', 'w', encoding='utf-8') as f:
        f.write(new_settings_content)
        
    # Make sure modules_form posts to admin_bp.modules_control
    modules_form = modules_form.replace("url_for('admin_bp.global_settings')", "url_for('admin_bp.modules_control')")
    
    # Create modules_control.html
    modules_page = f"""{{% extends "layout.html" %}}
{{% block title %}}Admin - Platform Modules Control{{% endblock %}}
{{% block flashes %}}{{% endblock %}}

{{% block content %}}
<div class="p-10 max-w-5xl mx-auto">
  <!-- Standardized Tile -->
  <div class="bg-white rounded-xl border border-slate-200 shadow-sm overflow-hidden">
    <!-- Top color strip -->
    <div class="h-2 w-full bg-gradient-to-r from-orange-500 via-amber-500 to-yellow-500"></div>
    
    <!-- Row 1: Header -->
    <div class="px-8 py-6 border-b border-slate-100 flex justify-between items-center bg-slate-50">
      <div>
        <h1 class="text-2xl font-extrabold text-slate-900 tracking-tight">Platform Modules Control</h1>
        <p class="text-sm text-slate-500 mt-1">Manage visibility on the Welcome page and Yoco Payment mode for each program.</p>
      </div>
      <a href="{{{{ url_for('admin_bp.subject_dashboard', subject='admin_secure') }}}}" class="inline-flex items-center text-sm font-medium text-slate-500 hover:text-slate-800 transition bg-white hover:bg-slate-50 px-4 py-2 rounded-lg border border-slate-200 shadow-sm">
        <svg class="w-4 h-4 mr-1.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M10 19l-7-7m0 0l7-7m-7 7h18"></path></svg>
        Back to Secure Setup
      </a>
    </div>

    <!-- Inner Content -->
    <div class="p-8">
      {{% with messages = get_flashed_messages(with_categories=true) %}}
        {{% if messages %}}
          {{% for category, message in messages %}}
            <div class="mb-6 rounded-lg p-4 {{% if category == 'error' %}}bg-red-50 text-red-800 border border-red-200{{% else %}}bg-green-50 text-green-800 border border-green-200{{% endif %}}">
              {{{{ message }}}}
            </div>
          {{% endfor %}}
        {{% endif %}}
      {{% endwith %}}
      
      {modules_form}
    </div>
  </div>
</div>
{{% endblock %}}
"""
    with open('templates/admin/modules_control.html', 'w', encoding='utf-8') as f:
        f.write(modules_page)
    print("Success")
else:
    print("Could not find Module Visibility block in settings.html")
