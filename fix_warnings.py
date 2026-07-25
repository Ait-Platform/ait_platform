import os

# 1. Update talent_dashboard.html
with open('templates/program_culturefire/talent_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

if 'Family-Friendly Policy' not in content:
    warning_html = '''
        <div class="bg-yellow-50 border-l-4 border-yellow-400 p-4 mb-4 rounded shadow-sm">
          <div class="flex items-start">
            <div class="flex-shrink-0">
              <svg class="h-6 w-6 text-yellow-600" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"/>
              </svg>
            </div>
            <div class="ml-3">
              <h3 class="text-sm font-bold text-yellow-800">Strict Family-Friendly Policy & AI Moderation</h3>
              <div class="mt-1 text-sm text-yellow-700">
                <p>Culture Fire is a strict family-friendly platform; <strong>this is NOT an adult site.</strong> All uploaded videos are automatically scanned by our AI safety system. Any videos containing explicit content, pornography, or severe violence will be immediately rejected and your account may be flagged. Additionally, videos are actively policed by the community.</p>
              </div>
            </div>
          </div>
        </div>
'''
    content = content.replace('<!-- Help Modal -->', f'<!-- Help Modal -->\n{warning_html}')
    with open('templates/program_culturefire/talent_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)


# 2. Update ad_dashboard.html
with open('templates/program_culturefire/ad_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

if 'Family-Friendly Policy' not in content:
    warning_html = '''
    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-4 mb-8 rounded shadow-sm">
      <h3 class="text-sm font-bold text-yellow-800">Strict Family-Friendly Policy & AI Moderation</h3>
      <p class="text-sm text-yellow-700 mt-1">Culture Fire is a strict family-friendly platform. All uploaded advertisements are scanned by our AI system. Explicit content, adult material, or violence will be automatically rejected. Ensure your ads are appropriate for all ages.</p>
    </div>
'''
    content = content.replace('<p class="text-gray-600 mb-8">Place your advertisements in upcoming Culture Fire shows. Ads cost 40 tokens per placement.</p>', 
                              f'<p class="text-gray-600 mb-4">Place your advertisements in upcoming Culture Fire shows. Ads cost 40 tokens per placement.</p>\n{warning_html}')
    with open('templates/program_culturefire/ad_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)


# 3. Update mc_dashboard.html
with open('templates/program_culturefire/mc_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

if 'Family-Friendly Policy' not in content:
    warning_html = '''
    <div class="bg-yellow-50 border-l-4 border-yellow-400 p-4 mb-8 rounded shadow-sm">
      <h3 class="text-sm font-bold text-yellow-800">Strict Family-Friendly Policy & AI Moderation</h3>
      <p class="text-sm text-yellow-700 mt-1">Culture Fire is a strict family-friendly platform. All MC video recordings are scanned by our AI system for inappropriate content. Please maintain a professional, family-friendly tone.</p>
    </div>
'''
    content = content.replace('<div class="grid grid-cols-1 lg:grid-cols-2 gap-8">', 
                              f'{warning_html}\n  <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">')
    with open('templates/program_culturefire/mc_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)

print("Done")
