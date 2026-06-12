import os

live_results_content = """{% extends "layout.html" %}

{% block title %}Live Results Selection{% endblock %}

{% block content %}
<div class="bg-white shadow rounded-lg overflow-hidden">
  
  <!-- Header strip -->
  <div class="h-2 bg-blue-600"></div>
  
  <!-- Row 1: Header -->
  <div class="px-6 py-4 flex items-center justify-between border-b">
    <h1 class="text-xl font-semibold">Live Results Selection</h1>
    <a href="{{ url_for('cultural_bp.live_showcase_dashboard') }}" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">
      Back
    </a>
  </div>

  <div class="p-6">
    <p class="text-gray-600 mb-6">Select a show below to view its live voting spreadsheet.</p>

    {% if shows %}
      <div class="grid grid-cols-1 gap-4">
        {% for show in shows %}
        <div class="border border-gray-200 rounded-lg p-4 flex flex-col md:flex-row justify-between items-center hover:bg-gray-50 transition">
          <div class="mb-2 md:mb-0">
            <h2 class="text-lg font-bold text-gray-800">{{ show.title }}</h2>
            <p class="text-sm text-gray-500">{{ show.category_item.name if show.category_item else 'General' }}</p>
          </div>
          <a href="{{ url_for('cultural_bp.pageant_results', show_id=show.id) }}" class="bg-yellow-500 text-white px-4 py-2 rounded hover:bg-yellow-600 font-medium">
            View Results
          </a>
        </div>
        {% endfor %}
      </div>
    {% else %}
      <div class="text-center py-10 text-gray-500 border rounded bg-gray-50">
        No results available.
      </div>
    {% endif %}
  </div>
</div>
{% endblock %}
"""

live_shows_static_content = """{% extends "layout.html" %}

{% block title %}Live Shows{% endblock %}

{% block content %}
<div class="bg-white shadow rounded-lg overflow-hidden">
  
  <!-- Header strip -->
  <div class="h-2 bg-blue-600"></div>
  
  <!-- Row 1: Header -->
  <div class="px-6 py-4 flex items-center justify-between border-b">
    <h1 class="text-xl font-semibold">Live Shows</h1>
    <a href="{{ url_for('cultural_bp.live_showcase_dashboard') }}" class="bg-blue-600 text-white px-3 py-1 rounded hover:bg-blue-700">
      Back
    </a>
  </div>

  <div class="p-6">
    <p class="text-gray-600 mb-6">These are the upcoming and active live events curated by administrators.</p>

    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
      
      <!-- Placeholder Card 1 -->
      <div class="border border-gray-200 rounded-lg overflow-hidden">
        <div class="bg-gray-100 h-32 flex items-center justify-center text-gray-400">
          <svg class="w-12 h-12" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 10l4.553-2.276A1 1 0 0121 8.618v6.764a1 1 0 01-1.447.894L15 14M5 18h8a2 2 0 002-2V8a2 2 0 00-2-2H5a2 2 0 00-2 2v8a2 2 0 002 2z"></path></svg>
        </div>
        <div class="p-4">
          <h3 class="font-bold text-lg mb-1">CFI Regional Pageant Finals</h3>
          <p class="text-sm text-gray-600 mb-4">A digital live broadcast of the regional selections.</p>
          <button class="w-full bg-blue-600 text-white py-2 rounded font-medium opacity-50 cursor-not-allowed">Coming Soon</button>
        </div>
      </div>

    </div>
  </div>
</div>
{% endblock %}
"""

with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_results_list.html', 'w', encoding='utf-8') as f:
    f.write(live_results_content)

with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\live_shows_static.html', 'w', encoding='utf-8') as f:
    f.write(live_shows_static_content)

print("Standardized live_results_list.html and live_shows_static.html")
