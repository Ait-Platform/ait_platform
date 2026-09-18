import re
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    text = f.read()

old_draft_view = """              <div class="bg-amber-50 border border-amber-200 rounded-xl shadow-sm p-6 text-center">
                  <i class="fas fa-pen-nib text-4xl text-amber-300 mb-4 block"></i>
                  <h3 class="font-bold text-amber-900 mb-1">Private Draft</h3>
                  <p class="text-sm text-amber-700 mb-4">This resolution is invisible to the committee. When you are ready, publish it to the voting room.</p>
                  <form method="POST" action="{{ url_for('uip_bp.publish_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                      <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                      <button type="submit" class="w-full py-3 rounded-lg font-bold text-white bg-indigo-600 hover:bg-indigo-700 shadow-sm transition">
                          <i class="fas fa-paper-plane mr-2"></i> Publish to Committee
                      </button>
                  </form>
              </div>"""

new_draft_view = """              <div class="bg-amber-50 border border-amber-200 rounded-xl shadow-sm p-6 text-center">
                  <i class="fas fa-pen-nib text-4xl text-amber-300 mb-4 block"></i>
                  <h3 class="font-bold text-amber-900 mb-1">Private Draft</h3>
                  <p class="text-sm text-amber-700 mb-4">This resolution is invisible to the committee. When you are ready, publish it to the voting room.</p>
                  
                  <div class="space-y-3">
                      <a href="{{ url_for('uip_bp.edit_resolution', org_slug=org.slug, res_id=resolution.id) }}" class="block w-full py-3 rounded-lg font-bold text-amber-900 bg-white border border-amber-300 hover:bg-amber-100 shadow-sm transition">
                          <i class="fas fa-edit mr-2"></i> Edit Draft Text
                      </a>
                      <form method="POST" action="{{ url_for('uip_bp.publish_resolution', org_slug=org.slug, res_id=resolution.id) }}">
                          <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                          <button type="submit" class="w-full py-3 rounded-lg font-bold text-white bg-indigo-600 hover:bg-indigo-700 shadow-sm transition">
                              <i class="fas fa-paper-plane mr-2"></i> Publish to Committee
                          </button>
                      </form>
                  </div>
              </div>"""

text = text.replace(old_draft_view, new_draft_view)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated resolution_view.html with Edit Draft button")
