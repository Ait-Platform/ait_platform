import re

with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

idx = content.find('<!-- Manual Setup Modal -->')
if idx != -1:
    content = content[:idx] + '''<!-- Shop Profile Setup Modal -->
  <div id="manual-setup-modal" class="fixed inset-0 z-50 hidden flex items-center justify-center bg-slate-900 bg-opacity-50 p-4 sm:p-6" aria-labelledby="modal-title" role="dialog" aria-modal="true">
    <div class="bg-white rounded-2xl shadow-2xl w-full max-w-2xl overflow-y-auto max-h-[90vh] relative">
      <div class="px-8 py-5 border-b border-slate-200 bg-slate-50 flex justify-between items-center rounded-t-2xl">
        <h3 class="font-extrabold text-slate-800 text-xl">Shop Profile Setup</h3>
        <button onclick="document.getElementById('manual-setup-modal').classList.add('hidden')" class="text-slate-400 hover:text-slate-600 text-3xl leading-none">&times;</button>
      </div>
      
      <!-- Tabs Header -->
      <div class="flex border-b border-slate-200 bg-white">
        <button type="button" id="tab-btn-ai" onclick="switchProfileTab('ai')" class="flex-1 py-4 text-center font-bold text-sm border-b-2 border-indigo-600 text-indigo-600 hover:bg-slate-50 transition">
          <i class="fas fa-magic mr-2"></i> AI Auto-Fill
        </button>
        <button type="button" id="tab-btn-manual" onclick="switchProfileTab('manual')" class="flex-1 py-4 text-center font-bold text-sm border-b-2 border-transparent text-slate-500 hover:text-slate-700 hover:bg-slate-50 transition">
          <i class="fas fa-keyboard mr-2"></i> Manual Entry
        </button>
      </div>

      <form action="{{ url_for('mechanic_bp.onboarding_process') }}" method="POST" enctype="multipart/form-data">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
        
        <div class="p-8 space-y-6">
          
          <!-- TAB 1: AI UPLOAD -->
          <div id="tab-content-ai" class="block">
            <div class="bg-indigo-50 border border-indigo-200 rounded-xl p-8 text-center flex flex-col items-center justify-center">
              <div class="h-16 w-16 bg-indigo-100 rounded-full flex items-center justify-center mb-4">
                <i class="fas fa-camera text-2xl text-indigo-600"></i>
              </div>
              <h4 class="text-lg font-bold text-slate-800 mb-2">Scan Business Card or Letterhead</h4>
              <p class="text-sm text-slate-600 mb-6 max-w-sm">Upload a photo and our AI will automatically extract your business name, address, phone, and email.</p>
              
              <div class="w-full max-w-xs relative inline-block">
                <input type="file" id="ajax_card_upload" accept="image/*" class="absolute inset-0 w-full h-full opacity-0 cursor-pointer z-10">
                <div class="w-full py-3 px-4 bg-indigo-600 text-white rounded-lg font-bold shadow-sm hover:bg-indigo-700 transition flex items-center justify-center gap-2">
                  <i class="fas fa-upload"></i> Upload Image
                </div>
              </div>
              
              <div id="card_upload_status" class="mt-4 p-3 rounded-lg hidden w-full max-w-sm text-sm font-semibold flex items-center justify-center gap-2 mx-auto">
                <i class="fas fa-spinner fa-spin hidden" id="card_upload_spinner"></i>
                <span id="card_upload_text">Scanning...</span>
              </div>
            </div>
            
            <div class="mt-6 flex justify-end">
              <button type="button" onclick="switchProfileTab('manual')" class="text-indigo-600 font-bold hover:underline text-sm flex items-center">
                Skip & Enter Manually <i class="fas fa-arrow-right ml-1"></i>
              </button>
            </div>
          </div>

          <!-- TAB 2: MANUAL ENTRY -->
          <div id="tab-content-manual" class="hidden">
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">
              <div class="sm:col-span-2">
                <label class="block text-sm font-bold text-slate-700 mb-1">Business Name</label>
                <input type="text" name="business_name" value="{{ active_shop.business_name if active_shop else '' }}" required class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
              </div>
              <div class="sm:col-span-2">
                <label class="block text-sm font-bold text-slate-700 mb-1">Address</label>
                <input type="text" name="address" value="{{ active_shop.address if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Phone</label>
                <input type="text" name="phone" value="{{ active_shop.phone if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
              </div>
              <div>
                <label class="block text-sm font-bold text-slate-700 mb-1">Email</label>
                <input type="email" name="email" value="{{ active_shop.email if active_shop else '' }}" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-base p-3 transition">
              </div>
              <div class="sm:col-span-2">
                <label class="block text-sm font-bold text-slate-700 mb-1">Shop Logo (Optional)</label>
                {% if active_shop and active_shop.logo_url %}
                <div class="mb-3">
                  <img src="{{ url_for('static', filename='uploads/mechanic/' + active_shop.logo_url) }}" alt="Shop Logo" class="h-16 w-auto object-contain rounded border border-slate-200 shadow-sm bg-white p-1">
                </div>
                {% endif %}
                <input type="file" name="logo_file" accept="image/*" class="block w-full text-base text-slate-600 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-bold file:bg-indigo-100 file:text-indigo-700 hover:file:bg-indigo-200 transition cursor-pointer border-2 border-dashed border-indigo-200 p-4 rounded-lg">
                
                <div class="mt-6 border-t border-slate-200 pt-4">
                  <label class="block text-sm font-bold text-slate-700 mb-1">Custom Letterhead Banner (Optional)</label>
                  <p class="text-xs text-slate-500 mb-2">Upload a full-width image to replace the standard invoice header.</p>
                  {% if active_shop and active_shop.letterhead_url %}
                  <div class="mb-3">
                    <img src="{{ url_for('static', filename='uploads/mechanic/' + active_shop.letterhead_url) }}" alt="Shop Letterhead" class="h-16 w-full object-cover rounded border border-slate-200 shadow-sm bg-white">
                  </div>
                  {% endif %}
                  <input type="file" name="letterhead_file" accept="image/*" class="block w-full text-base text-slate-600 file:mr-4 file:py-2 file:px-4 file:rounded-lg file:border-0 file:text-sm file:font-bold file:bg-indigo-100 file:text-indigo-700 hover:file:bg-indigo-200 transition cursor-pointer border-2 border-dashed border-indigo-200 p-4 rounded-lg">
                </div>
              </div>
            </div>
          </div>
          
          <!-- ALWAYS VISIBLE: Terms & Conditions -->
          <div class="border-t border-slate-200 pt-6">
            <label class="block text-sm font-bold text-slate-700 mb-1">Terms & Conditions (Optional)</label>
            <p class="text-xs text-slate-500 mb-2">These will appear at the bottom of your Quotes and Invoices.</p>
            <div class="mb-2">
              <select id="tc-template-select" onchange="insertTCTemplate()" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-2 transition mb-2 bg-slate-50">
                <option value="">-- Or choose a template to load --</option>
                <option value="mechanic">General Mechanic</option>
                <option value="panel">Panel Beater</option>
                <option value="auto_elec">Auto Electrician</option>
                <option value="generic">Generic Business</option>
              </select>
            </div>
            <textarea id="tc-textarea" name="terms_conditions" rows="4" class="block w-full rounded-lg border-2 border-indigo-200 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 text-sm p-3 transition">{{ active_shop.terms_conditions if active_shop else '' }}</textarea>
          </div>
          
        </div>
        <div class="px-8 py-5 border-t border-slate-200 bg-slate-50 flex justify-end gap-3 rounded-b-2xl">
          <button type="button" onclick="document.getElementById('manual-setup-modal').classList.add('hidden')" class="rounded-xl border-2 border-slate-300 bg-white px-6 py-3 text-base font-bold text-slate-700 hover:bg-slate-50 transition shadow-sm">Cancel</button>
          <button type="submit" class="rounded-xl bg-indigo-600 px-6 py-3 text-base font-bold text-white hover:bg-indigo-700 shadow-md transition">Save Profile</button>
        </div>
      </form>
    </div>
  </div>


<script>
  // Tab Switching Logic
  function switchProfileTab(tab) {
    const btnAi = document.getElementById('tab-btn-ai');
    const btnManual = document.getElementById('tab-btn-manual');
    const contentAi = document.getElementById('tab-content-ai');
    const contentManual = document.getElementById('tab-content-manual');
    
    if (tab === 'ai') {
      btnAi.classList.add('border-indigo-600', 'text-indigo-600');
      btnAi.classList.remove('border-transparent', 'text-slate-500');
      btnManual.classList.remove('border-indigo-600', 'text-indigo-600');
      btnManual.classList.add('border-transparent', 'text-slate-500');
      contentAi.classList.remove('hidden');
      contentManual.classList.add('hidden');
    } else {
      btnManual.classList.add('border-indigo-600', 'text-indigo-600');
      btnManual.classList.remove('border-transparent', 'text-slate-500');
      btnAi.classList.remove('border-indigo-600', 'text-indigo-600');
      btnAi.classList.add('border-transparent', 'text-slate-500');
      contentManual.classList.remove('hidden');
      contentAi.classList.add('hidden');
    }
  }

  // AI Business Card Upload
  const cardUploadInput = document.getElementById('ajax_card_upload');
  const cardStatus = document.getElementById('card_upload_status');
  const cardStatusText = document.getElementById('card_upload_text');
  const cardSpinner = document.getElementById('card_upload_spinner');

  if (cardUploadInput) {
    cardUploadInput.addEventListener('change', function() {
      if (this.files && this.files[0]) {
        const formData = new FormData();
        formData.append('business_card_image', this.files[0]);

        // UI Feedback: Loading state
        cardStatus.classList.remove('hidden', 'bg-red-100', 'text-red-700', 'bg-green-100', 'text-green-700');
        cardStatus.classList.add('bg-indigo-100', 'text-indigo-700');
        cardSpinner.classList.remove('hidden');
        cardStatusText.textContent = 'Scanning image with AI... Please wait.';

        fetch('{{ url_for("mechanic_bp.upload_business_card") }}', {
          method: 'POST',
          body: formData,
          headers: {
            'X-CSRFToken': '{{ csrf_token() }}'
          }
        })
        .then(response => response.json())
        .then(data => {
          cardSpinner.classList.add('hidden');
          
          if (data.error) {
            // UI Feedback: Error state
            cardStatus.classList.remove('bg-indigo-100', 'text-indigo-700');
            cardStatus.classList.add('bg-red-100', 'text-red-700');
            cardStatusText.textContent = 'Error: ' + data.error;
            alert("AI Error: " + data.error);
          } else {
            // UI Feedback: Success state
            cardStatus.classList.remove('bg-indigo-100', 'text-indigo-700');
            cardStatus.classList.add('bg-green-100', 'text-green-700');
            cardStatusText.textContent = 'Success! Fields auto-filled.';
            
            // Auto-fill inputs
            if (data.ai_data) {
              if (data.ai_data.business_name) document.querySelector('input[name="business_name"]').value = data.ai_data.business_name;
              if (data.ai_data.address) document.querySelector('input[name="address"]').value = data.ai_data.address;
              if (data.ai_data.phone) document.querySelector('input[name="phone"]').value = data.ai_data.phone;
              if (data.ai_data.email) document.querySelector('input[name="email"]').value = data.ai_data.email;
            }
            
            // Switch to manual tab to show the filled results
            alert("Success! The AI has extracted your details. Switching to Manual Entry tab for you to review.");
            switchProfileTab('manual');
          }
        })
        .catch(err => {
          console.error(err);
          cardSpinner.classList.add('hidden');
          cardStatus.classList.remove('bg-indigo-100', 'text-indigo-700');
          cardStatus.classList.add('bg-red-100', 'text-red-700');
          cardStatusText.textContent = 'Error connecting to AI service.';
          alert("Error: Could not connect to the AI service.");
        });
      }
    });
  }
</script>

{% endblock %}
'''
    with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Replaced successfully")
else:
    print("Failed to find boundaries")
