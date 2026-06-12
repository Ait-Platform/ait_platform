import sys

file_path = r"D:\Users\yeshk\Documents\ait_platform\templates\school_billing\input_readings.html"
with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

dropzone_html = '''
      <!-- AI Dropzone for Readings -->
      <div class="mb-8 p-6 bg-slate-50 border-2 border-dashed border-blue-300 rounded-xl hover:border-blue-500 transition cursor-pointer relative" id="aiDropzone" onclick="document.getElementById('billFileInput').click()">
        <input type="file" id="billFileInput" class="hidden" accept=".pdf,image/*">
        <div class="text-center" id="dropzoneContent">
          <svg class="mx-auto h-12 w-12 text-blue-400 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
          </svg>
          <h3 class="text-lg font-bold text-slate-700">Upload this month's municipality bill</h3>
          <p class="text-sm text-slate-500 mt-1">Our AI will extract all meter readings, backwards-calculate your baselines, and auto-fill the inputs below.</p>
        </div>
        <div class="text-center hidden" id="dropzoneLoading">
          <svg class="animate-spin mx-auto h-10 w-10 text-blue-600 mb-3" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
            <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
            <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
          </svg>
          <p class="text-blue-600 font-semibold animate-pulse">AI is reading the meters...</p>
        </div>
      </div>
      
      <div id="aiErrorBox" class="hidden mb-8 p-4 bg-red-50 text-red-700 border border-red-200 rounded-lg text-sm font-semibold"></div>
'''

js_code = '''
<script>
  // AI Dropzone Logic
  const fileInput = document.getElementById('billFileInput');
  const dropzone = document.getElementById('aiDropzone');
  const content = document.getElementById('dropzoneContent');
  const loading = document.getElementById('dropzoneLoading');
  const errorBox = document.getElementById('aiErrorBox');

  dropzone.addEventListener('dragover', (e) => {
      e.preventDefault();
      dropzone.classList.add('border-blue-500', 'bg-blue-50');
  });

  dropzone.addEventListener('dragleave', () => {
      dropzone.classList.remove('border-blue-500', 'bg-blue-50');
  });

  dropzone.addEventListener('drop', (e) => {
      e.preventDefault();
      dropzone.classList.remove('border-blue-500', 'bg-blue-50');
      if (e.dataTransfer.files.length > 0) {
          handleFile(e.dataTransfer.files[0]);
      }
  });

  fileInput.addEventListener('change', (e) => {
      if (e.target.files.length > 0) {
          handleFile(e.target.files[0]);
      }
  });

  async function handleFile(file) {
      content.classList.add('hidden');
      loading.classList.remove('hidden');
      errorBox.classList.add('hidden');
      dropzone.style.pointerEvents = 'none';

      const formData = new FormData();
      formData.append('bill_file', file);

      try {
          // Get CSRF token
          const csrfToken = document.querySelector('input[name="csrf_token"]').value;

          const response = await fetch('/api/parse_readings', {
              method: 'POST',
              headers: {
                  'X-CSRFToken': csrfToken
              },
              body: formData
          });

          const result = await response.json();

          if (response.ok && result.readings) {
              // Successfully parsed
              applyReadings(result.readings);
          } else {
              throw new Error(result.error || 'Failed to extract readings from the bill.');
          }
      } catch (err) {
          errorBox.textContent = err.message;
          errorBox.classList.remove('hidden');
      } finally {
          content.classList.remove('hidden');
          loading.classList.add('hidden');
          dropzone.style.pointerEvents = 'auto';
          fileInput.value = ''; // Reset
      }
  }

  function applyReadings(readings) {
      // Create a map of extracted readings by normalized meter number
      const extracted = {};
      readings.forEach(r => {
          if (r.meter_number) {
              const key = r.meter_number.toString().trim().toLowerCase();
              extracted[key] = r;
          }
      });

      // Loop through all meter rows in the UI
      const rows = document.querySelectorAll('.meter-row');
      let matchCount = 0;
      
      rows.forEach(row => {
          // Extract the meter number text from the UI
          const meterDiv = row.querySelector('.text-lg.font-bold.text-slate-800');
          if (!meterDiv) return;
          const uiMeterNo = meterDiv.textContent.trim().toLowerCase();
          
          if (extracted[uiMeterNo]) {
              const r = extracted[uiMeterNo];
              matchCount++;
              
              // Apply new reading
              const newReadInput = row.querySelector('.new-read-input');
              if (newReadInput && r.current_reading) {
                  // Keep only numbers and decimals
                  newReadInput.value = r.current_reading.toString().replace(/[^0-9.]/g, '');
                  newReadInput.classList.add('ring-2', 'ring-green-400', 'border-green-400');
              }
              
              // Apply new date
              const newDateInput = row.querySelector('input[name^="date_"]');
              if (newDateInput && r.current_date) {
                  newDateInput.value = r.current_date;
              }
              
              // Apply prev reading (if the input exists because it's a baseline entry)
              const prevReadInput = row.querySelector('.prev-read-input');
              if (prevReadInput && r.previous_reading) {
                  prevReadInput.value = r.previous_reading.toString().replace(/[^0-9.]/g, '');
                  prevReadInput.classList.add('ring-2', 'ring-green-400', 'border-green-400');
              }
              
              // Apply prev date
              const prevDateInput = row.querySelector('input[name^="prev_date_"]');
              if (prevDateInput && r.previous_date) {
                  prevDateInput.value = r.previous_date;
              }
          }
      });
      
      if (matchCount === 0) {
         errorBox.textContent = "AI successfully read the bill, but none of the extracted meter numbers (" + readings.map(r=>r.meter_number).join(", ") + ") perfectly matched the meters assigned to this property.";
         errorBox.classList.remove('hidden');
      } else {
         // Success flash effect
         dropzone.classList.add('bg-green-50', 'border-green-500');
         content.innerHTML = '<h3 class="text-lg font-bold text-green-700">Successfully extracted ' + matchCount + ' meters!</h3><p class="text-sm text-green-600 mt-1">Review the highlighted inputs below.</p>';
      }
  }
</script>
'''

# Insert Dropzone below the description paragraph
target_tag = '<form action="{{ url_for(\'billing_bp.input_readings\', property_id=property.id) }}" method="POST">'
if target_tag in content and 'id="aiDropzone"' not in content:
    content = content.replace(target_tag, dropzone_html + '\n      ' + target_tag)

# Insert JS code before {% endblock %}
if '<script>' in content:
    content = content.replace('<script>', js_code + '\n<script>')
else:
    content = content.replace('{% endblock %}', js_code + '\n{% endblock %}')

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Updated input_readings.html")
