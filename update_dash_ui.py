with open('templates/program_mechanic/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

import re

# Add AI UI to modal
ui_original = '''        <div class="p-8 space-y-6">
          <p class="text-base text-slate-600 mb-2">Please enter your business details to configure your quotes and invoices.</p>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">'''

ui_new = '''        <div class="p-8 space-y-6">
          <p class="text-base text-slate-600 mb-2">Please enter your business details to configure your quotes and invoices.</p>

          <!-- AI Upload Section -->
          <div class="bg-indigo-50 border border-indigo-200 rounded-lg p-4 mb-4">
            <label class="block text-sm font-bold text-slate-700 mb-2">Upload Business Card / Letterhead for AI Auto-Fill <span class="text-xs text-slate-500 font-normal">(Optional)</span></label>
            <div class="flex flex-col sm:flex-row items-center gap-4">
              <input type="file" id="ajax_card_upload" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-600 file:text-white hover:file:bg-indigo-700 cursor-pointer border border-slate-300 rounded-md shadow-sm bg-white">
            </div>
            <p id="card_upload_status" class="text-sm text-indigo-600 font-semibold mt-2 hidden">Scanning Image with AI... Please wait.</p>
          </div>

          <div class="grid grid-cols-1 sm:grid-cols-2 gap-6">'''

content = content.replace(ui_original, ui_new)

# Add AJAX Script
script_append = '''

<script>
  // AI Business Card Upload
  const cardUploadInput = document.getElementById('ajax_card_upload');
  const cardStatus = document.getElementById('card_upload_status');

  if (cardUploadInput) {
    cardUploadInput.addEventListener('change', function() {
      if (this.files && this.files[0]) {
        const formData = new FormData();
        formData.append('business_card_image', this.files[0]);

        cardStatus.classList.remove('hidden');
        cardStatus.textContent = 'Scanning image with AI... Please wait.';

        fetch('{{ url_for("mechanic_bp.upload_business_card") }}', {
          method: 'POST',
          body: formData,
          headers: {
            'X-CSRFToken': '{{ csrf_token() }}'
          }
        })
        .then(response => response.json())
        .then(data => {
          if (data.error) {
            cardStatus.textContent = 'Error: ' + data.error;
            cardStatus.classList.remove('text-indigo-600');
            cardStatus.classList.add('text-red-600');
          } else {
            cardStatus.textContent = 'AI Scan Complete! Profile fields auto-filled.';
            cardStatus.classList.remove('text-indigo-600', 'text-red-600');
            cardStatus.classList.add('text-green-600');

            if (data.ai_data) {
              if (data.ai_data.business_name) document.querySelector('input[name="business_name"]').value = data.ai_data.business_name;
              if (data.ai_data.address) document.querySelector('input[name="address"]').value = data.ai_data.address;
              if (data.ai_data.phone) document.querySelector('input[name="phone"]').value = data.ai_data.phone;
              if (data.ai_data.email) document.querySelector('input[name="email"]').value = data.ai_data.email;
            }
          }
        })
        .catch(err => {
          console.error(err);
          cardStatus.textContent = 'Error connecting to AI service.';
          cardStatus.classList.remove('text-indigo-600');
          cardStatus.classList.add('text-red-600');
        });
      }
    });
  }
</script>
'''

content = content.replace('{% endblock %}', script_append + '\n{% endblock %}')

with open('templates/program_mechanic/dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated dashboard with UI and Script")
