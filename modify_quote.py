import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Replace the file input field with an AJAX-powered one and a hidden input
target_file_input = '''<div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Upload License Disk / VIN Image <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="file" name="license_disk_image" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 cursor-pointer border border-slate-300 rounded-md shadow-sm">
            </div>'''

replacement_file_input = '''<div class="col-span-full mb-4 hidden" id="vin-preview-container">
              <label class="block text-sm font-bold text-slate-700 mb-2">License Disk Preview</label>
              <div class="border rounded-md p-2 bg-slate-50">
                <img id="vin-preview-image" src="" alt="License Disk Preview" class="max-h-64 object-contain rounded">
              </div>
            </div>
            <div>
              <label class="block text-sm font-medium text-slate-700 mb-1">Upload License Disk / VIN Image <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
              <input type="file" id="ajax_disk_upload" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 cursor-pointer border border-slate-300 rounded-md shadow-sm">
              <input type="hidden" name="uploaded_disk_filename" id="uploaded_disk_filename">
              <p id="upload_status" class="text-xs text-indigo-600 font-semibold mt-1 hidden">Uploading...</p>
            </div>'''

content = content.replace(target_file_input, replacement_file_input)

# 2. Also, remove enctype="multipart/form-data" from the main form since we upload via AJAX now?
# Wait, we can keep it, it's fine. 
# But wait, earlier we had enctype="multipart/form-data". It's fine to keep it.

# 3. Add JS at the end of the file
js_code = '''
<script>
document.addEventListener('DOMContentLoaded', function() {
    const uploadInput = document.getElementById('ajax_disk_upload');
    const uploadStatus = document.getElementById('upload_status');
    const hiddenFilename = document.getElementById('uploaded_disk_filename');
    const previewContainer = document.getElementById('vin-preview-container');
    const previewImage = document.getElementById('vin-preview-image');

    if(uploadInput) {
        uploadInput.addEventListener('change', function() {
            if(this.files && this.files[0]) {
                const file = this.files[0];
                const formData = new FormData();
                formData.append('license_disk_image', file);
                // Also send csrf if needed
                const csrfToken = document.querySelector('input[name="csrf_token"]').value;
                
                uploadStatus.classList.remove('hidden');
                uploadStatus.textContent = "Uploading & loading preview...";

                fetch("{{ url_for('mechanic_bp.upload_disk') }}", {
                    method: 'POST',
                    headers: {
                        'X-CSRFToken': csrfToken
                    },
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    if(data.url) {
                        uploadStatus.textContent = "Uploaded! You can now read the details below.";
                        uploadStatus.classList.replace('text-indigo-600', 'text-green-600');
                        hiddenFilename.value = data.filename;
                        previewImage.src = data.url;
                        previewContainer.classList.remove('hidden');
                    } else {
                        uploadStatus.textContent = "Upload failed.";
                        uploadStatus.classList.replace('text-indigo-600', 'text-red-600');
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    uploadStatus.textContent = "Error during upload.";
                    uploadStatus.classList.replace('text-indigo-600', 'text-red-600');
                });
            }
        });
    }
});
</script>
'''

# append JS before endblock
if '{% endblock %}' in content:
    content = content.replace('{% endblock %}', js_code + '\n{% endblock %}')

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated quote_form.html")
