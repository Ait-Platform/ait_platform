import re

with open('templates/program_mechanic/job_card.html', 'r', encoding='utf-8') as f:
    content = f.read()

script = '''
<script>
document.addEventListener("DOMContentLoaded", function() {
    const uploadInput = document.getElementById('ajax_disk_upload');
    const uploadStatus = document.getElementById('upload_status');
    const missingVinInput = document.getElementById('missing_vin');

    if(uploadInput) {
        uploadInput.addEventListener('change', function() {
            if(this.files && this.files[0]) {
                const file = this.files[0];
                const formData = new FormData();
                formData.append('license_disk_image', file);
                const csrfToken = document.querySelector('input[name="csrf_token"]').value;
                
                uploadStatus.classList.remove('hidden');
                uploadStatus.textContent = "Analyzing license disk with AI...";
                uploadStatus.className = "text-xs text-indigo-600 font-semibold mt-1";

                fetch("{{ url_for('mechanic_bp.upload_disk') }}", {
                    method: 'POST',
                    headers: {
                        'X-CSRFToken': csrfToken
                    },
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        uploadStatus.textContent = "Error: " + data.error;
                        uploadStatus.className = "text-xs text-red-600 font-semibold mt-1";
                    } else if (data.ai_data && data.ai_data.vin) {
                        missingVinInput.value = data.ai_data.vin;
                        uploadStatus.textContent = "AI successfully extracted the VIN!";
                        uploadStatus.className = "text-xs text-green-600 font-bold mt-1";
                    } else {
                        uploadStatus.textContent = "AI could not clearly read the VIN. Please enter it manually.";
                        uploadStatus.className = "text-xs text-amber-600 font-semibold mt-1";
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    uploadStatus.textContent = "Network error during upload.";
                    uploadStatus.className = "text-xs text-red-600 font-semibold mt-1";
                });
            }
        });
    }
});
</script>
{% endblock %}
'''

content = content.replace('{% endblock %}', script)

with open('templates/program_mechanic/job_card.html', 'w', encoding='utf-8') as f:
    f.write(content)
