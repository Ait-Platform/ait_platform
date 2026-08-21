with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

js_original = '''                uploadStatus.textContent = "Uploading & loading preview...";

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
                        document.getElementById('vin-preview-link').href = data.url;
                        previewContainer.classList.remove('hidden');
                    } else {'''

js_new = '''                uploadStatus.textContent = "Uploading, analyzing license disk with AI, & loading preview...";

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
                        uploadStatus.textContent = "Uploaded!";
                        uploadStatus.classList.replace('text-indigo-600', 'text-green-600');
                        
                        if (data.ai_data) {
                            let filledCount = 0;
                            if (data.ai_data.reg) { document.querySelector('input[name="vehicle_reg"]').value = data.ai_data.reg; filledCount++; }
                            if (data.ai_data.vin) { document.querySelector('input[name="vin_number"]').value = data.ai_data.vin; filledCount++; }
                            if (data.ai_data.make) { document.querySelector('input[name="make"]').value = data.ai_data.make; filledCount++; }
                            if (data.ai_data.model) { document.querySelector('input[name="model"]').value = data.ai_data.model; filledCount++; }
                            if (data.ai_data.year) { document.querySelector('input[name="year"]').value = data.ai_data.year; filledCount++; }
                            
                            if (filledCount > 0) {
                                uploadStatus.textContent = "AI successfully analyzed the disk and auto-filled the vehicle details!";
                                uploadStatus.classList.add('font-bold');
                            } else {
                                uploadStatus.textContent = "AI could not clearly read the details. Please enter them manually.";
                                uploadStatus.classList.replace('text-green-600', 'text-amber-600');
                            }
                        } else {
                            uploadStatus.textContent = "Could not extract details via AI. Please enter them manually.";
                            uploadStatus.classList.replace('text-green-600', 'text-amber-600');
                        }
                        
                        hiddenFilename.value = data.filename;
                        previewImage.src = data.url;
                        document.getElementById('vin-preview-link').href = data.url;
                        previewContainer.classList.remove('hidden');
                    } else {'''

content = content.replace(js_original, js_new)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated quote_form.html")
