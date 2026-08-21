import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

preview_original = '''              <div class="col-span-full mb-4 hidden" id="vin-preview-container">
                <label class="block text-sm font-bold text-slate-700 mb-2">License Disk Preview</label>
                <div class="border rounded-md p-2 bg-slate-50">
                  <img id="vin-preview-image" src="" alt="License Disk Preview" class="max-h-64 object-contain rounded">
                </div>
              </div>'''

preview_new = '''              <div class="col-span-full mb-4 hidden" id="vin-preview-container">
                <label class="block text-sm font-bold text-slate-700 mb-2">License Disk Preview <span class="text-xs text-indigo-500 font-normal ml-2">(Click image to enlarge)</span></label>
                <div class="border rounded-md p-2 bg-slate-50 text-center">
                  <a id="vin-preview-link" href="#" target="_blank" title="Click to enlarge" class="inline-block">
                    <img id="vin-preview-image" src="" alt="License Disk Preview" class="max-h-64 object-contain rounded border border-slate-200 hover:shadow-md transition cursor-pointer">
                  </a>
                </div>
              </div>'''

content = content.replace(preview_original, preview_new)

# Also update the JS to set the href of the link
js_original = '''                      if(data.url) {
                          uploadStatus.textContent = "Uploaded! You can now read the details below.";
                          hiddenFilename.value = data.filename;
                          previewImage.src = data.url;
                          previewContainer.classList.remove('hidden');
                      }'''

js_new = '''                      if(data.url) {
                          uploadStatus.textContent = "Uploaded! You can now read the details below.";
                          hiddenFilename.value = data.filename;
                          previewImage.src = data.url;
                          document.getElementById('vin-preview-link').href = data.url;
                          previewContainer.classList.remove('hidden');
                      }'''

content = content.replace(js_original, js_new)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated quote_form.html")
