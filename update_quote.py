import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace the single Customer & Vehicle block with two separate blocks
block_original = '''          <div class="p-6">
            <h3 class="text-lg font-bold text-slate-800 mb-4">Customer & Vehicle Details</h3>
            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2 lg:grid-cols-3 mb-8 border-b border-slate-100 pb-8">
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Customer Name</label>
                <input type="text" name="customer_name" required autofocus class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Vehicle Registration</label>
                <input type="text" name="vehicle_reg" required class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">VIN Number <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="vin_number" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm uppercase" placeholder="17-Digit VIN">
              </div>
              <div class="col-span-full mb-4 hidden" id="vin-preview-container">
                <label class="block text-sm font-bold text-slate-700 mb-2">License Disk Preview <span class="text-xs text-indigo-500 font-normal ml-2">(Click image to enlarge)</span></label>
                <div class="border rounded-md p-2 bg-slate-50 text-center">
                  <a id="vin-preview-link" href="#" target="_blank" title="Click to enlarge" class="inline-block">
                    <img id="vin-preview-image" src="" alt="License Disk Preview" class="max-h-64 object-contain rounded border border-slate-200 hover:shadow-md transition cursor-pointer">
                  </a>
                </div>
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Upload License Disk / VIN Image <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="file" id="ajax_disk_upload" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-50 file:text-indigo-700 hover:file:bg-indigo-100 cursor-pointer border border-slate-300 rounded-md shadow-sm">
                <input type="hidden" name="uploaded_disk_filename" id="uploaded_disk_filename">
                <p id="upload_status" class="text-xs text-indigo-600 font-semibold mt-1 hidden">Uploading...</p>
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Make <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="make" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. Toyota">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Model <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="model" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. Hilux">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Year <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="number" name="year" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 2018">
              </div>
            </div>'''

block_new = '''          <div class="p-6">
            <h3 class="text-lg font-bold text-slate-800 mb-4 border-b border-slate-100 pb-2">Client Details</h3>
            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2 lg:grid-cols-3 mb-8 border-b border-slate-100 pb-8">
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Customer Name</label>
                <input type="text" name="customer_name" required autofocus class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Phone Number <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="tel" name="customer_phone" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 082 123 4567">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Email Address <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="email" name="customer_email" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="client@example.com">
              </div>
            </div>

            <h3 class="text-lg font-bold text-slate-800 mb-4 border-b border-slate-100 pb-2">Vehicle Details & AI Upload</h3>
            <div class="mb-6 bg-slate-50 border border-slate-200 rounded-lg p-4">
              <label class="block text-sm font-bold text-slate-700 mb-2">Upload License Disk for Auto-Fill <span class="text-xs text-slate-500 font-normal">(Optional)</span></label>
              <div class="flex items-center gap-4">
                <input type="file" id="ajax_disk_upload" accept="image/*" class="block w-full text-sm text-slate-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-indigo-600 file:text-white hover:file:bg-indigo-700 cursor-pointer border border-slate-300 rounded-md shadow-sm bg-white">
                <input type="hidden" name="uploaded_disk_filename" id="uploaded_disk_filename">
              </div>
              <p id="upload_status" class="text-sm text-indigo-600 font-semibold mt-2 hidden">Uploading...</p>
              
              <div class="col-span-full mt-4 hidden" id="vin-preview-container">
                <p class="text-xs font-bold text-slate-500 uppercase tracking-wider mb-2">License Disk Preview <span class="text-indigo-500 ml-2 normal-case">(Click image to enlarge)</span></p>
                <div class="text-center bg-white border border-slate-200 p-2 rounded">
                  <a id="vin-preview-link" href="#" target="_blank" title="Click to enlarge" class="inline-block">
                    <img id="vin-preview-image" src="" alt="License Disk Preview" class="max-h-48 object-contain rounded border border-slate-200 hover:shadow-md transition cursor-pointer">
                  </a>
                </div>
              </div>
            </div>

            <div class="grid grid-cols-1 gap-y-6 gap-x-4 sm:grid-cols-2 lg:grid-cols-4 mb-8 border-b border-slate-100 pb-8">
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Vehicle Registration</label>
                <input type="text" name="vehicle_reg" required class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">VIN Number <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="vin_number" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm uppercase" placeholder="17-Digit VIN">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Make <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="make" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. Toyota">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Model <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="model" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. Hilux">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Year <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="number" name="year" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 2018">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Engine No <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="engine_no" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">GVM (kg) <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="gvm" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 1940">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Tare (kg) <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="tare" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm" placeholder="e.g. 1096">
              </div>
              <div>
                <label class="block text-sm font-medium text-slate-700 mb-1">Disk License No <span class="text-xs text-slate-400 font-normal">(Optional)</span></label>
                <input type="text" name="disk_license_no" class="block w-full rounded-md border border-slate-300 px-3 py-2 shadow-sm focus:border-indigo-500 focus:outline-none focus:ring-2 focus:ring-indigo-500 sm:text-sm">
              </div>
            </div>'''

content = content.replace(block_original, block_new)

# Update the JS fetch handling
js_original = '''                            if (data.ai_data.make) { document.querySelector('input[name="make"]').value = data.ai_data.make; filledCount++; }
                            if (data.ai_data.model) { document.querySelector('input[name="model"]').value = data.ai_data.model; filledCount++; }
                            if (data.ai_data.year) { document.querySelector('input[name="year"]').value = data.ai_data.year; filledCount++; }'''

js_new = '''                            if (data.ai_data.make) { document.querySelector('input[name="make"]').value = data.ai_data.make; filledCount++; }
                            if (data.ai_data.model) { document.querySelector('input[name="model"]').value = data.ai_data.model; filledCount++; }
                            if (data.ai_data.year) { document.querySelector('input[name="year"]').value = data.ai_data.year; filledCount++; }
                            if (data.ai_data.engine_no) { document.querySelector('input[name="engine_no"]').value = data.ai_data.engine_no; filledCount++; }
                            if (data.ai_data.gvm) { document.querySelector('input[name="gvm"]').value = data.ai_data.gvm.replace('kg','').trim(); filledCount++; }
                            if (data.ai_data.tare) { document.querySelector('input[name="tare"]').value = data.ai_data.tare.replace('kg','').trim(); filledCount++; }
                            if (data.ai_data.disk_license_no) { document.querySelector('input[name="disk_license_no"]').value = data.ai_data.disk_license_no; filledCount++; }'''

content = content.replace(js_original, js_new)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated quote_form.html")
