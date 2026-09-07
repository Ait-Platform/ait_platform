import re

file_path = 'templates/uip/reception/new_interaction.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# 1. Update WHO section
old_who = '''                <label class="block text-sm font-bold text-slate-700 mb-1">Search Resident / Contact Email</label>
                <input autofocus type="email" name="resident_email" class="w-full rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500" placeholder="e.g. resident@manorgardens.com (Leave blank if anonymous/walk-in)">'''

new_who = '''                <label class="block text-sm font-bold text-slate-700 mb-1">Select Verified Resident (UIP Members Table)</label>
                <select name="resident_email" class="w-full py-3 rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 text-slate-700 bg-white">
                    <option value="">-- Anonymous / Unregistered Walk-in --</option>
                    <optgroup label="Good Standing (Paid Up)">
                        <option value="john.smith@example.com">John Smith (14 Manor Drive) - john.smith@example.com</option>
                        <option value="sarah.jenkins@example.com">Sarah Jenkins (22 Oak Avenue) - sarah.jenkins@example.com</option>
                        <option value="david.lee@example.com">David Lee (5 Pine Street) - david.lee@example.com</option>
                    </optgroup>
                    <optgroup label="Arrears (Not Paid)">
                        <option value="mike.ross@example.com">Mike Ross (11b Valley Road) - mike.ross@example.com</option>
                        <option value="emma.stone@example.com">Emma Stone (88 Ridge Way) - emma.stone@example.com</option>
                    </optgroup>
                </select>'''

text = text.replace(old_who, new_who)

# 2. Update WHAT section (replace input+datalist with a tall select)
old_what = '''                        <label class="block text-sm font-bold text-slate-700 mb-1">Short Title / Topic</label>
                        <input type="text" name="title" list="title-suggestions" required class="w-full rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500" placeholder="e.g. Broken Streetlight">
                        <datalist id="title-suggestions">
                            <option value="Safety & Security">
                            <option value="Crime to people">
                            <option value="Crime to property">
                            <option value="Water-related issues">
                            <option value="Electricity-related issues">
                            <option value="Sewerage-related issues">
                            <option value="Refuse Removal">
                            <option value="Maintenance of roads and street infrastructure">
                            <option value="Condition of Street lights">
                            <option value="Street Cleanliness">
                            <option value="Illegal Buildings & Construction">
                            <option value="Trucks & Illegal Vehicles">
                            <option value="Cleaning & Greening Areas">
                            <option value="Vagrancy & Loitering">
                            <option value="Public Spaces">
                            <option value="Noise Pollution">
                        </datalist>'''

new_what = '''                        <label class="block text-sm font-bold text-slate-700 mb-1">Short Title / Topic (Manor Gardens Survey Priorities)</label>
                        <select name="title" required class="w-full py-3 rounded border-slate-300 shadow-sm focus:border-indigo-500 focus:ring-indigo-500 bg-white">
                            <option value="">-- Select Specific Priority Issue --</option>
                            <option value="Safety & Security">1. Safety & Security</option>
                            <option value="Crime to people">2. Crime to people</option>
                            <option value="Crime to property">3. Crime to property</option>
                            <option value="Water-related issues">4. Water-related issues</option>
                            <option value="Electricity-related issues">5. Electricity-related issues</option>
                            <option value="Sewerage-related issues">6. Sewerage-related issues</option>
                            <option value="Refuse Removal">7. Refuse Removal</option>
                            <option value="Maintenance of roads and street infrastructure">8. Maintenance of roads and street infrastructure</option>
                            <option value="Condition of Street lights">9. Condition of Street lights</option>
                            <option value="Street Cleanliness">10. Street Cleanliness</option>
                            <option value="Illegal Buildings & Construction">11. Illegal Buildings & Construction</option>
                            <option value="Trucks & Illegal Vehicles">12. Trucks & Illegal Vehicles</option>
                            <option value="Cleaning & Greening Areas">13. Cleaning & Greening Areas</option>
                            <option value="Vagrancy & Loitering">14. Vagrancy & Loitering</option>
                            <option value="Public Spaces">15. Public Spaces</option>
                            <option value="Noise Pollution">16. Noise Pollution</option>
                        </select>'''

text = text.replace(old_what, new_what)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

