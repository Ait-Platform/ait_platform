with open("templates/program_uip/claim_committee.html", "r", encoding="utf-8") as f:
    text = f.read()

old_input = """        <div class="mb-6">
            <label class="block text-sm font-bold text-slate-700 mb-2">What is your role?</label>
            <input type="text" name="position" required placeholder="e.g. Head, Member, Advisor" class="w-full border-slate-300 rounded-lg px-4 py-2 focus:ring-indigo-500">
        </div>"""

new_input = """        <div class="mb-6">
            <label class="block text-sm font-bold text-slate-700 mb-2">What is your role?</label>
            <select name="position" required class="w-full border-slate-300 rounded-lg px-4 py-2 focus:ring-indigo-500">
                <option value="">-- Select Role --</option>
                <option value="Head / Chairperson">Head / Chairperson</option>
                <option value="Vice Head">Vice Head</option>
                <option value="General Member">General Member</option>
                <option value="Advisor">Advisor</option>
                <option value="Secretary">Secretary</option>
                <option value="Contractor / Provider">Contractor / Provider</option>
            </select>
        </div>"""

if old_input in text:
    text = text.replace(old_input, new_input)
    with open("templates/program_uip/claim_committee.html", "w", encoding="utf-8") as f:
        f.write(text)
    print("Patched claim_committee.html")
else:
    print("Could not find old_input")
