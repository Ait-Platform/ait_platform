import re

with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Remove DRAFT_KEY
content = content.replace("const DRAFT_KEY = 'billing_wizard_draft_{{ property.id }}';", "")

# 2. Update triggerAutoSave to send fetch request instead of localStorage
old_autosave = """      localStorage.setItem(DRAFT_KEY, JSON.stringify(wizardData));
      
      const ind = document.getElementById('autosave-indicator');
      ind.classList.remove('opacity-0');
      setTimeout(() => ind.classList.add('opacity-0'), 2000);
    }, 1000);"""

new_autosave = """      fetch("{{ url_for('billing_bp.save_architecture_draft', property_id=property.id) }}", {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'X-CSRFToken': '{{ csrf_token() }}' },
        body: JSON.stringify(wizardData)
      }).then(r => {
        if(r.ok) {
          const ind = document.getElementById('autosave-indicator');
          ind.classList.remove('opacity-0');
          setTimeout(() => ind.classList.add('opacity-0'), 2000);
        }
      });
    }, 1000);"""
content = content.replace(old_autosave, new_autosave)

# 3. Update loadDraft to parse draft_json instead of localStorage
old_loaddraft = """  function loadDraft() {
    const saved = localStorage.getItem(DRAFT_KEY);
    if (saved) {
      try {
        wizardData = JSON.parse(saved);"""

new_loaddraft = """  function loadDraft() {
    const saved = {{ draft_json|safe }};
    if (saved && saved.accounts) {
      try {
        wizardData = saved;"""
content = content.replace(old_loaddraft, new_loaddraft)

# 4. Update saveArchitecture to remove localStorage.removeItem
content = content.replace("localStorage.removeItem(DRAFT_KEY);", "")

with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
    f.write(content)

print("Updated setup_wizard.html to use server-side drafts.")
