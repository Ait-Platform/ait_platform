with open('templates/program_billing/setup_wizard.html', 'r', encoding='utf-8') as f:
    wizard = f.read()

js_logic = """
  function updateBulkVisibility() {
      const bw = document.getElementById('pm_bulk_water').value === 'yes';
      const be = document.getElementById('pm_bulk_elec').value === 'yes';
      const showBulk = bw || be;
      
      document.querySelectorAll('.bulk-col').forEach(el => {
          if (showBulk) el.classList.remove('hidden');
          else el.classList.add('hidden');
      });
      document.querySelectorAll('.bulk-col-adj').forEach(el => {
          if (showBulk) {
              el.classList.remove('col-span-6');
              el.classList.add('col-span-5');
          } else {
              el.classList.remove('col-span-5');
              el.classList.add('col-span-6');
          }
      });
  }
"""

if 'function updateBulkVisibility' not in wizard:
    wizard = wizard.replace('function gatherPropertyMap() {', js_logic + '\n  function gatherPropertyMap() {')
    
    event_listeners = """
    // Attach autosave listeners to all inputs
    document.getElementById('step-1').addEventListener('input', triggerAutoSave);
    document.getElementById('pm_bulk_water').addEventListener('change', updateBulkVisibility);
    document.getElementById('pm_bulk_elec').addEventListener('change', updateBulkVisibility);
"""
    wizard = wizard.replace("document.getElementById('step-1').addEventListener('input', triggerAutoSave);", event_listeners)
    
    with open('templates/program_billing/setup_wizard.html', 'w', encoding='utf-8') as f:
        f.write(wizard)
    print('Successfully injected updateBulkVisibility!')
else:
    print('updateBulkVisibility already exists!')
