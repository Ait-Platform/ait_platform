with open('templates/program_billing/manual_capture.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the packing logic in javascript
old_js = """        status: card.querySelector('.m-status').value,
        prev_date: card.querySelector('.m-pdate').value,
        // No readings packed during Phase 1
      });"""

new_js = """        status: card.querySelector('.m-status').value
        // No dates or readings packed during Phase 1
      });"""

content = content.replace(old_js, new_js)

with open('templates/program_billing/manual_capture.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Patched JS error in manual_capture.html")
