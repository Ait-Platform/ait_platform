import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add IDs to the inputs
content = content.replace(
    '''<input type="number" name="mileage"''',
    '''<input type="number" id="mileage_input" name="mileage"'''
)
content = content.replace(
    '''<input type="text" name="next_service_due"''',
    '''<input type="text" id="next_service_due_input" name="next_service_due"'''
)

# Add the JS listener at the end of the script block
js_addition = '''
  document.getElementById('mileage_input').addEventListener('input', function(e) {
      const val = parseInt(e.target.value);
      const nextInput = document.getElementById('next_service_due_input');
      if (!isNaN(val) && val > 0) {
          nextInput.value = (val + 10000) + " km";
      } else {
          nextInput.value = "";
      }
  });
</script>
'''

content = content.replace('</script>', js_addition, 1)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
