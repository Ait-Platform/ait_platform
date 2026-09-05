file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

bad = '''          } else {
              document.getElementById('step-counter-container').innerHTML = Step <span id="f-counter-global"></span> of 30;
          }</span> of 30;
          }'''

good = '''          } else {
              document.getElementById('step-counter-container').innerHTML = Step <span id="f-counter-global"></span> of 30;
          }'''

if bad in text:
    text = text.replace(bad, good)
else:
    print("WARNING: Could not find exact bad string")

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
