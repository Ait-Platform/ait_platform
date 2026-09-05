file_path = 'templates/program_sace/post_test/test.html'
with open(file_path, 'r', encoding='utf-8') as f: text = f.read()

text = text.replace('const checked = stepContainer.querySelector(input[name=""]:checked);', 'const checked = stepContainer.querySelector(input[name=""]:checked);')
text = text.replace('const groupContainer = stepContainer.querySelector(input[name=""]).closest(' + "'.bg-white');", 'const groupContainer = stepContainer.querySelector(input[name=""]).closest(' + "'.bg-white');")

with open(file_path, 'w', encoding='utf-8') as f: f.write(text)
