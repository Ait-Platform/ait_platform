import os

with open('templates/program_sace/post_test/test.html', 'r', encoding='utf-8') as f:
    lines = f.readlines()

for i in range(len(lines)):
    if 'const checked = stepContainer.querySelector(input[name=""]:checked);' in lines[i]:
        lines[i] = '            const checked = stepContainer.querySelector(input[name=""]:checked);\n'
    if 'const groupContainer = stepContainer.querySelector(input[name=""]).closest(' in lines[i]:
        lines[i] = '                const groupContainer = stepContainer.querySelector(input[name=""]).closest(".bg-white");\n'

with open('templates/program_sace/post_test/test.html', 'w', encoding='utf-8') as f:
    f.writelines(lines)
