with open('templates/program_sace/post_test/test.html', 'r', encoding='utf-8') as f:
    text = f.read()

bad1 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"]:checked)'
good1 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"]:checked)'
text = text.replace(bad1, good1)

bad2 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"])'
good2 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"])'
text = text.replace(bad2, good2)

with open('templates/program_sace/post_test/test.html', 'w', encoding='utf-8') as f:
    f.write(text)
