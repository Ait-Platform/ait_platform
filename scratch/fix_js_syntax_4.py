with open('templates/program_sace/post_test/test.html', 'r', encoding='utf-8') as f:
    text = f.read()

bad_1 = 'stepContainer.querySelector(input[name=""]:checked)'
good_1 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"]:checked)'
text = text.replace(bad_1, good_1)

bad_2 = 'stepContainer.querySelector(input[name=""])'
good_2 = 'stepContainer.querySelector(input[name="' + chr(36) + '{name}"])'
text = text.replace(bad_2, good_2)

with open('templates/program_sace/post_test/test.html', 'w', encoding='utf-8') as f:
    f.write(text)
