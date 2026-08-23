import re

with open('templates/program_mechanic/quote_form.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I want to inject a check for data.error right after if(data.url) {

regex = r'(if\s*\(data\.url\)\s*\{\s*)(uploadStatus\.textContent = "Uploaded!";\s*uploadStatus\.classList\.replace\(\'text-indigo-600\', \'text-green-600\'\);\s*)(if \s*\(data\.ai_data\))'

def replacer(match):
    error_logic = '''
                          if (data.error) {
                              uploadStatus.textContent = "AI Error: " + data.error;
                              uploadStatus.classList.replace('text-indigo-600', 'text-red-600');
                          } else '''
    
    return match.group(1) + match.group(2) + error_logic + match.group(3)

content = re.sub(regex, replacer, content)

with open('templates/program_mechanic/quote_form.html', 'w', encoding='utf-8') as f:
    f.write(content)
