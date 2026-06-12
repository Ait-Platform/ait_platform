import zipfile, re
z = zipfile.ZipFile('gemini api key.docx')
xml = z.read('word/document.xml').decode('utf-8')
text = ''.join(re.findall(r'<w:t[^>]*>(.*?)</w:t>', xml))
print("EXTRACTED TEXT:", text)
