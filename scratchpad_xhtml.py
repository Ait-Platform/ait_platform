import io
from xhtml2pdf import pisa
import logging

logging.basicConfig(level=logging.DEBUG)

html = '''<html><body><table>
<tr><td style="border: none;">1</td></tr>
</table></body></html>'''

out = io.BytesIO()
pisa.CreatePDF(html, dest=out, encoding='UTF-8')
print("Done border: none;")

html2 = '''<html><body><table>
<tr><td style="border: 0;">1</td></tr>
</table></body></html>'''
out2 = io.BytesIO()
pisa.CreatePDF(html2, dest=out2, encoding='UTF-8')
print("Done border: 0;")

html3 = '''<html><body><table>
<tr><td style="border-style: none;">1</td></tr>
</table></body></html>'''
out3 = io.BytesIO()
pisa.CreatePDF(html3, dest=out3, encoding='UTF-8')
print("Done border-style: none;")
