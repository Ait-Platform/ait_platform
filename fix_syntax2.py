import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

bad_string = """body = f"Hello,

Your {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.

Thank you for choosing us!\""""

good_string = """body = f\"\"\"Hello,\\n\\nYour {doc_type} #{job_card.job_number} is ready. We have attached a PDF copy for your records.\\n\\nThank you for choosing us!\"\"\""""

content = content.replace(bad_string, good_string)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
