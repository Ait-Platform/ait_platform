import re

with open('app/payments/paystack.py', 'r', encoding='utf-8') as f:
    content = f.read()

old_logic = """                    if not existing:
                        meta_email = tx_data.get("metadata", {}).get("email") or tx_data.get("customer", {}).get("email", "")
                        meta_subject = tx_data.get("metadata", {}).get("subject", "")
                        if meta_email and meta_subject:"""

new_logic = """                    if not existing:
                        metadata = tx_data.get("metadata", {})
                        if isinstance(metadata, str):
                            import json
                            try:
                                metadata = json.loads(metadata)
                            except:
                                metadata = {}
                        meta_email = email or metadata.get("email") or tx_data.get("customer", {}).get("email", "")
                        meta_subject = subject or metadata.get("subject", "")
                        if meta_email and meta_subject:"""

content = content.replace(old_logic, new_logic)

with open('app/payments/paystack.py', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed paystack synchronous fallback metadata parsing")
