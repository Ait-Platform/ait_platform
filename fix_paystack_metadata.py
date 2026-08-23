import re

with open('app/payments/paystack.py', 'r', encoding='utf-8') as f:
    content = f.read()

replacement1 = '''                        if not existing:
                            metadata = tx_data.get("metadata", {})
                            if isinstance(metadata, str):
                                import json
                                try:
                                    metadata = json.loads(metadata)
                                except:
                                    metadata = {}
                            meta_email = metadata.get("email") or tx_data.get("customer", {}).get("email", "")
                            meta_subject = metadata.get("subject", "")
                            if meta_email and meta_subject:'''

content = re.sub(
    r"                        if not existing:\s*meta_email = tx_data\.get\(\"metadata\", \{\}\)\.get\(\"email\"\) or tx_data\.get\(\"customer\", \{\}\)\.get\(\"email\", \"\"\)\s*meta_subject = tx_data\.get\(\"metadata\", \{\}\)\.get\(\"subject\", \"\"\)\s*if meta_email and meta_subject:",
    replacement1,
    content,
    flags=re.DOTALL
)

replacement2 = '''        if event_type == "charge.success":
            tx_data = data.get("data", {})
            metadata = tx_data.get("metadata", {})
            if isinstance(metadata, str):
                import json
                try:
                    metadata = json.loads(metadata)
                except:
                    metadata = {}
            
            email = metadata.get("email") or tx_data.get("customer", {}).get("email", "")
            subject = metadata.get("subject", "")'''

content = re.sub(
    r"        if event_type == \"charge\.success\":\s*tx_data = data\.get\(\"data\", \{\}\)\s*metadata = tx_data\.get\(\"metadata\", \{\}\)\s*email = metadata\.get\(\"email\"\) or tx_data\.get\(\"customer\", \{\}\)\.get\(\"email\", \"\"\)\s*subject = metadata\.get\(\"subject\", \"\"\)",
    replacement2,
    content,
    flags=re.DOTALL
)

replacement3 = '''        if subject.endswith("_topup"):
            from app.models.auth import AitTokenWallet, AitTokenTransaction
            total = int(transaction.get("amount", 0)) if transaction else 0
            if total > 0:
                metadata = transaction.get("metadata", {})
                if isinstance(metadata, str):
                    import json
                    try:
                        metadata = json.loads(metadata)
                    except:
                        metadata = {}
                tokens_purchased = metadata.get("tokens_purchased")
                if tokens_purchased is None:'''

content = re.sub(
    r"        if subject\.endswith\(\"_topup\"\):\s*from app\.models\.auth import AitTokenWallet, AitTokenTransaction\s*total = int\(transaction\.get\(\"amount\", 0\)\) if transaction else 0\s*if total > 0:\s*tokens_purchased = transaction\.get\(\"metadata\", \{\}\)\.get\(\"tokens_purchased\"\)\s*if tokens_purchased is None:",
    replacement3,
    content,
    flags=re.DOTALL
)

with open('app/payments/paystack.py', 'w', encoding='utf-8') as f:
    f.write(content)
