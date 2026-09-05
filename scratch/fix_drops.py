import re

with open('migrations/versions/7da57fffdba9_sync_db_to_models.py', 'r', encoding='utf-8') as f:
    text = f.read()

# We need to make sure debtors_token_transaction is dropped BEFORE debtors_wallet
# and cfi_token_transaction BEFORE cfi_wallet

# Find all drop_table calls
drops = re.findall(r"op\.drop_table\('.*?'\)", text)

# Put the transaction tables first in the drop list
new_drops = []
for d in drops:
    if 'transaction' in d:
        new_drops.append(d)

for d in drops:
    if 'transaction' not in d:
        new_drops.append(d)

# Replace the block of drops with the reordered block
# wait, it's easier to just do it via regex substitution
text = text.replace("op.drop_table('debtors_wallet')", "")
text = text.replace("op.drop_table('debtors_token_transaction')", "op.drop_table('debtors_token_transaction')\n    op.drop_table('debtors_wallet')")

text = text.replace("op.drop_table('cfi_wallet')", "")
text = text.replace("op.drop_table('cfi_token_transaction')", "op.drop_table('cfi_token_transaction')\n    op.drop_table('cfi_wallet')")

with open('migrations/versions/7da57fffdba9_sync_db_to_models.py', 'w', encoding='utf-8') as f:
    f.write(text)
