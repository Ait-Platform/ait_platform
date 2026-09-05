with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

bad_block = '''    from app.models.auth import AitTokenWallet, AitTokenTransaction
    from sqlalchemy import text'''

good_block = '''    job_card = MechJobCard.query.get_or_404(id)
    if job_card.status == 'Quote':
        from app.models.auth import AitTokenWallet, AitTokenTransaction
        from sqlalchemy import text'''

content = content.replace(bad_block, good_block)

# And now we must fix the indentation of lines 1568 to 1583 (which were 4 spaces, but now need to be 8 spaces because they are inside the if block!)
# Actually, I'll just rewrite the entire block.
