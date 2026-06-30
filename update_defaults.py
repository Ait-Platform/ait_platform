with open('app/program_budget/routes.py', 'r', encoding='utf-8') as f:
    text = f.read()

old_defaults = '''    defaults = [
        ("Bank", "asset"),
        ("Cash", "asset"),
        ("Salary", "income"),
        ("Other Income", "income"),
        ("Home Loan", "liability"),
        ("Credit Card", "liability"),
        ("Groceries", "expense"),
        ("Transport", "expense"),
        ("Lights & Water", "expense"),
        ("Entertainment", "expense"),
    ]'''

new_defaults = '''    defaults = [
        ("Bank", "asset"),
        ("Cash", "asset"),
        ("Savings", "asset"),
        ("Salary", "income"),
        ("Side Hustle", "income"),
        ("Other Income", "income"),
        ("Home Loan", "liability"),
        ("Vehicle Loan", "liability"),
        ("Credit Card", "liability"),
        ("Personal Loan", "liability"),
        ("Groceries", "expense"),
        ("Transport", "expense"),
        ("Lights & Water", "expense"),
        ("Entertainment", "expense"),
        ("School Fees", "expense"),
        ("Insurance", "expense"),
        ("Medical", "expense"),
        ("Clothing", "expense"),
    ]'''

text = text.replace(old_defaults, new_defaults)

with open('app/program_budget/routes.py', 'w', encoding='utf-8') as f:
    f.write(text)
print("Updated defaults")
