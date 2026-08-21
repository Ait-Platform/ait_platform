import re

with open('templates/program_mechanic/mock_bill.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace("Your 30-Day Trial Has Ended!", "Insufficient Tokens: Time to Top Up!")
content = content.replace("We hope you've enjoyed standardizing your workshop with our platform. Let's take a look at the immense value you generated this month!", "You've run out of tokens to perform this action. Let's take a look at the immense value you've generated so far, and how you can easily cover token costs!")

with open('templates/program_mechanic/mock_bill.html', 'w', encoding='utf-8') as f:
    f.write(content)
