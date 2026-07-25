with open('app/program_culturalfire/routes.py', 'r') as f:
    content = f.read()

content = content.replace(
    'from app.models.culturalfire import CfiPrivateShowGroup\n        for member in memberships:',
    'from app.models.culturalfire import CfiPrivateShowGroup, CfiShowAccess\n        for member in memberships:'
)

content = content.replace(
    'from app.models.culturalfire import CfiPrivateShowGroup\n    is_private_show',
    'from app.models.culturalfire import CfiPrivateShowGroup, CfiShowAccess\n    is_private_show'
)

content = content.replace(
    'def unlock_private_show(show_id):\n    show = CfiShow.query.get_or_404(show_id)',
    'def unlock_private_show(show_id):\n    from app.models.culturalfire import CfiShowAccess\n    show = CfiShow.query.get_or_404(show_id)'
)

with open('app/program_culturalfire/routes.py', 'w') as f:
    f.write(content)
print("Done")
