import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Find the render_template line
regex = r'(total_debtors_count = len\(all_debtors\) if "all_debtors" in locals\(\) else 0\s*)(return render_template\("program_mechanic/job_cards_list\.html", job_cards=job_cards, debtors_with_balances=debtors_with_balances, total_debtors_count=total_debtors_count, all_debtors=all_debtors if "all_debtors" in locals\(\) else \[\]\))'

replacement = r'''\1all_vehicles = []
    if active_shop:
        all_vehicles = MechVehicle.query.join(MechClient).filter(MechClient.user_id == current_user.id).all()
    
    return render_template("program_mechanic/job_cards_list.html", job_cards=job_cards, debtors_with_balances=debtors_with_balances, total_debtors_count=total_debtors_count, all_debtors=all_debtors if "all_debtors" in locals() else [], all_vehicles=all_vehicles)'''

content = re.sub(regex, replacement, content)

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
