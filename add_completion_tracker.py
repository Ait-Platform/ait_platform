import re

with open('app/program_mechanic/routes.py', 'r', encoding='utf-8') as f:
    content = f.read()

completion_logic = '''
            # 4. Job Completed (Billed)
            if job.status == 'Billed':
                timeline.append({
                    "timestamp": job.updated_at or job.created_at,
                    "date": (job.updated_at or job.created_at).strftime('%Y-%m-%d'),
                    "time": "Workshop",
                    "event": f"Job Card #{job.job_number} Marked as Completed/Billed",
                    "color": "green",
                    "icon": "fa-check-double"
                })
'''

# Insert it before the # Add Payments from the Client Ledger line
content = content.replace("    # Add Payments from the Client Ledger", completion_logic + "    # Add Payments from the Client Ledger")

with open('app/program_mechanic/routes.py', 'w', encoding='utf-8') as f:
    f.write(content)
