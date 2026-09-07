import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace documents array in provider_documents
old_docs_array = '''    documents = [
        {
            "id": "app1",
            "title": "Application Form 1",
            "description": "The primary SACE application form.",
            "is_tracked": "app1" in tracked_ids
        },
        {
            "id": "f_cv",
            "title": "Facilitator Compliance Portfolio",
            "description": "Details of Presenters/Facilitators names, Certified copies of ID, Qualifications and comprehensive CV.",
            "is_tracked": "f_cv" in tracked_ids
        },
        {
            "id": "app2",
            "title": "Application Form 2",
            "description": "The secondary SACE application form.",
            "is_tracked": "app2" in tracked_ids
        }
    ]'''

new_docs_array = '''    documents = [
        {
            "id": "app1",
            "title": "Application Form 1",
            "description": "The primary SACE application form.",
            "is_tracked": "app1" in tracked_ids
        },
        {
            "id": "app2",
            "title": "Application Form 2",
            "description": "The secondary SACE application form.",
            "is_tracked": "app2" in tracked_ids
        },
        {
            "id": "tt",
            "title": "Reading Timetable (T/T)",
            "description": "Workshop schedule and session breakdown.",
            "is_tracked": "tt" in tracked_ids
        },
        {
            "id": "f_cv",
            "title": "Facilitator CVs & Compliance",
            "description": "Details of Presenters, Certified copies of ID, and comprehensive CV.",
            "is_tracked": "f_cv" in tracked_ids
        },
        {
            "id": "f_guide",
            "title": "Facilitator Manual",
            "description": "Educator slide notes and methodology guide.",
            "is_tracked": "f_guide" in tracked_ids
        },
        {
            "id": "ip_pledge",
            "title": "AIT IP Pledge",
            "description": "Blank Intellectual Property Pledge for manual signing.",
            "is_tracked": "ip_pledge" in tracked_ids
        }
    ]'''
text = text.replace(old_docs_array, new_docs_array)

# Replace doc_titles map
old_doc_titles = '''    doc_titles = {
        "app1": "SACE Application Form 1",
        "f_cv": "Facilitator Compliance Portfolio",
        "app2": "SACE Application Form 2"
    }'''

new_doc_titles = '''    doc_titles = {
        "app1": "SACE Application Form 1",
        "app2": "SACE Application Form 2",
        "tt": "Reading Timetable (T/T)",
        "f_cv": "Facilitator CVs & Compliance",
        "f_guide": "Facilitator Manual",
        "ip_pledge": "AIT IP Pledge"
    }'''
text = text.replace(old_doc_titles, new_doc_titles)

# Replace doc_file_map
old_doc_file_map = '''    doc_file_map = {
        "app1": "pdf/App_Form_1.pdf",
        "f_cv": "pdf/Facilitator_CVs.pdf",
        "app2": "pdf/App_Form_2.pdf"
    }'''

new_doc_file_map = '''    doc_file_map = {
        "app1": "pdf/App_Form_1.pdf",
        "app2": "pdf/App_Form_2.pdf",
        "tt": "pdf/Reading Timetable.pdf",
        "f_cv": "pdf/Facilitator_CVs.pdf",
        "f_guide": "pdf/F_Guide.pdf",
        "ip_pledge": "pdf/IP_Pledge.pdf"
    }'''
text = text.replace(old_doc_file_map, new_doc_file_map)

# Replace doc_names inside email action
old_doc_names = '''            doc_names = {
                "app1": "SACE Application Form 1",
                "f_cv": "Facilitator Compliance Portfolio",
                "app2": "SACE Application Form 2"
            }'''

new_doc_names = '''            doc_names = {
                "app1": "SACE Application Form 1",
                "app2": "SACE Application Form 2",
                "tt": "Reading Timetable (T/T)",
                "f_cv": "Facilitator CVs & Compliance",
                "f_guide": "Facilitator Manual",
                "ip_pledge": "AIT IP Pledge"
            }'''
text = text.replace(old_doc_names, new_doc_names)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

