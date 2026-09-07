import re

file_path = 'app/program_sace/routes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace documents array in provider_documents
old_docs_array = '''    documents = [
        {
            "id": "1",
            "title": "Provider Application Form",
            "description": "General application form for provider approval.",
            "is_tracked": "1" in tracked_ids
        },
        {
            "id": "2",
            "title": "Professional Development Activity Form for 2 Hours TO 5 Days Programs (1)",
            "description": "Application form for short duration activities.",
            "is_tracked": "2" in tracked_ids
        },
        {
            "id": "3",
            "title": "Professional Development Activity Application form of duration from 6 days upwards",
            "description": "Application form for long duration activities.",
            "is_tracked": "3" in tracked_ids
        },
        {
            "id": "4",
            "title": "Facilitator CVs",
            "description": "Curriculum Vitae of the programme facilitators.",
            "is_tracked": "4" in tracked_ids
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

text = text.replace(old_docs_array, new_docs_array)

# Replace doc_titles map
old_doc_titles = '''    doc_titles = {
        "1": "SACE Provider Application Form",
        "2": "SACE Activity Application Form",
        "3": "SACE Endorsement Guidelines",
        "4": "Facilitator CVs"
    }'''

new_doc_titles = '''    doc_titles = {
        "app1": "SACE Application Form 1",
        "f_cv": "Facilitator Compliance Portfolio",
        "app2": "SACE Application Form 2"
    }'''
text = text.replace(old_doc_titles, new_doc_titles)

# Replace doc_file_map
old_doc_file_map = '''    doc_file_map = {
        "1": "pdf/App_Form_1.pdf",
        "2": "pdf/App_Form_1.pdf", # Same dummy file for now
        "3": "pdf/App_Form_2.pdf",
        "4": "pdf/Facilitator_CVs.pdf"
    }'''

new_doc_file_map = '''    doc_file_map = {
        "app1": "pdf/App_Form_1.pdf",
        "f_cv": "pdf/Facilitator_CVs.pdf",
        "app2": "pdf/App_Form_2.pdf"
    }'''
text = text.replace(old_doc_file_map, new_doc_file_map)

# Replace doc_names inside email action
old_doc_names = '''            doc_names = {
                "1": "Provider Application Form",
                "2": "Professional Development Activity Form (2-5 Days)",
                "3": "Professional Development Activity Form (6+ Days)",
                "4": "Facilitator CVs"
            }'''

new_doc_names = '''            doc_names = {
                "app1": "SACE Application Form 1",
                "f_cv": "Facilitator Compliance Portfolio",
                "app2": "SACE Application Form 2"
            }'''
text = text.replace(old_doc_names, new_doc_names)


with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

