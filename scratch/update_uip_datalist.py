import re

file_path = 'templates/uip/reception/new_interaction.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_datalist = '''                        <datalist id="title-suggestions">
                            <option value="Broken Streetlight">
                            <option value="Pothole Report">
                            <option value="Noise Complaint">
                            <option value="Security Incident">
                            <option value="Illegal Dumping">
                            <option value="Levy Enquiry">
                        </datalist>'''

new_datalist = '''                        <datalist id="title-suggestions">
                            <option value="Safety & Security">
                            <option value="Crime to people">
                            <option value="Crime to property">
                            <option value="Water-related issues">
                            <option value="Electricity-related issues">
                            <option value="Sewerage-related issues">
                            <option value="Refuse Removal">
                            <option value="Maintenance of roads and street infrastructure">
                            <option value="Condition of Street lights">
                            <option value="Street Cleanliness">
                            <option value="Illegal Buildings & Construction">
                            <option value="Trucks & Illegal Vehicles">
                            <option value="Cleaning & Greening Areas">
                            <option value="Vagrancy & Loitering">
                            <option value="Public Spaces">
                            <option value="Noise Pollution">
                        </datalist>'''

text = text.replace(old_datalist, new_datalist)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

print("Updated datalist.")
