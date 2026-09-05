import re

with open('templates/program_sace/simulator.html', 'r', encoding='utf-8') as f:
    text = f.read()

text = text.replace('teachers to check in', 'participants to check in')
text = text.replace('Teachers Connected', 'Participants Connected')
text = text.replace('Teacher View Active', 'Participant View Active')
text = text.replace('Push to Teacher\\'s Device', 'Push to Participant\\'s Device')
text = text.replace('Teacher response recorded', 'Participant response recorded')
text = text.replace('participating teacher', 'participant')
text = text.replace('>Teacher<', '>Participant<')

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(text)
