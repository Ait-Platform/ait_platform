import re

# 1. Update R's pledge modal
file1 = 'templates/program_sace/provisioning_map.html'
with open(file1, 'r', encoding='utf-8') as f:
    text1 = f.read()

old_pledge_r = '''<div class="p-6 overflow-y-auto text-slate-600 text-sm leading-relaxed space-y-4">'''
new_pledge_r = '''<div class="p-6 overflow-y-auto text-slate-900 font-bold text-base leading-relaxed space-y-4">'''

text1 = text1.replace(old_pledge_r, new_pledge_r)
with open(file1, 'w', encoding='utf-8') as f:
    f.write(text1)


# 2. Update A's pledge modal
file2 = 'templates/program_sace/auditor_pledge.html'
with open(file2, 'r', encoding='utf-8') as f:
    text2 = f.read()

old_pledge_a = '''<div class="bg-slate-50 border border-slate-200 p-6 rounded-lg text-slate-900 font-medium leading-relaxed space-y-4 mb-8">'''
new_pledge_a = '''<div class="bg-slate-50 border border-slate-200 p-6 rounded-lg text-slate-900 font-bold text-base leading-relaxed space-y-4 mb-8">'''

text2 = text2.replace(old_pledge_a, new_pledge_a)
with open(file2, 'w', encoding='utf-8') as f:
    f.write(text2)

print("Darkened pledges.")
