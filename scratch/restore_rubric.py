import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# Replace the dropdowns with radio buttons for Section B in app-view-31
old_rubric = r'<div class="space-y-4 text-sm">\s*<div class="flex justify-between items-center"><span class="w-2/3">Clear vocalization</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>\s*<div class="flex justify-between items-center"><span class="w-2/3">Correct hand positioning</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>\s*<div class="flex justify-between items-center"><span class="w-2/3">Pacing for learners</span> <select class="w-1/3 p-1 border rounded bg-slate-50"><option value="3">3</option></select></div>\s*</div>'

new_rubric = '''<div class="space-y-4 text-sm">
      <div class="flex flex-col mb-3">
        <span class="font-semibold mb-2">Clear vocalization</span>
        <div class="flex space-x-2">
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_vocalization" value="0" class="mr-1"> 0</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_vocalization" value="1" class="mr-1"> 1</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_vocalization" value="2" class="mr-1"> 2</label>
          <label class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_vocalization" value="3" checked class="mr-1"> 3</label>
        </div>
      </div>
      <div class="flex flex-col mb-3">
        <span class="font-semibold mb-2">Correct hand positioning</span>
        <div class="flex space-x-2">
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_positioning" value="0" class="mr-1"> 0</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_positioning" value="1" class="mr-1"> 1</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_positioning" value="2" class="mr-1"> 2</label>
          <label class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_positioning" value="3" checked class="mr-1"> 3</label>
        </div>
      </div>
      <div class="flex flex-col mb-3">
        <span class="font-semibold mb-2">Pacing for learners</span>
        <div class="flex space-x-2">
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_pacing" value="0" class="mr-1"> 0</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_pacing" value="1" class="mr-1"> 1</label>
          <label class="flex-1 text-center py-2 bg-slate-50 border border-slate-200 rounded cursor-pointer hover:bg-indigo-50"><input type="radio" name="rubric_pacing" value="2" class="mr-1"> 2</label>
          <label class="flex-1 text-center py-2 bg-indigo-100 border border-indigo-300 rounded cursor-pointer"><input type="radio" name="rubric_pacing" value="3" checked class="mr-1"> 3</label>
        </div>
      </div>
    </div>'''

text = re.sub(old_rubric, new_rubric, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)
