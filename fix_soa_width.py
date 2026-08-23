import re

with open('templates/program_debtors/soa_template.html', 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace(
    '''<table class="w-full text-left text-sm border-collapse border border-gray-300">
        <thead>
            <tr class="bg-gray-100 text-gray-800">
                <th class="py-2 px-3 font-bold border border-gray-300 w-24">Date</th>
                <th class="py-2 px-3 font-bold border border-gray-300">Details</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-28">Debit</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-28">Credit</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-32">Balance</th>
            </tr>
        </thead>''',
    '''<table class="w-full text-left text-sm border-collapse border border-gray-300" style="table-layout: fixed;">
        <thead>
            <tr class="bg-gray-100 text-gray-800">
                <th class="py-2 px-3 font-bold border border-gray-300 w-[15%]">Date</th>
                <th class="py-2 px-3 font-bold border border-gray-300 w-[40%]">Details</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-[15%]">Debit</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-[15%]">Credit</th>
                <th class="py-2 px-3 font-bold border border-gray-300 text-right w-[15%]">Balance</th>
            </tr>
        </thead>'''
)

# And also add word-wrap to the details TD
content = content.replace(
    '''<td class="py-2 px-3 border border-gray-300">''',
    '''<td class="py-2 px-3 border border-gray-300 break-words" style="word-wrap: break-word; overflow-wrap: break-word;">'''
)

with open('templates/program_debtors/soa_template.html', 'w', encoding='utf-8') as f:
    f.write(content)

