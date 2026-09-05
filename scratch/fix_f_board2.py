import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

correct_js = """
<script>
document.addEventListener('DOMContentLoaded', function() {
    const mockAttendees = [
        {name: "Sipho Mkhize", sace: "11498322"},
        {name: "Jane Naidoo", sace: "10933418"},
        {name: "Tshepo Modise", sace: "12554901"},
        {name: "Sarah van der Merwe", sace: "10884321"},
        {name: "Zanele Ndlovu", sace: "11667234"},
        {name: "Bradley Cooper", sace: "10238475"},
        {name: "Priya Patel", sace: "11334902"},
        {name: "Thabo Mbeki", sace: "10112345"}
    ];
    const rosterList = document.getElementById('roster-list');
    const counter = document.getElementById('permanent-counter');
    if(rosterList) {
        rosterList.innerHTML = '';
        mockAttendees.forEach(person => {
            rosterList.innerHTML += '<li class="p-4 hover:bg-white transition flex justify-between items-center">' +
                '<div><p class="font-bold text-slate-800">' + person.name + '</p>' +
                '<p class="text-xs text-slate-500 font-mono">SACE: ' + person.sace + '</p></div>' +
                '<span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs font-bold"><i class="fas fa-check-circle mr-1"></i> Verified</span></li>';
        });
        if(counter) counter.innerText = mockAttendees.length;
    }
});
</script>
"""

# replace the last {% endblock %}
text = text.replace('{% endblock %}', correct_js + '\n{% endblock %}')

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
