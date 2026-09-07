html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

modal_html = '''
<!-- IP Pledge Modal -->
<div id="pledge-modal" class="fixed inset-0 z-[60] bg-slate-900 bg-opacity-75 hidden flex items-center justify-center p-4 backdrop-blur-sm">
    <div class="bg-white rounded-xl shadow-2xl w-full max-w-2xl overflow-hidden border border-slate-200 flex flex-col max-h-[90vh]">
        
        <!-- Header -->
        <div class="px-6 py-4 border-b border-slate-100 flex justify-between items-center bg-slate-50">
            <h3 class="font-bold text-xl text-slate-800"><i class="fas fa-file-contract text-indigo-600 mr-2"></i> Intellectual Property Pledge</h3>
            <button onclick="closePledgeModal()" class="text-slate-400 hover:text-slate-600 transition">
                <i class="fas fa-times text-xl"></i>
            </button>
        </div>
        
        <!-- Body -->
        <div class="p-6 overflow-y-auto text-slate-600 text-sm leading-relaxed space-y-4">
            <p>
                By accessing this Control Centre, you acknowledge that the <strong>I Learn to Read English Using the LITRE Method</strong>, including the digital LITRE Simulator, visual framework, and associated teaching methodologies, are the protected Intellectual Property of the Archoney Institute of Technology (AIT).
            </p>
            <p>
                You agree that:
            </p>
            <ul class="list-disc list-inside space-y-2 ml-2">
                <li>Access is provided solely for the purpose of SACE endorsement evaluation.</li>
                <li>No part of the interactive platform or its proprietary methods may be copied, reproduced, or distributed.</li>
                <li>Any generated auditor access codes are strictly for official evaluation use.</li>
            </ul>
        </div>
        
        <!-- Footer -->
        <div class="px-6 py-4 bg-slate-50 border-t border-slate-100 flex justify-end gap-3">
            <button type="button" onclick="closePledgeModal()" class="px-4 py-2 bg-white border border-slate-300 text-slate-700 hover:bg-slate-50 font-bold rounded-lg transition shadow-sm">
                {% if has_pledged %}Close{% else %}Cancel{% endif %}
            </button>
            
            {% if not has_pledged %}
            <form action="{{ url_for('sace_bp.provisioning_pledge') }}" method="POST" class="inline">
                <input type="hidden" name="csrf_token" value="{{ csrf_token() }}"/>
                <button type="submit" class="px-6 py-2 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow-md transition flex items-center">
                    <i class="fas fa-check mr-2"></i> I Agree & Unlock
                </button>
            </form>
            {% endif %}
        </div>
    </div>
</div>

<script>
function openPledgeModal() {
    document.getElementById('pledge-modal').classList.remove('hidden');
}
function closePledgeModal() {
    document.getElementById('pledge-modal').classList.add('hidden');
}
</script>
'''

text = text.replace('<!-- Post-Pledge Registration Modal -->', modal_html + '\n<!-- Post-Pledge Registration Modal -->')

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
