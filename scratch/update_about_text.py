import re

html_path = 'templates/program_sace/provisioning_map.html'
with open(html_path, 'r', encoding='utf-8') as f:
    text = f.read()

old_about = '''            <p class="text-slate-600 text-base leading-relaxed mb-2">
                Due to the proprietary nature of the AIT LITRE Simulator and interactive methodology, standard PDF document review is insufficient. AIT has generated this secure portal to allow you to seamlessly provision SACE Auditors, generating secure access links for them to evaluate the digital framework firsthand while maintaining intellectual property compliance, and an integrated document tracker allowing you to monitor when required forms are viewed or downloaded.
            </p>'''

new_about = '''            <p class="text-slate-600 text-base leading-relaxed mb-2">
                AIT has generated this secure portal to allow you to seamlessly assign and monitor SACE Auditors and document flows by :
            </p>
            <ul class="list-disc list-inside text-slate-600 text-base leading-relaxed mb-2 ml-2 space-y-1">
                <li>generating secure access links for them to evaluate the digital framework</li>
                <li>integrating document tracker allowing you to monitor all forms and auditors.</li>
                <li>maintaining intellectual property compliance,</li>
            </ul>'''

text = text.replace(old_about, new_about)

with open(html_path, 'w', encoding='utf-8') as f:
    f.write(text)
