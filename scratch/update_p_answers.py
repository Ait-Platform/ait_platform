import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    text = f.read()

# Modify submitPoll to send local message if embedded
old_submit_poll = r"""    function submitPoll\(pollId, answer, btn\) \{
        btn\.classList\.add\('ring-4', 'ring-indigo-300'\);
        const csrfToken = document\.querySelector\('meta\[name="csrf-token"\]'\)\.getAttribute\('content'\); 
        fetch\('/sace/workshop/submit_poll'"""

new_submit_poll = """    function submitPoll(pollId, answer, btn) {
        btn.classList.add('ring-4', 'ring-indigo-300');
        
        if (isEmbedded) {
            window.parent.postMessage({action: 'participantAnswer', type: 'poll', pollId: pollId, answer: answer}, '*');
        }
        
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content'); 
        fetch('/sace/workshop/submit_poll'"""

text = re.sub(old_submit_poll, new_submit_poll, text)

# Modify submitLog to send local message if embedded
old_submit_log = r"""    function submitLog\(message\) \{
        const csrfToken = document\.querySelector\('meta\[name="csrf-token"\]'\)\.getAttribute\('content'\); 
        fetch\('/sace/workshop/submit_interaction'"""

new_submit_log = """    function submitLog(message) {
        if (isEmbedded) {
            window.parent.postMessage({action: 'participantAnswer', type: 'log', message: message}, '*');
        }
        
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content'); 
        fetch('/sace/workshop/submit_interaction'"""

text = re.sub(old_submit_log, new_submit_log, text)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(text)
