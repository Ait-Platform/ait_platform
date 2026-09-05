import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add submitLog function
submit_log_js = """
    function submitLog(message) {
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content'); 
        fetch('/sace/workshop/submit_interaction', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({ slide: currentSlide, data: message })
        })
        .then(() => alert(message + ' (Logged for SACE Report)'));
    }
    
    // Initialize
"""
content = content.replace("// Initialize", submit_log_js)

# Update buttons to use submitLog instead of raw alert
content = content.replace("onclick=\"alert('Activity Completed!')\"", "onclick=\"submitLog('Completed Kinesthetic Drill: Vowel Hops')\"")
content = content.replace("alert('Peer Drill Logged!');", "submitLog('Completed Kinesthetic Drill: Number Map Peer Assessment');")
content = content.replace("alert('Flashcard Activity Logged!');", "submitLog('Completed Tactile Engagement: Consonant Flashcards');")
content = content.replace("onclick=\"alert('Notes submitted for CPTD portfolio.')\"", "onclick=\"submitLog('Submitted Demographic Adaptation Strategy')\"")

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
