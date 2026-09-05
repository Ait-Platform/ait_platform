import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Add submitPoll to JS
submit_poll_js = """
    function submitPoll(pollId, answer, btn) {
        btn.classList.add('ring-4', 'ring-indigo-300');
        const csrfToken = document.querySelector('meta[name="csrf-token"]').getAttribute('content'); 
        fetch('/sace/workshop/submit_poll', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRFToken': csrfToken
            },
            body: JSON.stringify({ poll_id: pollId, data: answer })
        })
        .then(() => {
            setTimeout(() => alert('Response Recorded: Your answer is logged on the facilitator dashboard.'), 200);
        });
    }
"""
content = content.replace("function selectAnswer(btn) {\n        btn.classList.add('ring-4', 'ring-indigo-300');\n        setTimeout(() => {\n            alert('Response Recorded: Your answer is logged on the facilitator dashboard.');\n        }, 600);\n    }", submit_poll_js)

# Update Slide 0 Buttons
content = content.replace('''onclick="selectAnswer(this)">FALSE (No Crisis)</button>''', '''onclick="submitPoll('poll_crisis', 'FALSE', this)">FALSE (No Crisis)</button>''')
content = content.replace('''onclick="selectAnswer(this)">TRUE (Crisis Exists)</button>''', '''onclick="submitPoll('poll_crisis', 'TRUE', this)">TRUE (Crisis Exists)</button>''')

# Update Slide 3 Buttons
content = content.replace('''onclick="alert('Logged!')">A) Lack of resources/books</button>''', '''onclick="submitPoll('poll_root_cause', 'A', this)">A) Lack of resources/books</button>''')
content = content.replace('''onclick="alert('Logged!')">B) Overcrowded classrooms</button>''', '''onclick="submitPoll('poll_root_cause', 'B', this)">B) Overcrowded classrooms</button>''')
content = content.replace('''onclick="alert('Logged!')">C) Language barriers (HL vs English)</button>''', '''onclick="submitPoll('poll_root_cause', 'C', this)">C) Language barriers (HL vs English)</button>''')
content = content.replace('''onclick="alert('Logged!')">D) Outdated teaching methods</button>''', '''onclick="submitPoll('poll_root_cause', 'D', this)">D) Outdated teaching methods</button>''')

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
