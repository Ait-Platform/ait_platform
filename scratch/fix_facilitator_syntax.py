import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

# I will find the exact string that is currently broken and replace it.
broken_text = """        function startWorkshop() {
        sessionState = 'active'; 
        currentSlide = 1; 
        updateView();
    }})
            .then(() => fetchState());
    }"""

fixed_text = """        function startWorkshop() {
        sessionState = 'active'; 
        currentSlide = 1; 
        updateView();
    }"""

content = content.replace(broken_text, fixed_text)

# Also fix resetWorkshop which was similarly broken? Let's check it.
broken_reset = """        function resetWorkshop() {
        if(confirm('End workshop?')) {
            sessionState = 'lobby'; 
            currentSlide = 0; 
            updateView();
        }
    }
                })
                .then(() => location.reload());
            }
        }"""

fixed_reset = """        function resetWorkshop() {
        if(confirm('End workshop?')) {
            sessionState = 'lobby'; 
            currentSlide = 0; 
            updateView();
        }
    }"""
content = content.replace(broken_reset, fixed_reset)


with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Fixed JS syntax error in facilitator dashboard")
