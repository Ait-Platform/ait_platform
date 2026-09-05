import re

def refactor():
    with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
        iw_html = f.read()

    projector_match = re.search(r'<!-- Projector Screen \(Slides\) -->(.*?)<!-- Participant App Interface \(The Interactive Compliance\) -->', iw_html, re.DOTALL)
    projector_html = projector_match.group(1).strip()
    
    with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
        fd_html = f.read()
        
    fd_html_new = re.sub(
        r'<!-- Projector Preview -->(.*?)<!-- Facilitator Controls -->',
        f'<!-- Projector Screen (Slides) -->\n<div class="flex flex-col">\n{projector_html}\n</div>\n\n<!-- Facilitator Controls -->',
        fd_html,
        flags=re.DOTALL
    )
    
    with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
        f.write(fd_html_new)
        
    iw_html_new = re.sub(r'<div class="flex-grow grid grid-cols-1 lg:grid-cols-2 gap-8">.*?<!-- Participant App Interface \(The Interactive Compliance\) -->', '<div class="flex-grow flex justify-center">\n<!-- Participant App Interface (The Interactive Compliance) -->', iw_html, flags=re.DOTALL)
    
    iw_html_new = iw_html_new.replace(
        '<div class=\"bg-slate-50 border-2 border-slate-200 rounded-xl overflow-hidden flex flex-col relative\">',
        '<div class=\"bg-slate-50 border-2 border-slate-200 rounded-xl overflow-hidden flex flex-col relative w-full max-w-md shadow-2xl\">'
    )
    
    with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
        f.write(iw_html_new)

if __name__ == '__main__':
    refactor()
