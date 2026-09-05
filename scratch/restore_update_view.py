import re

with open('templates/program_sace/facilitator_dashboard.html', 'r', encoding='utf-8') as f:
    text = f.read()

bad_update_view = r"""        if \(sessionState === 'lobby'\) {
            document\.getElementById\('controls-lobby'\)\.classList\.remove\('hidden'\);
            document\.getElementById\('controls-active'\)\.classList\.add\('hidden'\);
            const lobbySlide = document\.getElementById\('slide-lobby'\);
            lobbySlide\.classList\.remove\('hidden'\);
            lobbySlide\.classList\.add\('flex'\);
            activeSlide\.classList\.remove\('hidden'\);
            activeSlide\.classList\.add\('flex'\);
        }

        
        }"""

good_update_view = """        if (sessionState === 'lobby') {
            document.getElementById('controls-lobby').classList.remove('hidden');
            document.getElementById('controls-active').classList.add('hidden');
            const lobbySlide = document.getElementById('slide-lobby');
            lobbySlide.classList.remove('hidden');
            lobbySlide.classList.add('flex');
            document.getElementById('slide-counter').innerText = "Lobby";
            return;
        }

        // ACTIVE STATE
        document.getElementById('controls-lobby').classList.add('hidden');
        document.getElementById('controls-active').classList.remove('hidden');

        const activeSlide = document.getElementById('slide-' + currentSlide);
        if (activeSlide) {
            activeSlide.classList.remove('hidden');
            activeSlide.classList.add('flex');
        }"""

text = re.sub(bad_update_view, good_update_view, text)

with open('templates/program_sace/facilitator_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(text)
