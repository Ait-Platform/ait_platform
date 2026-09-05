import re

file_path = 'templates/program_sace/simulator.html'
with open(file_path, 'r', encoding='utf-8') as f:
    text = f.read()

# I will replace everything from <script> to the end of the file.
new_script = '''<script>
    const simSteps = [
        { slide: -1, view: 'f' },
        { slide: 0, view: 'f' },
        { slide: 1, view: 'f' },
        { slide: 1, view: 'p', appView: 0 },
        { slide: 2, view: 'f' },
        { slide: 3, view: 'f' },
        { slide: 4, view: 'f' },
        { slide: 5, view: 'f' },
        { slide: 5, view: 'p', appView: 2 },
        { slide: 6, view: 'f' },
        { slide: 6, view: 'p', appView: 3 },
        { slide: 7, view: 'f' },
        { slide: 8, view: 'f' },
        { slide: 8, view: 'p', appView: 4 },
        { slide: 9, view: 'f' },
        { slide: 10, view: 'f' },
        { slide: 11, view: 'f' },
        { slide: 12, view: 'f' },
        { slide: 12, view: 'p', appView: 5 },
        { slide: 13, view: 'f' },
        { slide: 13, view: 'p', appView: 8 },
        { slide: 14, view: 'f' },
        { slide: 15, view: 'f' },
        { slide: 15, view: 'p', appView: 6 },
        { slide: 16, view: 'f' },
        { slide: 17, view: 'f' },
        { slide: 18, view: 'f' },
        { slide: 19, view: 'f' },
        { slide: 20, view: 'f' },
        { slide: 21, view: 'f' },
        { slide: 21, view: 'p', appView: 12 },
        { slide: 22, view: 'f' },
        { slide: 23, view: 'f' },
        { slide: 24, view: 'f' },
        { slide: 24, view: 'p', appView: 9 },
        { slide: 25, view: 'f' },
        { slide: 25, view: 'p', appView: 10 },
        { slide: 26, view: 'f' },
        { slide: 26, view: 'p', appView: 7 },
        { slide: 27, view: 'f' },
        { slide: 27, view: 'p', appView: 11 }
    ];

    let currentStepIndex = 0;
    
    const mockAttendees = [
        "Sipho Mkhize", "Jane Naidoo", "Sarah van der Merwe", "Tshepo Modise", 
        "Fatima Patel", "David Smith", "Lerato Ndlovu", "Pieter de Klerk", 
        "Aisha Khan", "Bongani Zungu", "Johan Botha", "Nadia Moodley", 
        "Thandeka Zuma", "Brenda Williams", "Ravi Govender", "AnAc Nel", 
        "Kgomotso Phiri", "Michael O'Connor", "Zanele Dlamini", "Yusuf Desai"
    ];

    function showTab(tabId) {
        document.getElementById('tab-a').classList.add('hidden');
        document.getElementById('tab-f').classList.remove('flex');
        document.getElementById('tab-f').classList.add('hidden');
        document.getElementById('tab-p').classList.remove('flex');
        document.getElementById('tab-p').classList.add('hidden');
        
        const inactiveClass = "flex items-center px-6 py-3 bg-slate-200 text-slate-600 font-bold rounded-t-lg transition border-b-2 border-slate-300 hover:bg-slate-300 cursor-pointer";
        document.getElementById('btn-tab-a').className = inactiveClass;
        document.getElementById('btn-tab-f').className = inactiveClass;
        document.getElementById('btn-tab-p').className = inactiveClass;
        
        if (tabId === 'a') {
            document.getElementById('tab-a').classList.remove('hidden');
            document.getElementById('btn-tab-a').className = "flex items-center px-6 py-3 bg-indigo-600 text-white font-bold rounded-t-lg transition border-b-2 border-indigo-400";
        } else if (tabId === 'f') {
            document.getElementById('tab-f').classList.remove('hidden');
            document.getElementById('tab-f').classList.add('flex');
            document.getElementById('btn-tab-f').className = "flex items-center px-6 py-3 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-slate-300";
            setLight('f', 'green');
            setLight('p', 'red');
        } else if (tabId === 'p') {
            document.getElementById('tab-p').classList.remove('hidden');
            document.getElementById('tab-p').classList.add('flex');
            document.getElementById('btn-tab-p').className = "flex items-center px-6 py-3 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-slate-300";
            setLight('p', 'green');
            setLight('f', 'red');
        }
    }

    function setLight(tab, color) {
        const light = document.getElementById('light-' + tab);
        if (!light) return;
        if (color === 'green') {
            light.className = "w-3 h-3 rounded-full bg-green-500 shadow-[0_0_12px_rgba(34,197,94,1)] mr-3 animate-pulse";
        } else {
            light.className = "w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3";
        }
    }

    function launchDemo() {
        currentStepIndex = 0;
        applyStep();
        document.getElementById("global-controller").style.display = "flex";
    }

    function playCurrentSlideAudio() {
        let step = simSteps[currentStepIndex];
        let fId = step.slide === -1 ? 'slide-lobby' : 'slide-' + step.slide;
        const fSlide = document.getElementById(fId);
        if (fSlide) {
            const audio = fSlide.querySelector('audio');
            if (audio) {
                const btn = document.getElementById('global-audio-btn');
                if (audio.paused) {
                    audio.play();
                    btn.innerHTML = '<i class="fas fa-pause mr-2"></i> Pause Audio';
                    btn.classList.replace('bg-purple-600', 'bg-amber-500');
                    btn.classList.replace('hover:bg-purple-500', 'hover:bg-amber-400');
                    btn.classList.replace('shadow-[0_0_15px_rgba(147,51,234,0.5)]', 'shadow-[0_0_15px_rgba(245,158,11,0.5)]');
                } else {
                    audio.pause();
                    btn.innerHTML = '<i class="fas fa-play mr-2"></i> Play Audio';
                    btn.classList.replace('bg-amber-500', 'bg-purple-600');
                    btn.classList.replace('hover:bg-amber-400', 'hover:bg-purple-500');
                    btn.classList.replace('shadow-[0_0_15px_rgba(245,158,11,0.5)]', 'shadow-[0_0_15px_rgba(147,51,234,0.5)]');
                }
            }
        }
    }

    function nextStep() {
        if (currentStepIndex < simSteps.length - 1) {
            currentStepIndex++;
            applyStep();
        } else {
            alert("Simulation Complete! Endorsement demonstration finished.");
            document.getElementById("global-controller").style.display = "none";
            showTab('a');
        }
    }

    function applyStep() {
        const step = simSteps[currentStepIndex];
        
        // Update Counter
        let displaySlide = step.slide < 0 ? 0 : step.slide + 1;
        document.getElementById('f-counter-global').innerText = displaySlide + " of 28";

        // Update F Slides
        document.querySelectorAll('.slide-container').forEach(el => {
            el.classList.remove('flex');
            el.classList.add('hidden');
        });
        
        let fId = step.slide === -1 ? 'slide-lobby' : 'slide-' + step.slide;
        const fSlide = document.getElementById(fId);
        if (fSlide) {
            fSlide.classList.remove('hidden');
            fSlide.classList.add('flex');
        }

        // Audio Logic: Pause everything first
        document.querySelectorAll('audio').forEach(audio => {
            audio.pause();
            audio.currentTime = 0;
        });
        
        // Show/hide the global audio button
        const audioBtn = document.getElementById('global-audio-btn');
        if (fSlide && fSlide.querySelector('audio')) {
            audioBtn.classList.remove('hidden');
            audioBtn.innerHTML = '<i class="fas fa-play mr-2"></i> Play Audio';
        } else {
            audioBtn.classList.add('hidden');
        }

        // Update P Views
        document.querySelectorAll('.app-view').forEach(el => {
            el.classList.add('hidden');
        });
        
        if (step.appView !== undefined) {
            const pView = document.getElementById('app-view-' + step.appView);
            if (pView) {
                pView.classList.remove('hidden');
            }
            triggerRandomDice(step.slide);
        } else {
            const waitView = document.getElementById('app-view-waiting');
            if (waitView) waitView.classList.remove('hidden');
        }
        
        // Switch to the correct tab automatically
        showTab(step.view);
    }

    function mockPoll(type) {
        alert("Participant response recorded locally! In the live app, this saves to PostgreSQL.");
    }

    function triggerRandomDice(slideNum) {
        if (slideNum === 1) { // 2Crisis (index 1) uses animateBar on slide-2 ? No, wait. 
            // In the original, it was checking slideNum 2 for slide-2. Now it is slide index 1.
            const trues = Math.floor(Math.random() * 15) + 5;
            const falses = 20 - trues;
            animateBar('slide-1', 0, trues, falses);
        }
        if (slideNum === 5) { // 6Root Cause
            animateBar('slide-5', 0, Math.floor(Math.random() * 10), Math.floor(Math.random() * 5), Math.floor(Math.random() * 3), 2);
        }
        if (slideNum === 6) { // 7Litre
            animateBar('slide-6', 0, Math.floor(Math.random() * 8), Math.floor(Math.random() * 8), Math.floor(Math.random() * 4));
        }
    }
    
    function animateBar(slideId, iter, ...values) {
        const slide = document.getElementById(slideId);
        if (!slide) return;
        const bars = slide.querySelectorAll('.bg-slate-700 > div, .w-full > div');
        if (bars.length === 0) return;
        
        let total = values.reduce((a, b) => a + b, 0) || 20;
        
        bars.forEach((bar, index) => {
            if (values[index] !== undefined) {
                let currentVal = iter < 10 ? Math.floor(Math.random() * total) : values[index];
                let pct = (currentVal / total) * 100;
                bar.style.width = pct + '%';
                bar.style.transition = 'width 0.5s ease';
            }
        });
        
        if (iter < 10) {
            setTimeout(() => animateBar(slideId, iter + 1, ...values), 500);
        }
    }

    document.addEventListener('DOMContentLoaded', function() {
        const rosterList = document.getElementById('roster-list');
        const counter = document.getElementById('permanent-counter');
        const counterLobby = document.getElementById('attendance-counter');
        
        if (rosterList) {
            rosterList.innerHTML = '';
            mockAttendees.forEach((person, index) => {
                let sace = "10" + Math.floor(Math.random() * 900000 + 100000);
                rosterList.innerHTML += '<li class="p-4 hover:bg-white transition flex justify-between items-center">' +
                    '<div><p class="font-bold text-slate-800">' + person + '</p>' +
                    '<p class="text-xs text-slate-500 font-mono">SACE: ' + sace + '</p></div>' +
                    '<span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs font-bold"><i class="fas fa-check-circle mr-1"></i> Verified</span></li>';
            });
        }
        if (counter) counter.innerText = mockAttendees.length;
        if (counterLobby) counterLobby.innerText = mockAttendees.length;
    });

    showTab('a');
</script>'''

text = re.sub(r'<script>.*?</script>', new_script, text, flags=re.DOTALL)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(text)

