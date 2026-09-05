import os

with open('scratch/raw_f_slides.html', 'r', encoding='utf-8') as f:
    f_slides = f.read()

with open('scratch/raw_p_views.html', 'r', encoding='utf-8') as f:
    p_views = f.read()

simulator_html = f"""{{% extends "layout.html" %}}

{{% block title %}}SACE Endorsement Simulator{{% endblock %}}

{{% block content %}}
<!-- STRICT AGENT.MD TILE STYLING -->
<div class="container mx-auto px-4 py-8 max-w-7xl">
    <div class="bg-white rounded-xl shadow-xl overflow-hidden border border-slate-200 flex flex-col h-[85vh]">
        
        <!-- Color Strip (Agent.md Rule 3) -->
        <div class="h-3 w-full bg-indigo-600"></div>
        
        <!-- Row 1: Header (Agent.md Rule 4) -->
        <div class="p-6 border-b border-slate-100 flex justify-between items-center">
            <h1 class="text-2xl font-extrabold text-slate-800"><i class="fas fa-desktop text-indigo-600 mr-2"></i> SACE Endorsement Simulator</h1>
            <a href="{{{{ url_for('core_bp.dashboard') }}}}" class="text-slate-500 hover:text-slate-700 font-semibold"><i class="fas fa-arrow-left mr-1"></i> Back to Dashboard</a>
        </div>

        <!-- Traffic Robot Tabs -->
        <div class="flex space-x-2 border-b-2 border-slate-700 pb-0 bg-slate-50 pt-2 px-4 shadow-inner z-10">
            <button id="btn-tab-a" onclick="showTab('a')" class="flex items-center px-6 py-3 bg-indigo-600 text-white font-bold rounded-t-lg transition border-b-2 border-indigo-400">
                <i class="fas fa-book-open mr-2"></i> Guide (A)
            </button>
            <button id="btn-tab-f" onclick="showTab('f')" class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700">
                <div id="light-f" class="w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3"></div> Facilitator (F)
            </button>
            <button id="btn-tab-p" onclick="showTab('p')" class="flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700">
                <div id="light-p" class="w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3"></div> Participant (P)
            </button>
        </div>

        <!-- Main Content Area -->
        <div class="flex-grow overflow-hidden relative bg-slate-100">
            
            <!-- ========================================== -->
            <!-- TAB A: GUIDE -->
            <!-- ========================================== -->
            <div id="tab-a" class="w-full h-full p-8 overflow-y-auto bg-white">
                <h2 class="text-3xl font-extrabold text-indigo-900 mb-6">SACE Auditor Guide</h2>
                <p class="text-lg text-slate-700 mb-8">Welcome to the Endorsement Simulator. This tool guides you through the exact flow of the workshop using the real slides.</p>
                
                <div class="bg-slate-50 p-6 rounded-xl border border-slate-200 mb-8">
                    <h3 class="font-bold text-slate-800 mb-4"><i class="fas fa-traffic-light text-indigo-600 mr-2"></i> The Traffic Robot System</h3>
                    <p class="mb-2">Follow the <strong>Green Light</strong> to understand how the facilitator controls the teacher's device.</p>
                    <ul class="list-disc list-inside text-slate-600 space-y-2">
                        <li>When <strong>F is Green</strong>, you are the Facilitator. Use the 'Next Slide' button to present.</li>
                        <li>When <strong>P is Green</strong>, you are the Teacher. Interact with the content that just arrived on your screen.</li>
                    </ul>
                </div>

                <button onclick="launchDemo()" class="px-8 py-4 bg-indigo-600 hover:bg-indigo-700 text-white font-black text-xl rounded-xl shadow-lg transition flex items-center">
                    Launch Full SACE Program <i class="fas fa-arrow-right ml-3"></i>
                </button>
            </div>

            <!-- ========================================== -->
            <!-- TAB F: FACILITATOR -->
            <!-- ========================================== -->
            <div id="tab-f" class="w-full h-full hidden flex-col relative bg-slate-900">
                <div class="flex-grow relative overflow-hidden">
                    {f_slides}
                </div>
                
                <!-- Persistent F Controller -->
                <div class="bg-slate-800 p-4 border-t border-slate-700 flex justify-between items-center shadow-lg z-50">
                    <button onclick="prevFSlide()" class="px-6 py-3 bg-slate-700 hover:bg-slate-600 text-white font-bold rounded-lg transition">
                        <i class="fas fa-chevron-left mr-2"></i> Prev Slide
                    </button>
                    <div class="text-center text-slate-400 font-mono">
                        Slide <span id="f-counter">0</span> / 11
                    </div>
                    <div class="flex space-x-3">
                        <button onclick="peekParticipant()" class="px-6 py-3 bg-indigo-600 hover:bg-indigo-500 text-white font-bold rounded-lg transition shadow-[0_0_15px_rgba(79,70,229,0.5)]">
                            <i class="fas fa-mobile-alt mr-2"></i> View Teacher's Device
                        </button>
                        <button onclick="nextFSlide()" class="px-6 py-3 bg-emerald-600 hover:bg-emerald-500 text-white font-bold rounded-lg transition shadow-[0_0_15px_rgba(16,185,129,0.5)]">
                            Next Slide <i class="fas fa-chevron-right ml-2"></i>
                        </button>
                    </div>
                </div>
            </div>

            <!-- ========================================== -->
            <!-- TAB P: PARTICIPANT -->
            <!-- ========================================== -->
            <div id="tab-p" class="w-full h-full hidden flex-col bg-slate-100">
                <div class="flex-grow p-4 md:p-8 flex items-start justify-center overflow-y-auto">
                    <!-- Mobile Frame Mockup -->
                    <div class="w-full max-w-sm bg-white rounded-[2rem] border-[12px] border-slate-800 shadow-2xl overflow-hidden h-[750px] flex flex-col relative">
                        <div class="bg-indigo-600 p-4 text-center text-white font-bold shadow-md z-10 flex justify-between items-center">
                            <span>AIT App</span>
                            <span class="text-xs bg-green-400 text-green-900 px-2 py-1 rounded-full"><i class="fas fa-link mr-1"></i>Synced</span>
                        </div>
                        
                        <div class="flex-grow p-4 overflow-y-auto bg-slate-50" id="app-view-container">
                            {p_views}
                        </div>
                    </div>
                </div>
                
                <!-- Persistent P Return -->
                <div class="p-4 border-t border-slate-200 bg-white text-center shadow-[0_-5px_15px_rgba(0,0,0,0.05)] z-50">
                    <button onclick="showTab('f')" class="px-8 py-3 bg-slate-800 hover:bg-slate-700 text-white font-bold rounded-lg transition">
                        <i class="fas fa-undo mr-2"></i> Return to Facilitator View
                    </button>
                </div>
            </div>

        </div>
    </div>
</div>

<script>
    let currentSlide = 0;
    const totalSlides = 11;
    
    const mockAttendees = [
        "Sipho Mkhize", "Jane Naidoo", "Sarah van der Merwe", "Tshepo Modise", 
        "Fatima Patel", "David Smith", "Lerato Ndlovu", "Pieter de Klerk", 
        "Aisha Khan", "Bongani Zungu", "Johan Botha", "Nadia Moodley", 
        "Thandeka Zuma", "Brenda Williams", "Ravi Govender", "Ané Nel", 
        "Kgomotso Phiri", "Michael O'Connor", "Zanele Dlamini", "Yusuf Desai"
    ];

    function showTab(tabId) {{
        document.getElementById('tab-a').classList.add('hidden');
        document.getElementById('tab-f').classList.remove('flex');
        document.getElementById('tab-f').classList.add('hidden');
        document.getElementById('tab-p').classList.remove('flex');
        document.getElementById('tab-p').classList.add('hidden');
        
        const inactiveClass = "flex items-center px-6 py-3 bg-slate-800 text-slate-500 font-bold rounded-t-lg transition border-b-2 border-transparent opacity-50 hover:opacity-100 hover:bg-slate-700";
        document.getElementById('btn-tab-a').className = inactiveClass;
        document.getElementById('btn-tab-f').className = inactiveClass;
        document.getElementById('btn-tab-p').className = inactiveClass;
        
        if (tabId === 'a') {{
            document.getElementById('tab-a').classList.remove('hidden');
            document.getElementById('btn-tab-a').className = "flex items-center px-6 py-3 bg-indigo-600 text-white font-bold rounded-t-lg transition border-b-2 border-indigo-400";
        }} else if (tabId === 'f') {{
            document.getElementById('tab-f').classList.remove('hidden');
            document.getElementById('tab-f').classList.add('flex');
            document.getElementById('btn-tab-f').className = "flex items-center px-6 py-3 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-slate-300";
            setLight('f', 'green');
            setLight('p', 'red');
        }} else if (tabId === 'p') {{
            document.getElementById('tab-p').classList.remove('hidden');
            document.getElementById('tab-p').classList.add('flex');
            document.getElementById('btn-tab-p').className = "flex items-center px-6 py-3 bg-white text-slate-900 font-bold rounded-t-lg transition border-b-2 border-slate-300";
            setLight('p', 'green');
            setLight('f', 'red');
        }}
    }}

    function setLight(tab, color) {{
        const light = document.getElementById('light-' + tab);
        if (!light) return;
        if (color === 'green') {{
            light.className = "w-3 h-3 rounded-full bg-green-500 shadow-[0_0_12px_rgba(34,197,94,1)] mr-3 animate-pulse";
        }} else {{
            light.className = "w-3 h-3 rounded-full bg-red-600 shadow-[0_0_8px_rgba(220,38,38,0.5)] mr-3";
        }}
    }}

    function launchDemo() {{
        currentSlide = 0;
        updateSlides();
        showTab('f');
    }}

    function prevFSlide() {{
        if (currentSlide > 0) {{
            currentSlide--;
            updateSlides();
        }}
    }}

    function nextFSlide() {{
        if (currentSlide < totalSlides) {{
            currentSlide++;
            updateSlides();
            
            // Random Dice Roll for mock surveys
            triggerRandomDice(currentSlide);
            
            // Auto-pop the P tab if it's an interactive slide to show cause and effect
            if ([0, 3, 4, 5, 8, 9, 10, 11].includes(currentSlide)) {{
                setTimeout(() => {{
                    peekParticipant();
                }}, 1500);
            }}
        }}
    }}

    function peekParticipant() {{
        showTab('p');
    }}

    function updateSlides() {{
        // Update F Slides
        document.querySelectorAll('.slide-container').forEach(el => {{
            el.classList.remove('flex');
            el.classList.add('hidden');
        }});
        
        let fId = currentSlide === 0 ? 'slide-lobby' : 'slide-' + currentSlide;
        const fSlide = document.getElementById(fId);
        if (fSlide) {{
            fSlide.classList.remove('hidden');
            fSlide.classList.add('flex');
        }}
        
        document.getElementById('f-counter').innerText = currentSlide;

        // Update P Views (exactly matching the old logic)
        let pIndex = 1; // default projector view
        if (currentSlide === 0) pIndex = 0;
        if (currentSlide === 3) pIndex = 2;
        if (currentSlide === 4) pIndex = 3;
        if (currentSlide === 5) pIndex = 4;
        if (currentSlide === 8) pIndex = 5;
        if (currentSlide === 9) pIndex = 8;
        if (currentSlide === 10) pIndex = 6;
        if (currentSlide === 11) pIndex = 9;

        document.querySelectorAll('.app-view').forEach(el => {{
            el.classList.add('hidden');
        }});
        const pView = document.getElementById('app-view-' + pIndex);
        if (pView) {{
            pView.classList.remove('hidden');
        }}
    }}

    function mockPoll(type) {{
        alert("Teacher response recorded locally! In the live app, this saves to PostgreSQL.");
    }}

    // Make the survey results jump like a tossing dice
    function triggerRandomDice(slideNum) {{
        // Slide 2: Pre-test true/false
        if (slideNum === 2) {{
            const trues = Math.floor(Math.random() * 15) + 5; // 5 to 20
            const falses = 20 - trues;
            animateBar('slide-2', 0, trues, falses);
        }}
        // Slide 4: Root Cause A/B/C/D
        if (slideNum === 4) {{
            animateBar('slide-4', 0, Math.floor(Math.random() * 10), Math.floor(Math.random() * 5), Math.floor(Math.random() * 3), 2);
        }}
        // Slide 5: Top Challenges
        if (slideNum === 5) {{
            animateBar('slide-5', 0, Math.floor(Math.random() * 8), Math.floor(Math.random() * 8), Math.floor(Math.random() * 4));
        }}
    }}
    
    function animateBar(slideId, iter, ...values) {{
        const slide = document.getElementById(slideId);
        if (!slide) return;
        const bars = slide.querySelectorAll('.bg-slate-700 > div, .w-full > div');
        if (bars.length === 0) return;
        
        let total = values.reduce((a, b) => a + b, 0) || 20;
        
        bars.forEach((bar, index) => {{
            if (values[index] !== undefined) {{
                // Add some random jitter if iter < 10 to simulate real-time voting
                let currentVal = iter < 10 ? Math.floor(Math.random() * total) : values[index];
                let pct = (currentVal / total) * 100;
                bar.style.width = pct + '%';
                bar.style.transition = 'width 0.5s ease';
            }}
        }});
        
        if (iter < 10) {{
            setTimeout(() => animateBar(slideId, iter + 1, ...values), 500);
        }}
    }}

    // Populate the lobby roster
    document.addEventListener('DOMContentLoaded', function() {{
        const rosterList = document.getElementById('roster-list');
        const counter = document.getElementById('permanent-counter');
        const counterLobby = document.getElementById('attendance-counter');
        
        if (rosterList) {{
            rosterList.innerHTML = '';
            mockAttendees.forEach((person, index) => {{
                let sace = "10" + Math.floor(Math.random() * 900000 + 100000);
                rosterList.innerHTML += '<li class="p-4 hover:bg-white transition flex justify-between items-center">' +
                    '<div><p class="font-bold text-slate-800">' + person + '</p>' +
                    '<p class="text-xs text-slate-500 font-mono">SACE: ' + sace + '</p></div>' +
                    '<span class="bg-green-100 text-green-800 px-2 py-1 rounded text-xs font-bold"><i class="fas fa-check-circle mr-1"></i> Verified</span></li>';
            }});
        }}
        if (counter) counter.innerText = mockAttendees.length;
        if (counterLobby) counterLobby.innerText = mockAttendees.length;
    }});

    // Setup initial state
    showTab('a');
</script>
{{% endblock %}}
"""

with open('templates/program_sace/simulator.html', 'w', encoding='utf-8') as f:
    f.write(simulator_html)
