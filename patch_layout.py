import re

with open("templates/program_uip/public_about.html", "r", encoding="utf-8") as f:
    text = f.read()

# 1. Remove the fa-city icon from the title
text = text.replace('<i class="fas fa-city text-indigo-600 mr-2"></i> Urban Improvement Precincts (UIP)', 'Urban Improvement Precincts (UIP)')

# 2. Extract the Pricing Block and change grid to 2 columns
# Let's use standard string replacement for the grid structure
old_section = """        <div class="grid md:grid-cols-3 gap-6 mb-12 items-stretch">
            <div class="p-6 bg-slate-50 rounded-xl border border-slate-200 shadow-sm flex flex-col h-full">
                <h3 class="text-xl font-bold text-slate-800 mb-3 mt-2">Resident Reporting</h3>
                <p class="text-slate-600 flex-grow">Quickly snap photos and report localized issues like potholes, streetlights, or security concerns directly to your precinct.</p>
            </div>
            <div class="p-6 bg-slate-50 rounded-xl border border-slate-200 shadow-sm flex flex-col h-full">
                <h3 class="text-xl font-bold text-slate-800 mb-3 mt-2">Smart Work Orders</h3>
                <p class="text-slate-600 flex-grow">Issues are automatically converted into tracked work orders for contractors, ensuring rapid resolution and municipal oversight.</p>
            </div>
            <!-- Column 3: The Pricing Wallet -->
            <div class="p-6 bg-gradient-to-b from-slate-900 to-indigo-900 rounded-xl shadow-lg border border-indigo-900 flex flex-col h-full relative overflow-hidden">
                <div class="absolute top-0 right-0 bg-amber-500 text-amber-950 text-xs font-bold px-3 py-1 rounded-bl-lg uppercase tracking-wide">
                    AIT Package Pricing
                </div>
                <h3 class="text-xl font-bold text-white mb-3 mt-4">Token Wallet Pricing</h3>
                <p class="text-indigo-200 mb-5 text-sm flex-grow">The Organization operates on a transparent token wallet system to fund platform administration.</p>
                
                <div class="bg-white/10 rounded-lg p-4 border border-white/20">
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-white font-bold text-md">100 Free Tokens</span>
                    </div>
                    <div class="text-indigo-200 text-xs mb-3 pb-3 border-b border-white/10">Included instantly for the Organization.</div>
                    
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-white font-bold text-md">50 Tokens / month</span>
                    </div>
                    <div class="text-indigo-200 text-xs mb-3 pb-3 border-b border-white/10">Thereafter. Reviewed yearly.</div>
                    
                    <div class="text-center bg-indigo-950/50 rounded py-1.5 mt-2">
                        <span class="text-amber-400 font-bold text-xs tracking-widest">1 TOKEN = 1 ZAR</span>
                    </div>
                </div>
            </div>
        </div>"""

new_section = """        <div class="grid md:grid-cols-2 gap-6 mb-8 items-stretch">
            <div class="p-8 bg-slate-100 rounded-xl border border-slate-200 shadow-md flex flex-col h-full transition hover:shadow-lg">
                <h3 class="text-xl font-bold text-slate-800 mb-3">Resident Reporting</h3>
                <p class="text-slate-600 flex-grow text-lg">Quickly snap photos and report localized issues like potholes, streetlights, or security concerns directly to your precinct.</p>
            </div>
            <div class="p-8 bg-slate-100 rounded-xl border border-slate-200 shadow-md flex flex-col h-full transition hover:shadow-lg">
                <h3 class="text-xl font-bold text-slate-800 mb-3">Smart Work Orders</h3>
                <p class="text-slate-600 flex-grow text-lg">Issues are automatically converted into tracked work orders for contractors, ensuring rapid resolution and municipal oversight.</p>
            </div>
        </div>
        
        <!-- Full Width Pricing Block Below -->
        <div class="max-w-3xl mx-auto mb-12">
            <div class="p-8 bg-gradient-to-b from-slate-900 to-indigo-900 rounded-2xl shadow-xl border border-indigo-900 relative overflow-hidden">
                <div class="absolute top-0 right-0 bg-amber-500 text-amber-950 text-xs font-bold px-4 py-1.5 rounded-bl-lg uppercase tracking-wider shadow-sm">
                    AIT Package Pricing
                </div>
                <div class="text-center mb-6">
                    <h3 class="text-2xl font-black text-white mb-2 mt-2">Token Wallet Pricing</h3>
                    <p class="text-indigo-200 text-md max-w-lg mx-auto">The Organization operates on a transparent token wallet system to fund platform administration.</p>
                </div>
                
                <div class="bg-white/10 rounded-xl p-6 border border-white/20 max-w-xl mx-auto backdrop-blur-sm">
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-white font-bold text-lg">100 Free Tokens</span>
                    </div>
                    <div class="text-indigo-200 text-sm mb-4 pb-4 border-b border-white/10">Included instantly for the Organization upon genesis.</div>
                    
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-white font-bold text-lg">50 Tokens / month</span>
                    </div>
                    <div class="text-indigo-200 text-sm mb-5 pb-5 border-b border-white/10">Thereafter. Reviewed yearly.</div>
                    
                    <div class="text-center bg-indigo-950/60 rounded-lg py-3 border border-indigo-900/50 shadow-inner">
                        <span class="text-amber-400 font-bold text-sm tracking-widest">1 TOKEN = 1 ZAR</span>
                    </div>
                </div>
            </div>
        </div>"""

text = text.replace(old_section, new_section)

with open("templates/program_uip/public_about.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated public_about.html layout")
