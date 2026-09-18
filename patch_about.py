import re

with open("templates/program_uip/public_about.html", "r", encoding="utf-8") as f:
    text = f.read()

old_block = """            <div class="p-8 bg-gradient-to-b from-slate-900 to-indigo-900 rounded-2xl shadow-xl border border-indigo-900 relative overflow-hidden">
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
            </div>"""

new_block = """            <div class="p-8 bg-slate-100 rounded-2xl shadow-md border border-slate-200 relative overflow-hidden">
                <div class="absolute top-0 right-0 bg-indigo-100 text-indigo-800 text-xs font-bold px-4 py-1.5 rounded-bl-lg uppercase tracking-wider">
                    AIT Package Pricing
                </div>
                <div class="text-center mb-6">
                    <h3 class="text-2xl font-black text-slate-900 mb-2 mt-2">Token Wallet Pricing</h3>
                    <p class="text-slate-600 text-md max-w-lg mx-auto">The Organization operates on a transparent token wallet system to fund platform administration.</p>
                </div>
                
                <div class="bg-white rounded-xl p-6 border border-slate-200 max-w-xl mx-auto shadow-sm">
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-slate-900 font-bold text-lg">100 Free Tokens</span>
                    </div>
                    <div class="text-slate-500 text-sm mb-4 pb-4 border-b border-slate-100">Included instantly for the Organization upon genesis.</div>
                    
                    <div class="flex items-center justify-between mb-1">
                        <span class="text-slate-900 font-bold text-lg">50 Tokens / month</span>
                    </div>
                    <div class="text-slate-500 text-sm mb-5 pb-5 border-b border-slate-100">Thereafter. Reviewed yearly.</div>
                    
                    <div class="text-center bg-slate-50 rounded-lg py-3 border border-slate-200">
                        <span class="text-slate-700 font-black text-sm tracking-widest">1 TOKEN = 1 ZAR</span>
                    </div>
                </div>
            </div>"""

text = text.replace(old_block, new_block)

with open("templates/program_uip/public_about.html", "w", encoding="utf-8") as f:
    f.write(text)
print("Updated pricing UI")
