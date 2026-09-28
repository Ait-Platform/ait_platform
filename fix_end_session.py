import re

filepath = 'templates/program_uip/register_import.html'
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Remove the End Session button from the top
search_top_btn = '''    <form method="post">
        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
        <input type="hidden" name="operation" value="reset_batch">
        <button type="submit" class="px-3 py-1.5 border border-slate-600 text-slate-300 hover:text-white hover:bg-slate-700 rounded text-xs font-bold transition">End Session</button>
    </form>'''

content = content.replace(search_top_btn, "")

# Add Finish button at the bottom of the table
search_bottom = '''            </tbody>
        </table>
    </div>
</div>
{% endif %}'''

replace_bottom = '''            </tbody>
        </table>
    </div>
    
    {% if import_status.relationships %}
    <div class="bg-emerald-50 border-t border-emerald-200 p-6 flex items-center justify-between">
        <div>
            <h3 class="text-lg font-bold text-emerald-800"><i class="fas fa-check-circle mr-2"></i> Vault Upload Complete</h3>
            <p class="text-sm text-emerald-700 mt-1">All three core tables have been successfully imported and linked.</p>
        </div>
        <form method="post">
            <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
            <input type="hidden" name="operation" value="reset_batch">
            <button type="submit" class="px-6 py-3 bg-emerald-600 hover:bg-emerald-700 text-white font-bold rounded-lg shadow transition flex items-center">
                Close Wizard & Return to Dashboard <i class="fas fa-arrow-right ml-2"></i>
            </button>
        </form>
    </div>
    {% endif %}
    
</div>
{% endif %}'''

content = content.replace(search_bottom, replace_bottom)

with open(filepath, 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated UI to move End Session to the final step")
