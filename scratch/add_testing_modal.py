import re

# 1. Add reset_progress route to app/program_sace/routes.py
routes_file = 'app/program_sace/routes.py'
with open(routes_file, 'r', encoding='utf-8') as f: text = f.read()

reset_route = '''@sace_bp.route('/sace/reset_progress', methods=['POST'])
@login_required
def reset_evaluator_progress():
    from app.models.sace import SaceWorkshopInteraction
    from app.extensions import db
    from sqlalchemy import text as sa_text
    
    # Delete SACE Hub Interactions
    SaceWorkshopInteraction.query.filter_by(user_id=current_user.id).delete()
    
    # Delete Reading Course Progress
    db.session.execute(sa_text("DELETE FROM rdp_lesson_progress WHERE user_id = :uid"), {"uid": current_user.id})
    db.session.execute(sa_text("DELETE FROM rdp_enrollment WHERE user_id = :uid"), {"uid": current_user.id})
    
    db.session.commit()
    flash("Testing Mode: Progress has been completely reset. You are starting from the beginning.", "success")
    return redirect(url_for('sace_bp.reading_hub'))

@sace_bp.route('/sace/hub/reading')'''

if "def reset_evaluator_progress" not in text:
    text = text.replace("@sace_bp.route('/sace/hub/reading')", reset_route)
    with open(routes_file, 'w', encoding='utf-8') as f: f.write(text)

# 2. Add Modal to reading_hub.html
hub_file = 'templates/program_sace/reading_hub.html'
with open(hub_file, 'r', encoding='utf-8') as f: hub_text = f.read()

modal_html = '''
        <!-- Testing Mode Reset Modal -->
        {% if completed > 0 and percent < 100 %}
        <div id="testing-modal" class="fixed inset-0 bg-slate-900 bg-opacity-75 z-50 flex items-center justify-center backdrop-blur-sm">
            <div class="bg-white rounded-2xl shadow-2xl max-w-lg w-full p-8 relative transform transition-all text-center border-t-8 border-indigo-600">
                <div class="mx-auto w-16 h-16 bg-indigo-100 rounded-full flex items-center justify-center mb-6">
                    <i class="fas fa-flask text-3xl text-indigo-600"></i>
                </div>
                <h2 class="text-2xl font-black text-slate-800 mb-2">Testing Mode Detected</h2>
                <p class="text-slate-600 mb-8">You have existing progress saved on your account. Would you like to continue from where you left off, or start from the beginning to test the entire flow again?</p>
                
                <div class="flex flex-col space-y-3">
                    <button onclick="document.getElementById('testing-modal').style.display='none'" class="w-full py-3 bg-indigo-600 hover:bg-indigo-700 text-white font-bold rounded-lg shadow transition">
                        <i class="fas fa-play mr-2"></i> Continue from where I left off
                    </button>
                    <form action="{{ url_for('sace_bp.reset_evaluator_progress') }}" method="POST">
                        <input type="hidden" name="csrf_token" value="{{ csrf_token() }}">
                        <button type="submit" class="w-full py-3 bg-slate-100 hover:bg-red-50 text-slate-700 hover:text-red-700 font-bold rounded-lg border border-slate-200 hover:border-red-200 transition">
                            <i class="fas fa-redo mr-2"></i> Start from the beginning (Wipe Progress)
                        </button>
                    </form>
                </div>
            </div>
        </div>
        {% endif %}
'''

if "testing-modal" not in hub_text:
    # Insert modal right after <!-- Header -->
    hub_text = hub_text.replace('<!-- Roadmap Container -->', modal_html + '\n        <!-- Roadmap Container -->')
    with open(hub_file, 'w', encoding='utf-8') as f: f.write(hub_text)
