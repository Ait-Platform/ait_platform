import os
import re

routes_path = r'D:\Users\yeshk\Documents\ait_platform\app\program_culturalfire\routes.py'

with open(routes_path, 'r', encoding='utf-8') as f:
    routes_content = f.read()

# 1. Replace apply_judge logic
old_logic = """    if current_judges >= 5:
        # Rollover: Find next active show where user is NOT a participant and judges < 5
        all_active_shows = CfiShow.query.filter_by(status='active').order_by(CfiShow.start_date.asc()).all()
        found_alternative = False
        for alt_show in all_active_shows:
            # Skip the original full show
            if alt_show.id == original_show.id:
                continue
                
            # Is user participant?
            if CfiTalentSubmission.query.filter_by(show_id=alt_show.id, user_id=current_user.id).first():
                continue
                
            # Are there < 5 judges?
            alt_judges = CfiJudgeAssignment.query.filter_by(show_id=alt_show.id).count()
            if alt_judges < 5:
                # Is user already a judge for this alternative?
                if CfiJudgeAssignment.query.filter_by(show_id=alt_show.id, judge_id=current_user.id).first():
                    continue
                
                target_show = alt_show
                found_alternative = True
                break
                
        if found_alternative:
            flash(f"'{original_show.title}' was full, so you have been automatically assigned to judge '{target_show.title}' instead!", "info")
        else:
            flash("All current shows are fully judged! Thank you for volunteering.", "info")
            return redirect(url_for('cultural_bp.showcase_dashboard'))
            
    # Assign to target_show"""

new_logic = """    if current_judges >= 5:
        flash("The judging panel for this show is already full. Please choose another show, or wait for the next pageant.", "warning")
        return redirect(url_for('cultural_bp.showcase_dashboard'))
        
    # Assign to target_show"""

if old_logic in routes_content:
    routes_content = routes_content.replace(old_logic, new_logic)
    print("Replaced apply_judge logic")
else:
    print("Could not find apply_judge logic")


# 2. Add admin routes at the end of the file if they don't exist
admin_routes = """
@cultural_bp.route("/admin/cultural_fire")
@login_required
def admin_dashboard():
    # Verify admin role
    if not any(r.role == 'Admin' for r in current_user.roles):
        flash("Unauthorized access.", "danger")
        return redirect(url_for('main.index'))
        
    shows = CfiShow.query.all()
    return render_template("program_culturefire/admin_dashboard.html", shows=shows)

@cultural_bp.route("/show/<int:show_id>/admin_scores")
@login_required
def admin_scores(show_id):
    # Verify admin role
    if not any(r.role == 'Admin' for r in current_user.roles):
        flash("Unauthorized access.", "danger")
        return redirect(url_for('main.index'))
        
    show = CfiShow.query.get_or_404(show_id)
    
    # Get all judges assigned to this show
    judges = CfiJudgeAssignment.query.filter_by(show_id=show.id).options(joinedload(CfiJudgeAssignment.judge)).all()
    
    # Get all contestants
    if show.category_item and show.category_item.name == "Pageant":
        contestants_items = CfiSegmentItem.query.filter_by(show_id=show.id).all()
        contestants_by_user = {}
        for item in contestants_items:
            uid = item.enrollment.user_id if item.enrollment else None
            if uid:
                if uid not in contestants_by_user:
                    name = item.enrollment.biodata.full_name if (item.enrollment and item.enrollment.biodata) else "Unknown"
                    contestants_by_user[uid] = {"name": name, "items": []}
                contestants_by_user[uid]["items"].append(item)
                
        # Get all votes for this show
        # Note: Votes are linked via segment_item_id
        item_ids = [item.id for item in contestants_items]
        votes = CfiShowcaseVote.query.filter(CfiShowcaseVote.segment_item_id.in_(item_ids)).all() if item_ids else []
        
        # Build scoresheet: {contestant_id: {judge_id: total_score}}
        scoresheet = {}
        for uid, data in contestants_by_user.items():
            scoresheet[uid] = {"name": data["name"], "scores": {}}
            for j in judges:
                scoresheet[uid]["scores"][j.judge_id] = 0
                
        for vote in votes:
            # find which user this item belongs to
            uid = next((u for u, data in contestants_by_user.items() if any(i.id == vote.segment_item_id for i in data["items"])), None)
            if uid and vote.user_id in scoresheet[uid]["scores"]:
                scoresheet[uid]["scores"][vote.user_id] += vote.score
                
        contestants_list = list(scoresheet.values())
        
    else:
        # Talent show
        submissions = CfiTalentSubmission.query.filter_by(show_id=show.id).all()
        sub_ids = [s.id for s in submissions]
        votes = CfiShowcaseVote.query.filter(CfiShowcaseVote.submission_id.in_(sub_ids)).all() if sub_ids else []
        
        scoresheet = {}
        for sub in submissions:
            name = sub.talent_name or "Unknown"
            if sub.user_enrollment and sub.user_enrollment.biodata:
                name += f" ({sub.user_enrollment.biodata.full_name})"
            scoresheet[sub.id] = {"name": name, "scores": {}}
            for j in judges:
                scoresheet[sub.id]["scores"][j.judge_id] = 0
                
        for vote in votes:
            if vote.submission_id in scoresheet and vote.user_id in scoresheet[vote.submission_id]["scores"]:
                scoresheet[vote.submission_id]["scores"][vote.user_id] += vote.score
                
        contestants_list = list(scoresheet.values())

    return render_template("program_culturefire/admin_scores.html", show=show, judges=judges, contestants=contestants_list)
"""

if "def admin_dashboard():" not in routes_content:
    routes_content += admin_routes
    print("Added admin routes")

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(routes_content)

# 3. Update watch_show.html scoring UI size
watch_path = r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\watch_show.html'
with open(watch_path, 'r', encoding='utf-8') as f:
    watch_html = f.read()

old_ui = """          <div id="scoreContainer" class="hidden flex items-center space-x-2">
            <label for="scoreInput" class="font-semibold text-gray-300">Score:</label>
            <input type="number" id="scoreInput" min="1" max="10" class="w-16 px-2 py-1 text-black rounded font-bold text-center" placeholder="/10">
            <span id="scoreMaxLabel" class="text-xs text-gray-400">/ 10</span>
            <button id="btnVote" onclick="voteItem()" class="bg-purple-600 text-white px-4 py-1.5 rounded hover:bg-purple-700 font-bold shadow transition">Submit Score</button>
          </div>"""

new_ui = """          <div id="scoreContainer" class="hidden flex items-center space-x-3">
            <label for="scoreInput" class="font-semibold text-gray-300 text-lg">Score:</label>
            <input type="number" id="scoreInput" min="1" max="10" class="w-20 px-3 py-2 text-black rounded font-bold text-center text-lg" placeholder="/10">
            <span id="scoreMaxLabel" class="text-sm text-gray-400">/ 10</span>
            <button id="btnVote" onclick="voteItem()" class="bg-purple-600 text-white px-5 py-2 rounded hover:bg-purple-700 font-bold shadow-lg transition text-lg">Submit Score</button>
          </div>"""

if old_ui in watch_html:
    watch_html = watch_html.replace(old_ui, new_ui)
    with open(watch_path, 'w', encoding='utf-8') as f:
        f.write(watch_html)
    print("Updated watch_show.html UI")
else:
    print("Could not find old UI in watch_show.html")


# 4. Create admin_dashboard.html
admin_dash_content = """{% extends "layout.html" %}

{% block title %}Cultural Fire Admin Dashboard{% endblock %}

{% block content %}
<div class="bg-white shadow rounded-lg overflow-hidden max-w-6xl mx-auto">
  <div class="h-2 bg-blue-800"></div>
  
  <div class="px-6 py-4 border-b flex justify-between items-center bg-gray-50">
    <h1 class="text-2xl font-bold text-gray-800">Cultural Fire Admin Dashboard</h1>
    <a href="{{ url_for('main.index') }}" class="bg-gray-300 text-gray-800 px-4 py-2 rounded hover:bg-gray-400">Back</a>
  </div>

  <div class="p-6">
    <h2 class="text-xl font-semibold mb-4 text-gray-700">Manage Shows & Scores</h2>
    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-gray-200 border">
        <thead class="bg-gray-100">
          <tr>
            <th class="px-6 py-3 text-left text-xs font-bold text-gray-500 uppercase">Show Title</th>
            <th class="px-6 py-3 text-left text-xs font-bold text-gray-500 uppercase">Status</th>
            <th class="px-6 py-3 text-center text-xs font-bold text-gray-500 uppercase">Actions</th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          {% for show in shows %}
          <tr class="hover:bg-gray-50">
            <td class="px-6 py-4 font-medium text-gray-900">{{ show.title }}</td>
            <td class="px-6 py-4 text-sm text-gray-500">
              <span class="px-2 py-1 rounded text-xs font-bold {% if show.status == 'active' %}bg-green-100 text-green-800{% else %}bg-gray-100 text-gray-800{% endif %}">
                {{ show.status|capitalize }}
              </span>
            </td>
            <td class="px-6 py-4 text-center">
              <a href="{{ url_for('cultural_bp.admin_scores', show_id=show.id) }}" class="bg-indigo-600 text-white px-4 py-2 rounded hover:bg-indigo-700 text-sm font-semibold inline-block">
                View Judge Score Sheet
              </a>
            </td>
          </tr>
          {% else %}
          <tr>
            <td colspan="3" class="px-6 py-8 text-center text-gray-500">No shows available.</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
</div>
{% endblock %}
"""

with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\admin_dashboard.html', 'w', encoding='utf-8') as f:
    f.write(admin_dash_content)
print("Created admin_dashboard.html")

# 5. Create admin_scores.html
admin_scores_content = """{% extends "layout.html" %}

{% block title %}Admin Score Sheet - {{ show.title }}{% endblock %}

{% block content %}
<div class="bg-white shadow rounded-lg overflow-hidden max-w-7xl mx-auto">
  <div class="h-2 bg-indigo-600"></div>
  
  <div class="px-6 py-4 border-b flex justify-between items-center bg-gray-50">
    <h1 class="text-2xl font-bold text-gray-800">{{ show.title }} - Judge Score Sheet</h1>
    <a href="{{ url_for('cultural_bp.admin_dashboard') }}" class="bg-gray-300 text-gray-800 px-4 py-2 rounded hover:bg-gray-400">Back</a>
  </div>

  <div class="p-6">
    <p class="text-gray-600 mb-6">Detailed breakdown of scores assigned by individual judges.</p>

    <div class="overflow-x-auto">
      <table class="min-w-full divide-y divide-gray-200 border">
        <thead class="bg-indigo-50">
          <tr>
            <th class="px-6 py-3 text-left text-sm font-bold text-gray-700 uppercase tracking-wider border-r">Contestant</th>
            {% for judge in judges %}
            <th class="px-4 py-3 text-center text-xs font-bold text-indigo-800 uppercase tracking-wider border-r">
              {{ judge.judge.first_name }} {{ judge.judge.last_name }}<br>
              <span class="text-gray-500 text-[10px]">Judge {{ loop.index }}</span>
            </th>
            {% endfor %}
            <th class="px-6 py-3 text-center text-sm font-bold text-gray-900 uppercase tracking-wider bg-gray-100">Total Score</th>
          </tr>
        </thead>
        <tbody class="bg-white divide-y divide-gray-200">
          {% for contestant in contestants %}
          <tr class="hover:bg-gray-50">
            <td class="px-6 py-4 whitespace-nowrap border-r font-semibold text-gray-900">{{ contestant.name }}</td>
            
            {% set ns = namespace(total=0) %}
            {% for judge in judges %}
            <td class="px-4 py-4 whitespace-nowrap border-r text-center font-medium">
              {% set score = contestant.scores.get(judge.judge_id, 0) %}
              {% if score > 0 %}
                <span class="text-green-700">{{ score }}</span>
              {% else %}
                <span class="text-gray-300">-</span>
              {% endif %}
              {% set ns.total = ns.total + score %}
            </td>
            {% endfor %}
            
            <td class="px-6 py-4 whitespace-nowrap text-center bg-gray-50 font-bold text-lg text-gray-900">{{ ns.total }}</td>
          </tr>
          {% else %}
          <tr>
            <td colspan="{{ judges|length + 2 }}" class="px-6 py-8 text-center text-gray-500">No contestants or scores available yet.</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  </div>
</div>
{% endblock %}
"""

with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\admin_scores.html', 'w', encoding='utf-8') as f:
    f.write(admin_scores_content)
print("Created admin_scores.html")
