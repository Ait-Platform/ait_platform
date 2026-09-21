import re

# 1. Fix the Tile 5 href in all 3 workspaces
href_old = '<a href="#" class="group block relative overflow-hidden rounded-2xl border {% if first_tabled_res %}'
href_new = '<a href="{% if first_tabled_res %}{{ url_for(\'uip_bp.view_resolution\', org_slug=org.slug, res_id=first_tabled_res.id) }}{% else %}#{% endif %}" class="group block relative overflow-hidden rounded-2xl border {% if first_tabled_res %}'

for template in ["secretary_workspace.html", "chairman_workspace.html", "treasurer_workspace.html"]:
    path = f"templates/program_uip/dashboards/{template}"
    with open(path, "r", encoding="utf-8") as f:
        html = f.read()
    
    html = html.replace(href_old, href_new)
    
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


# 2. Fix the Colors in resolution_view.html by using guaranteed Tailwind classes (emerald/rose) or inline styles
with open("templates/program_uip/dashboards/resolution_view.html", "r", encoding="utf-8") as f:
    res_html = f.read()

# Replace YEA
res_html = re.sub(
    r'<button type="submit" name="vote" value="YEA".*?YEA\s*</button>',
    r'<button type="submit" name="vote" value="YEA" style="background-color: #d1fae5; color: #065f46; border: 2px solid #34d399;" class="flex-1 py-3 rounded-lg font-black transition shadow-sm text-center hover:opacity-80">\n                        YEA\n                    </button>',
    res_html,
    flags=re.DOTALL
)

# Replace NAY
res_html = re.sub(
    r'<button type="submit" name="vote" value="NAY".*?NAY\s*</button>',
    r'<button type="submit" name="vote" value="NAY" style="background-color: #ffe4e6; color: #9f1239; border: 2px solid #fb7185;" class="flex-1 py-3 rounded-lg font-black transition shadow-sm text-center hover:opacity-80">\n                        NAY\n                    </button>',
    res_html,
    flags=re.DOTALL
)

# Replace ABSTAIN
res_html = re.sub(
    r'<button type="submit" name="vote" value="ABSTAIN".*?ABSTAIN\s*</button>',
    r'<button type="submit" name="vote" value="ABSTAIN" style="background-color: #fef3c7; color: #92400e; border: 2px solid #fbbf24;" class="flex-1 py-3 rounded-lg font-black transition shadow-sm text-center hover:opacity-80">\n                        ABSTAIN\n                    </button>',
    res_html,
    flags=re.DOTALL
)

with open("templates/program_uip/dashboards/resolution_view.html", "w", encoding="utf-8") as f:
    f.write(res_html)

print("Applied UX fixes: dynamic hrefs and inline colors")
