with open("templates/program_uip/navigation.html", "r", encoding="utf-8") as f:
    text = f.read()

# Update Treasurer's Sidebar
text = text.replace(
    '<a href="#"><i class="fas fa-users w-5"></i> Ratepayer Registry (Coming Soon)</a>',
    '<a href="{{ url_for(\'uip_bp.member_list\', org_slug=org.slug) }}"><i class="fas fa-users w-5"></i> Ratepayer Registry</a>'
)

# Update Secretary's Sidebar (it was a dead link '#')
text = text.replace(
    '<a href="#"><i class="fas fa-users w-5"></i> Members Register</a>',
    '<a href="{{ url_for(\'uip_bp.member_list\', org_slug=org.slug) }}"><i class="fas fa-users w-5"></i> Members Register</a>'
)

with open("templates/program_uip/navigation.html", "w", encoding="utf-8") as f:
    f.write(text)

print("Updated navigation.html to link the working Ratepayer Registry")
