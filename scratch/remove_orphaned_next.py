import re

with open('templates/program_sace/interactive_workshop.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Completely remove the orphaned Next button block
content = re.sub(r'<button onclick="changeSlide\(1\)".*?</script>', '', content, flags=re.DOTALL)
# Also strip out the dangling </div>
content = re.sub(r'Next <i class="fas fa-arrow-right ml-2"></i>\s*</button>\s*</div>', '', content, flags=re.DOTALL)

with open('templates/program_sace/interactive_workshop.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Removed orphaned Next button")
