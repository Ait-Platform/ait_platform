with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\watch_show.html', 'r', encoding='utf-8') as f:
    content = f.read()

# Replace HTML button
old_button_html = """          <button id="btnVote" onclick="voteItem()" class="bg-purple-600 text-white px-4 py-1.5 rounded hidden hover:bg-purple-700 font-bold shadow transition">Vote ?</button>"""
new_button_html = """          {% if is_judge %}
          <div id="scoreContainer" class="hidden flex items-center space-x-2">
            <label for="scoreInput" class="text-sm font-bold text-gray-300">Score:</label>
            <input type="number" id="scoreInput" min="1" max="10" class="w-16 text-black rounded px-2 py-1 text-sm font-bold text-center border-2 border-transparent focus:border-purple-500 focus:outline-none shadow-inner" placeholder="0">
            <span id="scoreMaxLabel" class="text-gray-400 text-sm font-bold mr-2">/ 10</span>
            <button id="btnVote" onclick="voteItem()" class="bg-purple-600 text-white px-4 py-1.5 rounded hover:bg-purple-700 font-bold shadow transition">Submit Score</button>
          </div>
          {% endif %}"""

if old_button_html in content:
    content = content.replace(old_button_html, new_button_html)

# Update loadItem JS to handle score visibility and max limits
old_load_item_vote = """  // Show vote button
  btnVote.classList.remove('hidden');"""
new_load_item_vote = """  // Show score container if judge
  const scoreContainer = document.getElementById('scoreContainer');
  if (scoreContainer) {
    scoreContainer.classList.remove('hidden');
    const scoreInput = document.getElementById('scoreInput');
    const scoreMaxLabel = document.getElementById('scoreMaxLabel');
    // Check if it's a talent segment or regular
    const isTalent = (submissions[index].segment_type === 'talent' || '{{ show.category_item.name }}' !== 'Pageant');
    const maxScore = isTalent ? 20 : 10;
    scoreInput.max = maxScore;
    scoreMaxLabel.textContent = '/ ' + maxScore;
    scoreInput.value = ''; // clear previous
  }"""
if old_load_item_vote in content:
    content = content.replace(old_load_item_vote, new_load_item_vote)

old_stop_show_vote = """  btnVote.classList.add('hidden');"""
new_stop_show_vote = """  const scoreContainer = document.getElementById('scoreContainer');
  if (scoreContainer) scoreContainer.classList.add('hidden');"""
if old_stop_show_vote in content:
    content = content.replace(old_stop_show_vote, new_stop_show_vote)

# Update voteItem logic
old_vote_item_func = """function voteItem() {
  if (currentIndex === -1) return;
  const subId = submissions[currentIndex].id;
  
  fetch('{{ url_for("cultural_bp.vote_item") }}', {
    method: 'POST',
    headers: { 
      'Content-Type': 'application/json',
      'X-CSRFToken': '{{ csrf_token() }}'
    },
    body: JSON.stringify({ 
      submission_id: subId,
      type: '{{ "pageant" if show.category_item and show.category_item.name == "Pageant" else "talent" }}'
    })
  })
  .then(res => res.json()).then(data => {
    if(data.success) {
      alert("Vote recorded successfully!");
    } else {
      alert("Error: " + data.message);
    }
  }).catch(err => {
    alert("Error recording vote");
  });
}"""

new_vote_item_func = """function voteItem() {
  if (currentIndex === -1) return;
  const subId = submissions[currentIndex].id;
  const scoreInput = document.getElementById('scoreInput');
  const score = scoreInput ? parseInt(scoreInput.value) : 0;
  
  if (!score || score < 1 || (score > 10 && scoreInput.max == 10) || score > 20) {
    alert("Please enter a valid score between 1 and " + (scoreInput ? scoreInput.max : 10));
    return;
  }
  
  fetch('{{ url_for("cultural_bp.vote_item") }}', {
    method: 'POST',
    headers: { 
      'Content-Type': 'application/json',
      'X-CSRFToken': '{{ csrf_token() }}'
    },
    body: JSON.stringify({ 
      submission_id: subId,
      type: '{{ "pageant" if show.category_item and show.category_item.name == "Pageant" else "talent" }}',
      score: score
    })
  })
  .then(res => res.json()).then(data => {
    if(data.success) {
      alert("Score recorded successfully!");
      if (currentIndex < submissions.length - 1) {
        nextItem();
      }
    } else {
      alert("Error: " + data.message);
    }
  }).catch(err => {
    alert("Error recording score");
  });
}"""

if old_vote_item_func in content:
    content = content.replace(old_vote_item_func, new_vote_item_func)

with open(r'D:\Users\yeshk\Documents\ait_platform\templates\program_culturefire\watch_show.html', 'w', encoding='utf-8') as f:
    f.write(content)
print("Updated watch_show.html successfully.")
