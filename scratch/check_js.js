const fs = require('fs');

const text = fs.readFileSync('templates/program_sace/facilitator_dashboard.html', 'utf8');

// Extract all scripts
const scriptRegex = /<script>([\s\S]*?)<\/script>/gi;
let match;
let scriptContent = '';
while ((match = scriptRegex.exec(text)) !== null) {
    scriptContent += match[1] + '\n';
}

// Remove Jinja tags
scriptContent = scriptContent.replace(/\{\{.*?\}\}/g, '""');
scriptContent = scriptContent.replace(/\{%.*?%\}/g, '');

try {
    new Function(scriptContent);
    console.log("Syntax is valid!");
} catch (e) {
    console.error("Syntax Error: " + e.message);
}
