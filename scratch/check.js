const fs = require('fs');
const html = fs.readFileSync('templates/program_sace/simulator.html', 'utf8');

const regex = /<script>([\s\S]*?)<\/script>/g;
let match;
while ((match = regex.exec(html)) !== null) {
    const jsCode = match[1];
    try {
        new Function(jsCode);
        console.log("JS compiled successfully.");
    } catch (e) {
        console.log("JS ERROR:", e);
    }
}
