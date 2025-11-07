// tailwind.config.js
module.exports = {
  content: ["./templates/**/*.html", "./app/**/*.py"],
  safelist: [
    // bar classes
    "bg-emerald-600","bg-emerald-400","bg-amber-500","bg-rose-600","bg-blue-600",
    // chip classes (soft pills)
    "bg-emerald-50","text-emerald-700","border-emerald-200",
    "bg-amber-50","text-amber-700","border-amber-200",
    "bg-rose-50","text-rose-700","border-rose-200",
    "bg-blue-50","text-blue-700","border-blue-200",
  ],
  // ...
}
