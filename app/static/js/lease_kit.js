(function () {
  'use strict';
  const documentView = document.getElementById('lease-kit-document');
  if (!documentView) return;
  const pdf = document.getElementById('lease-kit-pdf');
  if (pdf) {
    const print = document.getElementById('lease-kit-print');
    if (print) print.onclick = function () { window.open(pdf.src + '#print', '_blank', 'noopener'); };
    return;
  }
  let parent = documentView.parentElement;
  while (parent && parent !== document.documentElement) {
    parent.classList.add('kit-print-ancestor');
    parent = parent.parentElement;
  }
  if (window.location.hash === '#print') window.addEventListener('load', function () {
    const print = document.getElementById('lease-kit-print');
    if (print && !print.disabled) window.print();
  });
}());
