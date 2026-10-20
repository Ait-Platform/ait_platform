(function () {
  'use strict';
  const documentView = document.getElementById('lease-kit-document');
  if (!documentView) return;
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
