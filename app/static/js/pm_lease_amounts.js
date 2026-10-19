(()=>{
 const rent=document.getElementById('amounts-monthly_rent'),output=document.getElementById('amounts-monthly-rent-words');
 const ones='zero one two three four five six seven eight nine ten eleven twelve thirteen fourteen fifteen sixteen seventeen eighteen nineteen'.split(' '),tens='zero ten twenty thirty forty fifty sixty seventy eighty ninety'.split(' ');
 function below(n){const parts=[];if(n>=100){parts.push(ones[Math.floor(n/100)]+' hundred');n%=100;}if(n)parts.push(n<20?ones[n]:tens[Math.floor(n/10)]+(n%10?'-'+ones[n%10]:''));return parts.join(' and ')||'zero';}
 function integer(n){if(!n)return 'zero';const parts=[];for(const [value,label] of [[1e9,'billion'],[1e6,'million'],[1000,'thousand']]){const count=Math.floor(n/value);n%=value;if(count)parts.push(below(count)+' '+label);}if(n)parts.push((parts.length&&n<100?'and ':'')+below(n));return parts.join(' ');}
 function updateWords(){if(!/^[0-9]{1,12}(?:\.[0-9]{1,2})?$/.test(rent.value)){output.textContent='';return;}const parts=rent.value.split('.'),whole=Number(parts[0]),cents=Number((parts[1]||'').padEnd(2,'0'));output.textContent='('+integer(whole)+' rand and '+integer(cents)+(cents===1?' cent':' cents')+')';}
 if(rent&&output){rent.addEventListener('input',updateWords);updateWords();}
 const selection=document.getElementById('amounts-parking_applicability'),group=document.getElementById('amounts-parking-details-group'),details=document.getElementById('amounts-parking_details');
 if(selection&&group&&details){function updateParking(){group.hidden=selection.value!=='other';details.required=selection.value==='other';}selection.addEventListener('change',updateParking);updateParking();}
})();
