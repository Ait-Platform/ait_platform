(()=>{
 const months=document.getElementById('lease-duration-months'),start=document.getElementById('lease-commencement-date');if(!months||!start)return;
 const expiry=document.getElementById('lease-expiry-date'),notice=document.getElementById('lease-renewal-notice-date');
 const day=(y,m,d)=>{const v=new Date(0);v.setUTCFullYear(y,m,d);v.setUTCHours(0,0,0,0);return v;};
 const last=(y,m)=>day(y,m+1,0).getUTCDate();
 const shift=(v,n,end=false)=>{const target=v.getUTCFullYear()*12+v.getUTCMonth()+n,y=Math.floor(target/12),m=target-y*12;return day(y,m,end&&v.getUTCDate()===last(v.getUTCFullYear(),v.getUTCMonth())?last(y,m):Math.min(v.getUTCDate(),last(y,m)));};
 const iso=v=>v.toISOString().slice(0,10);
 function calculate(){expiry.value='';notice.value='';if(!/^\d{4}-\d{2}-\d{2}$/.test(start.value)||!/^\d+$/.test(months.value))return;
 const n=Number(months.value),parts=start.value.split('-').map(Number),v=day(parts[0],parts[1]-1,parts[2]);if(n<1||n>1200||iso(v)!==start.value)return;
 const e=shift(v,n);e.setUTCDate(e.getUTCDate()-1);const r=shift(e,-3,true);if(e.getUTCFullYear()>9999||r.getUTCFullYear()<1)return;expiry.value=iso(e);notice.value=iso(r);}
 months.addEventListener('input',calculate);start.addEventListener('input',calculate);calculate();
})();
