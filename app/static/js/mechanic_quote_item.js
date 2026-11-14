(() => {
  const form=document.getElementById('mechanic-item-edit');
  if(!form)return;
  const decimal=(text,places)=>{
    const match=/^(\d+)(?:\.(\d+))?$/.exec(text);
    if(!match || (match[2] || '').length>places)return null;
    return BigInt(match[1])*10n**BigInt(places)+BigInt((match[2] || '').padEnd(places,'0'));
  };
  const calculate=()=>{
    const quantity=decimal(form.elements.quantity.value,2),rate=decimal(form.elements.rate.value,2);
    const cents=quantity===null || rate===null ? 0n:(quantity*rate+50n)/100n;
    document.getElementById('mechanic-line-total').value=`${cents/100n}.${String(cents%100n).padStart(2,'0')}`;
  };
  form.addEventListener('input',calculate);form.addEventListener('change',calculate);calculate();
})();
