// ASTRA server: node server.js (Node 18+, tanpa dependency)
// Env: PORT, OWNERS=emailowner1,emailowner2, MAX_USERS, DB
const http=require('http'),fs=require('fs'),cr=require('crypto');
const PORT=process.env.PORT||3000,FILE=process.env.DB||'./astra-db.json',MAXU=+process.env.MAX_USERS||50000;
const OWN=(process.env.OWNERS||'').toLowerCase().split(',').filter(Boolean);
let db={users:{},mail:{},tok:{},com:[],dm:{},next:100001};try{db=JSON.parse(fs.readFileSync(FILE))}catch{}
let dirty=0;const sv=()=>dirty=1;
setInterval(()=>{if(dirty){dirty=0;fs.writeFile(FILE+'.tmp',JSON.stringify(db),e=>!e&&fs.rename(FILE+'.tmp',FILE,()=>{}))}},2000);
const ph=(p,s)=>cr.scryptSync(p,s,32).toString('hex');
const J=(r,c,o)=>{r.writeHead(c,{'content-type':'application/json','access-control-allow-origin':'*','access-control-allow-headers':'content-type,authorization','access-control-allow-methods':'GET,POST,OPTIONS'});r.end(JSON.stringify(o))};
const ER=(r,c,e)=>J(r,c,{e});
const hits={};setInterval(()=>{for(const k in hits)delete hits[k]},60000);
const RL=['free','go50','go75','go250','y1','y2'];
const pub=u=>({id:u.id,name:u.name,avatar:u.avatar,status:u.status,role:OWN.includes(u.email)?'owner':RL.includes((u.data||{}).plan)?u.data.plan:'free'});
const me=u=>({...pub(u),email:u.email,bg:u.bg,wall:u.wall,fx:u.fx,data:u.data});
http.createServer((q,r)=>{
 const ip=q.socket.remoteAddress;hits[ip]=(hits[ip]||0)+1;
 if(q.method==='OPTIONS')return J(r,204,{});
 if(hits[ip]>300)return ER(r,429,'Terlalu banyak permintaan, tunggu sebentar');
 let b='',big=0;q.on('data',d=>{b+=d;if(b.length>3e6&&!big){big=1;ER(r,413,'Data terlalu besar')}});
 q.on('end',()=>{if(big)return;try{h(q,r,b?JSON.parse(b):{})}catch(e){console.error(e);ER(r,500,'Kesalahan di server')}})
}).listen(PORT,()=>console.log('ASTRA server di port '+PORT));
const ok=(s,n)=>typeof s==='string'&&s.length<=n;
function h(q,r,b){
 const[p,qs]=q.url.split('?'),P=new URLSearchParams(qs||''),S=p.split('/').filter(Boolean),c=S[1],m=q.method;
 if(S[0]!=='api')return ER(r,404,'Tidak ditemukan');
 if(c==='ping')return J(r,200,{ok:1});
 if(c==='register'||c==='login'){
  const em=String(b.email||'').trim().toLowerCase(),pw=String(b.pass||'');
  if(!/^\S+@\S+\.\S+$/.test(em)||pw.length<6||pw.length>200)return ER(r,400,'Email atau password tidak valid (password minimal 6 karakter)');
  let u=db.users[db.mail[em]];
  if(c==='register'){
   if(u)return ER(r,409,'Email sudah terdaftar, silakan masuk');
   if(Object.keys(db.users).length>=MAXU)return ER(r,503,'Server penuh, pendaftaran ditutup sementara');
   const salt=cr.randomBytes(16).toString('hex');u=db.users[db.next]={id:String(db.next),email:em,name:em.split('@')[0].slice(0,24),salt,ph:ph(pw,salt),avatar:'',bg:'',wall:'',fx:'',status:'',fr:[],data:{},ep:0};db.mail[em]=db.next++;
  }else if(!u||ph(pw,u.salt)!==u.ph)return ER(r,401,'Email atau password salah');
  const t=cr.randomBytes(24).toString('hex');db.tok[t]={id:u.id,ep:u.ep};sv();return J(r,200,{token:t,user:me(u),data:u.data});
 }
 const k=db.tok[(q.headers.authorization||'').slice(7)],u=k&&db.users[k.id];
 if(!u||k.ep!==u.ep)return ER(r,401,'Sesi habis, silakan masuk lagi');
 if(c==='me')return J(r,200,me(u));
 if(c==='profile'&&m==='POST'){
  if(b.name!=null){const n=String(b.name).trim();if(n.length<1||n.length>24)return ER(r,400,'Nama 1-24 karakter');u.name=n}
  for(const[f,l]of[['avatar',4e5],['bg',6e5],['wall',6e5],['status',80],['fx',20]])if(b[f]!=null){if(!ok(b[f],l))return ER(r,413,'Data '+f+' terlalu besar');u[f]=b[f]}
  sv();return J(r,200,me(u));
 }
 if(c==='password'&&m==='POST'){if(ph(String(b.old||''),u.salt)!==u.ph)return ER(r,403,'Password lama salah');const n=String(b.new||'');if(n.length<6)return ER(r,400,'Password baru minimal 6 karakter');u.salt=cr.randomBytes(16).toString('hex');u.ph=ph(n,u.salt);sv();return J(r,200,{ok:1})}
 if(c==='sync'&&m==='POST'){if(JSON.stringify(b.data||{}).length>2e5)return ER(r,413,'Data sinkron terlalu besar');u.data=b.data||{};sv();return J(r,200,{ok:1})}
 if(c==='logoutall'){u.ep++;sv();return J(r,200,{ok:1})}
 if(c==='delete'&&m==='POST'){delete db.users[u.id];delete db.mail[u.email];sv();return J(r,200,{ok:1})}
 if(c==='user'){const x=db.users[S[2]];return x?J(r,200,pub(x)):ER(r,404,'ID tidak ditemukan')}
 if(c==='friend'&&m==='POST'){const x=db.users[String(b.id)];if(!x||x.id===u.id)return ER(r,404,'ID tidak ditemukan');if(!u.fr.includes(x.id))u.fr.push(x.id);if(!x.fr.includes(u.id))x.fr.push(u.id);sv();return J(r,200,pub(x))}
 if(c==='friends')return J(r,200,u.fr.map(i=>db.users[i]).filter(Boolean).map(pub));
 if(c==='com'||c==='dm'){
  let L;if(c==='com')L=db.com;else{const o=S[2];if(!db.users[o])return ER(r,404,'ID tidak ditemukan');L=db.dm[[u.id,o].sort().join('-')]||=[]}
  if(m==='POST'){
   if(!['text','stk'].includes(b.k)||!ok(b.b,b.k==='stk'?3e5:1000)||!b.b)return ER(r,400,'Pesan tidak valid');
   L.push({i:Date.now()+Math.random(),t:Date.now(),u:u.id,k:b.k,b:b.b});if(L.length>300)L.splice(0,L.length-300);sv();
  }
  const s=+P.get('since')||0,ms=L.filter(x=>x.t>s),us={};ms.forEach(x=>us[x.u]=pub(db.users[x.u]||{id:x.u,name:'?',role:'free'}));
  return J(r,200,{m:ms,u:us});
 }
 ER(r,404,'Tidak ditemukan');
}
