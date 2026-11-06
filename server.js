// ASTRA server: node server.js (Node 18+, tanpa dependency)
// Env: PORT, OWNERS=emailowner1,emailowner2, MAX_USERS, DB
const http=require('http'),fs=require('fs'),cr=require('crypto');
const PORT=process.env.PORT||3000,FILE=process.env.DB||'./astra-db.json',MAXU=+process.env.MAX_USERS||50000;
const OWN=(process.env.OWNERS||'').toLowerCase().split(',').filter(Boolean);
const MK=['img','vid','file'];
let db={users:{},mail:{},tok:{},com:[],dm:{},next:100001};try{db=JSON.parse(fs.readFileSync(FILE))}catch{}
const clean=()=>{const f=a=>(a||[]).filter(m=>!MK.includes(m.k)||m.b);db.com=f(db.com);for(const k in db.dm)db.dm[k]=f(db.dm[k])};clean();
let dirty=0;const sv=()=>dirty=1;
// Penyimpanan permanen (opsional, gratis): isi env UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN
const RU=process.env.UPSTASH_REDIS_REST_URL,RT=process.env.UPSTASH_REDIS_REST_TOKEN;
const rcmd=a=>fetch(RU,{method:'POST',headers:{authorization:'Bearer '+RT,'content-type':'application/json'},body:JSON.stringify(a)}).then(r=>r.json());
setInterval(()=>{if(dirty){dirty=0;const t=JSON.stringify(db,function(k,v){return k==='b'&&MK.includes(this.k)?undefined:v});fs.writeFile(FILE+'.tmp',t,e=>!e&&fs.rename(FILE+'.tmp',FILE,()=>{}));if(RU)rcmd(['SET','astra-db',t]).catch(e=>{dirty=1;console.error('Upstash gagal',e.message)})}},2000);
const ph=(p,s)=>cr.scryptSync(p,s,32).toString('hex');
const J=(r,c,o)=>{r.writeHead(c,{'content-type':'application/json','access-control-allow-origin':'*','access-control-allow-headers':'content-type,authorization','access-control-allow-methods':'GET,POST,OPTIONS'});r.end(JSON.stringify(o))};
const ER=(r,c,e)=>J(r,c,{e});
const flush=()=>{const t=JSON.stringify(db,function(k,v){return k==='b'&&MK.includes(this.k)?undefined:v});try{fs.writeFileSync(FILE,t)}catch{}return RU?Promise.race([rcmd(['SET','astra-db',t]),new Promise(r=>setTimeout(r,2500))]).catch(()=>{}):Promise.resolve()};
for(const sg of['SIGTERM','SIGINT'])process.on(sg,()=>flush().finally(()=>process.exit(0)));
const hits={};setInterval(()=>{for(const k in hits)delete hits[k]},60000);
const RL=['free','go50','go75','go250','y1','y2'];
/* Akses admin disimpan di server (u.adm), tidak bisa dipalsukan lewat sinkron data. Hanya hash kode yang disimpan di sini. */
const sha=s=>cr.createHash('sha256').update(s).digest('hex'),OWH='c8d8389a2310ceef7fae2fdd36927d583e45b854d42943b5b6e6a07b02729564',VIPH=new Set(["7b153f87a01031daa0c15f626f571de239879eb9def69f6bc90950c1bcca934c", "4bef8465249b0e3c62eebc7364de380231a9357aaeacde3a14e6458d33814709", "52c5d30cb54bb549a4b29e6c1ab0bfa9f19777c23fd28206f6b7445a91b9eae7", "b4cc94adfbb3e091ca801ec1973c7697e51fd9994381300c934e212a42ddf267", "08be2fac21dd2f777d8421f822eedc4d841f7647d07b59491f989f38356c7a1c"]);
const isRoot=u=>OWN.includes(u.email)||u.adm==='root',isAdm=u=>isRoot(u)||u.adm==='vip';
const pub=u=>({id:u.id,name:u.name,avatar:u.avatar,status:u.status,role:isAdm(u)?'owner':RL.includes((u.data||{}).plan)?u.data.plan:'free'});
const me=u=>({...pub(u),adm:isRoot(u)?'root':u.adm||'',email:u.email,bg:u.bg,wall:u.wall,fx:u.fx,data:u.data});
const srv=http.createServer((q,r)=>{
 const ip=String(q.headers['x-forwarded-for']||'').split(',')[0].trim()||q.socket.remoteAddress;hits[ip]=(hits[ip]||0)+1;
 if(q.method==='OPTIONS')return J(r,204,{});
 if(hits[ip]>600)return ER(r,429,'Terlalu banyak permintaan, tunggu sebentar');
 let b='',big=0;q.on('data',d=>{if(big)return;b+=d;if(b.length>4e6){big=1;b='';ER(r,413,'Ukuran file terlalu besar untuk dikirim')}});
 q.on('end',()=>{if(big)return;try{h(q,r,b?JSON.parse(b):{})}catch(e){console.error(e);ER(r,500,'Kesalahan di server')}})
});(async()=>{if(RU)try{const r=await rcmd(['GET','astra-db']);if(r.result){db=JSON.parse(r.result);clean()}console.log('Data dimuat dari Upstash')}catch(e){console.error('Gagal memuat Upstash',e.message)}
 srv.listen(PORT,()=>console.log('ASTRA server di port '+PORT))})();

/* ===== Bot keamanan + WebSocket ===== */
const BOT={id:'bot',name:'Bot Keamanan ASTRA',avatar:'',status:'Pemantau 24 jam',role:'owner'};
const TOS='📜 Ketentuan Layanan Komunitas ASTRA Live Chat\n1. Dilarang membagikan kode program.\n2. Dilarang mengirim link tidak jelas atau penipuan. Hanya link tepercaya (TikTok, Google, YouTube, dll).\n3. Situs judi, ilegal, atau haram = langsung di-BAN.\n4. Dilarang spam.\n5. Semua chat komunitas (pesan, foto, dan stiker) direset otomatis setiap 2 jam sekali (00.00, 02.00, 04.00, dst WIB). Chat pribadi dan akunmu tidak ikut direset.\n6. Kamu bisa menghapus pesanmu sendiri dengan menekan pesan lalu pilih Hapus.\nBot keamanan memantau semua obrolan 24 jam secara real-time.';
const OKD=['tiktok.com','google.com','youtube.com','youtu.be','instagram.com','facebook.com','wa.me','whatsapp.com','github.com','wikipedia.org','x.com','twitter.com','spotify.com','telegram.org','t.me'];
const BAD=/\b(slot\s?(gacor|online|maxwin|dana)|rtp\s?slot|togel|judi\s?(online|bola|slot)|sbobet|casino|maxwin|bandar\s?(togel|judi|bola)|pinjol|taruhan|bet\s?88|situs\s?(judi|slot)|bokep)\b/i;
const CODE=/```|<\/?(script|html|div|body)\b|function\s*\w*\s*\(|\b(const|let|var)\s+\w+\s*=|import\s+.+\s+from|#include|console\.log|document\.\w|def\s+\w+\(|public\s+static|SELECT\s.+\sFROM|=>\s*\{/i;
const LINK=/(?:https?:\/\/|www\.)\S+|\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:com|net|org|id|xyz|top|club|bet|vip|site|online|link|ru|cc|info|shop|live|app|me)\b\S*/gi;
const clients=new Set();
const frame=s=>{const p=Buffer.from(s),l=p.length;const h=l<126?Buffer.from([0x81,l]):l<65536?Buffer.from([0x81,126,l>>8,l&255]):Buffer.concat([Buffer.from([0x81,127,0,0,0,0]),Buffer.from([(l>>>24)&255,(l>>>16)&255,(l>>>8)&255,l&255])]);return Buffer.concat([h,p])};
const wsend=(c,o)=>{try{c.sock.write(frame(JSON.stringify(o)))}catch{}};
const cast=o=>clients.forEach(c=>c.r==='com'&&wsend(c,o));
const castU=(ids,o)=>clients.forEach(c=>ids.includes(c.u.id)&&wsend(c,o));
function mod(t){
 if(BAD.test(t))return['ban','Konten judi/ilegal/haram'];
 for(const l of t.match(LINK)||[]){const hs=l.replace(/^https?:\/\//i,'').replace(/^www\./i,'').split(/[\/?#]/)[0].toLowerCase();if(BAD.test(l)||/\.(bet|vip)$/.test(hs))return['ban','Link situs ilegal/judi'];if(!OKD.some(d=>hs===d||hs.endsWith('.'+d)))return['del','Link tidak dikenal/berpotensi penipuan']}
 if(CODE.test(t))return['del','Dilarang membagikan kode'];
 return null}
function push(m){db.com.push(m);capMedia(db.com);if(db.com.length>300)db.com.splice(0,db.com.length-300);sv();cast({t:'m',m,u:{bot:BOT,[m.u]:db.users[m.u]?pub(db.users[m.u]):BOT}})}
const say=b=>push({i:Date.now()+Math.random(),t:Date.now(),u:'bot',k:'bot',b});
function postCom(u,k,b,nm){
 const adm=isAdm(u);
 if(u.ban&&!isRoot(u))return'Akunmu di-BAN dari komunitas karena melanggar ketentuan';
 const n=Date.now();u.rt=(u.rt||[]).filter(x=>n-x<5000);if(!adm&&u.rt.push(n)>5)return'Pelan-pelan, kamu terdeteksi spam';
 {const v=adm?null:k==='text'?mod(b):(nm&&BAD.test(nm)?['ban','Nama file berisi konten judi/ilegal']:null);if(v){if(v[0]==='ban')u.ban=1;else if((u.st=(u.st||0)+1)>=3)u.ban=1;
  say(`🛡️ Pesan ${u.name} dihapus: ${v[1]}. ${u.ban?u.name+' telah di-BAN dari komunitas.':'Peringatan '+u.st+'/3.'}`);sv();return v[1]+(u.ban?' (kamu di-BAN)':'')}}
 push({i:Date.now()+Math.random(),t:Date.now(),u:u.id,k,b,n:nm});return''}
srv.on('upgrade',(q,sock)=>{
 const U=new URL(q.url,'http://x'),k=db.tok[U.searchParams.get('t')],u=k&&db.users[k.id];
 if(U.pathname!=='/ws'||!u||k.ep!==u.ep||!q.headers['sec-websocket-key']){sock.destroy();return}
 sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+cr.createHash('sha1').update(q.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')+'\r\n\r\n');
 const c={sock,u,r:U.searchParams.get('r')};clients.add(c);if(c.r==='com'){if(!u.tos){u.tos=1;sv();say('👋 Selamat datang '+u.name+' di Komunitas ASTRA Live Chat!\n\n'+TOS)}wsend(c,{t:'tos',b:TOS})}let buf=Buffer.alloc(0);
 sock.on('data',d=>{buf=Buffer.concat([buf,d]);while(buf.length>=2){
  let o=buf[1]&127,off=2;if(o===126){if(buf.length<4)break;o=buf.readUInt16BE(2);off=4}else if(o===127){if(buf.length<10)break;o=buf.readUInt32BE(6);off=10}
  if(o>4e6){wsend(c,{t:'warn',b:'File terlalu besar'});sock.destroy();return}const msk=buf[1]&128?buf.slice(off,off+4):null;if(msk)off+=4;if(buf.length<off+o)break;
  const op=buf[0]&15;let pl=buf.slice(off,off+o);buf=buf.slice(off+o);if(msk)pl=Buffer.from(pl.map((x,i)=>x^msk[i%4]));
  if(op===8){sock.end();return}
  if(op===1&&c.r==='com'){try{const m=JSON.parse(pl.toString());if(!valid(m.k,m.b,m.n)){wsend(c,{t:'warn',b:why(m.k,m.b,m.n)});continue}const e=postCom(u,m.k,m.b,m.n);if(e)wsend(c,{t:'warn',b:e})}catch{}}}});
 sock.on('close',()=>clients.delete(c));sock.on('error',()=>clients.delete(c))});
setInterval(()=>clients.forEach(c=>{try{c.sock.write(Buffer.from([0x89,0]))}catch{}}),25000);
/* reset chat komunitas tiap 2 jam sekali (batas genap WIB: 00.00, 02.00, ...). Hanya komunitas; akun, chat pribadi, dan pengaturan tidak disentuh */
const wib=()=>String(Math.floor((Date.now()+7*36e5)/72e5));
function reset(){const d=wib();if(db.cday===d)return;const first=!db.cday;db.cday=d;if(first){sv();return}db.com=[];sv();cast({t:'clear',day:d});say('🧹 Chat komunitas sudah direset otomatis (tiap 2 jam sekali) supaya server tetap ringan.')}
reset();setInterval(reset,30000);
/* status otomatis hilang setelah 24 jam, dibuang dari penyimpanan supaya tidak menumpuk */
function purgeStat(){let ch=0;for(const k in db.users){const s=db.users[k].stat;if(s&&Date.now()-s.t>864e5){db.users[k].stat=null;ch=1}}if(ch)sv()}
purgeStat();setInterval(purgeStat,6e5);
const ok=(s,n)=>typeof s==='string'&&s.length<=n;
const KINDS={text:1e3,stk:3e5,img:12e5,vid:28e5,file:205e4},DURL=/^data:[a-z]+\/[\w.+-]+(;[\w=.-]+)*;base64,[A-Za-z0-9+\/=]+$/;
const why=(k,b,n)=>!KINDS[k]?'Jenis pesan tidak dikenal':!b?'Pesan kosong':!ok(b,KINDS[k])?'Ukuran terlalu besar (maks '+(KINDS[k]/1e6*.75).toFixed(1)+' MB)':k!=='text'&&!DURL.test(b)?'Format file tidak valid':(k==='img'||k==='stk')&&!b.startsWith('data:image/')?'File bukan gambar':k==='vid'&&!b.startsWith('data:video/')?'File bukan video':/\.(exe|apk|bat|cmd|js|vbs|scr|msi|jar|sh)$/i.test(n||'')?'Jenis file ini diblokir':'Pesan tidak valid';
const valid=(k,b,n)=>KINDS[k]&&b&&ok(b,KINDS[k])&&(k==='text'||DURL.test(b)&&(k==='file'||b.startsWith(k==='vid'?'data:video/':'data:image/')))&&(n==null||ok(n,120))&&!/\.(exe|apk|bat|cmd|js|vbs|scr|msi|jar|sh)$/i.test(n||'');
const capMedia=L=>{let m=L.filter(x=>MK.includes(x.k)),tot=m.reduce((a,x)=>a+(x.b?x.b.length:0),0);while(m.length>30||(tot>25e6&&m.length>1)){tot-=m[0].b?m[0].b.length:0;L.splice(L.indexOf(m[0]),1);m.shift()}};
function h(q,r,b){
 const[p,qs]=q.url.split('?'),P=new URLSearchParams(qs||''),S=p.split('/').filter(Boolean),c=S[1],m=q.method;
 if(S[0]!=='api')return ER(r,404,'Tidak ditemukan');
 if(c==='ping')return J(r,200,{ok:1,v:9});
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
 /* ===== ADMIN ===== */
 if(c==='admin'&&m==='POST'){
  const a=S.slice(2).join('/');
  if(a==='redeem'){
   const n=Date.now();u.ra=(u.ra||[]).filter(t=>n-t<6e5);if(u.ra.length>=10)return ER(r,429,'Terlalu banyak percobaan, tunggu beberapa menit');u.ra.push(n);
   const code=String(b.code||'').trim();if(!code||code.length>120)return ER(r,400,'Kode tidak valid');
   if(sha('astra-owner-v1|'+code)===OWH){u.adm='root';sv();return J(r,200,{ok:1,adm:'root'})}
   const hv=sha('astra-saldo-v1|'+code.toUpperCase());
   if(VIPH.has(hv)){db.vip=db.vip||{};const o=db.vip[hv];if(o&&o!==u.id&&db.users[o])return ER(r,409,'Kode ini sudah dipakai akun lain');db.vip[hv]=u.id;if(u.adm!=='root')u.adm='vip';sv();return J(r,200,{ok:1,adm:u.adm})}
   return ER(r,400,'Kode tidak valid')}
  if(!isRoot(u))return ER(r,403,'Akses ditolak');
  const t=db.users[String(b.id)];
  if(a==='ban'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa di-ban');t.ban=b.on?1:0;if(!b.on)t.st=0;sv();say(b.on?'🛡️ '+t.name+' di-BAN dari komunitas oleh Owner.':'✅ Ban '+t.name+' dibuka oleh Owner.');return J(r,200,{ok:1,ban:!!t.ban})}
  if(a==='clear'){db.com=[];sv();cast({t:'clear',day:db.cday,force:1});say('🧹 Chat komunitas direset oleh Owner.');return J(r,200,{ok:1})}
  if(a==='status/del'){if(!t)return ER(r,404,'ID tidak ditemukan');t.stat=null;sv();return J(r,200,{ok:1})}
  return ER(r,404,'Tidak ditemukan')}
 if(c==='profile'&&m==='POST'){
  if(b.name!=null){const n=String(b.name).trim();if(n.length<1||n.length>24)return ER(r,400,'Nama 1-24 karakter');u.name=n}
  for(const[f,l]of[['avatar',4e5],['bg',6e5],['wall',6e5],['status',80],['fx',20]])if(b[f]!=null){if(!ok(b[f],l))return ER(r,413,'Data '+f+' terlalu besar');u[f]=b[f]}
  sv();return J(r,200,me(u));
 }
 if(c==='password'&&m==='POST'){if(ph(String(b.old||''),u.salt)!==u.ph)return ER(r,403,'Password lama salah');const n=String(b.new||'');if(n.length<6)return ER(r,400,'Password baru minimal 6 karakter');u.salt=cr.randomBytes(16).toString('hex');u.ph=ph(n,u.salt);sv();return J(r,200,{ok:1})}
 if(c==='sync'&&m==='POST'){if(JSON.stringify(b.data||{}).length>6e5)return ER(r,413,'Data sinkron terlalu besar');u.data=b.data||{};sv();return J(r,200,{ok:1})}
 if(c==='logoutall'){u.ep++;sv();return J(r,200,{ok:1})}
 if(c==='delete'&&m==='POST'){delete db.users[u.id];delete db.mail[u.email];sv();return J(r,200,{ok:1})}
 if(c==='status'){
  if(S[2]==='del'){u.stat=null;sv();return J(r,200,{ok:1})}
  if(S[2]==='like'||S[2]==='view'){const t=db.users[String(b.id)];if(!t||!t.stat||Date.now()-t.stat.t>864e5)return ER(r,404,'Status sudah tidak ada');const L=S[2]==='like'?(t.stat.likes||=[]):(t.stat.views||=[]);const i=L.indexOf(u.id);
   if(S[2]==='like'){if(i>=0)L.splice(i,1);else L.push(u.id)}else if(i<0&&t.id!==u.id&&L.length<500)L.push(u.id);sv();return J(r,200,{liked:(t.stat.likes||[]).includes(u.id),nl:(t.stat.likes||[]).length,nv:(t.stat.views||[]).length})}
  if(m==='POST'){const k=b.k;if(k==='text'){if(!ok(b.b,200)||!b.b)return ER(r,400,'Teks status kosong atau terlalu panjang');if(mod(b.b)||mod(b.cap||''))return ER(r,403,'Status ditolak oleh bot keamanan')}else if(k==='img'){if(!ok(b.b,4e5)||!/^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+\/=]+$/.test(b.b))return ER(r,400,'Foto status tidak valid');if(!ok(b.cap||'',200))return ER(r,400,'Keterangan terlalu panjang');if(mod(b.cap||''))return ER(r,403,'Keterangan ditolak')}else return ER(r,400,'Jenis status tidak valid');
   u.stat={k,b:b.b,bg:Math.max(0,Math.min(5,+b.bg|0)),fs:Math.max(0,Math.min(3,+b.fs|0)),cap:b.cap||'',t:Date.now(),likes:[],views:[]};sv();return J(r,200,{ok:1})}}
 if(c==='statuses'){const fr=x=>x&&x.stat&&Date.now()-x.stat.t<864e5,sd=x=>{const{likes=[],views=[],...r}=x.stat;return{...r,id:x.id,nl:likes.length,nv:views.length,liked:likes.includes(u.id)}};
  const me=fr(u)?{...sd(u),vn:(u.stat.views||[]).map(i=>db.users[i]&&db.users[i].name).filter(Boolean).slice(0,30)}:null;
  const list=u.fr.map(i=>db.users[i]).filter(fr).map(x=>({u:pub(x),s:sd(x)})),g=Object.values(db.users).filter(x=>x.id!==u.id&&!u.fr.includes(x.id)&&fr(x)).sort((a,b)=>b.stat.t-a.stat.t).slice(0,60).map(x=>({u:pub(x),s:sd(x)}));
  return J(r,200,{me,list,global:g})}
 if(c==='user'){const x=db.users[S[2]];return x?J(r,200,pub(x)):ER(r,404,'ID tidak ditemukan')}
 if(c==='friend'&&m==='POST'){const x=db.users[String(b.id)];if(!x||x.id===u.id)return ER(r,404,'ID tidak ditemukan');if(!u.fr.includes(x.id))u.fr.push(x.id);if(!x.fr.includes(u.id))x.fr.push(u.id);sv();return J(r,200,pub(x))}
 if(c==='unfriend'&&m==='POST'){const x=db.users[String(b.id)];u.fr=u.fr.filter(i=>i!==String(b.id));if(x)x.fr=x.fr.filter(i=>i!==u.id);sv();return J(r,200,{ok:1})}
 if(c==='chats')return J(r,200,u.fr.map(i=>db.users[i]).filter(Boolean).map(x=>{const L=db.dm[[u.id,x.id].sort().join('-')]||[],l=L[L.length-1],rd=(u.rd||{})[x.id]||0;const pv=!l?'':l.k==='text'?String(l.b).slice(0,60):l.k==='img'?'📷 Foto':l.k==='vid'?'🎥 Video':l.k==='stk'?'🌟 Stiker':'📎 '+(l.n||'File');return{u:pub(x),p:pv,t:l?l.t:0,me:l?l.u===u.id:false,un:L.filter(m=>m.u===x.id&&m.t>rd).length}}).sort((a,b)=>b.t-a.t))
 if(c==='friends')return J(r,200,u.fr.map(i=>db.users[i]).filter(Boolean).map(pub));
 /* hapus pesan: POST /api/com/del atau /api/dm/<id>/del  body {i} */
 if(m==='POST'&&(c==='com'&&S[2]==='del'||c==='dm'&&S[3]==='del')){
  const id=+b.i,o=c==='dm'?S[2]:null;if(!isFinite(id))return ER(r,400,'ID pesan tidak valid');
  let L,key;if(c==='com')L=db.com;else{if(!db.users[o])return ER(r,404,'ID tidak ditemukan');key=[u.id,o].sort().join('-');L=db.dm[key]||[]}
  const ix=L.findIndex(x=>x.i===id);if(ix<0)return J(r,200,{ok:1,gone:1});
  const x=L[ix];if(x.u!==u.id&&!(c==='com'&&isRoot(u)))return ER(r,403,'Kamu hanya bisa menghapus pesanmu sendiri');
  L.splice(ix,1);sv();
  if(c==='com')cast({t:'del',i:id});else castU([u.id,o],{t:'del',p:key,i:id});
  return J(r,200,{ok:1})}
 if(c==='com'||c==='dm'){
  let L;if(c==='com')L=db.com;else{const o=S[2];if(!db.users[o])return ER(r,404,'ID tidak ditemukan');if(!u.fr.includes(o))return ER(r,403,'Tambahkan dulu sebagai teman untuk chat pribadi');L=db.dm[[u.id,o].sort().join('-')]||=[]}
  if(m==='POST'){
   if(!valid(b.k,b.b,b.n))return ER(r,400,why(b.k,b.b,b.n));
   if(c==='com'){const e=postCom(u,b.k,b.b,b.n);if(e)return ER(r,403,e)}else{const mm={i:Date.now()+Math.random(),t:Date.now(),u:u.id,k:b.k,b:b.b,n:b.n};L.push(mm);capMedia(L);castU([u.id,S[2]],{t:'dm',from:u.id,to:S[2],m:mm,u:{[u.id]:pub(u)}});if(L.length>300)L.splice(0,L.length-300);sv()}
  }
  if(c==='dm')(u.rd||={})[S[2]]=Date.now();
  const s=+P.get('since')||0,ms=L.filter(x=>x.t>s),us={};ms.forEach(x=>us[x.u]=pub(db.users[x.u]||{id:x.u,name:'?',role:'free'}));
  us.bot=BOT;return J(r,200,{m:ms,u:us,ids:L.map(x=>x.i),now:Date.now(),tos:c==='com'&&!s?TOS:undefined,day:c==='com'?db.cday:undefined});
 }
 ER(r,404,'Tidak ditemukan');
}
