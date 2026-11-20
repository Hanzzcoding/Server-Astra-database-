// ASTRA server: node server.js (Node 18+, tanpa dependency)
// Env: PORT, OWNERS=emailowner1,emailowner2, MAX_USERS, DB
const http=require('http'),fs=require('fs'),cr=require('crypto');
const PORT=process.env.PORT||3000,FILE=process.env.DB||'./astra-db.json',MAXU=+process.env.MAX_USERS||50000;
const OWN=(process.env.OWNERS||'').toLowerCase().split(',').filter(Boolean);
const MK=['img','vid','file','aud'];
let db={users:{},mail:{},tok:{},com:[],dm:{},next:100001};try{db=JSON.parse(fs.readFileSync(FILE))}catch(e){if(fs.existsSync(FILE))console.error('PERINGATAN: berkas data rusak ('+e.message+'), mencoba cadangan');try{db=JSON.parse(fs.readFileSync(FILE+'.tmp'))}catch{}}
const MX=()=>{for(const id in db.users){const u=db.users[id],d=u.data||{};if(!u.pl&&d.plan&&d.plan!=='free')u.pl={id:d.plan,exp:+d.planExp||0}}db.vip=db.vip||{};db.used=db.used||{};db.codes=db.codes||{};const n=Date.now();for(const t in db.tok)if(!db.tok[t].c)db.tok[t].c=n};MX();
const clean=()=>{const f=a=>(a||[]).filter(m=>!MK.includes(m.k)||m.b);db.com=f(db.com);for(const k in db.dm)db.dm[k]=f(db.dm[k])};clean();
let dirty=0,lastUp=0,loaded=!process.env.UPSTASH_REDIS_REST_URL;const sv=()=>dirty=1,fast=()=>{dirty=1;lastUp=0};
// Penyimpanan permanen (opsional, gratis): isi env UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN
const RU=process.env.UPSTASH_REDIS_REST_URL,RT=process.env.UPSTASH_REDIS_REST_TOKEN;
const rcmd=a=>(store.cmds++,fetch(RU,{method:'POST',headers:{authorization:'Bearer '+RT,'content-type':'application/json'},body:JSON.stringify(a)}).then(r=>r.json()));
const rpipe=a=>fetch(RU+'/pipeline',{method:'POST',headers:{authorization:'Bearer '+RT,'content-type':'application/json'},body:JSON.stringify(a)}).then(r=>r.json());
const rep=function(k,v){return k==='b'&&MK.includes(this.k)?undefined:v};
/* Data dipecah jadi beberapa kunci kecil (foto profil, background, data sinkron tiap pengguna, chat komunitas, tiap chat pribadi)
   supaya tidak melewati batas ukuran satu permintaan Upstash dan tidak ada data yang hilang saat server restart. */
const HEAVY=['avatar','bg','wall','cls'];
/* ===== file video wallpaper: disimpan di disk (hemat RAM), cadangan di Upstash per potongan ===== */
const path=require('path'),WPDIR=path.join(path.dirname(path.resolve(FILE)),'astra-wp'),WPRAW=450000,wpSent={},wpLeg=[];
try{fs.mkdirSync(WPDIR,{recursive:true})}catch{}
const wpPath=f=>path.join(WPDIR,String(f).replace(/[^\w]/g,'')),wpTmp=f=>path.join(WPDIR,'t-'+String(f).replace(/[^\w]/g,''));
const wpUnl=p=>{try{fs.unlinkSync(p)}catch{}};
/* unduh video/gambar dari link ke server (aman: hanya https, tolak alamat internal, maks 30 MB) */
const https=require('https'),dnsP=require('dns').promises,net=require('net');
const privIP=ip=>{if(net.isIPv6(ip)){const l=ip.toLowerCase();if(l==='::1'||l==='::'||/^f[cd]/.test(l)||/^fe[89ab]/.test(l))return true;const m=/^::ffff:(\d+\.\d+\.\d+\.\d+)$/.exec(l);return m?privIP(m[1]):false}
 const p=ip.split('.').map(Number);return p[0]===0||p[0]===10||p[0]===127||p[0]>=224||(p[0]===169&&p[1]===254)||(p[0]===172&&p[1]>=16&&p[1]<=31)||(p[0]===192&&p[1]===168)||(p[0]===100&&p[1]>=64&&p[1]<=127)};
const wpSniff=b=>b.length<12?'':b.slice(4,8).toString()==='ftyp'?'video/mp4':(b[0]===0x1a&&b[1]===0x45&&b[2]===0xdf&&b[3]===0xa3)?'video/webm':(b[0]===0x89&&b.slice(1,4).toString()==='PNG')?'image/png':(b[0]===0xff&&b[1]===0xd8&&b[2]===0xff)?'image/jpeg':b.slice(0,4).toString()==='GIF8'?'image/gif':(b.slice(0,4).toString()==='RIFF'&&b.slice(8,12).toString()==='WEBP')?'image/webp':'';
let wpBusy=0;
async function wpDl(url,id){let u;try{u=new URL(url)}catch{throw new Error('Link tidak valid')}
 for(let hop=0;hop<5;hop++){
  if(u.protocol!=='https:')throw new Error('Link harus https://');if(u.username||u.password)throw new Error('Link tidak valid');
  let ad;try{ad=await dnsP.lookup(u.hostname,{all:true})}catch{throw new Error('Alamat link tidak ditemukan')}
  if(!ad.length||ad.some(x=>privIP(x.address)))throw new Error('Alamat link tidak diizinkan');
  const res=await new Promise((ok,no)=>{const rq=https.get(u,{lookup:(h,o,cb)=>cb(null,ad[0].address,ad[0].family),timeout:20000,headers:{'user-agent':'Mozilla/5.0 (compatible; AstraBot)',accept:'video/*,image/*;q=0.9,*/*;q=0.5'}},ok);rq.on('timeout',()=>rq.destroy(new Error('Server link terlalu lama merespons')));rq.on('error',no)});
  if(res.statusCode>=300&&res.statusCode<400&&res.headers.location){res.resume();try{u=new URL(res.headers.location,u)}catch{throw new Error('Pengalihan link tidak valid')}continue}
  if(res.statusCode!==200){res.resume();throw new Error('Link tidak bisa diunduh (kode '+res.statusCode+')')}
  const cl=+res.headers['content-length']||0;if(cl>WPMAX){res.resume();throw new Error('File di link lebih dari 30 MB')}
  return await new Promise((ok,no)=>{const ws=fs.createWriteStream(wpTmp(id));let n=0,head=Buffer.alloc(0),dead=0;ws.on('error',e=>fail(e));const tm=setTimeout(()=>fail(new Error('Unduhan terlalu lama (maks 90 detik)')),9e4);
   const fail=e=>{if(dead)return;dead=1;clearTimeout(tm);res.destroy();ws.destroy();wpUnl(wpTmp(id));no(e)};
   res.on('data',d=>{if(dead)return;n+=d.length;if(head.length<16)head=Buffer.concat([head,d]).subarray(0,16);if(n>WPMAX)return fail(new Error('File di link lebih dari 30 MB'));if(!ws.write(d)){res.pause();ws.once('drain',()=>res.resume())}});
   res.on('error',fail);res.on('aborted',()=>fail(new Error('Unduhan terputus')));
   res.on('end',()=>{if(dead)return;ws.end(()=>{clearTimeout(tm);const mime=wpSniff(head);if(!n||!mime){wpUnl(wpTmp(id));return no(new Error('Link ini bukan video MP4/WebM/MOV atau gambar PNG/JPG/WebP/GIF'))}ok({n,mime,name:decodeURIComponent((u.pathname.split('/').pop()||'link').slice(0,60))})})})})}
 throw new Error('Terlalu banyak pengalihan link')}
const wpErr=(m,e)=>{store.err='wallpaper '+m+': '+e.message;store.errAt=Date.now();console.error('Upstash wallpaper gagal',m,e.message)};
function shardsOf(d){const sh={},us={};
 for(const id in d.users){const l={...d.users[id]},hv=[];for(const f of HEAVY)if(l[f]){sh['astra-u:'+id+':'+f]=l[f];hv.push(f);delete l[f]}
  if(l.data&&Object.keys(l.data).length){sh['astra-u:'+id+':data']=JSON.stringify(l.data);hv.push('data')}delete l.data;
  if(l.stx&&l.stx.length){sh['astra-u:'+id+':stx']=JSON.stringify(l.stx);hv.push('stx')}delete l.stx;l._h=hv;us[id]=l}
 sh['astra-com']=JSON.stringify(d.com,rep);const dmk=Object.keys(d.dm).filter(k=>(d.dm[k]||[]).length);for(const k of dmk)sh['astra-dm:'+k]=JSON.stringify(d.dm[k],rep);
 {const{pay,wp,wpf,spin,spd,...rest}=d;sh['astra-db']=JSON.stringify({...rest,users:us,com:[],dm:{},dmk},rep);const wm={};for(const f in wpSent)if(wpf&&wpf[f])wm[f]=wpSent[f];sh['astra-cfg']=JSON.stringify({prices:d.prices||{},pay:pay||{},pv:d.payv||0,wp:wp||null,wpm:wm,sp:spin||null,spd:spd||null})}return sh}
const sent={};let needMig=0,busy=0;const store={up:!!process.env.UPSTASH_REDIS_REST_URL,ok:0,err:'',errAt:0,cmds:0,dbKB:0,bootOk:0};
function writeLocal(){const t=JSON.stringify(db,rep);fs.writeFile(FILE+'.tmp',t,e=>!e&&fs.rename(FILE+'.tmp',FILE,()=>{}))}
async function wpPush(){let ok=true;
 while(wpLeg.length){const[f,n]=wpLeg[0];try{const ks=[];for(let i=0;i<n;i++)ks.push(['DEL','astra-wpf:'+f+':'+i]);for(let i=0;i<ks.length;i+=20){const rr=await rpipe(ks.slice(i,i+20));if(!Array.isArray(rr))throw new Error('pipeline gagal')}wpLeg.shift()}catch(e){ok=false;wpErr('hapus lama',e);break}}
 for(const f of Object.keys(wpSent))if(!db.wpf||!db.wpf[f]){const m=wpSent[f];try{for(let i=0;i<m.c;i+=20){const ks=[];for(let j=i;j<Math.min(m.c,i+20);j++)ks.push(['DEL','astra-wpb:'+f+':'+j]);const rr=await rpipe(ks);if(!Array.isArray(rr)||rr.some(x=>x&&x.error))throw new Error('hapus gagal')}delete wpSent[f]}catch(e){ok=false;wpErr('hapus '+f,e)}}
 for(const f in (db.wpf||{})){if(wpSent[f])continue;let fd=null;try{const sz=fs.statSync(wpPath(f)).size,c=Math.ceil(sz/WPRAW),buf=Buffer.alloc(WPRAW);fd=fs.openSync(wpPath(f),'r');
   for(let i=0;i<c;i++){const n=fs.readSync(fd,buf,0,WPRAW,i*WPRAW),r=await rcmd(['SET','astra-wpb:'+f+':'+i,buf.subarray(0,n).toString('base64')]);if(r.error)throw new Error(r.error)}
   fs.closeSync(fd);fd=null;wpSent[f]={c,n:sz,m:db.wpf[f].m}}catch(e){ok=false;if(fd)try{fs.closeSync(fd)}catch{}wpErr('unggah '+f,e)}}
 return ok}
async function pushUp(){if(!RU||!loaded)return true;let ok=true;if(!await wpPush())ok=false;const sh=shardsOf(db);store.dbKB=Math.round((sh['astra-db']||'').length/1024);
 {const w={};for(const id in db.users){const u=db.users[id];if(u.sal!==undefined)w[id]=[u.sal,u.wv||0,(u.stx||[]).slice(0,8)]}const wj=JSON.stringify(w);
  if(sent['astra-wal']!==wj){try{const r=await rcmd(['SET','astra-wal',wj]);if(r.error)throw new Error(r.error);sent['astra-wal']=wj}catch(e){ok=false;{store.err='astra-wal: '+e.message;store.errAt=Date.now();console.error('Upstash gagal astra-wal',e.message)}}}}
 if(!needMig){const m0=sh['astra-db'];if(sent['astra-db']!==m0){try{const r=await rcmd(['SET','astra-db',m0]);if(r.error)throw new Error(r.error);sent['astra-db']=m0}catch(e){ok=false;{store.err='astra-db: '+e.message;store.errAt=Date.now();console.error('Upstash gagal astra-db',e.message)}}}}
 for(const k of Object.keys(sh)){if(k==='astra-db'||sent[k]===sh[k])continue;try{const r=await rcmd(['SET',k,sh[k]]);if(r.error)throw new Error(r.error);sent[k]=sh[k]}catch(e){ok=false;{store.err=k+': '+e.message;store.errAt=Date.now();console.error('Upstash gagal',k,e.message)}}}
 for(const k of Object.keys(sent))if(k!=='astra-wal'&&!(k in sh)){try{await rcmd(['DEL',k]);delete sent[k]}catch(e){}}
 if(needMig&&!ok)return false;
 const m=sh['astra-db'];if(sent['astra-db']!==m){try{const r=await rcmd(['SET','astra-db',m]);if(r.error)throw new Error(r.error);sent['astra-db']=m}catch(e){ok=false;{store.err='astra-db: '+e.message;store.errAt=Date.now();console.error('Upstash gagal astra-db',e.message)}}}
 if(ok){needMig=0;store.ok=Date.now();store.err=''}return ok}
function reconcile(w,src){let n=0;for(const id in w){const u=db.users[id],x=w[id];if(!u||!x)continue;bal(u);if((x[1]||0)>(u.wv||0)){u.sal=Math.max(0,Math.floor(+x[0]||0));u.wv=x[1];if(Array.isArray(x[2])&&x[2].length)u.stx=x[2];n++}}if(n)console.log('Dompet dipulihkan dari '+src+': '+n+' akun');return n}
async function wpFetch(f,m){const p=wpPath(f);try{if(fs.statSync(p).size===m.n)return}catch{}
 const fd=fs.openSync(p,'w');try{for(let i=0;i<m.c;i+=10){const ks=[];for(let j=i;j<Math.min(m.c,i+10);j++)ks.push(['GET','astra-wpb:'+f+':'+j]);const rr=await rpipe(ks);if(!Array.isArray(rr)||rr.some(x=>!x||x.error||x.result==null))throw new Error('wallpaper potongan gagal dimuat');for(const x of rr)fs.writeSync(fd,Buffer.from(x.result,'base64'))}}finally{fs.closeSync(fd)}
 if(fs.statSync(p).size!==m.n)throw new Error('wallpaper ukuran tidak cocok')}
async function loadCfg(){let c=null;try{const rc=await rcmd(['GET','astra-cfg']);if(rc.error)throw new Error(rc.error);if(rc.result){sent['astra-cfg']=rc.result;c=JSON.parse(rc.result)}}catch(e){console.error('loadCfg:',e.message);return}
 if(!c)return;db.pay=c.pay||{};if(c.pv)db.payv=c.pv;if(c.prices&&Object.keys(c.prices).length&&!Object.keys(db.prices||{}).length)db.prices=c.prices;
 if(c.sp)db.spin=c.sp;if(c.spd)db.spd=c.spd;
 if(c.wp){db.wp=c.wp;db.wpf={};
  for(const f in (c.wpm||{})){const m=c.wpm[f];await wpFetch(f,m);db.wpf[f]={n:m.n,m:m.m};wpSent[f]=m}
  for(const f in (c.wpc||{})){if(db.wpf[f])continue;const ks=[];for(let i=0;i<c.wpc[f];i++)ks.push(['GET','astra-wpf:'+f+':'+i]);const rr=await rpipe(ks);if(!Array.isArray(rr)||rr.some(x=>!x||x.error||x.result==null))throw new Error('wallpaper potongan lama gagal dimuat');const mm=/^data:([^;]+);base64,([\s\S]*)$/.exec(rr.map(x=>x.result).join(''));if(!mm)continue;const bf=Buffer.from(mm[2],'base64');fs.writeFileSync(wpPath(f),bf);db.wpf[f]={n:bf.length,m:mm[1]};wpLeg.push([f,c.wpc[f]])}}}
async function loadUp(){const r=await rcmd(['GET','astra-db']);if(r.error||(r.result===undefined))throw new Error(r.error||'respons Upstash tidak valid');if(!r.result)return;const m=JSON.parse(r.result);sent['astra-db']=r.result;
 const legacy=Array.isArray(m.com)&&m.com.length>0||Object.values(m.users||{}).some(u=>u&&(u.avatar||u.bg||u.wall||(u.data&&Object.keys(u.data).length)))||!m.dmk;
 if(legacy){db=m;needMig=1;MX();clean();await loadCfg();try{const rw=await rcmd(['GET','astra-wal']);if(rw.result)reconcile(JSON.parse(rw.result),'Upstash (cadangan)')}catch(e){}return}
 const keys=['astra-com'];for(const id in m.users)for(const f of m.users[id]._h||[])keys.push('astra-u:'+id+':'+f);for(const k of m.dmk||[])keys.push('astra-dm:'+k);
 const vals={};for(let i=0;i<keys.length;i+=20){const ch=keys.slice(i,i+20),rr=await rpipe(ch.map(k=>['GET',k]));if(!Array.isArray(rr)||rr.some(x=>x&&x.error))throw new Error('pipeline Upstash gagal');ch.forEach((k,j)=>{if(rr[j]&&rr[j].result!=null){vals[k]=rr[j].result;sent[k]=rr[j].result}})}
 m.com=vals['astra-com']?JSON.parse(vals['astra-com']):[];m.dm={};for(const k of m.dmk||[])m.dm[k]=vals['astra-dm:'+k]?JSON.parse(vals['astra-dm:'+k]):[];
 for(const id in m.users){const u=m.users[id];for(const f of u._h||[]){const v=vals['astra-u:'+id+':'+f];if(v==null)continue;u[f]=(f==='data'||f==='stx')?JSON.parse(v):v}delete u._h;if(!u.data)u.data={}}
 delete m.dmk;db=m;MX();clean();await loadCfg();try{const rw=await rcmd(['GET','astra-wal']);if(rw.result){sent['astra-wal']=rw.result;reconcile(JSON.parse(rw.result),'Upstash (cadangan)')}}catch(e){}}
setInterval(async()=>{if(!dirty||busy)return;busy=1;try{writeLocal();if(RU&&Date.now()-lastUp>12000){dirty=0;lastUp=Date.now();if(!await pushUp())dirty=1}else if(!RU)dirty=0}catch(e){console.error(e.message)}busy=0},2000);
/* token login: buang yang sudah lama dan batasi per akun supaya data tidak membengkak */
function pruneTok(){const n=Date.now(),per={};const ks=Object.keys(db.tok).sort((a,b)=>(db.tok[b].c||0)-(db.tok[a].c||0));let ch=0;for(const t of ks){const k=db.tok[t];per[k.id]=(per[k.id]||0)+1;if(n-(k.c||0)>60*864e5||per[k.id]>8||!db.users[k.id]){delete db.tok[t];ch=1}}if(ch)sv()}
setInterval(pruneTok,36e5);
const ph=(p,s)=>cr.scryptSync(p,s,32).toString('hex');
const J=(r,c,o)=>{r.writeHead(c,{'content-type':'application/json','access-control-allow-origin':'*','access-control-allow-headers':'content-type,authorization,x-mime','access-control-allow-methods':'GET,POST,OPTIONS'});r.end(JSON.stringify(o))};
const ER=(r,c,e)=>J(r,c,{e});
const flush=()=>{try{fs.writeFileSync(FILE,JSON.stringify(db,rep))}catch{}try{snapLocal('lite','shutdown')}catch{}return RU?Promise.race([pushUp(),new Promise(r=>setTimeout(r,20000))]).catch(()=>{}):Promise.resolve()};
for(const sg of['SIGTERM','SIGINT'])process.on(sg,()=>flush().finally(()=>process.exit(0)));
const hits={};setInterval(()=>{for(const k in hits)delete hits[k]},60000);
const RL=['free','go25','go50','go75','go150','go250','q3','y1','y2','y5'];
/* Akses admin disimpan di server (u.adm), tidak bisa dipalsukan lewat sinkron data. Hanya hash kode yang disimpan di sini. */
const sha=s=>cr.createHash('sha256').update(s).digest('hex'),OWH='c8d8389a2310ceef7fae2fdd36927d583e45b854d42943b5b6e6a07b02729564',VIPH=new Set(["7b153f87a01031daa0c15f626f571de239879eb9def69f6bc90950c1bcca934c", "4bef8465249b0e3c62eebc7364de380231a9357aaeacde3a14e6458d33814709", "52c5d30cb54bb549a4b29e6c1ab0bfa9f19777c23fd28206f6b7445a91b9eae7", "b4cc94adfbb3e091ca801ec1973c7697e51fd9994381300c934e212a42ddf267", "08be2fac21dd2f777d8421f822eedc4d841f7647d07b59491f989f38356c7a1c"]);
const isRoot=u=>OWN.includes(u.email)||u.adm==='root',isAdm=u=>isRoot(u)||u.adm==='vip';
const CODES={"d8dbf41d08a5e515abe450bde3d8a2875c39f133310bb1a266f5c142c7d86e26": "go50", "6d373b785dd8f8e767bfddae418edaf049f55b9dfcbecd25a66f92b40bf8f248": "go50", "bc16722b9b4294814079f54a24771ed0462d499bc3454985482ead29876367cd": "go50", "dab6e225348072ca01fffe18f877745620d743fe93d4015e55d2f4bce10fa115": "go50", "dae021a9b6fe270bfa7b5ee829ce7a613387c6f9c380dc28c219b6ad052f14ab": "go50", "492c69cbdd10b825d4d48e45ba326a83893cc76a449e1dc9f008f27a42d5539b": "go50", "617ac6a6623b7cfc3b1effe300c95546d56926ec0a3fdb32224eec387f833492": "go50", "fc6bf7dc70c294a6d34b744db62ca95ba9317dec5028204b6243c39cf1854006": "go50", "7935ec0ec196da1c57b384ffaeedc9162bca99d7e57013549043494909838a6f": "go50", "03e45e98c3196587defacd8fb275dc11c82a479a48ee4f985eb11789ba36edeb": "go50", "18723216a2d22188bb41519cd20e18a20d80e3a11a439bd8e41254a3e5bbf22b": "go75", "8963558a0704f060a641cc3219fb3703f2962279bab6107746bf151b725388ec": "go75", "5d2d60b35ec700d6605d162a1623a3e6c73df221faa81670352a22615e6c5273": "go75", "5a04817d9edea916d76bcf817825e6ae2eb31277c8782b444e5828aeb017f7da": "go75", "4f86713e670aea6db2382f9dd953fe962f5c7934446627b950d35ca939204704": "go75", "e5a669bb75d4b2013d8470538d622f2624cf03554ebabeb954a7af998c81f5f4": "go75", "aa0a95af8d6fb939db51d395bc52f8a4c4659c95d719a31e276bef836e6ee41a": "go75", "8865ca7a845f1346456e4b19f2998f61a1c34cfd80cdc1d8eb69320ad3e55625": "go75", "ac21d7a89b17f8f9fe04cb8a03ccdcca5d232dc9cae1018ee961052d8907df47": "go75", "982814c50f4f5d7ea417f6a910d126d295488b5fdea9b1330f312fabe2e128b1": "go75", "40b38fadf792c9b5d032f4cff00852b8db4320956fdccb68ad248f6b4126ae3b": "go250", "5a9f13fc6da3b90d73e3447f872b4db4c4486ccea45577847014994b5a3cb967": "go250", "598d8fad1df314aeedcc493a13dd30b245d0277ac7e78591ddb35d32a7fbeee9": "go250", "8093070a3c1a81fc134e4deba59e023582bb6299e99a8bb41be42bb79b69ea3a": "go250", "5b9548c44c0a6b777baffd1c39dab44602524c2f0c6654e2deb990fa77b715fd": "go250", "76bab73d2142a2a38691cedaa2a895735237aa06b09caa3dc07c2b41de838c87": "go250", "05b2dbffe9c523646ff2dc926c1a1a5dbffb12de5ebae1a8560caa9445161cd5": "go250", "7ebbd86746ffd78406f3c39273fd4ce01d7d643fd546c8726cd99fdaf6a7b7e9": "go250", "733750262bd67fced23f310e475f2a2a9922bcec8ce9b6039033b68ae7f606cc": "go250", "29aaa31152c0259e1533bf7228215825d155a2ce37e8949abbc13ae821757e5e": "go250", "2cdb7e160497c072a3cd703c6b5b0d8f3488d26ff3a5a9301f00053e5424168a": "go50", "a0279ffb1e273f0c4513511b4057d6471acca299a2eba0f95dcfcef5e7343411": "go50", "3dfff68f718e9e9ad26c040e72115eff40cbf7960d817d41a564532e8558c258": "go50", "5d65cfb32ca0b6c49858518b66672f14f7272c85d8550af3ff5300da1fb0250a": "go50", "483a915675977b474d38a96a3f477b03a8f032facc7c80dda9f5163bfff8f2de": "go50", "9deb87d9e9ac506bf0b12e9125b66d4850767bbe2507d7ab69095e3e025255b6": "go50", "2074f572c8536d02d525626080df1e437f551c23d1af201d3e6f7664f77a678d": "go50", "b5df279b50e8a5f06b3b385bb645ca586c0801468162e9b919d5af2d48092f78": "go50", "0056773ac449f1c6d1281968fb011a7464a134f47cd3705bdf004c2ac1cafae0": "go50", "5d62cf6cd329e4a6a8fbb30d95ffce48ce6c8176467dc9694ca6d7bd75165580": "go50", "acbadea3158ebf992eadaaa4495de31f0d328993b542685165e863601570edeb": "go50", "026758aa87755cb62b853b841ba830906e17a20d3b8264697ff860fc05e03659": "go50", "3dbebd758204de7b91466918b85f6fab3599eee19cedec60e157f298dc9ab7a8": "go50", "fb4c1d84272063264f5adb586f8e0fa6e407c7734fd303d9db0fa47356fb4521": "go50", "c2d9a23291e7d8bf8daeb4ed59a53474be478fd1a2b4c111c7337e1002a3e2b1": "go50", "b994f224b852606732d6e7d9fc4747dca6102f6293fb06808a0374e149dcad9b": "go75", "7e9b14ba052e96594905be26e97dc86e549d37ab8513867afa0c0d1476308a24": "go75", "6071d3e929d9fa601d4a1386bdd5d1e9b165ab595af679cbd1ac30d0e83113a2": "go75", "b676ee2fca8a1a56d7498291442eb5a4d92fac0f6d3e4a47ff6ed7e5e5a7f6ce": "go75", "c1db06b7864f6f12408675757ca045577dd651659156a80603731f198aee93a0": "go75", "be3d4bbb3d7a25401a40a16f7cb4d6c92ea897aa7a72c161eadafbd66802b20c": "go75", "16f4fb5c1585e56e41cb19abad70290cfd2ee3468b3f2994ad82cea5903e6797": "go75", "3fd7647b154b6742119dbc17b00b3cac4728f9089c4d93fd2ceda81984888b16": "go75", "2fd33705cdce19710a3e68cda1a589027425b35d296e4e35235169b8705cceac": "go75", "9ad94471d63abd62f2f615ae0ebab7b0c38581966dd738d5744a4832f320ac18": "go75", "e22d0c3dfa391e55e38fa9c976a9ceee33a0c606be419336ebfee185a13d24e2": "go75", "6b9fa7f71cf7274e9acff3fed322b2cbb9412aa0df51cbdaaa15dc650d92534c": "go75", "2fac58c1e20df18c32e5bd3b5ccdd91bf8a030811491a32b618992cd94e7e153": "go75", "8d281cf4f0817f7890b1174b89cec2a4437029a31e4adcf0efdbf02d9c9a80e0": "go75", "55262f635f4c60ca4422b10c879f841416e1b974e5461846409355b5544d33ed": "go75", "a898bac1c05254e6356c36fa5ed12797fee022a1797fe8d4d0faccdec943e56d": "go250", "178549e44c96f89b4e7ab14c8768108e6987fca47691265a4d04871436ef7b16": "go250", "677c01486cf1a5e1b0d9e0a336af401106ce0fc7d6b35584ef66652214fcbf6e": "go250", "1ab6503117642a11a591c91259acab1c4380da2faf90eaf1f61c1f849fb89bd5": "go250", "539a73e2d0a76bce42e55600edd34765aebeb62da23e1b914a1afab6d5410cc9": "go250", "a5468f34202a5efb85d5d2baddf74ba5316c295ece2f9f110b28a619a11b8b17": "go250", "06b73fe2791bbf0e698fd98fcaa1f0fb143cd86f13f182e10b3021e8ba919747": "go250", "ab7a57651337a042eba177d139495dc22143cc4320af1d0df85231687da43313": "go250", "a51ab75c5725bbe527780e1a982eca5c8e7c7d067e7910ee11637f48cef4ee79": "go250", "315cf769c906ed235b05a023697e13a17ba4e5f3a0f52f0391dfc80b25783d26": "go250", "7f4cfc6ae6c20883a2def28819d5d16f63fa255ae4cc61297c549b6c7ab57532": "go250", "fec7a3d4a4978bc683001f68d0daf0a7497e9a0a5747e53d844e5727be9dab2f": "go250", "fc50fba832c16938748bada80eb382a59e55e3b883d0200f2978c0fbfa5ee7af": "go250", "d3e7918d4ecf80a295a49ce3ecae30ce31b447d2846a67f154ff43a7038c5803": "go250", "93469b0fe09702f5db6b130a999594eea9d05b4847aafe0e901e19306e467031": "go250", "2dc1893eefd1a8ae9acfedc62f5145231d06b7718a0aba84aaba3ad28204ae0d": "go250", "d2c688ce682492c93c0dba353d2066ab4e0f16636e9d5d79ccd4194b919c941b": "go250", "f322c17a8699b18a28a7d96fa32ae257526ad10ecb756d4fbd00a377e5403b4d": "go250", "770ffd439420d5367133c0e908c9cfb91802bee408a9634dc51287bba4b64c2b": "go250", "2d1d7cb50a22870b95955197f37b1b2cb086fefc66cda190438ca66d45762df1": "go250"},SCODES={"38e7b95986dd5f48299e8f627100f8d06e426b797e5e8465784307e24dce5878": {"a": 10000}, "0a74b19ffdfd0af5221f66c8ed189706da8362c651101bd0f63838ca9fb9cecc": {"a": 15000}, "a6294cd9d1404660b9a5d0461adce49387ba037456d142e22a9f89b2317ccbdc": {"a": 10000}, "73c52080ab2f95e2e6d7848cfbc10a60f4ae9bba05a88c29aaec9bc5d3b0dd3d": {"a": 5000}, "72dfb0c46997beb4a8fa1cca06f0099e1ad1841e4aeefc0d364da108f7213948": {"a": 10000}, "04c24ea478daa4ceee757fd281802ba08c608010c2d8599ed11b2ea3c4ac149b": {"a": 15000}, "22a44910617ceb1e08fb37bb326559d025ba5841eec156e3931ad4662a58c9b6": {"a": 10000}, "3ea4c67900f6c78c03ae07e55551f1a06f9d7d379eb924f787521d4f6844c786": {"a": 5000}, "8338ff2ba0fd9bb467cb876d0b7272c39c51e8c6de2b748c830d1e16f9967dd4": {"a": 10000}, "a8fa042be88575ddc447fc5f730874adb0d3cb07de458c39e71cbea420af9d1d": {"a": 15000}, "fb8be25942fcd5210a356903b90a9ddd51ae71e03567586d848599d99fef24af": {"a": 10000}, "a2059dd3bae8c77987241ec89ed07990f0ff681d6c2342b2ee8c90f20806ddf0": {"a": 5000}, "1634aab6aee2b2df660508ffb06b48d25f4a6117b335bf1b5a81759f259dc635": {"a": 50000}, "925b22a1663c8a24664346dcaf913c6d62e9d9d7afaadec6553131588a6503f0": {"a": 15000}, "02ff6264f7ec6c0c01c1b662470d6918e3d7bdb6d415c2966baec30968b6f2ea": {"a": 10000}, "e9c7958c363f6f5852284ef671e867d6883def569d6e6a21bfe33fba0c66deb6": {"a": 5000}, "29d231547403dd4ea78f12501e518fff3866dd1328c71988063b1daf9ec1fc30": {"a": 10000}, "cbec8e37e9774eea5a83817a068ffd44b9308c37c6c5871679b34793f6abe118": {"a": 15000}, "749dbde2fe7ff52838aab907aad16607af6f17052bc229fce89972af0fba016b": {"a": 10000}, "3b28710d5c7ff8cedbe97fda8b4f984e6ae97f6da53a79d82e844918ee75e75a": {"a": 5000}, "1c4607323397cd48ced66eecbd6136a2850c58fde0a33823a1bfe43eb2c38bfa": {"a": 10000}, "b8240cfd29b866401b4630ab7635f272cc807e82933f33f17ebc4cec6b7c5720": {"a": 15000}, "6639dd5124157a9bc1269f7372bbdb6e9d4c2c372916eeed4f1ffd28cbb83c56": {"a": 10000}, "ad8e4a38456a5dfc1af2abc6d0eb7e6cc302235c26799d21893d7503da1a0f02": {"a": 5000}, "037b4eed39a736ca1de23b5e03123f37d6b5a6974d3af1a823a1329852c3902d": {"a": 10000}, "ac607d3405a8c35df99a1288a5d6b78bdc40198c72985c0ba6e290febc7fb3ee": {"a": 15000}, "ed1563caa1253f98ec357be75a51c2e83c221abcb851c620f9c65d3fa84c5c4d": {"a": 10000}, "284b17d730e48c1edeb1f6647ae774a1a8e35842ae5cb66c485dce7cccad6a46": {"a": 5000}, "b8a5a9e91fa12c2830cb5d5a3453de27203c4f8c6f348287fe5bed68d61aa4a7": {"a": 10000}, "a36c42c19444f8efa14a697e34f72a4bec6777bb58d7273f2ed1496fd442bbeb": {"a": 15000}, "f200f94f07da8208bb1f48ec119365aad4ad73dca029a6c0e95cd77295a296a5": {"a": 10000}, "98f0e2ab361d0c0540b8e667bf04239747648207da9fc8a20e4d04e73fbb9a9a": {"a": 5000}, "656334f99d2e546c474697bc65f256741758f995a999d8ee982e4e04e34ab7e9": {"a": 10000}, "d98306fb807e670a47ed0eee51fcb1a215eeb4da5f4ee87616632a1b02e33333": {"a": 15000}, "1ef7be2ca1d248cc06faef0659146d60d563d4c51aa067f598d366f1c16a7c69": {"a": 10000}, "52e48f19ac956be5eb3826c6ed1fd04734d41cd4f76025923afbc6bb7bf5801f": {"a": 5000}, "fed85361c6847ef3d6ffb4aef728800d68db60a93a9aa3fd5f9848169b3d3343": {"a": 10000}, "534988db46a5a30ddbd89f7392d92abf4e9aee1c854a904d78f060e2fce7ed3d": {"a": 15000}, "1e2f221c1add787fd1441bc9ff5625ce8a94266a3b100418fbe77d10feb230f1": {"a": 10000}, "9313cf1b54f2b45a3412ab0e8cef01e5cce273fdff2210689c835536f9cc0003": {"a": 5000}, "0526dbb6481242b7ddbcfa1201fbd4ee47aa04cae9f64e15af8e1dda5cd88308": {"a": 10000}, "2f3b7501a675d624a6b09bd0c5e971098df1a91aea0fe330c3f86cb9bb7a0bf8": {"a": 15000}, "38ea90c159120012277257925ce294e3864b4be820689fac96fe9c256c6ffb56": {"a": 10000}, "9e3cfc8c0d47c054b0b8d54ad740ae399eea8dfad2bd01bbfde7d82920db6edc": {"a": 5000}, "e64bf9df484c93831cef879bf9711f2114177602019c736506bd2684cec77d38": {"a": 10000}, "eb623cd85e1f6f53ab8459d9d0819c484f184ce032b2cddba045a8cb14b31cd8": {"a": 15000}, "f30913e3c437e8e0e6b174e25f5d78a36aca827f3643b4ba4ec3a1bbb7638233": {"a": 10000}, "432abdddfdedec23d415ec305b351a18e680a1c38a9d1b514ed3ed4d14e82088": {"a": 5000}, "d9528d9fd8f2ca06c3362f1e3cf257c718f70e815983f921f76cf85414552aad": {"a": 10000}, "7b153f87a01031daa0c15f626f571de239879eb9def69f6bc90950c1bcca934c": {"a": 200000, "u": 1}, "4bef8465249b0e3c62eebc7364de380231a9357aaeacde3a14e6458d33814709": {"a": 200000, "u": 1}, "52c5d30cb54bb549a4b29e6c1ab0bfa9f19777c23fd28206f6b7445a91b9eae7": {"a": 200000, "u": 1}, "b4cc94adfbb3e091ca801ec1973c7697e51fd9994381300c934e212a42ddf267": {"a": 200000, "u": 1}, "08be2fac21dd2f777d8421f822eedc4d841f7647d07b59491f989f38356c7a1c": {"a": 200000, "u": 1}};
/* verifikasi 2 langkah untuk kode owner utama (hanya hash yang disimpan) */
const H2P='853bef6e71d14d253816305cbf60f05ffb2f24a83c58340a3d6699b94791619e',H2F='1243dec314688caee290d019965530cdf3fbba5d02c4d5908a2f4b0dda53d98c';
const chal={};setInterval(()=>{const n=Date.now();for(const k in chal)if(chal[k].exp<n)delete chal[k]},60000);
/* saldo disimpan di server (tidak bisa disuntik dari aplikasi) */
const bal=u=>{if(u.sal===undefined){const d=u.data||{};u.sal=Math.max(0,Math.floor(+d.saldo||0));u.stx=Array.isArray(d.tx)?d.tx.slice(0,100):[]}return u.sal};
const addTx=(u,amount,note,ref)=>{bal(u);u.wv=(u.wv||0)+1;fast();try{jrn(u,amount,note)}catch{}(db.glog=db.glog||[]).unshift({t:Date.now(),u:u.name,id:u.id,a:amount,n:String(note||'').slice(0,70)});if(db.glog.length>150)db.glog.length=150;u.stx=[{t:Date.now(),amount,note,ref},...(u.stx||[])].slice(0,100)};
const credit=(u,a,note,ref)=>{u.sal=bal(u)+a;addTx(u,a,note,ref)};
/* ===== CADANGAN BERLAPIS: jurnal saldo, snapshot lokal, snapshot Upstash, pemulihan otomatis ===== */
const zl=require('zlib'),BKD=path.join(path.dirname(path.resolve(FILE)),'astra-backups'),JRN=path.join(BKD,'ledger.log');
try{fs.mkdirSync(BKD,{recursive:true})}catch{}
const BK={hash:'',chash:'',t:{lite:0,full:0,cloud:0},cerr:'',cn:0,boot:{add:0,upd:0,src:''}};
const jrn=(u,a,n)=>{try{fs.appendFile(JRN,JSON.stringify({t:Date.now(),id:u.id,a,b:u.sal,wv:u.wv,n:String(n||'').slice(0,60)})+'\n',()=>{})}catch{}};
const stampOf=t=>new Date(t+7*36e5).toISOString().slice(0,16).replace(/[-:]/g,'').replace('T','-');
const ledgerOf=()=>{const w={};for(const id in db.users){const u=db.users[id];if(u.sal!==undefined)w[id]=[u.sal,u.wv||0]}return w};
const snapBody=lite=>{const us={};for(const id in db.users){const x={...db.users[id]};for(const f of HEAVY)delete x[f];if(lite){delete x.data;if(x.stx)x.stx=x.stx.slice(0,20)}us[id]=x}return{v:1,t:Date.now(),lite:!!lite,users:us,mail:db.mail,codes:db.codes,used:db.used,vip:db.vip,prices:db.prices||{},pay:db.pay||{},words:db.words||[],next:db.next,fx:db.fx||null,dly:db.dly||null}};
const bkFiles=kind=>{try{return fs.readdirSync(BKD).filter(f=>f.startsWith(kind+'-')&&f.endsWith('.json.gz')).sort().reverse().map(f=>{const st=fs.statSync(path.join(BKD,f));return{f,sz:st.size,t:st.mtimeMs}})}catch{return[]}};
const bkPrune=(kind,keep)=>{const L=bkFiles(kind),seen=new Set();L.forEach((x,i)=>{if(i<keep)return;const d=x.f.slice(kind.length+1,kind.length+9);if(!seen.has(d)&&seen.size<14){seen.add(d);return}try{fs.unlinkSync(path.join(BKD,x.f))}catch{}})};
const snapLocal=(kind,why)=>{try{const body=snapBody(kind==='lite');body.why=why||'otomatis';const buf=zl.gzipSync(JSON.stringify(body));const f=kind+'-'+stampOf(Date.now())+(why&&why!=='otomatis'?'-m':'')+'.json.gz';fs.writeFileSync(path.join(BKD,f+'.tmp'),buf);fs.renameSync(path.join(BKD,f+'.tmp'),path.join(BKD,f));BK.t[kind]=Date.now();bkPrune(kind,kind==='lite'?48:24);try{if(fs.statSync(JRN).size>6e6)fs.renameSync(JRN,path.join(BKD,'ledger.old.log'))}catch{}return f}catch(e){console.error('snapshot lokal gagal',e.message);return null}};
const cloudIdx=async()=>{try{const r=await rcmd(['GET','astra-bk:idx']);return r&&r.result?JSON.parse(r.result):{list:[],n:0}}catch{return{list:[],n:0}}};
const snapCloud=async(why,daily)=>{if(!RU||!loaded)return{ok:0,e:'Upstash belum diatur'};try{const body=snapBody(true);body.why=why||'otomatis';const b64=zl.gzipSync(JSON.stringify(body)).toString('base64');if(b64.length>9e5)throw new Error('cadangan terlalu besar untuk Upstash gratis ('+Math.round(b64.length/1024)+' KB)');
  const ix=await cloudIdx(),n=ix.n||0,k='astra-bk:s'+(n%8),r=await rcmd(['SET',k,b64]);if(r.error)throw new Error(r.error);let tot=0,cnt=0;for(const id in db.users){tot+=db.users[id].sal||0;cnt++}
  const ent={k,t:Date.now(),why:why||'otomatis',sz:Math.round(b64.length/1024),acc:cnt,sum:tot};ix.list=[ent,...(ix.list||[]).filter(x=>x.k!==k)].slice(0,16);ix.n=n+1;
  if(daily){const dk='astra-bk:d'+new Date(Date.now()+7*36e5).getUTCDay(),r2=await rcmd(['SET',dk,b64]);if(!r2.error){ix.list=[{...ent,k:dk,why:'harian'},...ix.list.filter(x=>x.k!==dk)].slice(0,16)}}
  const r3=await rcmd(['SET','astra-bk:idx',JSON.stringify(ix)]);if(r3.error)throw new Error(r3.error);BK.t.cloud=Date.now();BK.cerr='';BK.cn++;return{ok:1,ent}}catch(e){BK.cerr=e.message;return{ok:0,e:e.message}}};
const readSnapLocal=f=>{if(!/^[a-z]+-[0-9-]+(-m)?\.json\.gz$/.test(f))throw new Error('nama berkas tidak valid');return JSON.parse(zl.gunzipSync(fs.readFileSync(path.join(BKD,f))))};
const readSnapCloud=async k=>{if(!/^astra-bk:[sd]\d$/.test(k))throw new Error('kunci tidak valid');const r=await rcmd(['GET',k]);if(!r||!r.result)throw new Error('cadangan cloud kosong');return JSON.parse(zl.gunzipSync(Buffer.from(r.result,'base64')))};
/* gabungkan snapshot ke data hidup: tidak pernah menurunkan saldo kecuali force */
const mergeSnap=(d,force,onlyWallet)=>{let add=0,upd=0,pl=0;const now=Date.now();
 for(const id in d.users){const bu=d.users[id];if(!bu||!bu.email||!bu.ph||!bu.salt)continue;const cu=db.users[id];
  if(!cu){if(onlyWallet)continue;if(db.mail[bu.email]&&db.mail[bu.email]!==id)continue;db.users[id]={...bu,avatar:'',bg:'',wall:''};db.mail[bu.email]=id;add++;continue}
  bal(cu);const bs=Math.max(0,Math.floor(+bu.sal||0));if(force?bs!==cu.sal:((bu.wv||0)>(cu.wv||0)||(!(cu.wv)&&cu.sal===0&&bs>0))){cu.sal=bs;cu.wv=Math.max(cu.wv||0,bu.wv||0)+1;if(Array.isArray(bu.stx)&&bu.stx.length)cu.stx=bu.stx;upd++}
  if(!onlyWallet&&bu.data&&(!cu.data||!Object.keys(cu.data).length))cu.data=bu.data;
  if(!onlyWallet&&bu.pl&&bu.pl.exp>now&&(!cu.pl||!cu.pl.id||cu.pl.id==='free')){cu.pl=bu.pl;pl++}}
 if(!onlyWallet){for(const k in(d.codes||{}))if(!db.codes[k])db.codes[k]=d.codes[k];for(const k in(d.used||{}))if(!db.used[k])db.used[k]=d.used[k];
  if(d.prices&&(force||!Object.keys(db.prices||{}).length))db.prices=d.prices;if(d.pay&&(force||!Object.keys(db.pay||{}).length)){db.pay=d.pay;db.payv=Date.now()}
  if(d.fx&&!db.fx)db.fx=d.fx;if(d.dly&&!db.dly)db.dly=d.dly;if(Array.isArray(d.words)&&!(db.words||[]).length)db.words=d.words;db.next=Math.max(db.next,+d.next||0)}
 if(add||upd||pl){fast();sv()}return{add,upd,pl}};
/* pemulihan otomatis saat server menyala: ambil snapshot terbaru (lokal + cloud) lalu gabungkan tanpa menurunkan saldo */
async function autoRecover(){let add=0,upd=0,src=[];
 try{const f=bkFiles('lite')[0]||bkFiles('full')[0];if(f){const r=mergeSnap(readSnapLocal(f.f),false,false);add+=r.add;upd+=r.upd;if(r.add||r.upd)src.push('lokal '+f.f)}}catch(e){console.error('autoRecover lokal',e.message)}
 if(RU)try{const ix=await cloudIdx(),e=(ix.list||[])[0];if(e){const r=mergeSnap(await readSnapCloud(e.k),false,false);add+=r.add;upd+=r.upd;if(r.add||r.upd)src.push('cloud '+e.k)}}catch(e){console.error('autoRecover cloud',e.message)}
 BK.boot={add,upd,src:src.join(', '),t:Date.now()};if(add||upd)console.log('PEMULIHAN OTOMATIS: '+add+' akun dikembalikan, '+upd+' saldo dipulihkan dari '+src.join(', '))}
setInterval(async()=>{try{if(!loaded)return;const h=cr.createHash('sha1').update(JSON.stringify(ledgerOf())).digest('hex'),n=Date.now();
 if(h!==BK.hash&&n-BK.t.lite>6e5){BK.hash=h;snapLocal('lite')}
 if(n-BK.t.full>36e5&&h!==BK.fh){BK.fh=h;snapLocal('full')}
 if(RU&&h!==BK.chash&&n-BK.t.cloud>18e5){const r=await snapCloud('otomatis',new Date(n+7*36e5).getUTCHours()===3);if(r.ok)BK.chash=h}}catch(e){console.error('bk tick',e.message)}},60000);
const snapNow=async why=>{const a=snapLocal('lite',why),b=snapLocal('full',why),c=await snapCloud(why,false);return{lite:a,full:b,cloud:c}};
const jrnRead=(id,lim)=>{try{const st=fs.statSync(JRN),len=Math.min(st.size,3e6),fd=fs.openSync(JRN,'r'),buf=Buffer.alloc(len);fs.readSync(fd,buf,0,len,st.size-len);fs.closeSync(fd);const out=[];for(const ln of buf.toString('utf8').split('\n').reverse()){if(!ln)continue;try{const o=JSON.parse(ln);if(!id||o.id===id)out.push(o)}catch{}if(out.length>=lim)break}return out}catch{return[]}};

const planOf=u=>{const p=u.pl;if(p)return(!p.exp||p.exp>Date.now())?p.id:'free';return(u.data||{}).plan||'free'};
const pexpOf=u=>{const p=u.pl;if(p)return(!p.exp||p.exp>Date.now())?(p.exp||0):0;return(u.data||{}).planExp||0};
const setPl=(u,id,days,ext)=>{const n=Date.now();if(id==='free'){u.pl={id:'free',exp:0};return}const same=ext&&u.pl&&u.pl.id===id&&u.pl.exp&&u.pl.exp>n;u.pl={id,exp:days?(same?u.pl.exp:n)+days*864e5:0}};
const plInfo=u=>({id:planOf(u),exp:pexpOf(u)});
const dat=u=>({...(u.data||{}),saldo:bal(u),tx:u.stx||[],plan:planOf(u),planExp:pexpOf(u)});
const nc=s=>String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,''),fmt=s=>(nc(s).match(/.{1,4}/g)||[]).join('-');
const seen={};
const isOn=id=>seen[id]&&Date.now()-seen[id]<120000||[...clients].some(c=>c.u.id===id);

const pub=u=>({id:u.id,name:u.name,avatar:u.avatar,status:u.status,role:isRoot(u)?'root':isAdm(u)?'owner':RL.includes(planOf(u))?planOf(u):'free',av:u.avatar?(u.avt||1):0});
/* ===== KEAMANAN AKUN: passkey (WebAuthn), kode/QR masuk, 2FA TOTP, perangkat ===== */
const s256=x=>cr.createHash('sha256').update(x).digest(),b64u=b=>Buffer.from(b).toString('base64url'),unb64u=s=>Buffer.from(String(s||''),'base64url');
const cborDec=buf=>{let o=0;const rd=()=>{const ib=buf[o++],mt=ib>>5,ai=ib&31;let n;if(ai<24)n=ai;else if(ai===24)n=buf[o++];else if(ai===25){n=buf.readUInt16BE(o);o+=2}else if(ai===26){n=buf.readUInt32BE(o);o+=4}else if(ai===27){n=Number(buf.readBigUInt64BE(o));o+=8}else throw new Error('cbor');
 if(mt===0)return n;if(mt===1)return -1-n;if(mt===2){const v=buf.subarray(o,o+n);o+=n;return v}if(mt===3){const v=buf.toString('utf8',o,o+n);o+=n;return v}
 if(mt===4){const a=[];for(let i=0;i<n;i++)a.push(rd());return a}if(mt===5){const m=new Map();for(let i=0;i<n;i++){const k=rd();m.set(k,rd())}return m}if(mt===7)return ai===20?false:ai===21?true:null;throw new Error('cbor tipe')};return rd()};
const PKC=new Map();setInterval(()=>{const n=Date.now();for(const[k,v]of PKC)if(n-v.t>3e5)PKC.delete(k)},60000);
const ipOf=q=>String(q.headers['x-forwarded-for']||'').split(',')[0].trim()||q.socket.remoteAddress||'',orgU=q=>{try{return new URL(q.headers.origin)}catch{return null}},hostOf=q=>{const o=orgU(q);return o?o.host:String(q.headers['x-forwarded-host']||q.headers.host||'localhost').split(',')[0].trim()},rpOf=q=>hostOf(q).replace(/:\d+$/,'').replace(/^\[|\]$/g,'');
const maskIp=ip=>String(ip).replace(/^::ffff:/,'').replace(/(\d+\.\d+)\.\d+\.\d+$/,'$1.•.•').replace(/(:[0-9a-f]*){3}$/i,':•:•:•');
const devOf=q=>{const ua=String(q.headers['user-agent']||''),os=/Android/i.test(ua)?'Android':/iPhone|iPad|iPod/i.test(ua)?'iOS':/Windows/i.test(ua)?'Windows':/Mac OS X|Macintosh/i.test(ua)?'macOS':/Linux|X11/i.test(ua)?'Linux':'Perangkat',br=/SamsungBrowser/i.test(ua)?'Samsung Internet':/Edg\//i.test(ua)?'Edge':/OPR\/|Opera/i.test(ua)?'Opera':/Firefox/i.test(ua)?'Firefox':/Chrome|CriOS/i.test(ua)?'Chrome':/Safari/i.test(ua)?'Safari':'Browser';return br+' di '+os};
const mkTok=(u,q,m)=>{const t=cr.randomBytes(24).toString('hex'),d=devOf(q),ip=ipOf(q),n=Date.now();db.tok[t]={id:u.id,ep:u.ep,c:n,ls:n,d,ip,m};const lg=u.lg||(u.lg=[]);lg.unshift({t:n,m,d,ip:maskIp(ip)});if(lg.length>20)lg.length=20;
 const dv=u.dv||(u.dv=[]);if(dv.length&&!dv.includes(d)){(u.inb=u.inb||[]).push({t:n,x:'🔐 Login baru dari '+d+' ('+({password:'password',passkey:'passkey',qr:'kode QR'}[m]||m)+'). Bukan kamu? Buka Keamanan Akun lalu keluarkan perangkat itu.'});if(u.inb.length>20)u.inb.shift()}if(!dv.includes(d)){dv.push(d);if(dv.length>12)dv.shift()}return t};
const SECF=()=>db.sec||(db.sec={qr:1,pk:1,maxq:1440});
/* WebAuthn */
const pkOrigin=(o,rp)=>{try{const u=new URL(o);return u.hostname===rp&&(u.protocol==='https:'||rp==='localhost'||rp==='127.0.0.1')}catch{return false}};
const pkClient=(b64,type,rp)=>{const cd=JSON.parse(unb64u(b64).toString('utf8'));if(cd.type!==type)throw new Error('Jenis permintaan passkey salah');const c=PKC.get(cd.challenge);if(!c||c.k!==type)throw new Error('Tantangan passkey kedaluwarsa, coba lagi');PKC.delete(cd.challenge);if(!pkOrigin(cd.origin,rp))throw new Error('Asal situs tidak cocok dengan passkey');return{c,cd,raw:unb64u(b64)}};
const pkAuth=ad=>{if(ad.length<37)throw new Error('authData rusak');const o={rp:ad.subarray(0,32),fl:ad[32],cnt:ad.readUInt32BE(33)};if(o.fl&0x40){const il=ad.readUInt16BE(53);o.aa=ad.subarray(37,53);o.id=ad.subarray(55,55+il);o.pk=cborDec(ad.subarray(55+il))}return o};
const pkKey=m=>{const kty=m.get(1),alg=m.get(3);let jwk;if(kty===2&&alg===-7){if(m.get(-1)!==1)throw new Error('Kurva passkey tidak didukung');jwk={kty:'EC',crv:'P-256',x:b64u(m.get(-2)),y:b64u(m.get(-3))}}else if(kty===3&&alg===-257)jwk={kty:'RSA',n:b64u(m.get(-1)),e:b64u(m.get(-2))};else if(kty===1&&alg===-8)jwk={kty:'OKP',crv:'Ed25519',x:b64u(m.get(-2))};else throw new Error('Jenis kunci passkey belum didukung');
 return{alg,spki:cr.createPublicKey({key:jwk,format:'jwk'}).export({type:'spki',format:'der'}).toString('base64')}};
const pkVerify=(pk,data,sig)=>{const key=cr.createPublicKey({key:Buffer.from(pk.pub,'base64'),format:'der',type:'spki'});return pk.alg===-8?cr.verify(null,data,key,sig):cr.verify('sha256',data,key,sig)};
const pkChal=(k,uid)=>{const ch=b64u(cr.randomBytes(32));PKC.set(ch,{t:Date.now(),k,uid});return ch};
/* TOTP */
const B32='ABCDEFGHIJKLMNOPQRSTUVWXYZ234567',b32e=buf=>{let bits=0,val=0,o='';for(const x of buf){val=(val<<8)|x;bits+=8;while(bits>=5){o+=B32[(val>>>(bits-5))&31];bits-=5}}if(bits>0)o+=B32[(val<<(5-bits))&31];return o},b32d=s=>{let bits=0,val=0;const o=[];for(const ch of String(s).toUpperCase().replace(/[^A-Z2-7]/g,'')){val=(val<<5)|B32.indexOf(ch);bits+=5;if(bits>=8){o.push((val>>>(bits-8))&255);bits-=8}}return Buffer.from(o)};
const totpAt=(sec,c)=>{const b=Buffer.alloc(8);b.writeBigUInt64BE(BigInt(c));const h=cr.createHmac('sha1',b32d(sec)).update(b).digest(),o=h[19]&15;return String((((h[o]&0x7f)<<24)|(h[o+1]<<16)|(h[o+2]<<8)|h[o+3])%1e6).padStart(6,'0')};
const t2chk=(u,code,sec)=>{const c0=Math.floor(Date.now()/3e4),s=sec||(u.t2&&u.t2.s);if(!/^\d{6}$/.test(code)||!s)return false;for(const d of[0,-1,1]){const c=c0+d;if(totpAt(s,c)===code&&(sec||c>(u.t2.lc||0))){if(!sec)u.t2.lc=c;return true}}return false};
const t2bk=(u,code)=>{const h=cr.createHash('sha256').update(String(code).toUpperCase().replace(/[^A-Z0-9]/g,'')).digest('hex'),i=((u.t2||{}).bk||[]).indexOf(h);if(i<0)return false;u.t2.bk.splice(i,1);return true};
const QA='ABCDEFGHJKMNPQRSTUVWXYZ23456789',qNorm=s=>String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,'').replace(/^ASTRA/,'').slice(0,12),qHash=s=>cr.createHash('sha256').update('qr:'+s).digest('hex');
const qGen=()=>{let s='';const b=cr.randomBytes(12);for(let i=0;i<12;i++)s+=QA[b[i]%QA.length];return s};
const me=u=>({...pub(u),adm:isRoot(u)?'root':u.adm||'',email:u.email,bg:u.bg,wall:u.wall,fx:u.fx,sal:bal(u),data:dat(u)});

/* ===== VIDEO WALLPAPER (kartu Saldo & Premium) – diatur Owner utama, tampil ke semua pengguna ===== */
const WPMAX=31457280,wpPend={};
const WPD=()=>({k:'',src:'',fid:'',mime:'',t:'',tp:'tr',ts:'iklan',z:1,x:50,y:50,st:0,en:0,mu:0});
function wpSlot(o,old){o=o||{};const d=WPD(),n=(v,a,b,df)=>{v=+v;return isFinite(v)?Math.min(b,Math.max(a,v)):df};
 const k=['file','link'].includes(o.k)?o.k:'';let src=String(o.src||'').trim();
 if(k==='link'&&!/^https:\/\/[^\s"'<>]{4,800}$/.test(src))return null;
 return{k,src:k==='link'?src:'',fid:k==='file'?String(o.fid||(old&&old.fid)||'').replace(/[^\w]/g,'').slice(0,24):'',mime:k==='file'?String(o.mime||(old&&old.mime)||'').slice(0,40):'',
  t:String(o.t||'').trim().slice(0,40),tp:['tl','tr','bl','br','tc','bc'].includes(o.tp)?o.tp:d.tp,ts:['iklan','glass','neon','plain'].includes(o.ts)?o.ts:d.ts,
  z:n(o.z,1,4,1),x:n(o.x,0,100,50),y:n(o.y,0,100,50),st:n(o.st,0,36000,0),en:n(o.en,0,36000,0),mu:o.mu?1:0}}
function wpPub(){const w=db.wp;if(!w)return null;const f=s=>s&&(s.k==='file'&&!(db.wpf&&db.wpf[s.fid])?null:s);return{mode:w.mode==='split'?'split':'same',a:f(w.a)||null,b:f(w.b)||null}}

/* ===== BROHAVEN: kota roleplay 3D multiplayer (aset & peta orisinal) ===== */
const GTOP=[[500,1000],[3000,5000],[7000,10000],[20000,25000],[50000,50000]];
const GMC=()=>db.gmc||(db.gmc={on:1,mul:1});
const gTop=cf=>Array.isArray(cf.top)&&cf.top.length?cf.top:GTOP;
const GVEH={sepeda:{n:'Sepeda Gunung',p:0,s:9},skuter:{n:'Skuter Matic',p:300,s:15},motor:{n:'Motor Sport',p:900,s:22},mobil:{n:'Mobil Sedan',p:1800,s:27},suv:{n:'SUV 4x4',p:3200,s:30},sport:{n:'Mobil Sport',p:6000,s:38},lambo:{n:'Lamborghini',p:30000,s:48}};
const GPR={hs:[0,0,100,100,200,150,180,250],tp:[0,150,250,400,200,300],bt:[0,60,100,150],ac:[0,80,120,200,150,1000,90,160,260,300]},GMX={sk:6,hs:8,hc:8,tp:6,tc:10,bt:4,bc:10,ac:10};
const gDef=()=>({sk:1,hs:1,hc:0,tp:0,tc:0,bt:0,bc:0,ac:0});
const gmOf=u=>u.gm||(u.gm={c:200,av:gDef(),own:[],veh:['sepeda'],vh:'sepeda',vc:0,job:'',xp:0,earn:0,hr:{t:0,c:0}});
const gmPub=u=>{const g=gmOf(u);return{c:g.c,av:g.av,own:g.own,veh:g.veh,vh:g.vh,vc:g.vc,job:g.job,xp:g.xp,lv:Math.floor(Math.sqrt((g.xp||0)/40))+1}};
const GW=(()=>{let sd=7;const R=()=>{sd|=0;sd=sd+0x6D2B79F5|0;let t=Math.imul(sd^sd>>>15,1|sd);t=t+Math.imul(t^t>>>7,61|t)^t;return((t^t>>>14)>>>0)/4294967296};
 const rd=[-160,-80,0,80,160],bld=[],SP={'3,3,0':['hall','Balai Kota',26,22,14],'2,3,1':['clothes','Toko Baju',20,18,8],'3,2,2':['dealer','Dealer Kendaraan',22,18,8],'1,3,0':['post','Kantor Pos',20,16,8],'4,3,3':['police','Kantor Polisi',20,16,9],'3,4,1':['fire','Pos Pemadam',22,16,9]};
 const near=(v)=>rd.reduce((a,b)=>Math.abs(v-b)<Math.abs(v-a)?b:a);let n=0;
 for(let bi=0;bi<6;bi++)for(let bj=0;bj<6;bj++){if(bi===2&&bj===2)continue;const cx=-200+80*bi,cz=-200+80*bj;
  for(let l=0;l<4;l++){const x=cx+(l&1?16.5:-16.5),z=cz+(l&2?16.5:-16.5),sp=SP[bi+','+bj+','+l],rx=near(x),rz=near(z);let f;if(Math.abs(x-rx)<=Math.abs(z-rz))f=rx>x?'e':'w';else f=rz>z?'s':'n';
   const w=sp?sp[2]:14+Math.floor(R()*8),d=sp?sp[3]:14+Math.floor(R()*7),h=sp?sp[4]:6+Math.floor(R()*5),pi=Math.floor(R()*8),ri=Math.floor(R()*5);
   const dx=f==='e'?w/2:f==='w'?-w/2:0,dz=f==='s'?d/2:f==='n'?-d/2:0,nx=f==='e'?1:f==='w'?-1:0,nz=f==='s'?1:f==='n'?-1:0;
   bld.push({id:'b'+(n++),t:sp?sp[0]:'house',nm:sp?sp[1]:'Rumah',x,z,w,d,h,pi,ri,f,dx:+(x+dx+nx*2.6).toFixed(1),dz:+(z+dz+nz*2.6).toFixed(1)})}}
 const ix=[];for(const a of rd)for(const b of rd)ix.push([a,b]);
 return{seed:7,half:240,rd,rw:14,bld,park:[-40,-40,66],ix}})();
const GWJ=JSON.stringify(GW),GB={};GW.bld.forEach(b=>GB[b.id]=b);
const GM={fires:[],kick:{},ann:''};
const gClients=()=>[...clients].filter(c=>c.r==='game'&&c.g);
const gsend=(c,o)=>wsend(c,o);
function gGive(c,amt,why){const u=c.u,g=gmOf(u),hr=Math.floor(Date.now()/36e5);if(!g.hr||g.hr.t!==hr)g.hr={t:hr,c:0};
 let a=Math.round(amt*(GMC().mul||1)*fxEvI().mult);const cap=4000;if(g.hr.c+a>cap)a=Math.max(0,cap-g.hr.c);
 if(a<=0){gsend(c,{t:'pay',a:0,c:g.c,w:'Batas gaji per jam tercapai, istirahat dulu ya'});return 0}
 g.hr.c+=a;g.c+=a;g.xp=(g.xp||0)+a;{const wk=fxWk();if(!g.we||g.we.w!==wk)g.we={w:wk,c:0};g.we.c+=a}sv();gsend(c,{t:'pay',a,c:g.c,w:why,lv:Math.floor(Math.sqrt(g.xp/40))+1});return a}
const gDist=(c,x,z)=>Math.hypot(c.g.x-x,c.g.z-z);
function gOrder(c){const g=gmOf(c.u);c.g.ord=null;
 if(g.job==='kurir'){const post=GW.bld.find(b=>b.t==='post');c.g.ord={k:'kurir',st:0,tx:post.dx,tz:post.dz,t0:Date.now(),fx:c.g.x,fz:c.g.z,msg:'Ambil paket di Kantor Pos'}}
 else if(g.job==='polisi'){const pts=[];while(pts.length<4){const p=GW.ix[Math.floor(Math.random()*GW.ix.length)];if(!pts.includes(p))pts.push(p)}c.g.ord={k:'polisi',st:0,pts,tx:pts[0][0],tz:pts[0][1],t0:Date.now(),fx:c.g.x,fz:c.g.z,msg:'Patroli: kunjungi 4 titik persimpangan'}}
 gsend(c,{t:'ord',o:c.g.ord&&{k:c.g.ord.k,st:c.g.ord.st,tx:c.g.ord.tx,tz:c.g.ord.tz,msg:c.g.ord.msg,pts:c.g.ord.pts,n:c.g.ord.st}})}
function gDeliver(c){const o=c.g.ord;if(!o||c.g.rm!=='w')return;if(gDist(c,o.tx,o.tz)>9)return;const dist=Math.hypot(o.tx-o.fx,o.tz-o.fz),el=(Date.now()-o.t0)/1e3;if(el<dist/45)return gsend(c,{t:'warn',b:'Terlalu cepat, tetap di jalan ya'});
 if(o.k==='kurir'){if(o.st===0){const hs=GW.bld.filter(b=>b.t==='house'),h=hs[Math.floor(Math.random()*hs.length)];c.g.ord={k:'kurir',st:1,tx:h.dx,tz:h.dz,t0:Date.now(),fx:o.tx,fz:o.tz,msg:'Antar paket ke '+h.nm+' (ID '+h.id.slice(1)+')'};gOrderSend(c)}
  else{gGive(c,30+Math.floor(dist/4),'Paket terkirim!');fxEv(c.u,'deliv',1);gOrder(c)}}
 else if(o.k==='polisi'){const i=o.st+1;if(i>=4){gGive(c,130,'Patroli selesai!');fxEv(c.u,'patrol',1);gOrder(c)}else{c.g.ord={...o,st:i,tx:o.pts[i][0],tz:o.pts[i][1],t0:Date.now(),fx:o.tx,fz:o.tz,msg:'Titik '+(i+1)+' dari 4'};gOrderSend(c)}}}
const gOrderSend=c=>{const o=c.g.ord;gsend(c,{t:'ord',o:o&&{k:o.k,st:o.st,tx:o.tx,tz:o.tz,msg:o.msg,pts:o.pts}})};
function gFires(){const f=GM.fires.map(x=>[x.id,x.x,x.z,Math.max(0,Math.round(x.hp))]);gClients().forEach(c=>gsend(c,{t:'fires',f}))}
setInterval(()=>{const now=Date.now();let ch=0;GM.fires=GM.fires.filter(f=>{if(now-f.t>24e4){ch=1;return false}return true});
 if(GMC().on&&gClients().length&&GM.fires.length<3&&Math.random()<.5){const hs=GW.bld.filter(b=>b.t==='house'),h=hs[Math.floor(Math.random()*hs.length)];GM.fires.push({id:'f'+now.toString(36),x:h.dx,z:h.dz,hp:100,t:now,by:{}});ch=1;gClients().forEach(c=>gmOf(c.u).job==='pemadam'&&gsend(c,{t:'ann',m:'🔥 Kebakaran di '+h.nm+'! Pemadam segera ke lokasi'}))}
 if(ch)gFires()},30000);
function gInfo(c,o){const g=gmOf(o.u);gsend(c,{t:'pi',id:o.u.id,n:o.u.name,av:g.av,jb:g.job,vh:g.vh,vc:g.vc,lv:Math.floor(Math.sqrt((g.xp||0)/40))+1,tt:ttlOf(o.u),rl:isRoot(o.u)?'root':isAdm(o.u)?'owner':''})}
function gmJoin(c){const u=c.u;fxEv(u,'bh',1);if(!GMC().on){wsend(c,{t:'off'});setTimeout(()=>{try{c.sock.end()}catch{}},200);return}
 if(isAB(u)||(GM.kick[u.id]||0)>Date.now()){wsend(c,{t:'off',b:'Kamu tidak bisa masuk Brohaven sekarang'});setTimeout(()=>{try{c.sock.end()}catch{}},200);return}
 clients.forEach(o=>{if(o!==c&&o.r==='game'&&o.u.id===u.id){wsend(o,{t:'off',b:'Akunmu masuk dari perangkat lain'});try{o.sock.end()}catch{}}});
 const sp=GW.bld.find(b=>b.t==='hall');c.g={x:sp.dx,y:0,z:sp.dz+4,r:0,a:0,v:'',rm:'w',ts:Date.now(),known:new Set(),ord:null,lc:0,lf:0};
 const g=gmOf(u);if(g.job==='kurir'||g.job==='polisi')gOrder(c);
 gsend(c,{t:'hi',id:u.id,gm:gmPub(u),fires:GM.fires.map(x=>[x.id,x.x,x.z,Math.round(x.hp)]),mul:GMC().mul||1,ev:fxEvI(),race:{pts:RACE_PTS,st:RACE_ST}})}
function gmLeft(c){if(!c.g)return;gClients().forEach(o=>{if(o.g.known.delete(c.u.id))gsend(o,{t:'pl',id:c.u.id})})}
const gClean=m=>String(m||'').replace(/\s+/g,' ').trim().slice(0,90);
function gmMsg(c,m){const u=c.u,g=c.g;if(!g||!m||typeof m.t!=='string')return;if(isAB(u)){try{c.sock.end()}catch{}return}const now=Date.now(),gm=gmOf(u);
 if(m.t==='s'){const x=+m.x,y=+m.y,z=+m.z,r=+m.r;if(![x,y,z,r].every(Number.isFinite)||Math.abs(x)>400||Math.abs(z)>400)return;const rm=/^(w|b\d{1,3})$/.test(m.rm)?m.rm:'w';
  const dt=Math.min(1,Math.max(.05,(now-g.ts)/1e3));if(!process.env.GMT&&rm===g.rm&&Math.hypot(x-g.x,z-g.z)>50*dt+5){gsend(c,{t:'tp',x:g.x,z:g.z,rm:g.rm});return}
  if(rm==='w'&&g.rm==='w'){const dd=Math.hypot(x-g.x,z-g.z);if(dd<60)g.da=(g.da||0)+dd;if(g.da>=40){fxEv(u,'dist',Math.floor(g.da));g.da=0}}g.ts=now;g.x=x;g.y=Math.max(0,Math.min(30,y));g.z=z;g.r=r;g.rm=rm;g.a=(m.a|0)>=0&&(m.a|0)<=6?(m.a|0):0;
  const vm=/^([a-z]+):(\d)$/.exec(String(m.v||''));g.v=vm&&gm.veh.includes(vm[1])&&GVEH[vm[1]]?vm[1]+':'+vm[2]:'';return}
 if(m.t==='c'){const tx=gClean(m.m);if(!tx||now-g.lc<1200)return;if(u.mute&&u.mute>now&&!isRoot(u))return gsend(c,{t:'warn',b:'Kamu di-mute oleh Owner'});if(BAD.test(tx)||LINK.test(tx)||CODE.test(tx)){LINK.lastIndex=0;return gsend(c,{t:'warn',b:'Pesan ditolak: link, judi, atau kode tidak diizinkan'})}LINK.lastIndex=0;g.lc=now;fxEv(u,'gchat',1);
  const rl=isRoot(u)?'root':isAdm(u)?'owner':'';gClients().forEach(o=>gsend(o,{t:'c',id:u.id,n:u.name,m:tx,rl,nr:o.g.rm===g.rm&&(g.rm!=='w'||Math.hypot(o.g.x-g.x,o.g.z-g.z)<160)?1:0}));return}
 if(m.t==='who'){if(now-(g.lw||0)<2500)return;g.lw=now;gsend(c,{t:'who',p:gClients().slice(0,150).map(o=>{const gg=gmOf(o.u);return[o.u.id,o.u.name,Math.floor(Math.sqrt((gg.xp||0)/40))+1,gg.job||'',o.g.rm==='w'?0:1,isRoot(o.u)?'root':isAdm(o.u)?'owner':'']})});return}
 if(m.t==='jo'){if(gm.job==='kurir'||gm.job==='polisi')gOrder(c);return}
 if(m.t==='jd'){gDeliver(c);return}
 if(m.t==='fx'){if(gm.job!=='pemadam'||g.rm!=='w'||now-g.lf<140)return;g.lf=now;const f=GM.fires.find(x=>x.id===m.id);if(!f||gDist(c,f.x,f.z)>11)return;f.hp-=3.2;f.by[u.id]=(f.by[u.id]||0)+3.2;
  if(f.hp<=0){GM.fires=GM.fires.filter(x=>x!==f);clients.forEach(o=>{if(o.r==='game'&&o.g&&f.by[o.u.id]>=20){gGive(o,90,'Api padam! Kerja bagus');fxEv(o.u,'fire',1)}});gFires()}else if((f._n=(f._n||0)+1)%12===0)gFires();return}}
setInterval(()=>{const G=gClients();if(!G.length)return;const now=Date.now(),by={};G.forEach(c=>(by[c.g.rm]=by[c.g.rm]||[]).push(c));
 G.forEach(c=>{const L=by[c.g.rm].filter(o=>o!==c);let P=L;if(c.g.rm==='w'){P=L.map(o=>({o,d:Math.hypot(o.g.x-c.g.x,o.g.z-c.g.z)})).filter(x=>x.d<150).sort((a,b)=>a.d-b.d).slice(0,45).map(x=>x.o)}
  const seen=new Set();const sn=P.map(o=>{seen.add(o.u.id);if(!c.g.known.has(o.u.id)){c.g.known.add(o.u.id);gInfo(c,o)}return[o.u.id,+o.g.x.toFixed(1),+o.g.y.toFixed(1),+o.g.z.toFixed(1),+o.g.r.toFixed(2),o.g.a,o.g.v]});
  for(const id of c.g.known)if(!seen.has(id)&&(now%5000<100))c.g.known.delete(id);
  gsend(c,{t:'sn',p:sn,n:G.length})})},100);
const srv=http.createServer((q,r)=>{
 const ip=String(q.headers['x-forwarded-for']||'').split(',')[0].trim()||q.socket.remoteAddress;hits[ip]=(hits[ip]||0)+1;
 if(q.method==='OPTIONS')return J(r,204,{});
 const p0=q.url.split('?')[0];
 if(hits[ip]>600)return ER(r,429,'Terlalu banyak permintaan, tunggu sebentar');

 if(q.method==='POST'&&p0==='/api/wpup'){const k=db.tok[(q.headers.authorization||'').slice(7)],u=k&&db.users[k.id];
  if(!u||k.ep!==u.ep||!isRoot(u)){q.resume();return ER(r,403,'Hanya Owner utama')}
  const P2=new URLSearchParams(q.url.split('?')[1]||''),id=(P2.get('id')||'').replace(/[^a-f0-9]/g,'').slice(0,16),off=Math.floor(+P2.get('off'))||0,total=Math.floor(+P2.get('total'))||0,mime=String(q.headers['x-mime']||'').toLowerCase();
  if(id.length<8){q.resume();return ER(r,400,'ID upload tidak valid')}
  const parts=[];let n=0,over=0;q.on('data',d=>{if(over)return;n+=d.length;if(n>2.2e6){over=1;parts.length=0;ER(r,413,'Potongan terlalu besar');return}parts.push(d)});
  q.on('end',()=>{if(over)return;const now=Date.now();for(const f in wpPend)if(now-wpPend[f].t>18e5){wpUnl(wpTmp(f));delete wpPend[f]}
   let pd=wpPend[id];
   if(off===0){if(!/^(video\/(mp4|webm|quicktime)|image\/(png|jpeg|webp|gif))$/.test(mime))return ER(r,400,'File harus video MP4/WebM/MOV atau gambar PNG/JPG/WebP/GIF');
    if(!(total>0))return ER(r,400,'File kosong');if(total>WPMAX)return ER(r,413,'File terlalu besar (maks 30 MB)');
    const ks=Object.keys(wpPend).filter(f=>f!==id);while(ks.length>=2){const f=ks.shift();wpUnl(wpTmp(f));delete wpPend[f]}
    try{fs.writeFileSync(wpTmp(id),'')}catch(e){return ER(r,500,'Disk server tidak bisa ditulis')}pd=wpPend[id]={t:now,n:0,total,mime,done:0}}
   if(!pd)return J(r,409,{e:'Sesi upload tidak ditemukan, ulangi',size:0});
   if(off!==pd.n)return J(r,409,{e:'Urutan potongan tidak cocok',size:pd.n});
   if(pd.n+n>pd.total)return ER(r,400,'Ukuran melebihi total');
   fs.appendFile(wpTmp(id),Buffer.concat(parts),er=>{if(er)return ER(r,500,'Gagal menulis file di server');pd.n+=n;pd.t=Date.now();if(pd.n===pd.total)pd.done=1;J(r,200,{ok:1,size:pd.n,done:pd.done,fid:id})})});return}
 let b='',big=0;q.on('data',d=>{if(big)return;b+=d;if(b.length>7.4e6){big=1;b='';ER(r,413,'Ukuran file terlalu besar untuk dikirim')}});
 q.on('end',()=>{if(big)return;try{h(q,r,b?JSON.parse(b):{})}catch(e){console.error(e);ER(r,500,'Kesalahan di server')}})
});(async()=>{
 let localDb=null;try{localDb=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch{}
 if(RU){let ok=0;for(let i=0;i<6&&!ok;i++){try{await loadUp();ok=1;console.log('Data dimuat dari Upstash')}catch(e){console.error('Gagal memuat Upstash (percobaan '+(i+1)+')',e.message);await new Promise(r=>setTimeout(r,2500*(i+1)))}}
  if(!ok){console.error('Upstash tidak bisa dimuat. Server berhenti agar data cloud tidak tertimpa data lama; Render akan menyalakan ulang otomatis.');process.exit(1)}loaded=true}
 if(localDb&&localDb.users){const w={};for(const id in localDb.users){const u=localDb.users[id];if(u&&u.sal!==undefined)w[id]=[u.sal,u.wv||0,u.stx]}reconcile(w,'file lokal');sv()}
 await autoRecover();snapLocal('lite','boot');
 try{for(const f of fs.readdirSync(WPDIR))if(f.startsWith('t-')||!(db.wpf&&db.wpf[f]))wpUnl(path.join(WPDIR,f))}catch{}
 srv.requestTimeout=10*60e3;srv.headersTimeout=65e3;
 srv.listen(PORT,()=>console.log('ASTRA server di port '+PORT))})();

/* ===== Bot keamanan + WebSocket ===== */
const BOT={id:'bot',name:'Bot Keamanan ASTRA',avatar:'',status:'Pemantau 24 jam',role:'owner'};
const TOS='📜 Ketentuan Layanan Komunitas ASTRA Live Chat\n1. Dilarang membagikan kode program.\n2. Dilarang mengirim link tidak jelas atau penipuan. Hanya link tepercaya (TikTok, Google, YouTube, dll).\n3. Situs judi, ilegal, atau haram = langsung di-BAN.\n4. Dilarang spam.\n5. Semua chat komunitas (pesan, foto, dan stiker) direset otomatis setiap 2 jam sekali (00.00, 02.00, 04.00, dst WIB). Chat pribadi dan akunmu tidak ikut direset.\n6. Kamu bisa menghapus pesanmu sendiri dengan menekan pesan lalu pilih Hapus.\nBot keamanan memantau semua obrolan 24 jam secara real-time.';
const OKD=['tiktok.com','google.com','youtube.com','youtu.be','instagram.com','facebook.com','wa.me','whatsapp.com','github.com','wikipedia.org','x.com','twitter.com','spotify.com','telegram.org','t.me'];
const BAD=/\b(slot\s?(gacor|online|maxwin|dana)|rtp\s?slot|togel|judi\s?(online|bola|slot)|sbobet|casino|maxwin|bandar\s?(togel|judi|bola)|pinjol|taruhan|bet\s?88|situs\s?(judi|slot)|bokep)\b/i;
const CODE=/```|<\/?(script|html|div|body)\b|function\s*\w*\s*\(|\b(const|let|var)\s+\w+\s*=|import\s+.+\s+from|#include|console\.log|document\.\w|def\s+\w+\(|public\s+static|SELECT\s.+\sFROM|=>\s*\{/i;
const LINK=/(?:https?:\/\/|www\.)\S+|\b[a-z0-9-]+(?:\.[a-z0-9-]+)*\.(?:com|net|org|id|xyz|top|club|bet|vip|site|online|link|ru|cc|info|shop|live|app|me)\b\S*/gi;
const clients=new Set();
const frame=(s,op=0x81)=>{const p=Buffer.isBuffer(s)?s:Buffer.from(s),l=p.length;const h=l<126?Buffer.from([op,l]):l<65536?Buffer.from([op,126,l>>8,l&255]):Buffer.concat([Buffer.from([op,127,0,0,0,0]),Buffer.from([(l>>>24)&255,(l>>>16)&255,(l>>>8)&255,l&255])]);return Buffer.concat([h,p])};
const wsend=(c,o)=>{try{c.sock.write(frame(JSON.stringify(o)))}catch{}};
const cast=o=>clients.forEach(c=>c.r==='com'&&wsend(c,o));
const castU=(ids,o)=>clients.forEach(c=>ids.includes(c.u.id)&&wsend(c,o));
function mod(t){
 if(BAD.test(t))return['ban','Konten judi/ilegal/haram'];
 {const lw=String(t).toLowerCase();if((db.words||[]).some(w=>lw.includes(w)))return['del','Kata dilarang oleh Owner']}
 for(const l of t.match(LINK)||[]){const hs=l.replace(/^https?:\/\//i,'').replace(/^www\./i,'').split(/[\/?#]/)[0].toLowerCase();if(BAD.test(l)||/\.(bet|vip)$/.test(hs))return['ban','Link situs ilegal/judi'];if(!OKD.some(d=>hs===d||hs.endsWith('.'+d)))return['del','Link tidak dikenal/berpotensi penipuan']}
 if(CODE.test(t))return['del','Dilarang membagikan kode'];
 return null}
function push(m){db.com.push(m);capMedia(db.com);if(db.com.length>300)db.com.splice(0,db.com.length-300);sv();cast({t:'m',m,u:{bot:BOT,[m.u]:db.users[m.u]?pub(db.users[m.u]):BOT}})}
const say=b=>push({i:Date.now()+Math.random(),t:Date.now(),u:'bot',k:'bot',b});

/* ===== Katalog gift live: 120 gift, dari Rp500 sampai Rp5.000.000 ===== */
const GRAW=`Jempol|👍|500;Tepuk Tangan|👏|500;Hati Kecil|❤️|500;Senyum|😊|500;Kopi|☕|1000;Permen|🍬|1000;Es Krim|🍦|1000;Bunga Matahari|🌻|1000;Kue Cubit|🧁|1000;Donat|🍩|1000;Bintang|⭐|1000;Pelangi|🌈|1000;Balon|🎈|1500;Cokelat|🍫|1500;Kupu-kupu|🦋|1500;Gelembung|🫧|1500;Es Teh Manis|🧋|1500;Pizza|🍕|2000;Burger|🍔|2000;Kentang Goreng|🍟|2000;Popcorn|🍿|2000;Tulip|🌷|2000;Kucing Lucu|🐱|2000;Anak Anjing|🐶|2000;Panda|🐼|2000;Api Semangat|🔥|2000;Petir|⚡|2000;Sakura|🌸|2000;Lollipop|🍭|2000;Pita Cantik|🎀|2000;
Mawar Merah|🌹|2500;Buket Mini|💐|3000;Teddy Bear|🧸|3000;Gitar|🎸|3000;Mikrofon|🎤|3000;Headset|🎧|3500;Kamera|📸|4000;Piala Mini|🏆|5000;Lonceng|🔔|5000;Konfeti|🎊|5000;Kembang Api|🎆|5000;Sparkler|🎇|5000;Hadiah Kecil|🎁|5000;Cincin|💍|6000;Sepatu Roda|🛼|6000;Sepeda|🚲|7000;Skateboard|🛹|7000;Bola Basket|🏀|7500;Bola Sepak|⚽|7500;Dadu|🎲|8000;Joystick|🎮|8000;Piano|🎹|8500;Drum|🥁|9000;Trompet|🎺|9000;Biola|🎻|9000;Lampu Ajaib|🪔|10000;Bola Kristal|🔮|10000;Kompas|🧭|10000;Layang-layang|🪁|10000;Roket Mini|🚀|10000;
Medali Emas|🥇|15000;Mahkota|👑|20000;Berlian Kecil|💎|20000;Sayap Malaikat|🪽|25000;Unicorn|🦄|25000;Naga Kecil|🐲|30000;Harimau|🐯|30000;Singa|🦁|30000;Elang|🦅|35000;Paus|🐳|35000;Lumba-lumba|🐬|40000;Perahu Layar|⛵|40000;Mobil Sport|🏎️|45000;Motor Balap|🏍️|45000;Helikopter|🚁|50000;Pesawat|✈️|50000;Kapal Pesiar|🛥️|55000;Pantai Pribadi|🏖️|60000;Kastil|🏰|60000;Bulan Sabit|🌙|65000;Komet|☄️|70000;Planet Cincin|🪐|70000;Matahari|🌞|75000;Pohon Emas|🌳|75000;Gunung Es|🏔️|75000;
Roket Astra|🚀|100000;Satelit|🛰️|100000;Pesta Meriah|🎉|125000;Panggung Teater|🎭|125000;Piala Emas|🏆|150000;Jam Emas|⌚|150000;Tas Mewah|👜|175000;Mobil Mewah|🚘|200000;Yacht|🛳️|200000;Jet Pribadi|🛩️|250000;Menara Astra|🗼|250000;Pulau Pribadi|🏝️|300000;Kota Astra|🏙️|300000;Pegasus|🐎|350000;Merak Kerajaan|🦚|350000;Harta Karun|💰|400000;Peti Koin Emas|🪙|400000;Galaksi|🌌|450000;Aurora|🌠|450000;Istana Kerajaan|🏯|500000;
Raja Astra|🤴|750000;Ratu Astra|👸|750000;Dewa Petir|🌩️|1000000;Naga Emas|🐉|1000000;Tsunami|🌊|1250000;Gunung Berapi|🌋|1500000;Supernova|💥|1500000;Mesin Waktu|⏳|2000000;Portal Dimensi|🌀|2000000;Bintang Jatuh|💫|2500000;Istana Langit|☁️|3000000;Mahkota Semesta|👑|3500000;Planet Astra|🪐|4000000;Alam Semesta|🌌|4500000;ASTRA LEGEND|💎|5000000`;
const tierOf=p=>p<2500?1:p<15000?2:p<100000?3:p<750000?4:5;
const GIFTS=GRAW.replace(/\n/g,'').split(';').map((x,i)=>{const [n,e,p]=x.split('|');return{id:'g'+(i+1),n,e,p:+p,tier:tierOf(+p)}});
const GMAP=Object.fromEntries(GIFTS.map(g=>[g.id,g]));
const PTAB={go25:{p:25000,d:30,n:'GO ASTRA Mini'},go50:{p:50000,d:30,n:'GO ASTRA Lite'},go75:{p:75000,d:30,n:'GO ASTRA Plus'},go150:{p:150000,d:30,n:'GO ASTRA Elite'},go250:{p:250000,d:30,n:'GO ASTRA Pro'},q3:{p:180000,d:90,n:'GO ASTRA Kuartal'},y1:{p:1000000,d:365,n:'GO ASTRA Tahunan'},y2:{p:2000000,d:730,n:'GO ASTRA 2 Tahun'},y5:{p:5000000,d:1825,n:'GO ASTRA Sultan'}};
const peff=id=>{const o=(db.prices||{})[id],b=PTAB[id],r=o&&o.p>=0&&(!o.until||o.until>Date.now())?o.p:b.p,s=saleOn();return s?Math.min(r,Math.floor(b.p*(1-s.pct/100))):r};
const ptab=()=>{const r={},s=saleOn();for(const id in PTAB){const o=(db.prices||{})[id];if(o&&o.p>=0&&(!o.until||o.until>Date.now()))r[id]={p:o.p,was:PTAB[id].p,note:o.note||'',until:o.until||0};if(s){const sp=Math.floor(PTAB[id].p*(1-s.pct/100));if(!r[id]||sp<r[id].p)r[id]={p:sp,was:PTAB[id].p,note:s.label+' -'+s.pct+'%',until:s.end}}}return r};
/* ===== EVENT SPIN: 1x/hari, undian di server, diatur Owner utama ===== */
const SPD=()=>({on:1,ttl:24,capBal:100000,pr:[
 {id:'d5',t:'disc',v:5,w:22,mx:0},{id:'d10',t:'disc',v:10,w:14,mx:0},{id:'d25',t:'disc',v:25,w:6,mx:50},{id:'d50',t:'disc',v:50,w:1,mx:3},
 {id:'b500',t:'bal',v:500,w:18,mx:0},{id:'b1k',t:'bal',v:1000,w:10,mx:0},{id:'b5k',t:'bal',v:5000,w:2,mx:5},
 {id:'p1',t:'plan',v:1,w:3,mx:5},{id:'z',t:'none',v:0,w:24,mx:0}]});
const wibDay=n=>new Date((n||Date.now())+7*36e5).toISOString().slice(0,10);
const toMid=()=>{const n=Date.now(),d=new Date(n+7*36e5);return Date.UTC(d.getUTCFullYear(),d.getUTCMonth(),d.getUTCDate()+1)-7*36e5-n};
const spCfg=()=>db.spin||(db.spin=SPD());
const dyCfg=()=>{const c=db.dly||(db.dly={on:1,amt:1000,amt7:1500});if(!Array.isArray(c.amts)||c.amts.length!==7)c.amts=[c.amt,c.amt,c.amt,c.amt,c.amt,c.amt,c.amt7].map(x=>Math.max(0,Math.floor(+x)||0));if(!c.prize||!['menit','jam','hari'].includes(c.prize.unit))c.prize={on:1,unit:'hari',val:1};return c};
const dyDiff=(a,b)=>Math.round((Date.parse(b)-Date.parse(a))/864e5);
const dyAdd=(d,n)=>new Date(Date.parse(d)+n*864e5).toISOString().slice(0,10);
const dyCyc=u=>{const t=wibDay(),c=u.dyc;if(c&&c.s){const k=dyDiff(c.s,t);if(k>=0&&k<7)return{s:c.s,c:(c.c||[]).slice()}}return{s:t,c:[]}};
const dyMs=p=>({menit:6e4,jam:36e5,hari:864e5}[p.unit]||864e5)*p.val;
/* ===== FX: misi, event, voucher, referral, flash sale, lencana, peringkat ===== */
const FXMIS=[
 {id:'chat',n:'Ngobrol dengan ASTRA',d:'Kirim 20 pesan ke ASTRA AI',per:'w',k:'chat',g:20,r:'sal',a:1000},
 {id:'login',n:'Rajin Login',d:'Klaim bonus harian 5 kali minggu ini',per:'w',k:'login',g:5,r:'sal',a:1500},
 {id:'bh3',n:'Warga Brohaven',d:'Masuk Brohaven 3 kali',per:'w',k:'bh',g:3,r:'coin',a:300},
 {id:'jobw',n:'Pekerja Keras',d:'Selesaikan 10 pekerjaan di Brohaven',per:'w',k:'job',g:10,r:'coin',a:800},
 {id:'n1',npc:'budi',n:'Antar Paket Pak Budi',d:'Selesaikan 2 pengantaran paket (job Kurir)',per:'d',k:'deliv',g:2,r:'coin',a:150},
 {id:'n2',npc:'sari',n:'Jalan Sehat Bu Sari',d:'Tempuh 800 meter (jalan atau berkendara)',per:'d',k:'dist',g:800,r:'coin',a:120},
 {id:'n6',npc:'sari',n:'Ngobrol Santai',d:'Kirim 3 pesan di chat Brohaven',per:'d',k:'gchat',g:3,r:'coin',a:80},
 {id:'n3',npc:'rina',n:'Patroli Warga',d:'Selesaikan 1 patroli (job Polisi)',per:'d',k:'patrol',g:1,r:'coin',a:200},
 {id:'n4',npc:'joko',n:'Pemadam Sukarela',d:'Padamkan 1 kebakaran (job Pemadam)',per:'d',k:'fire',g:1,r:'coin',a:250},
 {id:'n5',npc:'rey',n:'Lomba Balap Bang Rey',d:'Selesaikan 1 balapan keliling kota',per:'d',k:'race',g:1,r:'coin',a:300}];
const RACE_PTS=[[80,-80],[80,80],[-80,80],[-80,-80]],RACE_ST=[-80,-80];
const fxCfg=()=>{const c=db.fx||(db.fx={});
 if(!c.mo)c.mo={};
 if(!c.ev)c.ev={on:0,mult:2,wk:1,until:0,label:'Coins Ganda'};
 if(!c.sale)c.sale={on:0,pct:20,start:0,end:0,label:'Flash Sale'};
 if(!c.ref)c.ref={on:1,a:2000,b:1000,max:20};
 if(!c.rd)c.rd={on:1,p:2000};
 if(!c.race)c.race={on:1,t:45,c:300};
 if(!c.vch)c.vch={};return c};
const fxDefs=()=>{const mo=fxCfg().mo;return FXMIS.map(x=>({...x,...(mo[x.id]||{})}))};
const fxWk=()=>{const d=wibDay(),w=new Date(d+'T00:00:00Z').getUTCDay();return dyAdd(d,-((w+6)%7))};
const fxM=u=>{const m=u.ms||(u.ms={dk:'',wk:'',d:{},w:{},cd:[],cw:[]}),t=wibDay(),w=fxWk();if(m.dk!==t){m.dk=t;m.d={};m.cd=[]}if(m.wk!==w){m.wk=w;m.w={};m.cw=[]}return m};
const FXK={deliv:['deliv','job'],patrol:['patrol','job'],fire:['fire','job']};
const fxEv=(u,k,n)=>{n=Math.max(0,Math.min(5000,Math.floor(+n)||0));if(!n)return;const m=fxM(u),st=u.fxs||(u.fxs={});
 if(k==='bh'){const now=Date.now();if(now-(st.bt||0)<18e5)return;st.bt=now}
 for(const q of(FXK[k]||[k])){m.d[q]=Math.min(1e7,(m.d[q]||0)+n);m.w[q]=Math.min(1e7,(m.w[q]||0)+n);st[q]=Math.min(1e9,(st[q]||0)+n)}sv()};
const fxMis=u=>{const m=fxM(u);return fxDefs().filter(x=>x.on!==0).map(x=>({id:x.id,npc:x.npc||'',n:x.n,d:x.d.replace(/\d[\d.]*/,x.g),per:x.per,g:x.g,r:x.r,a:x.a,p:Math.min(x.g,(x.per==='d'?m.d:m.w)[x.k]||0),cl:(x.per==='d'?m.cd:m.cw).includes(x.id)}))};
const fxEvI=()=>{const e=fxCfg().ev,now=Date.now(),w=new Date(wibDay()+'T00:00:00Z').getUTCDay(),wk=!!e.wk&&(w===0||w===6),man=!!e.on&&(!e.until||e.until>now),act=man||wk;return{on:act,mult:act?e.mult:1,label:e.label,until:man?e.until:0,wk:wk&&!man}};
const saleOn=()=>{const s=fxCfg().sale;return s.on&&s.end>Date.now()&&(!s.start||s.start<=Date.now())?s:null};
const dyDays=u=>{const s=new Set(u.dyh||[]);((u.dyc||{}).c||[]).forEach(x=>s.add(x));return s};
const dyStreak=u=>{const s=dyDays(u);let t=wibDay();if(!s.has(t))t=dyAdd(t,-1);let n=0;while(s.has(t)&&n<999){n++;t=dyAdd(t,-1)}return n};
const dyBest=u=>Math.max(u.dyb||0,dyStreak(u));
const fxDy=(u,d,login)=>{const h=u.dyh||(u.dyh=[]);if(!h.includes(d))h.push(d);if(h.length>400)h.splice(0,h.length-400);u.dyb=Math.max(u.dyb||0,dyStreak(u));if(login)fxEv(u,'login',1)};
const BADGES=[
 ['l7','Setia 7 Hari','Streak login 7 hari',u=>dyBest(u)>=7],['l30','Legenda Login','Streak login 30 hari',u=>dyBest(u)>=30],
 ['ch','Sahabat ASTRA','Kirim 200 pesan ke ASTRA',u=>((u.fxs||{}).chat||0)>=200],['mm','Misi Master','Klaim 10 misi',u=>(u.fxm||0)>=10],
 ['dt','Duta ASTRA','Ajak 3 teman bergabung',u=>(u.rn||0)>=3],['kr','Kurir Cepat','Antar 25 paket di Brohaven',u=>((u.fxs||{}).deliv||0)>=25],
 ['pm','Pemadam Handal','Padamkan 10 kebakaran',u=>((u.fxs||{}).fire||0)>=10],['pj','Penjelajah','Tempuh 10 km di Brohaven',u=>((u.fxs||{}).dist||0)>=1e4],
 ['rc','Pebalap','Selesaikan 1 balapan',u=>((u.fxs||{}).race||0)>=1],['rj','Raja Jalanan','Finis balapan di bawah target waktu',u=>{const b=(u.gm||{}).rb;return b>0&&b<=fxCfg().race.t}],
 ['sl','Sultan Koin','Punya 100.000 koin Brohaven',u=>((u.gm||{}).c||0)>=1e5]];
const fxBdg=u=>BADGES.map(b=>({id:b[0],n:b[1],d:b[2],ok:!!b[3](u)}));
const ttlOf=u=>{const b=BADGES.find(x=>x[0]===u.ttl);return b&&b[3](u)?b[1]:''};
const refCode=u=>{if(!u.rc){db.rix=db.rix||{};let c;do c=cr.randomBytes(3).toString('hex').toUpperCase();while(db.rix[c]);u.rc=c;db.rix[c]=u.id;sv()}return u.rc};
const LBC={};
const fxLb=k=>{const c=LBC[k];if(c&&Date.now()-c.t<30000)return c.v;const arr=[],wk=fxWk();
 for(const id in db.users){const u=db.users[id];if(!u||isAB(u)||isRoot(u))continue;const g=u.gm||{};let v=0;
  if(k==='streak')v=dyStreak(u);else if(k==='coin')v=g.we&&g.we.w===wk?g.we.c:0;else if(k==='race')v=g.rw&&g.rw.w===wk?g.rw.t:0;
  if(v>0)arr.push({id,n:u.name,v,t:ttlOf(u)})}
 arr.sort((a,b)=>k==='race'?a.v-b.v:b.v-a.v);LBC[k]={t:Date.now(),v:arr};return arr};
const dyState=u=>{const cf=dyCfg(),t=wibDay(),cy=dyCyc(u),i=dyDiff(cy.s,t),done=cy.c.includes(t),slots=[];let miss=0;
 for(let k=0;k<7;k++){const d=dyAdd(cy.s,k),st=cy.c.includes(d)?'done':k<i?'miss':k===i?'today':'future';if(st==='miss')miss++;slots.push({k:k+1,d,st,amt:cf.amts[k]})}
 return{on:!!cf.on,can:!!cf.on&&!done,done,day:i+1,slots,miss,amt:cf.amts[i],amts:cf.amts,streak:cy.c.length,prize:cf.prize.on?{n:PTAB.go25.n,p:PTAB.go25.p,dur:cf.prize.val+' '+cf.prize.unit}:null,next:toMid()}};
const spStat=()=>{const d=wibDay();if(!db.spd||db.spd.d!==d)db.spd={d,n:0,bal:0,c:{},log:(db.spd&&db.spd.log)||[]};return db.spd};
const spLabel=p=>p.l||(p.t==='disc'?'Diskon '+p.v+'%':p.t==='bal'?'Saldo Rp'+Number(p.v).toLocaleString('id-ID'):p.t==='plan'?'Premium '+p.v+' hari':'Belum beruntung');
const spView=(adm)=>{const c=spCfg();return{on:!!c.on,ttl:c.ttl,capBal:c.capBal,pr:c.pr.map(p=>({id:p.id,t:p.t,v:p.v,l:spLabel(p),...(adm?{w:p.w,mx:p.mx,cl:p.l||''}:{})}))}};
const dcOf=u=>u.dc&&u.dc.exp>Date.now()?u.dc:null;
const spState=u=>{const c=spCfg();return{on:!!c.on,can:!!c.on&&u.sp!==wibDay(),dc:dcOf(u),next:toMid()}};
const spSan=b=>{const c=SPD(),o={on:b.on?1:0,ttl:Math.min(168,Math.max(1,Math.floor(+b.ttl)||24)),capBal:Math.min(1e8,Math.max(0,Math.floor(+b.capBal)||0)),pr:[]};
 if(!Array.isArray(b.pr)||b.pr.length<2||b.pr.length>12)return null;const ids=new Set();
 for(const x of b.pr){const t=['disc','bal','plan','none'].includes(x.t)?x.t:null;if(!t)return null;let v=Math.floor(+x.v)||0;if(t==='disc')v=Math.min(90,Math.max(1,v));else if(t==='bal')v=Math.min(1e6,Math.max(1,v));else if(t==='plan')v=Math.min(30,Math.max(1,v));else v=0;
  let id=String(x.id||'').replace(/[^\w]/g,'').slice(0,10)||('p'+Math.random().toString(36).slice(2,7));while(ids.has(id))id+='x';ids.add(id);
  o.pr.push({id,t,v,w:Math.min(1000,Math.max(0,+x.w||0)),mx:Math.min(100000,Math.max(0,Math.floor(+x.mx)||0)),l:String(x.l||'').trim().slice(0,24)})}
 if(!o.pr.some(p=>p.w>0))return null;return o};
const rp0=n=>'Rp'+Number(n).toLocaleString('id-ID');
const alog=(u,a,d)=>{db.alog=db.alog||[];db.alog.unshift({t:Date.now(),by:u.name,a,d:String(d||'').slice(0,140)});if(db.alog.length>150)db.alog.length=150;sv()};
/* verifikasi langkah ke-2: password akun dicek ulang di server */
const vpw=(u,vp)=>{const n=Date.now();if(u.vl&&u.vl>n)return'Verifikasi dikunci. Coba lagi '+Math.ceil((u.vl-n)/6e4)+' menit lagi';if(typeof vp!=='string'||!vp||ph(vp,u.salt)!==u.ph){u.vf=(u.vf||0)+1;if(u.vf>=5){u.vf=0;u.vl=n+10*6e4;return'Password salah 5 kali. Verifikasi dikunci 10 menit'}return'Password salah ('+u.vf+'/5)'}u.vf=0;return null};
const isBanned=u=>{if(!u.ban)return false;if(u.bu&&u.bu<Date.now()){u.ban=0;u.ab=0;u.bw='';u.bu=0;u.st=0;sv();return false}return true};
const isAB=u=>!!(u.ab&&isBanned(u)&&!isRoot(u));
let ownC=null;const ownId=()=>{if(ownC&&db.users[ownC]&&isRoot(db.users[ownC]))return ownC;ownC=db.users['100001']&&isRoot(db.users['100001'])?'100001':(Object.keys(db.users).find(i=>isRoot(db.users[i]))||'100001');return ownC};
const banInfo=u=>isAB(u)?{u:u.bu||0,w:u.bw||'',id:ownId(),n:u.name}:0;
const hm=t=>new Date(t+7*36e5).toISOString().slice(11,16)+' WIB';
function postCom(u,k,b,nm){
 const adm=isAdm(u),n=Date.now();
 if(isBanned(u)&&!isRoot(u))return'Akunmu di-BAN dari komunitas'+(u.bu?' sampai '+hm(u.bu):' karena melanggar ketentuan');
 if(u.mute&&u.mute>n&&!isRoot(u))return'Kamu di-mute oleh Owner sampai '+hm(u.mute);
 if(db.lock&&!adm)return'Komunitas sedang dikunci oleh Owner. Hanya Owner yang bisa mengirim pesan.';
 u.rt=(u.rt||[]).filter(x=>n-x<5000);if(!adm&&u.rt.push(n)>5)return'Pelan-pelan, kamu terdeteksi spam';
 if(!adm&&db.nomedia&&k!=='text'&&k!=='stk')return'Kirim media sedang dimatikan oleh Owner';
 if(!adm&&db.novoice&&k==='aud')return'Pesan suara sedang dimatikan oleh Owner';
 if(db.slow&&!adm){const w=db.slow*1000-(n-(u.ls||0));if(w>0)return'Mode lambat aktif: tunggu '+Math.ceil(w/1000)+' detik lagi';u.ls=n}
 if(k==='aud'&&!adm){if(b.length>AUDM)return'Pesan suara member maksimal 30 detik';u.vo=(u.vo||[]).filter(x=>n-x<36e5);if(u.vo.length&&n-u.vo[u.vo.length-1]<20000)return'Tunggu 20 detik sebelum kirim pesan suara lagi';if(u.vo.length>=10)return'Batas pesan suara member 10 per jam. Coba lagi nanti';u.vo.push(n)}
 {const v=adm?null:k==='text'?mod(b):(nm&&BAD.test(nm)?['ban','Nama file berisi konten judi/ilegal']:null);if(v){if(v[0]==='ban'){u.ban=1;u.bw=v[1]}else if((u.st=(u.st||0)+1)>=3){u.ban=1;u.bw='Pelanggaran berulang di komunitas'}
  say(`🛡️ Pesan ${u.name} dihapus: ${v[1]}. ${u.ban?u.name+' telah di-BAN dari komunitas.':'Peringatan '+u.st+'/3.'}`);sv();return v[1]+(u.ban?' (kamu di-BAN)':'')}}
 push({i:Date.now()+Math.random(),t:Date.now(),u:u.id,k,b,n:nm});return''}

/* ===== LIVE STREAMING (hanya Owner utama yang siaran). Video dipancarkan lewat server (relay), tidak disimpan. ===== */
const CL=Buffer.from([0x1F,0x43,0xB6,0x75]);let live=null;
const liveV=()=>[...clients].filter(c=>c.r==='live'&&!c.host);
const lcast=(o,f)=>clients.forEach(c=>c.r==='live'&&(!f||f(c))&&wsend(c,o));
const wbin=(c,fr)=>{try{if(c.sock.writableLength>4e6){c.sock.destroy();return}c.sock.write(fr)}catch{}};
const bf=(t,b)=>frame(Buffer.concat([Buffer.from([t]),b]),0x82);
const topOf=L=>Object.entries(L.top).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([id,v])=>({id,name:(db.users[id]||{}).name||'?',v}));
const lpub=()=>live&&{on:1,fx:live.fx,title:live.title,host:live.hn,hid:live.host,t:live.t,v:liveV().length,kind:live.kind,mime:live.mime,cm:live.cm,g:live.gifts,top:topOf(live)};
function snapSync(){if(!live)return;const n=liveV().filter(c=>c.snap).length;const on=n>0&&live.kind==='webm';if(on!==live.snapOn){live.snapOn=on;clients.forEach(c=>c.host&&wsend(c,{t:'snap',on}))}}
function liveJoin(c){const u=c.u;
 if(!live){wsend(c,{t:'end',none:1});setTimeout(()=>{try{c.sock.end()}catch{}},300);return}
 if(c.host){live.hostGone=0;clients.forEach(o=>{if(o!==c&&o.host){try{o.sock.destroy()}catch{}}});wsend(c,{t:'hello',host:1,live:lpub(),mute:[...live.mute]});live.snapOn=null;snapSync();lcast({t:'resume'},x=>!x.host);return}
 if(live.kind==='jpg')c.snap=true;
 wsend(c,{t:'hello',live:lpub(),me:{id:u.id,role:pub(u).role},cmt:live.cmt.slice(-30),snap:!!c.snap,ad:live.ad,txt:live.txt,fx:live.fx,goal:live.goal,pin:live.pinc});
 if(live.kind==='webm'&&!c.snap){if(live.init){wbin(c,bf(1,live.init));if(live.cur.length)wbin(c,bf(1,live.cur));c.ready=1}}
 else if(live.jpg)wbin(c,bf(2,live.jpg));
 clients.forEach(h=>h.host&&wsend(h,{t:'j',u:{id:u.id,name:u.name,role:pub(u).role}}));snapSync()}
function liveLeft(c){if(!live)return;if(c.host){if(live)live.hostGone=Date.now();lcast({t:'pause'},x=>!x.host)}else snapSync()}
function hostBin(buf){if(!live||buf.length<2)return;const t=buf[0],p=buf.slice(1),n=Date.now();
 if(t===1&&live.kind==='webm'){
  if(!live.init){live.head=Buffer.concat([live.head||Buffer.alloc(0),p]);const k=live.head.indexOf(CL);if(k>=0){live.init=live.head.slice(0,k);live.cur=live.head.slice(k);live.head=null;
    liveV().forEach(c=>{if(!c.snap&&!c.ready){wbin(c,bf(1,live.init));wbin(c,bf(1,live.cur));c.ready=1}})}else if(live.head.length>2e6)live.head=null;return}
  const k=p.indexOf(CL);live.cur=k>=0?p.slice(k):Buffer.concat([live.cur,p]);if(live.cur.length>6e6)live.cur=Buffer.alloc(0);
  const fr=bf(1,p);liveV().forEach(c=>{if(c.ready&&!c.snap)wbin(c,fr)})}
 else if(t===2){if(n-live.lj<90)return;live.lj=n;live.jpg=p;const fr=bf(2,p);liveV().forEach(c=>{if(c.snap)wbin(c,fr)})}}
function liveText(c,m){const u=c.u,n=Date.now();if(!live||!m)return;
 if(c.host){
  if(m.t==='meta'){const kind=m.kind==='jpg'?'jpg':'webm';if(m.reset||!live.kind){live.kind=kind;live.mime=String(m.mime||'').slice(0,120);live.init=null;live.cur=Buffer.alloc(0);live.head=null;live.jpg=null;liveV().forEach(v=>{v.ready=0;if(kind==='jpg')v.snap=true});lcast({t:'reinit',kind,mime:live.mime},x=>!x.host);live.snapOn=null;snapSync()}}
  else if(m.t==='cm'){live.cm=!!m.on;lcast({t:'cm',on:live.cm})}
  else if(m.t==='title'){live.title=String(m.b||'').slice(0,60)||live.title;lcast({t:'title',b:live.title})}
  else if(m.t==='ad'){const a=m.a||{},k=['img','vid','emb'].includes(a.k)?a.k:null,src=String(a.src||'');if(!k)return;
   const dat=src.startsWith('data:');if(dat){if(src.length>2.4e6||!(k==='img'?/^data:image\//:/^data:video\//).test(src)||!/^data:[a-z]+\/[\w.+-]+(;[\w=.-]+)*;base64,[A-Za-z0-9+\/=]+$/.test(src))return wsend(c,{t:'warn',b:'File iklan tidak valid / terlalu besar'})}
   else{if(!/^https:\/\/[^\s"'<>]{4,500}$/.test(src))return wsend(c,{t:'warn',b:'Link iklan harus https'});if(k==='emb'){const hs=src.replace(/^https:\/\//,'').split(/[\/?#]/)[0].toLowerCase();if(!/(^|\.)(youtube\.com|youtube-nocookie\.com|tiktok\.com|instagram\.com|vimeo\.com|facebook\.com|dailymotion\.com|twitch\.tv|streamable\.com)$/.test(hs))return wsend(c,{t:'warn',b:'Situs tidak didukung untuk embed'})}}
   live.ad={id:n,k,src,lab:!!a.lab,title:String(a.title||'').slice(0,40),dur:Math.max(3,Math.min(300,+a.dur||10)),t:n};lcast({t:'ad',a:live.ad},x=>!x.host)}
  else if(m.t==='adend'){if(live.ad){live.ad=null;lcast({t:'adend'},x=>!x.host)}}
  else if(m.t==='txt'){const L=(Array.isArray(m.list)?m.list:[]).slice(0,8).map(x=>({id:String(x.id||'').slice(0,12),s:String(x.s||'').slice(0,90),c:/^#[0-9a-f]{6}$/i.test(x.c)?x.c:'#ffffff',z:Math.max(10,Math.min(64,+x.z||22)),x:Math.max(0,Math.min(1,+x.x||0)),y:Math.max(0,Math.min(1,+x.y||0)),b:x.b?1:0,bg:x.bg?1:0})).filter(x=>x.s);live.txt=L;lcast({t:'txt',list:L},x=>!x.host)}
  else if(m.t==='fx'){const TH=['neon','kaca','api','pelangi','es','retro'],AN=['slide','pop','glow','bounce'];live.fx={theme:TH.includes(m.theme)?m.theme:'neon',anim:AN.includes(m.anim)?m.anim:'slide'};lcast({t:'fx',fx:live.fx})}
  else if(m.t==='goal'){if(!m.goal){live.goal=null}else{const tg=Math.floor(+m.goal.target);if(!(tg>=1000)||tg>1e9)return;live.goal={title:String(m.goal.title||'Target gift').slice(0,30),target:tg,base:live.gifts}}lcast({t:'goal',goal:live.goal})}
  else if(m.t==='pin'){const cm=live.cmt.find(x=>x.t==='c'&&String(x.id)===String(m.id));live.pinc=cm?{id:cm.id,u:cm.u,b:cm.b}:null;lcast({t:'pin',c:live.pinc})}
  else if(m.t==='lslow'){live.lslow=Math.max(0,Math.min(60,Math.floor(+m.n)||0));lcast({t:'lslow',n:live.lslow})}
  else if(m.t==='c'){const b=String(m.b||'').trim().slice(0,150);if(b)lc(u,b)}
  return}
 if(m.t==='lk'){if(n-(c.lk||0)<150)return;c.lk=n;live.lkn++;if(!live.lkt)live.lkt=setTimeout(()=>{if(live){lcast({t:'lk',n:Math.min(live.lkn,30)});live.lkn=0;live.lkt=0}},600);return}
 if(m.t==='c'){const b=String(m.b||'').trim().slice(0,150);if(!b)return;
  if(!live.cm&&!isRoot(u))return wsend(c,{t:'warn',b:'Komentar sedang dimatikan oleh host'});
  if(isBanned(u)&&!isRoot(u))return wsend(c,{t:'warn',b:'Akunmu di-BAN'});
  if(((u.mute&&u.mute>n)||live.mute.has(u.id))&&!isRoot(u))return wsend(c,{t:'warn',b:'Kamu di-mute di live ini'});
  if(!isAdm(u)){if(n-(c.lc||0)<Math.max(1200,(live.lslow||0)*1000))return wsend(c,{t:'warn',b:live.lslow?'Mode lambat: tunggu '+live.lslow+' detik antar komentar':'Pelan-pelan ya'});c.lc=n;const v=mod(b);if(v)return wsend(c,{t:'warn',b:'Komentar ditolak: '+v[1]})}
  lc(u,b)}}
function lc(u,b){const o={t:'c',id:Date.now()+Math.random(),ts:Date.now(),u:{id:u.id,name:u.name,role:pub(u).role,av:pub(u).av},b};live.cmt.push(o);if(live.cmt.length>60)live.cmt.shift();lcast(o)}
function endLive(reason){if(!live)return;const L=live;live=null;const sum={title:L.title,dur:Date.now()-L.t,gifts:L.gifts,peak:L.peak,top:topOf(L),reason};
 db.lastLive={t:L.t,dur:sum.dur,gifts:sum.gifts,peak:sum.peak,title:L.title};sv();
 clients.forEach(c=>{if(c.r==='live')wsend(c,{t:'end',sum:c.host?sum:{title:L.title,reason}})});
 setTimeout(()=>clients.forEach(c=>{if(c.r==='live'){try{c.sock.end()}catch{}}}),600);
 say('⏹️ Live "'+L.title+'" telah selesai. Terima kasih sudah menonton!')}
setInterval(()=>{if(!live)return;const n=Date.now(),v=liveV().length;live.peak=Math.max(live.peak,v);lcast({t:'v',n:v,g:live.gifts,top:topOf(live)});
 if(live.hostGone&&n-live.hostGone>45000)endLive('Siaran terputus');else if(!live.hostGone&&!clients.size)endLive('Siaran berhenti');else if(n-live.t>8*36e5)endLive('Durasi maksimum 8 jam tercapai');
 else if(!live.kind&&n-live.t>120000)endLive('Host tidak memulai siaran')},3000);
srv.on('upgrade',(q,sock)=>{
 const U=new URL(q.url,'http://x'),k=db.tok[U.searchParams.get('t')],u=k&&db.users[k.id];
 if(U.pathname!=='/ws'||!u||k.ep!==u.ep||!q.headers['sec-websocket-key']){sock.destroy();return}
 sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+cr.createHash('sha1').update(q.headers['sec-websocket-key']+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')+'\r\n\r\n');
 try{sock.setNoDelay(true)}catch{}
 const rr=U.searchParams.get('r'),c={sock,u,r:rr};
 if(rr==='live'){c.host=U.searchParams.get('host')==='1'&&isRoot(u)&&!!live&&live.host===u.id;c.snap=U.searchParams.get('snap')==='1'}
 clients.add(c);if(c.r==='com'){if(!u.tos){u.tos=1;sv();say('👋 Selamat datang '+u.name+' di Komunitas ASTRA Live Chat!\n\n'+TOS)}wsend(c,{t:'tos',b:TOS})}
 if(c.r==='live')liveJoin(c);
 if(c.r==='game')gmJoin(c);
 let buf=Buffer.alloc(0),frag=null;const LIM=c.host?32e5:4e6;
 sock.on('data',d=>{try{buf=Buffer.concat([buf,d]);while(buf.length>=2){
  const fin=buf[0]&128;let op=buf[0]&15,o=buf[1]&127,off=2;if(o===126){if(buf.length<4)break;o=buf.readUInt16BE(2);off=4}else if(o===127){if(buf.length<10)break;o=buf.readUInt32BE(6);off=10}
  if(o>LIM){wsend(c,{t:'warn',b:'File terlalu besar'});sock.destroy();return}const msk=buf[1]&128?buf.slice(off,off+4):null;if(msk)off+=4;if(buf.length<off+o)break;
  let pl=buf.slice(off,off+o);buf=buf.slice(off+o);if(msk)for(let i=0;i<pl.length;i++)pl[i]^=msk[i&3];
  if(op===0){if(!frag)continue;frag.p.push(pl);frag.n+=pl.length;if(frag.n>LIM){sock.destroy();return}if(!fin)continue;pl=Buffer.concat(frag.p);op=frag.op;frag=null}
  else if((op===1||op===2)&&!fin){frag={op,p:[pl],n:pl.length};continue}
  if(op===8){sock.end();return}
  if(op===9){try{sock.write(frame(pl,0x8A))}catch{}continue}
  if(op===1&&c.r==='com'){try{const m=JSON.parse(pl.toString());if(!valid(m.k,m.b,m.n)){wsend(c,{t:'warn',b:why(m.k,m.b,m.n)});continue}const e=postCom(u,m.k,m.b,m.n);if(e)wsend(c,{t:'warn',b:e})}catch{}}
  else if(op===1&&c.r==='live'){try{liveText(c,JSON.parse(pl.toString()))}catch{}}
  else if(op===1&&c.r==='game'){try{gmMsg(c,JSON.parse(pl.toString()))}catch{}}
  else if(op===2&&c.r==='live'&&c.host)hostBin(pl)}}catch(e){console.error('ws',e.message)}});
 const gone=()=>{if(clients.delete(c)){if(c.r==='live')liveLeft(c);if(c.r==='game')gmLeft(c)}};sock.on('close',gone);sock.on('error',gone)});
setInterval(()=>clients.forEach(c=>{try{c.sock.write(Buffer.from([0x89,0]))}catch{}}),25000);
/* reset chat komunitas tiap 2 jam sekali (batas genap WIB: 00.00, 02.00, ...). Hanya komunitas; akun, chat pribadi, dan pengaturan tidak disentuh */
const wib=()=>String(Math.floor((Date.now()+7*36e5)/72e5));
function reset(){const d=wib();if(db.cday===d)return;const first=!db.cday;db.cday=d;if(first){sv();return}db.com=[];sv();cast({t:'clear',day:d});say('🧹 Chat komunitas sudah direset otomatis (tiap 2 jam sekali) supaya server tetap ringan.')}
reset();setInterval(reset,30000);
/* status otomatis hilang setelah 24 jam, dibuang dari penyimpanan supaya tidak menumpuk */
function purgeStat(){let ch=0;for(const k in db.users){const s=db.users[k].stat;if(s&&Date.now()-s.t>864e5){db.users[k].stat=null;ch=1}}if(ch)sv()}
purgeStat();setInterval(purgeStat,6e5);
const ok=(s,n)=>typeof s==='string'&&s.length<=n;
const KINDS={text:1e3,stk:3e5,img:12e5,vid:28e5,file:205e4,aud:3e6},AUDM=4e5,DURL=/^data:[a-z]+\/[\w.+-]+(;[\w=.-]+)*;base64,[A-Za-z0-9+\/=]+$/;
const why=(k,b,n)=>!KINDS[k]?'Jenis pesan tidak dikenal':!b?'Pesan kosong':!ok(b,KINDS[k])?'Ukuran terlalu besar (maks '+(KINDS[k]/1e6*.75).toFixed(1)+' MB)':k!=='text'&&!DURL.test(b)?'Format file tidak valid':(k==='img'||k==='stk')&&!b.startsWith('data:image/')?'File bukan gambar':k==='vid'&&!b.startsWith('data:video/')?'File bukan video':k==='aud'&&!b.startsWith('data:audio/')?'Bukan rekaman suara':/\.(exe|apk|bat|cmd|js|vbs|scr|msi|jar|sh)$/i.test(n||'')?'Jenis file ini diblokir':'Pesan tidak valid';
const valid=(k,b,n)=>KINDS[k]&&b&&ok(b,KINDS[k])&&(k==='text'||DURL.test(b)&&(k==='file'||b.startsWith(k==='vid'?'data:video/':k==='aud'?'data:audio/':'data:image/')))&&(n==null||ok(n,120))&&!/\.(exe|apk|bat|cmd|js|vbs|scr|msi|jar|sh)$/i.test(n||'');
const capMedia=L=>{let m=L.filter(x=>MK.includes(x.k)),tot=m.reduce((a,x)=>a+(x.b?x.b.length:0),0);while(m.length>30||(tot>25e6&&m.length>1)){tot-=m[0].b?m[0].b.length:0;L.splice(L.indexOf(m[0]),1);m.shift()}};
function redeemCode(u,raw){
 const n=Date.now();u.ra=(u.ra||[]).filter(t=>n-t<6e5);if(u.ra.length>=15)return[429,{e:'Terlalu banyak percobaan, tunggu beberapa menit'}];u.ra.push(n);
 const code=String(raw||'').trim();if(code.length<4||code.length>120)return[400,{e:'Kode tidak valid'}];
 const up=code.toUpperCase(),done=(o)=>{sv();return[200,{ok:1,bal:bal(u),tx:u.stx||[],...o}]};
 const hv=sha('astra-saldo-v1|'+up);
 if(VIPH.has(hv)){const o=db.vip[hv];if(o&&o!==u.id&&db.users[o])return[409,{e:'Kode ini sudah dipakai akun lain'}];
  if(!o||!db.users[o]){db.vip[hv]=u.id;credit(u,200000,'Redeem kode Owner')}
  if(u.adm!=='root')u.adm='vip';return done({kind:'owner',unlimited:1,adm:u.adm})}
 const sc=SCODES[hv];if(sc){if(db.used[hv])return[409,{e:'Kode redeem saldo Astra ini telah dipakai dan tidak bisa dipakai lagi.'}];db.used[hv]=u.id;credit(u,sc.a,'Redeem kode saldo');return done({kind:'saldo',amount:sc.a})}
 const hp=sha('astra-v1|'+up),pid=CODES[hp];if(pid){if(db.used[hp])return[409,{e:'Kode ini sudah dipakai.'}];db.used[hp]=u.id;setPl(u,pid,(PTAB[pid]||{}).d||30,true);return done({kind:'plan',plan:pid,days:null})}
 const c=db.codes[nc(code)];if(!c)return[400,{e:'Kode tidak valid.'}];
 if(c.used)return[409,{e:'Kode ini sudah dipakai dan tidak bisa dipakai lagi.'}];
 if(c.exp&&c.exp<n)return[410,{e:'Kode ini sudah kedaluwarsa.'}];
 c.used={u:u.id,t:n};
 if(c.t==='saldo'){credit(u,c.a,'Redeem kode saldo');return done({kind:'saldo',amount:c.a})}
 setPl(u,c.plan,c.days==null?((PTAB[c.plan]||{}).d||30):c.days,true);return done({kind:'plan',plan:c.plan,days:c.days})}
function h(q,r,b){
 const[p,qs]=q.url.split('?'),P=new URLSearchParams(qs||''),S=p.split('/').filter(Boolean),c=S[1],m=q.method;
 if(S[0]!=='api')return ER(r,404,'Tidak ditemukan');
 if(c==='ping')return J(r,200,{ok:1,v:11});
 /* ===== rute login tanpa token: passkey, kode QR ===== */
 if(c==='passkey'&&S[2]==='login'&&m==='POST'){const sf=SECF();if(!sf.pk)return ER(r,403,'Login passkey sedang dimatikan Owner');const rp=rpOf(q);
  if(S[3]==='begin'){const em=String(b.email||'').trim().toLowerCase(),tu=db.users[db.mail[em]],al=tu&&tu.pk?tu.pk.map(x=>x.id):[];return J(r,200,{ch:pkChal('webauthn.get',''),rp,allow:al})}
  if(S[3]==='finish'){const ip=ipOf(q);hits['p'+ip]=(hits['p'+ip]||0)+1;if(hits['p'+ip]>20)return ER(r,429,'Terlalu banyak percobaan, tunggu semenit');
   try{const id=String(b.id||''),uid=db.pki&&db.pki[id],u=db.users[uid],pk=u&&(u.pk||[]).find(x=>x.id===id);if(!pk)throw new Error('Passkey ini tidak terdaftar di server');
    const rs=b.response||{},cl=pkClient(rs.clientDataJSON,'webauthn.get',rp),ad=unb64u(rs.authenticatorData),a=pkAuth(ad);if(!a.rp.equals(s256(rp)))throw new Error('Passkey bukan untuk situs ini');if(!(a.fl&1))throw new Error('Passkey tidak mengonfirmasi kehadiranmu');
    if(!pkVerify(pk,Buffer.concat([ad,s256(cl.raw)]),unb64u(rs.signature)))throw new Error('Tanda tangan passkey tidak valid');
    if((a.cnt||pk.cnt)&&a.cnt<=pk.cnt)throw new Error('Passkey ditolak (penghitung mundur, kemungkinan duplikat)');pk.cnt=a.cnt;pk.last=Date.now();const t=mkTok(u,q,'passkey');sv();return J(r,200,{token:t,user:me(u),data:dat(u)})}
   catch(e){return ER(r,401,e.message||'Login passkey gagal')}}}
 if(c==='qrlogin'&&S[2]==='use'&&m==='POST'){const sf=SECF();if(!sf.qr)return ER(r,403,'Login dengan QR/kode sedang dimatikan Owner');const ip=ipOf(q);hits['q'+ip]=(hits['q'+ip]||0)+1;if(hits['q'+ip]>10)return ER(r,429,'Terlalu banyak percobaan, tunggu semenit');
  const code=qNorm(b.code);if(code.length!==12)return ER(r,400,'Kode harus 12 karakter (contoh ABCD-EFGH-JKMN)');const h=qHash(code),e=(db.qc||{})[h];if(!e||e.exp<Date.now())return ER(r,401,'Kode salah atau sudah kedaluwarsa');const u=db.users[e.id];if(!u)return ER(r,401,'Akun tidak ditemukan');
  delete db.qc[h];const t=mkTok(u,q,'qr');sv();return J(r,200,{token:t,user:me(u),data:dat(u)})}
 if(c==='register'||c==='login'){
  const em=String(b.email||'').trim().toLowerCase(),pw=String(b.pass||'');
  if(!/^\S+@\S+\.\S+$/.test(em)||pw.length<6||pw.length>200)return ER(r,400,'Email atau password tidak valid (password minimal 6 karakter)');
  let u=db.users[db.mail[em]];
  if(c==='register'){
   if(u)return ER(r,409,'Email sudah terdaftar, silakan masuk');
   if(db.regClosed)return ER(r,403,'Pendaftaran akun baru sedang ditutup oleh Owner');
   if(Object.keys(db.users).length>=MAXU)return ER(r,503,'Server penuh, pendaftaran ditutup sementara');
   const salt=cr.randomBytes(16).toString('hex');u=db.users[db.next]={id:String(db.next),email:em,name:em.split('@')[0].slice(0,24),salt,ph:ph(pw,salt),avatar:'',bg:'',wall:'',fx:'',status:'',fr:[],data:{},ep:0,cr:Date.now()};db.mail[em]=db.next++;
  }else if(!u||ph(pw,u.salt)!==u.ph)return ER(r,401,'Email atau password salah');
  if(c==='login'&&u.t2&&u.t2.on){const otp=String(b.otp||'').replace(/\s/g,'');if(!otp)return J(r,428,{e:'Masukkan kode verifikasi 6 digit dari aplikasi authenticator (atau kode cadangan).',need2fa:1});hits['o'+u.id]=(hits['o'+u.id]||0)+1;if(hits['o'+u.id]>8)return ER(r,429,'Terlalu banyak percobaan kode, tunggu semenit');if(!(t2chk(u,otp)||t2bk(u,otp)))return ER(r,401,'Kode verifikasi salah atau sudah dipakai')}
  const t=mkTok(u,q,'password');sv();return J(r,200,{token:t,user:me(u),data:dat(u)});
 }
 if(c==='avatar'&&m==='GET'){const x=db.users[S[2]],mm=x&&x.avatar&&/^data:(image\/[\w.+-]+);base64,(.+)$/.exec(x.avatar);if(!mm)return ER(r,404,'Tidak ada foto');r.writeHead(200,{'content-type':mm[1],'cache-control':'public,max-age=86400','access-control-allow-origin':'*'});return r.end(Buffer.from(mm[2],'base64'))}

 if(c==='wallpaper'&&m==='GET')return J(r,200,{wp:wpPub(),v:db.wpv||0});

 if(c==='wpprev'&&m==='GET'){const k=db.tok[P.get('t')||''],u=k&&db.users[k.id],id=String(S[2]||'').replace(/[^\w]/g,''),pd=wpPend[id];
  if(!u||k.ep!==u.ep||!isRoot(u)||!pd||!pd.done)return ER(r,404,'Tidak ada file');let st;try{st=fs.statSync(wpTmp(id))}catch{return ER(r,404,'File belum siap')}
  const tot=st.size,H={'content-type':pd.mime,'accept-ranges':'bytes','cache-control':'no-store','access-control-allow-origin':'*'},rg=/^bytes=(\d*)-(\d*)$/.exec(q.headers.range||'');let a=0,e=tot-1,code=200;
  if(rg&&(rg[1]||rg[2])){a=rg[1]?+rg[1]:Math.max(0,tot-+rg[2]);e=rg[1]&&rg[2]?Math.min(+rg[2],tot-1):tot-1;if(a>e||a>=tot){r.writeHead(416,{'content-range':'bytes */'+tot});return r.end()}code=206;H['content-range']=`bytes ${a}-${e}/${tot}`}
  H['content-length']=e-a+1;r.writeHead(code,H);const rs=fs.createReadStream(wpTmp(id),{start:a,end:e});rs.on('error',()=>r.destroy());r.on('close',()=>rs.destroy());rs.pipe(r);return}
 if(c==='wpfile'&&m==='GET'){const fid=String(S[2]||'').replace(/[^\w]/g,''),mt=db.wpf&&db.wpf[fid];if(!mt)return ER(r,404,'Tidak ada file');let st;try{st=fs.statSync(wpPath(fid))}catch{return ER(r,404,'File belum siap')}
  const tot=st.size,H={'content-type':mt.m,'accept-ranges':'bytes','cache-control':'public,max-age=31536000,immutable','access-control-allow-origin':'*'},rg=/^bytes=(\d*)-(\d*)$/.exec(q.headers.range||'');let a=0,e=tot-1,code=200;
  if(rg&&(rg[1]||rg[2])){a=rg[1]?+rg[1]:Math.max(0,tot-+rg[2]);e=rg[1]&&rg[2]?Math.min(+rg[2],tot-1):tot-1;if(a>e||a>=tot){r.writeHead(416,{'content-range':'bytes */'+tot});return r.end()}code=206;H['content-range']=`bytes ${a}-${e}/${tot}`}
  H['content-length']=e-a+1;r.writeHead(code,H);const rs=fs.createReadStream(wpPath(fid),{start:a,end:e});rs.on('error',()=>r.destroy());r.on('close',()=>rs.destroy());rs.pipe(r);return}
 const k=db.tok[(q.headers.authorization||'').slice(7)],u=k&&db.users[k.id];
 if(!u||k.ep!==u.ep)return ER(r,401,'Sesi habis, silakan masuk lagi');
 if(Date.now()-(k.ls||0)>6e4)k.ls=Date.now();
 if(isAB(u)&&!(c==='hb'||c==='me'||c==='ping'||c==='logoutall'||(c==='dm'&&S[2]===ownId()&&S.length<=3)))return J(r,403,{e:'Akunmu di-BAN',ab:banInfo(u)});
  seen[u.id]=Date.now();
 if(db.maint&&!isAdm(u)&&c!=='hb')return ER(r,503,'Server sedang pemeliharaan: '+db.maint);
 if(c==='pay'&&m==='GET')return J(r,200,{pay:db.pay||{}});
 /* ===== FX routes ===== */
 if(c==='daily'&&S[2]==='redeem'&&m==='POST'){const cf=dyCfg(),rd=fxCfg().rd;if(!cf.on||!rd.on)return ER(r,403,'Tebus hari bolong sedang tidak aktif');
  const d=String(b.d||''),st=dyState(u),sl=st.slots.find(x=>x.d===d);if(!sl||sl.st!=='miss')return ER(r,400,'Tanggal ini tidak bisa ditebus');
  if(bal(u)<rd.p)return ER(r,400,'Saldo tidak cukup. Tebus butuh Rp'+rd.p.toLocaleString('id-ID'));
  u.sal=bal(u)-rd.p;addTx(u,-rd.p,'Tebus hari bolong '+d,'TB'+Date.now().toString(36).toUpperCase());const cy=dyCyc(u);cy.c.push(d);u.dyc={s:cy.s,c:cy.c};fxDy(u,d,false);sv();
  return J(r,200,{ok:1,bal:bal(u),tx:u.stx,rd,...dyState(u)})}
 if(c==='fx'){const cf=fxCfg();
  if(S[2]==='me'&&m==='GET'){const sl=saleOn();return J(r,200,{mis:fxMis(u),reset:{d:toMid(),w:toMid()+Math.max(0,6-((new Date(wibDay()+'T00:00:00Z').getUTCDay()+6)%7))*864e5},ref:{code:refCode(u),n:u.rn||0,used:!!u.rb,cfg:cf.ref},badges:fxBdg(u),ttl:u.ttl||'',streak:dyStreak(u),best:dyBest(u),rd:cf.rd,sale:sl?{pct:sl.pct,end:sl.end,label:sl.label}:null,ev:fxEvI(),race:cf.race})}
  if(S[2]==='pull'&&m==='GET'){const an=cf.ann&&cf.ann.until>Date.now()?{id:cf.ann.id,text:cf.ann.text}:null,inb=u.inb||[];if(inb.length){u.inb=[];sv()}return J(r,200,{ann:an,inb,sale:saleOn()?{pct:saleOn().pct,end:saleOn().end,label:saleOn().label}:null,ev:fxEvI()})}
  if(S[2]==='ev'&&m==='POST'){const k=String(b.k);if(!['chat','bh'].includes(k))return ER(r,400,'Event tidak valid');const now=Date.now();if(now-(u._fe||0)<1500)return J(r,200,{ok:1,skip:1});u._fe=now;fxEv(u,k,1);return J(r,200,{ok:1})}
  if(S[2]==='claim'&&m==='POST'){const x=fxMis(u).find(q=>q.id===String(b.id));if(!x)return ER(r,404,'Misi tidak ada');if(x.cl)return ER(r,429,'Misi ini sudah diklaim');if(x.p<x.g)return ER(r,400,'Misi belum selesai');
   const mm=fxM(u);(x.per==='d'?mm.cd:mm.cw).push(x.id);u.fxm=(u.fxm||0)+1;let msg;
   if(x.r==='sal'){credit(u,x.a,'Misi: '+x.n,'MS'+Date.now().toString(36).toUpperCase());msg='+Rp'+x.a.toLocaleString('id-ID')+' masuk saldo'}else{gmOf(u).c+=x.a;msg='+'+x.a.toLocaleString('id-ID')+' koin Brohaven'}
   sv();return J(r,200,{ok:1,msg,bal:bal(u),tx:u.stx,gm:gmPub(u),mis:fxMis(u),badges:fxBdg(u)})}
  if(S[2]==='vch'&&m==='POST'){const k=(u._vh=(u._vh||0)+1);if(hits['v'+u.id]>8)return ER(r,429,'Terlalu sering mencoba, tunggu semenit');hits['v'+u.id]=(hits['v'+u.id]||0)+1;
   const code=String(b.code||'').trim().toUpperCase().replace(/[^A-Z0-9_-]/g,'').slice(0,24),v=cf.vch[code];if(!v||!v.on)return ER(r,404,'Kode tidak valid');
   if(v.exp&&v.exp<Date.now())return ER(r,410,'Kode sudah kedaluwarsa');if(v.max&&(v.n||0)>=v.max)return ER(r,410,'Kuota kode habis');v.by=v.by||{};if(v.by[u.id])return ER(r,409,'Kamu sudah memakai kode ini');
   v.by[u.id]=1;v.n=(v.n||0)+1;let msg;if(v.k==='sal'){credit(u,v.a,'Voucher '+code,'VC'+Date.now().toString(36).toUpperCase());msg='+Rp'+v.a.toLocaleString('id-ID')+' masuk saldo'}else{gmOf(u).c+=v.a;msg='+'+v.a.toLocaleString('id-ID')+' koin Brohaven'}
   sv();return J(r,200,{ok:1,msg,bal:bal(u),tx:u.stx,gm:gmPub(u)})}
  if(S[2]==='ref'&&m==='POST'){const rf=cf.ref;if(!rf.on)return ER(r,403,'Program ajak teman sedang tidak aktif');if(u.rb)return ER(r,409,'Kamu sudah pernah memakai kode ajakan');
   const code=String(b.code||'').trim().toUpperCase().replace(/[^A-Z0-9]/g,'').slice(0,12),inv=db.users[String((db.rix||{})[code])];if(!inv||inv.id===u.id)return ER(r,404,'Kode ajakan tidak ditemukan');if((inv.rn||0)>=rf.max)return ER(r,410,'Kode ini sudah mencapai batas ajakan');
   u.rb=inv.id;inv.rn=(inv.rn||0)+1;if(rf.b>0)credit(u,rf.b,'Bonus kode ajakan','RF'+Date.now().toString(36).toUpperCase());if(rf.a>0)credit(inv,rf.a,'Bonus ajak teman: '+u.name,'RF'+Date.now().toString(36).toUpperCase());sv();
   return J(r,200,{ok:1,msg:'Kode diterima! +Rp'+rf.b.toLocaleString('id-ID')+' masuk saldo',bal:bal(u),tx:u.stx})}
  if(S[2]==='title'&&m==='POST'){const id=String(b.id||'');if(!id){u.ttl='';sv();return J(r,200,{ok:1,ttl:''})}const bd=BADGES.find(x=>x[0]===id);if(!bd||!bd[3](u))return ER(r,400,'Lencana belum kamu dapatkan');u.ttl=id;sv();return J(r,200,{ok:1,ttl:id})}
  if(S[2]==='lb'&&m==='GET'){const k=['streak','coin','race'].includes(S[3])?S[3]:'streak',arr=fxLb(k),wk=fxWk(),i=arr.findIndex(x=>x.id===u.id);return J(r,200,{k,top:arr.slice(0,10).map(x=>({n:x.n,v:x.v,t:x.t,me:x.id===u.id})),me:i>=0?{rank:i+1,v:arr[i].v}:null,wk})}
  return ER(r,404,'Tidak ditemukan')}
 if(c==='gm'&&S[2]==='race'&&m==='POST'){const cf=fxCfg().race;if(!cf.on)return ER(r,403,'Balapan sedang ditutup');const oc=gClients().find(o=>o.u.id===u.id);if(!oc)return ER(r,400,'Masuk dulu ke Brohaven');const g=gmOf(u),now=Date.now();
  if(S[3]==='start'){if(oc.g.rm!=='w'||gDist(oc,RACE_ST[0],RACE_ST[1])>28)return ER(r,400,'Dekati garis start dulu');oc.g.rs=now;return J(r,200,{ok:1,pts:RACE_PTS})}
  if(S[3]==='finish'){const el=(now-(oc.g.rs||0))/1e3;if(!oc.g.rs||el>600)return ER(r,400,'Balapan belum dimulai');if(gDist(oc,RACE_ST[0],RACE_ST[1])>30)return ER(r,400,'Kamu belum sampai garis finis');oc.g.rs=0;
   if(el<12)return ER(r,400,'Waktu tidak masuk akal');const t=Math.round(el*10)/10,wk=fxWk();fxEv(u,'race',1);if(!g.rb||t<g.rb)g.rb=t;if(!g.rw||g.rw.w!==wk)g.rw={w:wk,t:0};if(!g.rw.t||t<g.rw.t)g.rw.t=t;
   const dk=wibDay();if(!g.rr||g.rr.d!==dk)g.rr={d:dk,n:0};let pay=0;if(t<=cf.t&&g.rr.n<5){g.rr.n++;pay=gGive(oc,cf.c,'Juara balap! '+t+' dtk')}sv();return J(r,200,{ok:1,t,best:g.rb,pay,target:cf.t,gm:gmPub(u)})}}
 if(c==='gm'){const cf=GMC();if(!cf.on&&!isRoot(u))return ER(r,403,'Brohaven sedang ditutup Owner');const g=gmOf(u),sub=S[2];
  if(sub==='world'&&m==='GET'){r.writeHead(200,{'content-type':'application/json','cache-control':'public,max-age=3600','access-control-allow-origin':'*'});return r.end(GWJ)}
  if(sub==='me'&&m==='GET')return J(r,200,{on:!!cf.on,gm:gmPub(u),pr:GPR,veh:GVEH,mul:cf.mul||1,top:gTop(cf),sal:bal(u),ev:fxEvI(),race:{pts:RACE_PTS,st:RACE_ST,cfg:fxCfg().race},mis:fxMis(u)});
  if(sub==='top'&&m==='POST'){const L=gTop(cf),i=Math.floor(+b.i),p=L[i];if(!p)return ER(r,400,'Paket tidak ada');if(bal(u)<p[1])return ER(r,400,'Saldo Astra tidak cukup. Butuh Rp'+p[1].toLocaleString('id-ID')+', saldomu Rp'+bal(u).toLocaleString('id-ID'));if(g.c+p[0]>1e9)return ER(r,400,'Koin terlalu banyak');const ref='GM'+Date.now().toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();u.sal=bal(u)-p[1];addTx(u,-p[1],'Top up '+p[0]+' koin Brohaven',ref);g.c+=p[0];sv();return J(r,200,{ok:1,gm:gmPub(u),sal:u.sal,bal:u.sal,tx:u.stx,add:p[0],ref})}
  if(sub==='av'&&m==='POST'){const a=b.av||{},o={};for(const k in GMX){const v=Math.floor(+a[k]);if(!(v>=0&&v<GMX[k]))return ER(r,400,'Pilihan tidak valid');if(GPR[k]&&GPR[k][v]>0&&!g.own.includes(k+v))return ER(r,403,'Item belum kamu beli');o[k]=v}g.av=o;sv();gClients().forEach(x=>x.g.known.delete(u.id));return J(r,200,{ok:1,gm:gmPub(u)})}
  if(sub==='buy'&&m==='POST'){const k=String(b.k),v=b.v;let pr,key;if(k==='veh'){if(!GVEH[v])return ER(r,400,'Kendaraan tidak ada');if(g.veh.includes(v))return ER(r,400,'Sudah kamu miliki');pr=GVEH[v].p;key=v}
   else if(GPR[k]&&Number.isInteger(+v)&&GPR[k][+v]!=null){pr=GPR[k][+v];key=k+(+v);if(g.own.includes(key)||pr===0)return ER(r,400,'Item ini gratis atau sudah kamu miliki')}else return ER(r,400,'Item tidak valid');
   if(g.c<pr)return ER(r,402,'Koin kurang '+(pr-g.c)+'. Kerja dulu di Brohaven!');g.c-=pr;if(k==='veh')g.veh.push(v);else g.own.push(key);sv();return J(r,200,{ok:1,gm:gmPub(u)})}
  if(sub==='veh'&&m==='POST'){const v=String(b.v),cc=Math.floor(+b.c);if(!g.veh.includes(v))return ER(r,403,'Kendaraan belum kamu miliki');g.vh=v;if(cc>=0&&cc<8)g.vc=cc;sv();gClients().forEach(x=>x.g.known.delete(u.id));return J(r,200,{ok:1,gm:gmPub(u)})}
  if(sub==='job'&&m==='POST'){const j=String(b.j||'');if(j&&!['kurir','polisi','pemadam'].includes(j))return ER(r,400,'Pekerjaan tidak ada');g.job=j;sv();gClients().forEach(x=>{x.g.known.delete(u.id);if(x.u.id===u.id){x.g.ord=null;if(j==='kurir'||j==='polisi')gOrder(x);else gsend(x,{t:'ord',o:null})}});return J(r,200,{ok:1,gm:gmPub(u)})}
  return ER(r,404,'Tidak ditemukan')}
 if(c==='daily'&&m==='GET')return J(r,200,{...dyState(u),rd:fxCfg().rd,ev:fxEvI()});
 if(c==='daily'&&S[2]==='claim'&&m==='POST'){const cf=dyCfg();if(!cf.on)return ER(r,403,'Bonus harian sedang tidak aktif');const t=wibDay(),cy=dyCyc(u);if(cy.c.includes(t))return ER(r,429,'Bonus hari ini sudah diklaim. Reset tengah malam WIB');
  const i=dyDiff(cy.s,t),amt=cf.amts[i]||0,perfect=i===6&&[0,1,2,3,4,5].every(k=>cy.c.includes(dyAdd(cy.s,k)));
  if(amt>0){bal(u);u.sal=bal(u)+amt;addTx(u,amt,'Bonus login harian hari ke-'+(i+1),'DL'+Date.now().toString(36).toUpperCase())}
  cy.c.push(t);u.dyc={s:cy.s,c:cy.c};fxDy(u,t,true);u.dyl={d:t,st:cy.c.length};let pz='';
  if(i===6&&cf.prize.on){if(!perfect)pz='Hadiah GO ASTRA Mini hanya untuk yang login 7 hari penuh tanpa bolong. Coba lagi di putaran berikutnya!';else if(isRoot(u))pz='Premium tidak berlaku untuk Owner (sudah unlimited)';else{const ms=dyMs(cf.prize),cur=planOf(u);if(cur==='free'||!PTAB[cur])u.pl={id:'go25',exp:Date.now()+ms};else{const ex=pexpOf(u);if(ex)u.pl.exp=ex+ms}pz='Selamat! Kamu dapat '+PTAB.go25.n+' selama '+cf.prize.val+' '+cf.prize.unit;alog(u,'bonus harian','hadiah '+PTAB.go25.n+' '+cf.prize.val+' '+cf.prize.unit)}}
  sv();return J(r,200,{ok:1,add:amt,pz,bal:bal(u),tx:u.stx,pl:plInfo(u),...dyState(u)})}
 if(c==='spin'&&m==='GET')return J(r,200,{...spView(false),...spState(u),hist:spStat().log.slice(0,8)});
 if(c==='spin'&&S[2]==='go'&&m==='POST'){const cf=spCfg();if(!cf.on)return ER(r,403,'Event Spin sedang tidak aktif');const today=wibDay();
  if(u.sp===today)return ER(r,429,'Jatah spin hari ini sudah dipakai. Reset tengah malam WIB');
  const st=spStat();u.sp=today;
  let el=cf.pr.map((p,i)=>({p,i})).filter(({p})=>p.w>0&&!(p.mx&&(st.c[p.id]||0)>=p.mx)&&!(p.t==='bal'&&cf.capBal&&st.bal+p.v>cf.capBal));
  if(!el.length)el=cf.pr.map((p,i)=>({p,i})).filter(({p})=>p.t==='none');if(!el.length)el=[{p:{id:'z',t:'none',v:0},i:Math.max(0,cf.pr.findIndex(p=>p.t==='none'))}];
  const tot=el.reduce((a,x)=>a+(x.p.w||1),0);let pk=cr.randomInt(1e9)/1e9*tot,ch=el[el.length-1];for(const x of el){pk-=(x.p.w||1);if(pk<0){ch=x;break}}
  const p=ch.p,lab=spLabel(p);let msg='';
  if(p.t==='bal'){credit(u,p.v,'Hadiah Spin: '+lab,'SP'+Date.now().toString(36).toUpperCase());st.bal+=p.v;msg='Saldo '+rp0(p.v)+' masuk ke saldomu'}
  else if(p.t==='plan'){if(isRoot(u))msg='Premium tidak berlaku untuk Owner (sudah unlimited)';else{const cur=planOf(u);if(cur==='free'||!PTAB[cur]){setPl(u,'go25',p.v,false);msg=PTAB.go25.n+' gratis '+p.v+' hari'}else{const ex=pexpOf(u);if(ex){setPl(u,cur,0,false);u.pl.exp=ex+p.v*864e5}msg='Masa aktif '+PTAB[cur].n+' +'+p.v+' hari'}}}
  else if(p.t==='disc'){const ex=dcOf(u);u.dc={pct:Math.max(p.v,ex?ex.pct:0),exp:Math.max(Date.now()+cf.ttl*36e5,ex?ex.exp:0)};msg='Diskon '+u.dc.pct+' persen untuk pembelian role, berlaku '+cf.ttl+' jam'}
  else msg='Belum beruntung hari ini. Coba lagi besok!';
  u.sl={t:Date.now(),l:lab};st.n++;st.c[p.id]=(st.c[p.id]||0)+1;if(p.t!=='none')st.log.unshift({t:Date.now(),u:u.name,id:u.id,l:lab});if(st.log.length>40)st.log.length=40;
  sv();fast();return J(r,200,{ok:1,idx:ch.i,prize:{id:p.id,t:p.t,v:p.v,l:lab},msg,dc:dcOf(u),bal:bal(u),tx:u.stx||[],pl:plInfo(u),next:toMid()})}
 if(c==='hb')return J(r,200,{ok:1,maint:db.maint||'',pl:plInfo(u),adm:isRoot(u)?'root':u.adm||'',pr:ptab(),payv:db.payv||0,wpv:db.wpv||0,sp:spState(u),bal:bal(u),tx:u.stx||[],live:lpub()||null,gift:u.gift||null,ban:isBanned(u)?(u.bu||1):0,ab:banInfo(u)});
 if(c==='gift'&&S[2]==='ack'&&m==='POST'){u.gift=null;sv();return J(r,200,{ok:1})}
 /* ===== rute keamanan akun (butuh login) ===== */
 if(c==='security'&&m==='GET'){const cur=(q.headers.authorization||'').slice(7),ss=Object.entries(db.tok).filter(([t,k])=>k.id===u.id&&k.ep===u.ep).map(([t,k])=>({sid:s256(t).toString('hex').slice(0,12),d:k.d||'Perangkat lama',ip:maskIp(k.ip||''),m:k.m||'password',c:k.c,ls:k.ls||k.c,cur:t===cur})).sort((a,b2)=>b2.ls-a.ls).slice(0,20),now=Date.now();
  return J(r,200,{email:u.email,pk:(u.pk||[]).map(x=>({id:x.id,n:x.n,t:x.t,last:x.last||0})),t2:!!(u.t2&&u.t2.on),bk:u.t2&&u.t2.bk?u.t2.bk.length:0,qr:Object.entries(db.qc||{}).filter(([h,e])=>e.id===u.id&&e.exp>now).map(([h,e])=>({h:h.slice(0,12),exp:e.exp,d:e.d})),ss,lg:(u.lg||[]).slice(0,12),sf:SECF(),host:rpOf(q)})}
 if(c==='passkey'&&m==='POST'){const sf=SECF();if(!sf.pk)return ER(r,403,'Passkey sedang dimatikan Owner');const rp=rpOf(q);
  if(S[2]==='reg'&&S[3]==='begin'){if((u.pk||[]).length>=8)return ER(r,400,'Maksimal 8 passkey per akun');return J(r,200,{ch:pkChal('webauthn.create',u.id),rp,uid:b64u(Buffer.from('astra:'+u.id)),email:u.email,name:u.name,ex:(u.pk||[]).map(x=>x.id)})}
  if(S[2]==='reg'&&S[3]==='finish'){try{const rs=b.response||{},cl=pkClient(rs.clientDataJSON,'webauthn.create',rp);if(cl.c.uid!==u.id)throw new Error('Tantangan bukan untuk akun ini');const att=cborDec(unb64u(rs.attestationObject)),ad=att.get('authData'),a=pkAuth(ad);if(!a.rp.equals(s256(rp)))throw new Error('Passkey bukan untuk situs ini');if(!(a.fl&1)||!a.id)throw new Error('Data passkey tidak lengkap');
    const k=pkKey(a.pk),id=b64u(a.id);if(db.pki&&db.pki[id])throw new Error('Passkey ini sudah terdaftar');db.pki=db.pki||{};(u.pk=u.pk||[]).push({id,pub:k.spki,alg:k.alg,cnt:a.cnt,n:String(b.name||'').trim().slice(0,30)||devOf(q),t:Date.now(),last:0});db.pki[id]=u.id;sv();return J(r,200,{ok:1,id})}catch(e){return ER(r,400,e.message||'Gagal menyimpan passkey')}}
  if(S[2]==='del'){const i=(u.pk||[]).findIndex(x=>x.id===String(b.id));if(i<0)return ER(r,404,'Passkey tidak ditemukan');delete db.pki[u.pk[i].id];u.pk.splice(i,1);sv();return J(r,200,{ok:1})}
  if(S[2]==='rename'){const p=(u.pk||[]).find(x=>x.id===String(b.id));if(!p)return ER(r,404,'Passkey tidak ditemukan');p.n=String(b.name||'').trim().slice(0,30)||p.n;sv();return J(r,200,{ok:1})}}
 if(c==='qrlogin'&&m==='POST'&&S[2]!=='use'){const sf=SECF();
  if(S[2]==='create'){if(!sf.qr)return ER(r,403,'Login QR sedang dimatikan Owner');hits['qc'+u.id]=(hits['qc'+u.id]||0)+1;if(hits['qc'+u.id]>10)return ER(r,429,'Terlalu sering membuat kode, tunggu semenit');db.qc=db.qc||{};const now=Date.now();for(const h in db.qc)if(db.qc[h].exp<now)delete db.qc[h];
   if(Object.values(db.qc).filter(e=>e.id===u.id).length>=5)return ER(r,400,'Maksimal 5 kode aktif. Hapus yang lama dulu');const mins=Math.max(1,Math.min(sf.maxq||1440,Math.floor(+b.mins)||10)),code=qGen(),pretty=code.replace(/(.{4})(?=.)/g,'$1-');db.qc[qHash(code)]={id:u.id,exp:now+mins*6e4,d:devOf(q)};sv();
   const proto=orgU(q)?orgU(q).protocol.replace(':',''):String(q.headers['x-forwarded-proto']||'').split(',')[0]||(rpOf(q)==='localhost'||rpOf(q)==='127.0.0.1'?'http':'https');return J(r,200,{code:pretty,url:proto+'://'+hostOf(q)+'/#qr='+code,exp:now+mins*6e4,mins})}
  if(S[2]==='revoke'){let n=0;for(const h in db.qc)if(db.qc[h].id===u.id&&(!b.h||h.startsWith(String(b.h))))delete db.qc[h],n++;sv();return J(r,200,{ok:1,n})}}
 if(c==='2fa'&&m==='POST'){
  if(S[2]==='setup'){if(u.t2&&u.t2.on)return ER(r,400,'2FA sudah aktif');const sec=b32e(cr.randomBytes(20));u.t2tmp=sec;sv();return J(r,200,{secret:sec.replace(/(.{4})/g,'$1 ').trim(),uri:'otpauth://totp/ASTRA%20AI:'+encodeURIComponent(u.email)+'?secret='+sec+'&issuer=ASTRA%20AI&digits=6&period=30'})}
  if(S[2]==='enable'){if(!u.t2tmp)return ER(r,400,'Mulai dari menu Aktifkan 2FA dulu');if(!t2chk(u,String(b.code||'').replace(/\s/g,''),u.t2tmp))return ER(r,401,'Kode salah, cek jam HP dan coba lagi');const codes=[];for(let i=0;i<8;i++){let s='';const rb=cr.randomBytes(10);for(let j=0;j<10;j++)s+=QA[rb[j]%QA.length];codes.push(s.slice(0,5)+'-'+s.slice(5))}
   u.t2={s:u.t2tmp,on:1,lc:Math.floor(Date.now()/3e4),bk:codes.map(x=>cr.createHash('sha256').update(x.replace('-','')).digest('hex'))};delete u.t2tmp;sv();return J(r,200,{ok:1,codes})}
  if(S[2]==='disable'){if(!(u.t2&&u.t2.on))return ER(r,400,'2FA belum aktif');if(ph(String(b.pass||''),u.salt)!==u.ph)return ER(r,403,'Password salah');const cd=String(b.code||'').replace(/\s/g,'');if(!(t2chk(u,cd)||t2bk(u,cd)))return ER(r,401,'Kode verifikasi salah');delete u.t2;sv();return J(r,200,{ok:1})}
  if(S[2]==='codes'){if(!(u.t2&&u.t2.on))return ER(r,400,'2FA belum aktif');if(!t2chk(u,String(b.code||'').replace(/\s/g,'')))return ER(r,401,'Kode verifikasi salah');const codes=[];for(let i=0;i<8;i++){let s='';const rb=cr.randomBytes(10);for(let j=0;j<10;j++)s+=QA[rb[j]%QA.length];codes.push(s.slice(0,5)+'-'+s.slice(5))}u.t2.bk=codes.map(x=>cr.createHash('sha256').update(x.replace('-','')).digest('hex'));sv();return J(r,200,{ok:1,codes})}}
 if(c==='session'&&S[2]==='revoke'&&m==='POST'){const sid=String(b.sid||'');let n=0;for(const t in db.tok){const k=db.tok[t];if(k.id===u.id&&s256(t).toString('hex').slice(0,12)===sid){if(t===(q.headers.authorization||'').slice(7))return ER(r,400,'Ini perangkat yang sedang kamu pakai. Gunakan Keluar biasa');delete db.tok[t];n++}}sv();return J(r,200,{ok:1,n})}
 if(c==='me')return J(r,200,me(u));
 /* ===== LIVE ===== */
 if(c==='live'){
  const a=S[2];
  if(a==='gifts')return J(r,200,{list:GIFTS});
  if(a==='status')return J(r,200,{live:lpub()||null});
  if(m!=='POST')return ER(r,404,'Tidak ditemukan');
  if(a==='gift'){
   if(!live)return ER(r,409,'Live sedang tidak berlangsung');
   if(u.id===live.host)return ER(r,400,'Host tidak bisa mengirim gift ke diri sendiri');
   const g=GMAP[String(b.gid)],q=Math.floor(+b.qty)||1;if(!g||q<1||q>99)return ER(r,400,'Gift tidak valid');
   const n=Date.now();u.gr=(u.gr||[]).filter(t=>n-t<1e4);if(u.gr.length>=15)return ER(r,429,'Terlalu cepat, tunggu sebentar');
   const total=g.p*q;if(total>2e7)return ER(r,400,'Maksimal Rp20.000.000 per pengiriman');
   if(bal(u)<total)return ER(r,400,'Saldo tidak cukup');
   const hh=db.users[live.host];if(!hh)return ER(r,400,'Host tidak ditemukan');
   u.gr.push(n);u.sal=bal(u)-total;addTx(u,-total,'Gift '+g.n+(q>1?' ×'+q:'')+' di live '+hh.name);
   credit(hh,total,'Gift '+g.n+(q>1?' ×'+q:'')+' dari '+u.name);live.gifts+=total;live.top[u.id]=(live.top[u.id]||0)+total;sv();
   const ev={t:'g',id:n+Math.random(),ts:n,u:{id:u.id,name:u.name,role:pub(u).role,av:pub(u).av},g,q,total};live.cmt.push(ev);if(live.cmt.length>60)live.cmt.shift();
   lcast(ev);lcast({t:'v',n:liveV().length,g:live.gifts,top:topOf(live)});
   return J(r,200,{ok:1,bal:u.sal,tx:u.stx,total})}
  if(!isRoot(u))return ER(r,403,'Hanya Owner utama yang bisa mengelola live');
  if(a==='start'){
   if(live)return ER(r,409,'Sudah ada live yang berjalan');
   const title=String(b.title||'').trim().slice(0,60)||'Live ASTRA';
   live={host:u.id,hn:u.name,title,t:Date.now(),kind:null,mime:'',init:null,cur:Buffer.alloc(0),head:null,jpg:null,ad:null,txt:[],fx:{theme:'neon',anim:'slide'},goal:null,pinc:null,lslow:0,lkn:0,gifts:0,top:{},peak:0,cm:true,mute:new Set(),cmt:[],lj:0,hostGone:Date.now(),snapOn:null};
   say('🔴 '+u.name+' sedang LIVE: "'+title+'". Ketuk tombol LIVE merah di atas layar atau tombol Tonton di bawah ini!');alog(u,'live mulai',title);
   return J(r,200,{ok:1,live:lpub()})}
  if(a==='stop'){endLive('Siaran selesai');alog(u,'live selesai','');return J(r,200,{ok:1})}
  if(a==='mod'){
   if(!live)return ER(r,409,'Live tidak berlangsung');const id=String(b.id),t=db.users[id];if(!t||isRoot(t))return ER(r,400,'Tidak bisa');
   if(b.act==='unmute')live.mute.delete(id);else live.mute.add(id);
   if(b.act==='kick')clients.forEach(c=>{if(c.r==='live'&&c.u.id===id){try{c.sock.end()}catch{}}});
   alog(u,'live '+b.act,t.name+' (ID '+t.id+')');return J(r,200,{ok:1})}
  return ER(r,404,'Tidak ditemukan')}
 /* ===== REDEEM (semua jenis kode diperiksa di server, sekali pakai) ===== */
 if(c==='redeem'&&m==='POST'){const [st,o]=redeemCode(u,b.code);return J(r,st,o)}
 /* ===== DOMPET ===== */
 if(c==='wallet'){
  if(S[2]==='spend'&&m==='POST'){const a=Math.floor(+b.amount);if(!(a>0)||a>1e9)return ER(r,400,'Jumlah tidak valid');if(bal(u)<a)return ER(r,400,'Saldo tidak cukup');u.sal=bal(u)-a;addTx(u,-a,String(b.note||'Pembayaran').slice(0,60));sv();return J(r,200,{ok:1,bal:u.sal,tx:u.stx})}
  if(S[2]==='topup'&&m==='POST'){if(process.env.AUTOTOPUP!=='1')return ER(r,403,'Top up otomatis belum diaktifkan di server. Hubungi admin untuk menambah saldo.');const a=Math.floor(+b.amount);if(!(a>0)||a>1e8)return ER(r,400,'Jumlah tidak valid');credit(u,a,'Top up '+String(b.ref||'').slice(0,20));sv();return J(r,200,{ok:1,bal:u.sal,tx:u.stx})}
  if(S[2]==='buyplan'&&m==='POST'){const id=String(b.plan),pl=PTAB[id];if(!pl)return ER(r,400,'Paket tidak valid');const pe=peff(id),dc=dcOf(u),dp=dc?Math.max(0,Math.floor(pl.p*(1-dc.pct/100))):Infinity,price=Math.min(pe,dp),usedDc=!!dc&&dp<pe;
   if(bal(u)<price)return ER(r,400,'Saldo tidak cukup');const ref='PL'+Date.now().toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();
   u.sal=bal(u)-price;addTx(u,-price,'Beli '+pl.n,ref);setPl(u,id,pl.d,true);if(usedDc)u.dc=null;sv();return J(r,200,{ok:1,ref,bal:u.sal,tx:u.stx,days:pl.d,price,name:pl.n,pl:plInfo(u),dc:dcOf(u),usedDc})}
  if(S[2]==='giftrole'&&m==='POST'){
   const n=Date.now();u.tr=(u.tr||[]).filter(t=>n-t<6e4);if(u.tr.length>=5)return ER(r,429,'Terlalu sering, tunggu sebentar');
   {const ve=vpw(u,b.vp);if(ve)return ER(r,403,ve)}
   const t=db.users[String(b.to)],pl0=PTAB[String(b.plan)];if(!t||t.id===u.id)return ER(r,404,'Penerima tidak ditemukan');if(!pl0)return ER(r,400,'Paket tidak valid');if(isAdm(t))return ER(r,400,'Akun Owner tidak bisa dihadiahi role');const pl={...pl0,p:peff(String(b.plan))};
   if(bal(u)<pl.p)return ER(r,400,'Saldo tidak cukup');
   const ref='RL'+n.toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();u.tr.push(n);
   u.sal=bal(u)-pl.p;addTx(u,-pl.p,'Hadiah role '+pl.n+' untuk '+t.name+' (ID '+t.id+')',ref);t.gift={plan:String(b.plan),days:pl.d,from:u.name,t:n};setPl(t,String(b.plan),pl.d,true);sv();
   return J(r,200,{ok:1,ref,t:n,bal:u.sal,tx:u.stx,plan:pl.n,days:pl.d,price:pl.p,to:{id:t.id,name:t.name,role:pub(t).role,av:pub(t).av}})}
  if(S[2]==='transfer'&&m==='POST'){
   const n=Date.now();u.tr=(u.tr||[]).filter(t=>n-t<6e4);if(u.tr.length>=5)return ER(r,429,'Terlalu sering transfer, tunggu sebentar');{const ve=vpw(u,b.vp);if(ve)return ER(r,403,ve)}
   const a=Math.floor(+b.amount),t=db.users[String(b.to)],note=String(b.note||'').trim().slice(0,40);
   if(!t||t.id===u.id)return ER(r,404,'Penerima tidak ditemukan');
   if(!(a>=1000))return ER(r,400,'Minimal transfer Rp1.000');if(a>1e7)return ER(r,400,'Maksimal Rp10.000.000 per transfer');
   if(note&&mod(note))return ER(r,403,'Catatan ditolak oleh bot keamanan');
   if(bal(u)<a)return ER(r,400,'Saldo tidak cukup');
   const ref='TF'+n.toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();u.tr.push(n);
   u.sal=bal(u)-a;addTx(u,-a,'Transfer ke '+t.name+' (ID '+t.id+')'+(note?' · '+note:''),ref);
   credit(t,a,'Terima dari '+u.name+' (ID '+u.id+')'+(note?' · '+note:''),ref);sv();
   return J(r,200,{ok:1,ref,t:n,amount:a,note,bal:u.sal,tx:u.stx,to:{id:t.id,name:t.name,role:pub(t).role,av:pub(t).av}})}
  return J(r,200,{bal:bal(u),tx:u.stx||[]})}
 /* ===== DAFTAR PENGGUNA (untuk transfer): f=all|on|off, q=cari nama/ID ===== */
 if(c==='users'){const qq=(P.get('q')||'').trim().toLowerCase(),f=P.get('f')||'all',off=Math.max(0,+P.get('off')||0),lim=Math.min(40,+P.get('lim')||30);
  let L=Object.values(db.users).filter(x=>x.id!==u.id).map(x=>({x,on:isOn(x.id)}));const tot={all:L.length,on:L.filter(y=>y.on).length};tot.off=tot.all-tot.on;
  if(f==='on')L=L.filter(y=>y.on);else if(f==='off')L=L.filter(y=>!y.on);
  if(qq)L=L.filter(y=>y.x.id===qq||(y.x.name||'').toLowerCase().includes(qq));
  L.sort((a,b)=>(b.on-a.on)||((seen[b.x.id]||0)-(seen[a.x.id]||0))||(a.x.name||'').localeCompare(b.x.name||''));
  return J(r,200,{list:L.slice(off,off+lim).map(y=>{const p=pub(y.x);return{id:p.id,name:p.name,role:p.role,av:p.av,on:y.on,seen:y.on?0:(seen[y.x.id]||0)}}),total:L.length,count:tot})}
 /* ===== ADMIN ===== */
 if(c==='admin'&&m==='POST'){
  const a=S.slice(2).join('/');
  if(a==='redeem'){
   const code=String(b.code||'').trim();
   if(sha('astra-owner-v1|'+code)!==OWH){const [st,o]=redeemCode(u,code);return J(r,st,o)}
   const n=Date.now();if(u.lk&&u.lk>n)return ER(r,429,'Terkunci karena terlalu banyak percobaan. Coba lagi '+Math.ceil((u.lk-n)/6e4)+' menit lagi');
   const ch=cr.randomBytes(16).toString('hex');chal[ch]={u:u.id,st:1,f:0,exp:n+5*6e4};return J(r,200,{need:'pin',ch})}
  if(a==='verify'){
   const n=Date.now(),x=chal[String(b.ch||'')];if(!x||x.u!==u.id||x.exp<n)return ER(r,400,'Sesi verifikasi habis, masukkan kode redeem lagi');
   if(u.lk&&u.lk>n){delete chal[b.ch];return ER(r,429,'Terkunci. Coba lagi '+Math.ceil((u.lk-n)/6e4)+' menit lagi')}
   const v=String(b.v||'').trim().toLowerCase().replace(/^te[sk]{1,2}k?\s+/,'');
   const good=x.st===1?sha('astra-2fa-pin|'+String(b.v||'').trim())===H2P:sha('astra-2fa-final|'+v)===H2F;
   if(!good){x.f++;if(x.f>=3){delete chal[b.ch];u.lk=n+15*6e4;return ER(r,429,'Terlalu banyak salah. Akun dikunci 15 menit')}return ER(r,403,'Verifikasi salah ('+x.f+'/3)')}
   if(x.st===1){x.st=2;x.exp=n+5*6e4;return J(r,200,{need:'final'})}
   delete chal[b.ch];u.adm='root';u.lk=0;if(!u.rootCr){u.rootCr=1;credit(u,200000,'Redeem kode Owner')}sv();return J(r,200,{ok:1,kind:'owner',unlimited:1,adm:'root',bal:bal(u),tx:u.stx})}
  if(!isRoot(u))return ER(r,403,'Akses ditolak');
  if(a==='codes'){const L=Object.values(db.codes).sort((x,y)=>y.at-x.at).slice(0,400).map(c=>({c:c.c,t:c.t,a:c.a,plan:c.plan,days:c.days,exp:c.exp||0,at:c.at,note:c.note||'',used:c.used?{t:c.used.t,id:c.used.u,name:(db.users[c.used.u]||{}).name||'(akun dihapus)'}:null}));return J(r,200,{list:L,now:Date.now()})}
  if(a==='code/new'){
   const t=b.t==='plan'?'plan':'saldo',cnt=Math.max(1,Math.min(50,Math.floor(+b.count)||1)),n=Date.now();
   const o={t,at:n,note:String(b.note||'').slice(0,40)};
   if(t==='saldo'){o.a=Math.floor(+b.a);if(!(o.a>=1000&&o.a<=1e9))return ER(r,400,'Nominal saldo minimal Rp1.000')}
   else{o.plan=String(b.plan||'');if(!RL.includes(o.plan)||o.plan==='free')return ER(r,400,'Paket tidak valid');o.days=Math.floor(+b.days);if(!(o.days>=0&&o.days<=3650))return ER(r,400,'Masa aktif tidak valid');}
   const h=Math.floor(+b.expH)||0;if(h<0||h>24*3650)return ER(r,400,'Masa berlaku kode tidak valid');if(h)o.exp=n+h*36e5;
   const cu=nc(b.custom);if(cu&&(cnt>1||cu.length<4||cu.length>40))return ER(r,400,cu&&cnt>1?'Kode custom hanya untuk satu kode':'Kode custom 4-40 huruf/angka');
   const out=[];for(let i=0;i<cnt;i++){let k=cu;if(!k){do{k=Array.from({length:16},()=>'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'[cr.randomInt(32)]).join('')}while(db.codes[k])}
    if(db.codes[k])return ER(r,409,'Kode '+fmt(k)+' sudah ada');db.codes[k]={...o,c:fmt(k)};out.push(fmt(k))}
   sv();return J(r,200,{ok:1,codes:out})}
  if(a==='code/del'){const k=nc(b.c);if(!db.codes[k])return ER(r,404,'Kode tidak ditemukan');delete db.codes[k];sv();return J(r,200,{ok:1})}
  const t=db.users[String(b.id)];
  if(a==='ban'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa di-ban');
   const dur=Math.max(0,Math.floor(+b.dur)||0),why=String(b.reason||'').trim().slice(0,60);
   t.ban=b.on?1:0;t.ab=b.on?1:0;t.bw=b.on?why:'';t.bu=b.on&&dur?Date.now()+dur*6e4:0;if(!b.on)t.st=0;sv();castU([t.id],{t:'ban',ab:banInfo(t)});
   alog(u,b.on?'ban':'buka ban',t.name+' (ID '+t.id+')'+(b.on?(dur?' · '+dur+' menit':' · permanen')+(why?' · '+why:''):''));
   say(b.on?'🛡️ '+t.name+' di-BAN dari komunitas oleh Owner'+(dur?' selama '+(dur>=1440?Math.round(dur/1440)+' hari':dur>=60?Math.round(dur/60)+' jam':dur+' menit'):' (permanen)')+(why?'. Alasan: '+why:'.'):'✅ Ban '+t.name+' dibuka oleh Owner.');
   return J(r,200,{ok:1,ban:!!t.ban})}
  if(a==='mute'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa di-mute');const dur=Math.max(0,Math.floor(+b.dur)||0);t.mute=dur?Date.now()+dur*6e4:0;sv();alog(u,dur?'mute':'buka mute',t.name+' (ID '+t.id+')'+(dur?' · '+dur+' menit':''));return J(r,200,{ok:1,mute:t.mute})}
  if(a==='saldo'){if(!t)return ER(r,404,'ID tidak ditemukan');const v=Math.trunc(+b.amount);if(!v||Math.abs(v)>1e9)return ER(r,400,'Nominal tidak valid');const d=v<0?-Math.min(-v,bal(t)):v;if(!d)return ER(r,400,'Saldo pengguna sudah 0');
   if(d>0)credit(t,d,String(b.note||'Penambahan saldo oleh Owner').slice(0,50));else{t.sal=bal(t)+d;addTx(t,d,String(b.note||'Pengurangan saldo oleh Owner').slice(0,50))}sv();alog(u,'saldo',t.name+' (ID '+t.id+') '+(d>0?'+':'')+d);return J(r,200,{ok:1,bal:t.sal})}
  if(a==='plan'){if(!t)return ER(r,404,'ID tidak ditemukan');const p=String(b.plan||'');if(p!=='free'&&!RL.includes(p))return ER(r,400,'Paket tidak valid');const days=Math.max(0,Math.min(3650,Math.floor(+b.days)||0));t.gift={plan:p,days,from:'Owner',t:Date.now(),rev:p==='free'};setPl(t,p,days,false);sv();alog(u,p==='free'?'cabut premium':'beri premium',t.name+' (ID '+t.id+') '+p+(days?' '+days+' hari':' unlimited'));return J(r,200,{ok:1})}
  if(a==='user/del'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa dihapus');alog(u,'hapus akun',t.name+' (ID '+t.id+')');delete db.users[t.id];delete db.mail[t.email];clients.forEach(c=>{if(c.u.id===t.id){try{c.sock.destroy()}catch{}}});sv();return J(r,200,{ok:1})}
  if(a==='users'){const qq=String(b.q||'').trim().toLowerCase(),off=Math.max(0,+b.off||0),lim=Math.min(40,+b.lim||25),n=Date.now();
   let L=Object.values(db.users);if(qq)L=L.filter(x=>x.id===qq||(x.name||'').toLowerCase().includes(qq)||(x.email||'').includes(qq));
   if(b.f==='ban')L=L.filter(x=>isBanned(x));else if(b.f==='on')L=L.filter(x=>isOn(x.id));else if(b.f==='adm')L=L.filter(x=>isAdm(x));
   if(b.sort==='sal')L.sort((x,y)=>bal(y)-bal(x));else L.sort((x,y)=>(isOn(y.id)-isOn(x.id))||((y.cr||0)-(x.cr||0)));
   return J(r,200,{total:L.length,list:L.slice(off,off+lim).map(x=>({id:x.id,name:x.name,email:x.email,role:pub(x).role,adm:x.adm||'',plan:planOf(x),pexp:pexpOf(x),sal:bal(x),ban:isBanned(x),ab:x.ab?1:0,bw:x.bw||'',bu:x.bu||0,mute:x.mute>n?x.mute:0,on:isOn(x.id),seen:seen[x.id]||0,cr:x.cr||0,av:pub(x).av}))})}
  if(a==='stats'){const us=Object.values(db.users),n=Date.now(),cs=Object.values(db.codes);
   return J(r,200,{users:us.length,online:us.filter(x=>isOn(x.id)).length,banned:us.filter(x=>isBanned(x)).length,muted:us.filter(x=>x.mute>n).length,admins:us.filter(x=>isAdm(x)).length,saldo:us.reduce((a,x)=>a+bal(x),0),com:db.com.length,codes:{free:cs.filter(c=>!c.used&&!(c.exp&&c.exp<n)).length,used:cs.filter(c=>c.used).length},live:lpub()||null,lastLive:db.lastLive||null,slow:db.slow||0,lock:!!db.lock,pin:db.pin||null,maint:db.maint||'',nomedia:!!db.nomedia,novoice:!!db.novoice,store:{...store,loaded},regClosed:!!db.regClosed,words:(db.words||[]).length,up:Math.round(process.uptime()),mem:Math.round(process.memoryUsage().rss/1e6),gifts:GIFTS.length})}
  if(a==='com'){let ch=[];
   if(b.slow!=null){db.slow=Math.max(0,Math.min(600,Math.floor(+b.slow)||0));ch.push('mode lambat '+db.slow+'s')}
   if(b.lock!=null){db.lock=!!b.lock;ch.push(db.lock?'komunitas dikunci':'komunitas dibuka')}
   if(b.pin!=null){const p=String(b.pin).trim().slice(0,300);db.pin=p?{b:p,t:Date.now()}:null;ch.push(p?'pin pengumuman':'hapus pin')}
   if(b.nomedia!=null){db.nomedia=!!b.nomedia;ch.push(db.nomedia?'media dimatikan':'media dibuka')}
   if(b.novoice!=null){db.novoice=!!b.novoice;ch.push(db.novoice?'suara dimatikan':'suara dibuka')}
   if(b.announce){say('📢 PENGUMUMAN OWNER: '+String(b.announce).trim().slice(0,400));ch.push('pengumuman')}
   sv();alog(u,'komunitas',ch.join(', '));return J(r,200,{ok:1,slow:db.slow||0,lock:!!db.lock,pin:db.pin||null})}
  if(a==='purge'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Tidak bisa');const del=db.com.filter(x=>x.u===t.id);db.com=db.com.filter(x=>x.u!==t.id);del.forEach(x=>cast({t:'del',i:x.i}));sv();alog(u,'hapus semua chat',t.name+' ('+del.length+' pesan)');return J(r,200,{ok:1,n:del.length})}
  if(a==='price'){const id=String(b.plan);if(!PTAB[id])return ER(r,400,'Paket tidak valid');db.prices=db.prices||{};if(b.off){delete db.prices[id];alog(u,'harga normal',id)}else{const p=Math.floor(+b.p);if(!(p>=0)||p>1e8)return ER(r,400,'Harga tidak valid');const mins=Math.max(0,Math.floor(+b.mins)||0);db.prices[id]={p,note:String(b.note||'').trim().slice(0,90),until:mins?Date.now()+mins*6e4:0};alog(u,'harga',id+' '+PTAB[id].p+' → '+p+(mins?' · '+mins+' menit':''));if(p<PTAB[id].p)say('🏷️ PROMO! '+PTAB[id].n+' turun dari Rp'+PTAB[id].p.toLocaleString('id-ID')+' jadi Rp'+p.toLocaleString('id-ID')+(db.prices[id].note?' · '+db.prices[id].note:'')+'. Cek menu Upgrade!')}sv();return J(r,200,{ok:1,pr:ptab()})}
  if(a==='pay'){const id=String(b.plan);if(!PTAB[id])return ER(r,400,'Paket tidak valid');db.pay=db.pay||{};
   if(b.off){delete db.pay[id]}else{const link=String(b.link||'').trim();if(link&&!/^https:\/\/[^\s"'<>]{4,600}$/.test(link))return ER(r,400,'Link pembayaran harus diawali https://');
    let qr=b.qr===undefined?(db.pay[id]||{}).qr||'':String(b.qr||'');if(qr&&(!/^data:image\/(png|jpeg|webp);base64,[A-Za-z0-9+\/=]+$/.test(qr)||qr.length>4e5))return ER(r,400,'Gambar QR harus PNG/JPG/WebP dan maks ±290 KB');
    if(!link&&!qr)delete db.pay[id];else db.pay[id]={link,qr,note:String(b.note||'').slice(0,80)}}
   db.payv=Date.now();sv();alog(u,'pembayaran',id+(b.off?' dihapus':' diperbarui'));return J(r,200,{ok:1,pay:db.pay})}
  if(a==='wpfetch'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');if(wpBusy)return ER(r,429,'Masih mengunduh video lain, tunggu sebentar');
   const id=cr.randomBytes(8).toString('hex'),now=Date.now();for(const f in wpPend)if(now-wpPend[f].t>18e5){wpUnl(wpTmp(f));delete wpPend[f]}
   const ks=Object.keys(wpPend);while(ks.length>=2){const f=ks.shift();wpUnl(wpTmp(f));delete wpPend[f]}
   wpBusy=1;wpDl(String(b.url||'').trim(),id).then(o=>{wpPend[id]={t:Date.now(),n:o.n,total:o.n,mime:o.mime,done:1};J(r,200,{ok:1,fid:id,size:o.n,mime:o.mime,name:o.name})}).catch(e=>ER(r,400,e.message||'Gagal mengunduh link')).finally(()=>{wpBusy=0});return}
  if(a==='gm'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const cf=GMC();
   if(b.act==='set'){cf.on=b.on?1:0;cf.mul=Math.min(10,Math.max(.1,+b.mul||1));sv();if(!cf.on)gClients().forEach(c=>{wsend(c,{t:'off',b:'Brohaven ditutup oleh Owner'});try{c.sock.end()}catch{}});alog(u,'brohaven',cf.on?'dibuka x'+cf.mul:'ditutup')}
   if(b.act==='top'){const L=(Array.isArray(b.top)?b.top:[]).slice(0,8).map(x=>[Math.floor(+x[0]),Math.floor(+x[1])]);if(!L.length||L.some(x=>!(x[0]>=1&&x[0]<=1e7&&x[1]>=100&&x[1]<=1e7)))return ER(r,400,'Paket tidak valid: koin 1-10.000.000, harga Rp100-10.000.000');cf.top=L;sv();alog(u,'brohaven','paket top up '+L.length)}
   if(b.act==='ann'){const m2=gClean(b.m);if(m2){gClients().forEach(c=>gsend(c,{t:'ann',m:'📢 '+m2}));alog(u,'brohaven','pengumuman: '+m2.slice(0,40))}}
   if(b.act==='kick'){const id=String(b.id);GM.kick[id]=Date.now()+3e5;clients.forEach(c=>{if(c.r==='game'&&c.u.id===id){wsend(c,{t:'off',b:'Kamu dikeluarkan dari Brohaven oleh Owner'});try{c.sock.end()}catch{}}});alog(u,'brohaven','kick '+id)}
   if(b.act==='give'){const t=db.users[String(b.id)];if(!t)return ER(r,404,'Pengguna tidak ada');const a2=Math.max(-1e6,Math.min(1e6,Math.floor(+b.a)||0)),g2=gmOf(t);g2.c=Math.max(0,g2.c+a2);sv();clients.forEach(c=>{if(c.r==='game'&&c.u.id===t.id)gsend(c,{t:'pay',a:a2,c:g2.c,w:a2>=0?'Hadiah dari Owner':'Koin dikurangi Owner'})});alog(u,'brohaven','koin '+a2+' ke '+t.id)}
   return J(r,200,{cfg:{on:!!cf.on,mul:cf.mul||1,top:gTop(cf)},online:gClients().map(c=>({id:c.u.id,name:c.u.name,rm:c.g.rm,x:Math.round(c.g.x),z:Math.round(c.g.z),job:gmOf(c.u).job,c:gmOf(c.u).c,v:c.g.v})),fires:GM.fires.length})}
  if(['purge','restore','airdrop','clear','saldo'].includes(a)&&Date.now()-(BK.pre||0)>6e4){BK.pre=Date.now();snapLocal('lite','sebelum-'+a)}
  if(a==='bk'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const act=b.act||'status';
   const stat=async()=>{const ix=RU?await cloudIdx():{list:[]};let tot=0,cnt=0;for(const id in db.users){tot+=db.users[id].sal||0;cnt++}let js=0;try{js=fs.statSync(JRN).size}catch{}
    return{up:!!RU,loaded,acc:cnt,sum:tot,lite:bkFiles('lite').slice(0,14),full:bkFiles('full').slice(0,8),cloud:ix.list||[],jr:js,t:BK.t,cerr:BK.cerr,boot:BK.boot,store:{ok:store.ok,err:store.err,errAt:store.errAt},dir:BKD}};
   if(act==='status')return stat().then(x=>J(r,200,x)).catch(e=>ER(r,500,e.message));
   if(act==='now')return snapNow('manual oleh '+u.name).then(async x=>{alog(u,'cadangan manual',(x.lite?'lokal ':'')+(x.cloud&&x.cloud.ok?'+cloud':''));J(r,200,{res:x,...await stat()})}).catch(e=>ER(r,500,e.message));
   if(act==='restore')return Promise.resolve().then(()=>b.src==='cloud'?readSnapCloud(String(b.k)):readSnapLocal(String(b.k))).then(async d=>{const pre=snapLocal('lite','sebelum-pulih'),res=mergeSnap(d,!!b.force,b.mode==='wallet');alog(u,'pulihkan dari cadangan server',(b.force?'PAKSA ':'')+(b.mode==='wallet'?'saldo ':'penuh ')+res.add+' akun, '+res.upd+' saldo');J(r,200,{ok:1,...res,pre,...await stat()})},e=>ER(r,400,'Gagal membaca cadangan: '+e.message));
   if(act==='jr'){const id=String(b.id||'');const t2=db.users[id];return J(r,200,{id,name:t2?t2.name:'?',sal:t2?bal(t2):null,list:jrnRead(id,40)})}
   if(act==='jrecover'){const id=String(b.id||''),t2=db.users[id];if(!t2)return ER(r,404,'ID tidak ditemukan');const l=jrnRead(id,1)[0];if(!l)return ER(r,404,'Tidak ada catatan jurnal untuk akun ini');const old=bal(t2);t2.sal=Math.max(0,Math.floor(l.b));addTx(t2,t2.sal-old,'Pemulihan dari jurnal (oleh Owner)');sv();alog(u,'pulihkan saldo dari jurnal',t2.name+' '+old+' → '+t2.sal);return J(r,200,{ok:1,old,sal:t2.sal})}
   return ER(r,400,'Aksi tidak dikenal')}
  if(a==='own'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const cf=fxCfg(),act=b.act;
   if(act==='audit'){const us=Object.values(db.users);let tot=0,neg=0,weak=0,zero=0;const top=[];for(const x of us){const s=bal(x);if(!(s>=0)||!Number.isFinite(s))neg++;if(s>0&&!x.wv)weak++;if(!s)zero++;tot+=Number.isFinite(s)?s:0;top.push(x)}top.sort((p,q)=>bal(q)-bal(p));
    return J(r,200,{acc:us.length,tot,neg,weak,zero,top:top.slice(0,10).map(x=>({id:x.id,n:x.name,s:bal(x)})),glog:(db.glog||[]).slice(0,25),ann:cf.ann||null})}
   if(act==='find'){const q=String(b.q||'').trim().toLowerCase();if(q.length<2)return ER(r,400,'Ketik minimal 2 huruf');const L=[];for(const x of Object.values(db.users)){if(String(x.id)===q||(x.email||'').toLowerCase().includes(q)||(x.name||'').toLowerCase().includes(q)){L.push({id:x.id,n:x.name,e:x.email,s:bal(x),wv:x.wv||0,pl:planOf(x),ban:!!isBanned(x),st:dyStreak(x),tx:(x.stx||[]).slice(0,12),jr:jrnRead(x.id,8)});if(L.length>=6)break}}return J(r,200,{list:L})}
   if(act==='ann'){const t2=String(b.text||'').trim().slice(0,300);if(!b.on||!t2){delete cf.ann;sv();alog(u,'pengumuman','dihapus');return J(r,200,{ann:null})}const h=Math.max(1,Math.min(720,Math.floor(+b.hrs)||24));cf.ann={id:Date.now(),text:t2,until:Date.now()+h*36e5,by:u.name};sv();alog(u,'pengumuman',t2.slice(0,60));return J(r,200,{ann:cf.ann})}
   if(act==='notif'){const t2=String(b.text||'').trim().slice(0,200),to=String(b.to||'').trim();if(!t2)return ER(r,400,'Isi pesan dulu');const tu=db.users[to];if(!tu)return ER(r,404,'ID pengguna tidak ditemukan');(tu.inb=tu.inb||[]).push({t:Date.now(),x:t2});if(tu.inb.length>20)tu.inb.shift();sv();alog(u,'notifikasi pribadi',tu.name+': '+t2.slice(0,50));return J(r,200,{ok:1,to:tu.name})}
   if(act==='resetlb'){for(const x of Object.values(db.users)){if(x.gm){delete x.gm.we;delete x.gm.rw}}for(const k in LBC)delete LBC[k];sv();alog(u,'reset peringkat mingguan','');return J(r,200,{ok:1})}
   return ER(r,400,'Aksi tidak dikenal')}
  if(a==='sec'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const sf=SECF();if(b.act==='set'){sf.qr=b.qr?1:0;sf.pk=b.pk?1:0;sf.maxq=Math.max(5,Math.min(10080,Math.floor(+b.maxq)||1440));alog(u,'keamanan login','qr='+sf.qr+' passkey='+sf.pk+' maks='+sf.maxq);sv()}
   let pk=0,t2=0,qa=0;for(const x of Object.values(db.users)){pk+=(x.pk||[]).length;if(x.t2&&x.t2.on)t2++}for(const e of Object.values(db.qc||{}))if(e.exp>Date.now())qa++;return J(r,200,{...sf,pk_n:pk,t2_n:t2,qr_n:qa,tok_n:Object.keys(db.tok).length})}
  if(a==='fx'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const cf=fxCfg(),act=b.act,N=(v,lo,hi)=>Math.max(lo,Math.min(hi,Math.floor(+v)||0));
   const out=()=>({mis:fxDefs(),ev:{...cf.ev,now:fxEvI()},sale:{...cf.sale,live:!!saleOn()},ref:cf.ref,rd:cf.rd,race:cf.race,vch:Object.entries(cf.vch).map(([k,v])=>({code:k,k:v.k,a:v.a,max:v.max||0,n:v.n||0,exp:v.exp||0,on:v.on?1:0}))});
   if(act==='mis'){for(const q of(Array.isArray(b.mis)?b.mis:[])){const d=FXMIS.find(x=>x.id===q.id);if(!d)continue;cf.mo[d.id]={g:N(q.g,1,1e6),a:N(q.a,0,1e6),on:q.on===0?0:1,r:q.r==='sal'?'sal':'coin'}}alog(u,'misi','ubah misi')}
   else if(act==='ev'){const e=b.ev||{};cf.ev={on:e.on?1:0,mult:N(e.mult,1,10)||2,wk:e.wk?1:0,until:e.hrs>0?Date.now()+N(e.hrs,1,720)*36e5:(e.keep?cf.ev.until:0),label:String(e.label||'Coins Ganda').slice(0,30)};alog(u,'event brohaven',(cf.ev.on?'aktif x':'mati x')+cf.ev.mult)}
   else if(act==='sale'){const s=b.sale||{},mins=N(s.mins,0,43200);const st0=+s.at>Date.now()?+s.at:Date.now();cf.sale={on:s.on?1:0,pct:N(s.pct,1,90)||20,start:st0,end:s.on?(mins>0?st0+mins*6e4:cf.sale.end):0,label:String(s.label||'Flash Sale').slice(0,30)};if(cf.sale.on&&!(cf.sale.end>Date.now()))return ER(r,400,'Isi durasi flash sale (menit)');alog(u,'flash sale',cf.sale.on?'-'+cf.sale.pct+'% '+mins+' menit':'mati')}
   else if(act==='ref'){const x=b.ref||{};cf.ref={on:x.on?1:0,a:N(x.a,0,1e5),b:N(x.b,0,1e5),max:N(x.max,1,500)};alog(u,'ajak teman','ubah')}
   else if(act==='rd'){const x=b.rd||{};cf.rd={on:x.on?1:0,p:N(x.p,0,1e5)};alog(u,'tebus hari','Rp'+cf.rd.p)}
   else if(act==='race'){const x=b.race||{};cf.race={on:x.on?1:0,t:N(x.t,15,600),c:N(x.c,0,1e5)};alog(u,'balap','ubah')}
   else if(act==='vch'){const x=b.v||{},code=String(x.code||'').trim().toUpperCase().replace(/[^A-Z0-9_-]/g,'').slice(0,24);if(code.length<3)return ER(r,400,'Kode minimal 3 karakter (huruf/angka)');if(!['sal','coin'].includes(x.k))return ER(r,400,'Jenis hadiah tidak valid');const am=N(x.a,1,1e6);if(!(am>=1))return ER(r,400,'Nominal 1 - 1.000.000');
    const old=cf.vch[code]||{};cf.vch[code]={k:x.k,a:am,max:N(x.max,0,1e6),exp:N(x.days,0,3650)>0?Date.now()+N(x.days,0,3650)*864e5:0,on:x.on===0?0:1,n:old.n||0,by:old.by||{}};alog(u,'voucher',code+' '+x.k+' '+am)}
   else if(act==='vchdel'){delete cf.vch[String(b.code||'').toUpperCase()];alog(u,'voucher','hapus '+b.code)}
   sv();return J(r,200,out())}
  if(a==='daily'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');const cf=dyCfg();
   if(b.act==='set'){const am=(Array.isArray(b.amts)?b.amts:[]).slice(0,7).map(x=>Math.floor(+x));if(am.length!==7||am.some(x=>!(x>=0&&x<=1e6)))return ER(r,400,'Nominal tiap hari harus 0 - 1.000.000');const pz=b.prize||{},un=['menit','jam','hari'].includes(pz.unit)?pz.unit:'hari',mx={menit:525600,jam:8760,hari:365}[un],val=Math.floor(+pz.val);if(pz.on&&!(val>=1&&val<=mx))return ER(r,400,'Durasi hadiah harus 1 - '+mx+' '+un);
    cf.on=b.on?1:0;cf.amts=am;cf.amt=am[0];cf.amt7=am[6];cf.prize={on:pz.on?1:0,unit:un,val:val>=1?Math.min(mx,val):1};sv();alog(u,'bonus harian',(cf.on?'aktif ':'mati ')+am.join('/')+' · hadiah '+(cf.prize.on?cf.prize.val+' '+un:'mati'))}
   return J(r,200,{cfg:{on:!!cf.on,amts:cf.amts,prize:cf.prize}})}
  if(a==='spin'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');
   if(b.act==='set'){const o=spSan(b);if(!o)return ER(r,400,'Pengaturan tidak valid: butuh 2-12 hadiah dan minimal satu hadiah dengan peluang di atas 0');db.spin=o;sv();fast();alog(u,'spin',(o.on?'aktif':'mati')+' · '+o.pr.length+' hadiah');}
   if(b.act==='reset'){db.spin=SPD();sv();fast();alog(u,'spin','pengaturan dikembalikan ke bawaan')}
   if(b.act==='clear'){db.spd=null;sv()}
   const st=spStat();return J(r,200,{cfg:spView(true),stat:{d:st.d,n:st.n,bal:st.bal,c:st.c,log:st.log.slice(0,20)},users:Object.values(db.users).filter(x=>x.sp===st.d).length})}
  if(a==='wallpaper'){if(!isRoot(u))return ER(r,403,'Hanya Owner utama');
   if(b.act==='get')return J(r,200,{wp:wpPub(),v:db.wpv||0,files:Object.fromEntries(Object.entries(db.wpf||{}).map(([k,v])=>[k,v.n]))});
   if(b.act==='reset'){for(const f in (db.wpf||{}))wpUnl(wpPath(f));db.wp=null;db.wpf={};db.wpv=Date.now();sv();alog(u,'wallpaper','kembali ke bawaan');return J(r,200,{ok:1,wp:null,v:db.wpv})}
   const old=db.wp||{},nf=Object.assign({},db.wpf||{}),mk=(o,od)=>{if(!o||o.k===''||o.k==null)return null;
    if(o.k==='file'&&o.file)return'bad';
    if(o.k==='file'&&o.fid&&wpPend[o.fid]&&wpPend[o.fid].done){const pd=wpPend[o.fid];nf[o.fid]={n:pd.n,m:pd.mime,tmp:1};return wpSlot({...o,mime:pd.mime})}
    if(o.k==='file'&&!(o.fid&&nf[o.fid]))return'nofile';return wpSlot(o,od)};
   const A=mk(b.a,old.a),B=b.mode==='split'?mk(b.b,old.b):null;
   if(A==='bad'||B==='bad')return ER(r,400,'File harus video MP4/WebM/MOV atau gambar PNG/JPG/WebP/GIF');if(A==='big'||B==='big')return ER(r,413,'File terlalu besar. Pangkas/kompres dulu atau pakai link.');
   if(A==='nofile'||B==='nofile')return ER(r,400,'File tidak ditemukan, unggah ulang');
   if(b.a&&b.a.k==='link'&&!A||b.mode==='split'&&b.b&&b.b.k==='link'&&!B)return ER(r,400,'Link harus diawali https://');
   const keep=new Set([A&&A.fid,B&&B.fid].filter(Boolean));try{for(const k of keep)if(nf[k]&&nf[k].tmp){fs.renameSync(wpTmp(k),wpPath(k));delete nf[k].tmp}}catch(e){console.error('wallpaper pindah file',e.message);return ER(r,500,'Gagal menyimpan file di server')}
   for(const k in nf)if(!keep.has(k)){wpUnl(wpPath(k));delete nf[k]}
   if(!A&&!B){db.wp=null;db.wpf={}}else{db.wpf=nf;db.wp={mode:b.mode==='split'?'split':'same',a:A,b:B}}
   for(const f of [A&&A.fid,B&&B.fid])if(f)delete wpPend[f];for(const f in wpPend)if(Date.now()-wpPend[f].t>18e5){wpUnl(wpTmp(f));delete wpPend[f]}db.wpv=Date.now();sv();fast();alog(u,'wallpaper','video lama dihapus dari server · '+(db.wp?db.wp.mode+' · '+[A,B].filter(Boolean).map(x=>x.k).join('/'):'bawaan'));return J(r,200,{ok:1,wp:wpPub(),v:db.wpv})}
  if(a==='backup'){const us={};for(const id in db.users){const x={...db.users[id]};for(const f of HEAVY)delete x[f];us[id]=x}alog(u,'unduh cadangan','');return J(r,200,{v:1,t:Date.now(),users:us,mail:db.mail,codes:db.codes,used:db.used,vip:db.vip,prices:db.prices||{},pay:db.pay||{},words:db.words||[],next:db.next})}
  if(a==='restore'){const d=b.data;if(!d||d.v!==1||typeof d.users!=='object')return ER(r,400,'File cadangan tidak valid');let add=0,upd=0;
   for(const id in d.users){const bu=d.users[id];if(!bu||!bu.email||!bu.ph||!bu.salt)continue;const cu=db.users[id];
    if(!cu){if(db.mail[bu.email]&&db.mail[bu.email]!==id)continue;db.users[id]={...bu,avatar:'',bg:'',wall:''};db.mail[bu.email]=id;add++;continue}
    bal(cu);const bs=Math.max(0,Math.floor(+bu.sal||0));if(b.force?bs!==cu.sal:((bu.wv||0)>(cu.wv||0)||(!(cu.wv)&&cu.sal===0&&bs>0))){cu.sal=bs;cu.wv=Math.max(cu.wv||0,bu.wv||0)+1;if(Array.isArray(bu.stx)&&bu.stx.length)cu.stx=bu.stx;upd++}
    if(bu.data&&(!cu.data||!Object.keys(cu.data).length))cu.data=bu.data}
   for(const k in (d.codes||{}))if(!db.codes[k])db.codes[k]=d.codes[k];for(const k in (d.used||{}))if(!db.used[k])db.used[k]=d.used[k];
   if(d.prices&&(b.force||!Object.keys(db.prices||{}).length))db.prices=d.prices;if(d.pay&&(b.force||!Object.keys(db.pay||{}).length)){db.pay=d.pay;db.payv=Date.now()}
   if(Array.isArray(d.words)&&!(db.words||[]).length)db.words=d.words;db.next=Math.max(db.next,+d.next||0);fast();sv();alog(u,'pulihkan cadangan',add+' akun baru, '+upd+' saldo');return J(r,200,{ok:1,add,upd})}
  if(a==='prices')return J(r,200,{base:Object.fromEntries(Object.entries(PTAB).map(([k,v])=>[k,{p:v.p,n:v.n,d:v.d}])),pr:ptab(),raw:db.prices||{},pay:db.pay||{}})
  if(a==='txlog')return J(r,200,{list:(db.glog||[]).slice(0,100)})
  if(a==='words'){db.words=db.words||[];const w=String(b.w||'').trim().toLowerCase().slice(0,30);
   if(b.act==='add'&&w.length>=2&&!db.words.includes(w)&&db.words.length<200){db.words.push(w);alog(u,'kata dilarang +',w)}else if(b.act==='del'){db.words=db.words.filter(x=>x!==w);alog(u,'kata dilarang -',w)}sv();return J(r,200,{ok:1,list:db.words})}
  if(a==='maint'){db.maint=b.on?(String(b.msg||'Mohon tunggu sebentar').trim().slice(0,120)):'';sv();alog(u,'pemeliharaan',db.maint||'selesai');return J(r,200,{ok:1,maint:db.maint})}
  if(a==='logout'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Tidak bisa');t.ep=(t.ep||0)+1;clients.forEach(c=>{if(c.u.id===t.id){try{c.sock.destroy()}catch{}}});sv();alog(u,'paksa keluar',t.name+' (ID '+t.id+')');return J(r,200,{ok:1})}
  if(a==='airdrop'){const v=Math.floor(+b.amount);if(!(v>=100)||v>1e7)return ER(r,400,'Nominal 100 - 10.000.000 per akun');let L=Object.values(db.users).filter(x=>!isRoot(x)&&!isBanned(x));if(b.who!=='all')L=L.filter(x=>isOn(x.id));if(L.length>2000)return ER(r,400,'Terlalu banyak penerima');
   if(b.dry)return J(r,200,{n:L.length,total:L.length*v});const note=String(b.note||'Bagi-bagi saldo dari Owner').slice(0,40);L.forEach(x=>credit(x,v,note));sv();alog(u,'bagi saldo',L.length+' akun × '+v);say('🎁 OWNER membagikan saldo '+v.toLocaleString('id-ID')+' ke '+L.length+' akun'+(b.who==='all'?'':' yang sedang online')+'!');return J(r,200,{ok:1,n:L.length,total:L.length*v})}
  if(a==='export'){const us=Object.values(db.users);return J(r,200,{list:us.map(x=>({id:x.id,name:x.name,email:x.email,role:pub(x).role,saldo:bal(x),ban:isBanned(x)?1:0,dibuat:x.cr||0}))})}
  if(a==='reg'){db.regClosed=!b.open;sv();alog(u,'pendaftaran',db.regClosed?'ditutup':'dibuka');return J(r,200,{ok:1,regClosed:!!db.regClosed})}
  if(a==='log')return J(r,200,{list:db.alog||[]})
  if(a==='clear'){db.com=[];sv();cast({t:'clear',day:db.cday,force:1});say('🧹 Chat komunitas direset oleh Owner.');return J(r,200,{ok:1})}
  if(a==='status/del'){if(!t)return ER(r,404,'ID tidak ditemukan');t.stat=null;sv();return J(r,200,{ok:1})}
  return ER(r,404,'Tidak ditemukan')}
 if(c==='profile'&&m==='POST'){
  if(b.name!=null){const n=String(b.name).trim();if(n.length<1||n.length>24)return ER(r,400,'Nama 1-24 karakter');u.name=n}
  for(const[f,l]of[['avatar',4e5],['bg',6e5],['wall',6e5],['status',80],['fx',20]])if(b[f]!=null){if(!ok(b[f],l))return ER(r,413,'Data '+f+' terlalu besar');u[f]=b[f];if(f==='avatar')u.avt=Date.now()}
  sv();return J(r,200,me(u));
 }
 if(c==='password'&&m==='POST'){if(ph(String(b.old||''),u.salt)!==u.ph)return ER(r,403,'Password lama salah');const n=String(b.new||'');if(n.length<6)return ER(r,400,'Password baru minimal 6 karakter');u.salt=cr.randomBytes(16).toString('hex');u.ph=ph(n,u.salt);sv();return J(r,200,{ok:1})}
 if(c==='sync'&&m==='POST'){if(JSON.stringify(b.data||{}).length>6e5)return ER(r,413,'Data sinkron terlalu besar');bal(u);const d={...(b.data||{})};delete d.saldo;delete d.tx;if(!u.pl&&d.plan&&d.plan!=='free'&&RL.includes(d.plan))u.pl={id:d.plan,exp:+d.planExp||0};d.plan=planOf(u);d.planExp=pexpOf(u);u.data=d;sv();return J(r,200,{ok:1})}
 if(c==='cloud'){
  if(m==='POST'){let cl={};try{cl=JSON.parse(u.cls||'{}')}catch(e){}
   for(const[k,l,ty]of[['chats',7e5,'a'],['stk',3e5,'a'],['ai',4e3,'o']]){if(b[k]==null)continue;if(ty==='a'?!Array.isArray(b[k]):(typeof b[k]!=='object'||Array.isArray(b[k])))return ER(r,400,'Format '+k+' salah');if(JSON.stringify(b[k]).length>l)return ER(r,413,'Data '+k+' terlalu besar');cl[k]=b[k]}
   cl.t=Date.now();u.cls=JSON.stringify(cl);sv();return J(r,200,{ok:1,t:cl.t})}
  let cl={};try{cl=JSON.parse(u.cls||'{}')}catch(e){}return J(r,200,cl)}
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
 if(c==='chats')return J(r,200,u.fr.map(i=>db.users[i]).filter(Boolean).map(x=>{const L=db.dm[[u.id,x.id].sort().join('-')]||[],l=L[L.length-1],rd=(u.rd||{})[x.id]||0;const pv=!l?'':l.k==='text'?String(l.b).slice(0,60):l.k==='img'?'📷 Foto':l.k==='vid'?'🎥 Video':l.k==='stk'?'🌟 Stiker':l.k==='aud'?'🎤 Pesan suara':'📎 '+(l.n||'File');return{u:pub(x),p:pv,t:l?l.t:0,me:l?l.u===u.id:false,un:L.filter(m=>m.u===x.id&&m.t>rd).length}}).sort((a,b)=>b.t-a.t))
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
  let L;if(c==='com')L=db.com;else{const o=S[2];if(!db.users[o])return ER(r,404,'ID tidak ditemukan');const tg=db.users[o],ap=isAB(u)&&isRoot(tg),rp2=isRoot(u)&&isAB(tg);
   if(ap&&m==='POST'){const n=Date.now();if(b.k!=='text'||String(b.b||'').length>500)return ER(r,400,'Selama di-ban hanya bisa kirim teks (maks 500 huruf)');u.bm=(u.bm||[]).filter(x=>n-x<36e5);if(u.bm.length>=20)return ER(r,429,'Terlalu banyak pesan, coba lagi nanti');if(u.bm.length&&n-u.bm[u.bm.length-1]<2000)return ER(r,429,'Pelan-pelan, tunggu 2 detik');u.bm.push(n)}
   if((ap||rp2)&&!u.fr.includes(o)){u.fr.push(o);tg.fr.includes(u.id)||tg.fr.push(u.id);sv()}
   if(!u.fr.includes(o))return ER(r,403,'Tambahkan dulu sebagai teman untuk chat pribadi');L=db.dm[[u.id,o].sort().join('-')]||=[]}
  if(m==='POST'){
   if(!valid(b.k,b.b,b.n))return ER(r,400,why(b.k,b.b,b.n));
   if(c==='com'){const e=postCom(u,b.k,b.b,b.n);if(e)return ER(r,403,e)}else{if(b.k==='aud'&&!isAdm(u)&&b.b.length>AUDM)return ER(r,413,'Pesan suara member maksimal 30 detik');const mm={i:Date.now()+Math.random(),t:Date.now(),u:u.id,k:b.k,b:b.b,n:b.n};L.push(mm);capMedia(L);castU([u.id,S[2]],{t:'dm',from:u.id,to:S[2],m:mm,u:{[u.id]:pub(u)}});if(L.length>300)L.splice(0,L.length-300);sv()}
  }
  if(c==='dm')(u.rd||={})[S[2]]=Date.now();
  const s=+P.get('since')||0,ms=L.filter(x=>x.t>s),us={};ms.forEach(x=>us[x.u]=pub(db.users[x.u]||{id:x.u,name:'?',role:'free'}));
  us.bot=BOT;return J(r,200,{m:ms,u:us,ids:L.map(x=>x.i),now:Date.now(),tos:c==='com'&&!s?TOS:undefined,day:c==='com'?db.cday:undefined,...(c==='com'?{slow:db.slow||0,lock:!!db.lock,pin:db.pin||null}:{})});
 }
 ER(r,404,'Tidak ditemukan');
}
