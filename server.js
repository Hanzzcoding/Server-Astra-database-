// ASTRA server: node server.js (Node 18+, tanpa dependency)
// Env: PORT, OWNERS=emailowner1,emailowner2, MAX_USERS, DB
const http=require('http'),fs=require('fs'),cr=require('crypto');
const PORT=process.env.PORT||3000,FILE=process.env.DB||'./astra-db.json',MAXU=+process.env.MAX_USERS||50000;
const OWN=(process.env.OWNERS||'').toLowerCase().split(',').filter(Boolean);
const MK=['img','vid','file','aud'];
let db={users:{},mail:{},tok:{},com:[],dm:{},next:100001};try{db=JSON.parse(fs.readFileSync(FILE))}catch{}
const MX=()=>{db.vip=db.vip||{};db.used=db.used||{};db.codes=db.codes||{};const n=Date.now();for(const t in db.tok)if(!db.tok[t].c)db.tok[t].c=n};MX();
const clean=()=>{const f=a=>(a||[]).filter(m=>!MK.includes(m.k)||m.b);db.com=f(db.com);for(const k in db.dm)db.dm[k]=f(db.dm[k])};clean();
let dirty=0,lastUp=0,loaded=!process.env.UPSTASH_REDIS_REST_URL;const sv=()=>dirty=1,fast=()=>{dirty=1;lastUp=0};
// Penyimpanan permanen (opsional, gratis): isi env UPSTASH_REDIS_REST_URL dan UPSTASH_REDIS_REST_TOKEN
const RU=process.env.UPSTASH_REDIS_REST_URL,RT=process.env.UPSTASH_REDIS_REST_TOKEN;
const rcmd=a=>fetch(RU,{method:'POST',headers:{authorization:'Bearer '+RT,'content-type':'application/json'},body:JSON.stringify(a)}).then(r=>r.json());
const rpipe=a=>fetch(RU+'/pipeline',{method:'POST',headers:{authorization:'Bearer '+RT,'content-type':'application/json'},body:JSON.stringify(a)}).then(r=>r.json());
const rep=function(k,v){return k==='b'&&MK.includes(this.k)?undefined:v};
/* Data dipecah jadi beberapa kunci kecil (foto profil, background, data sinkron tiap pengguna, chat komunitas, tiap chat pribadi)
   supaya tidak melewati batas ukuran satu permintaan Upstash dan tidak ada data yang hilang saat server restart. */
const HEAVY=['avatar','bg','wall'];
function shardsOf(d){const sh={},us={};
 for(const id in d.users){const l={...d.users[id]},hv=[];for(const f of HEAVY)if(l[f]){sh['astra-u:'+id+':'+f]=l[f];hv.push(f);delete l[f]}
  if(l.data&&Object.keys(l.data).length){sh['astra-u:'+id+':data']=JSON.stringify(l.data);hv.push('data')}delete l.data;l._h=hv;us[id]=l}
 sh['astra-com']=JSON.stringify(d.com,rep);const dmk=Object.keys(d.dm).filter(k=>(d.dm[k]||[]).length);for(const k of dmk)sh['astra-dm:'+k]=JSON.stringify(d.dm[k],rep);
 sh['astra-db']=JSON.stringify({...d,users:us,com:[],dm:{},dmk},rep);return sh}
const sent={};let needMig=0,busy=0;
function writeLocal(){const t=JSON.stringify(db,rep);fs.writeFile(FILE+'.tmp',t,e=>!e&&fs.rename(FILE+'.tmp',FILE,()=>{}))}
async function pushUp(){if(!RU||!loaded)return true;const sh=shardsOf(db);let ok=true;
 {const w={};for(const id in db.users){const u=db.users[id];if(u.sal!==undefined)w[id]=[u.sal,u.wv||0,(u.stx||[]).slice(0,30)]}const wj=JSON.stringify(w);
  if(sent['astra-wal']!==wj){try{const r=await rcmd(['SET','astra-wal',wj]);if(r.error)throw new Error(r.error);sent['astra-wal']=wj}catch(e){ok=false;console.error('Upstash gagal astra-wal',e.message)}}}
 if(!needMig){const m0=sh['astra-db'];if(sent['astra-db']!==m0){try{const r=await rcmd(['SET','astra-db',m0]);if(r.error)throw new Error(r.error);sent['astra-db']=m0}catch(e){ok=false;console.error('Upstash gagal astra-db',e.message)}}}
 for(const k of Object.keys(sh)){if(k==='astra-db'||sent[k]===sh[k])continue;try{const r=await rcmd(['SET',k,sh[k]]);if(r.error)throw new Error(r.error);sent[k]=sh[k]}catch(e){ok=false;console.error('Upstash gagal',k,e.message)}}
 for(const k of Object.keys(sent))if(k!=='astra-wal'&&!(k in sh)){try{await rcmd(['DEL',k]);delete sent[k]}catch(e){}}
 if(needMig&&!ok)return false;
 const m=sh['astra-db'];if(sent['astra-db']!==m){try{const r=await rcmd(['SET','astra-db',m]);if(r.error)throw new Error(r.error);sent['astra-db']=m}catch(e){ok=false;console.error('Upstash gagal astra-db',e.message)}}
 if(ok)needMig=0;return ok}
function reconcile(w,src){let n=0;for(const id in w){const u=db.users[id],x=w[id];if(!u||!x)continue;bal(u);if((x[1]||0)>(u.wv||0)){u.sal=Math.max(0,Math.floor(+x[0]||0));u.wv=x[1];if(Array.isArray(x[2])&&x[2].length)u.stx=x[2];n++}}if(n)console.log('Dompet dipulihkan dari '+src+': '+n+' akun');return n}
async function loadUp(){const r=await rcmd(['GET','astra-db']);if(r.error||(r.result===undefined))throw new Error(r.error||'respons Upstash tidak valid');if(!r.result)return;const m=JSON.parse(r.result);sent['astra-db']=r.result;
 const legacy=Array.isArray(m.com)&&m.com.length>0||Object.values(m.users||{}).some(u=>u&&(u.avatar||u.bg||u.wall||(u.data&&Object.keys(u.data).length)))||!m.dmk;
 if(legacy){db=m;needMig=1;MX();clean();try{const rw=await rcmd(['GET','astra-wal']);if(rw.result)reconcile(JSON.parse(rw.result),'Upstash (cadangan)')}catch(e){}return}
 const keys=['astra-com'];for(const id in m.users)for(const f of m.users[id]._h||[])keys.push('astra-u:'+id+':'+f);for(const k of m.dmk||[])keys.push('astra-dm:'+k);
 const vals={};for(let i=0;i<keys.length;i+=20){const ch=keys.slice(i,i+20),rr=await rpipe(ch.map(k=>['GET',k]));if(!Array.isArray(rr)||rr.some(x=>x&&x.error))throw new Error('pipeline Upstash gagal');ch.forEach((k,j)=>{if(rr[j]&&rr[j].result!=null){vals[k]=rr[j].result;sent[k]=rr[j].result}})}
 m.com=vals['astra-com']?JSON.parse(vals['astra-com']):[];m.dm={};for(const k of m.dmk||[])m.dm[k]=vals['astra-dm:'+k]?JSON.parse(vals['astra-dm:'+k]):[];
 for(const id in m.users){const u=m.users[id];for(const f of u._h||[]){const v=vals['astra-u:'+id+':'+f];if(v==null)continue;u[f]=f==='data'?JSON.parse(v):v}delete u._h;if(!u.data)u.data={}}
 delete m.dmk;db=m;MX();clean();try{const rw=await rcmd(['GET','astra-wal']);if(rw.result){sent['astra-wal']=rw.result;reconcile(JSON.parse(rw.result),'Upstash (cadangan)')}}catch(e){}}
setInterval(async()=>{if(!dirty||busy)return;busy=1;try{writeLocal();if(RU&&Date.now()-lastUp>6000){dirty=0;lastUp=Date.now();if(!await pushUp())dirty=1}else if(!RU)dirty=0}catch(e){console.error(e.message)}busy=0},2000);
/* token login: buang yang sudah lama dan batasi per akun supaya data tidak membengkak */
function pruneTok(){const n=Date.now(),per={};const ks=Object.keys(db.tok).sort((a,b)=>(db.tok[b].c||0)-(db.tok[a].c||0));let ch=0;for(const t of ks){const k=db.tok[t];per[k.id]=(per[k.id]||0)+1;if(n-(k.c||0)>60*864e5||per[k.id]>8||!db.users[k.id]){delete db.tok[t];ch=1}}if(ch)sv()}
setInterval(pruneTok,36e5);
const ph=(p,s)=>cr.scryptSync(p,s,32).toString('hex');
const J=(r,c,o)=>{r.writeHead(c,{'content-type':'application/json','access-control-allow-origin':'*','access-control-allow-headers':'content-type,authorization','access-control-allow-methods':'GET,POST,OPTIONS'});r.end(JSON.stringify(o))};
const ER=(r,c,e)=>J(r,c,{e});
const flush=()=>{try{fs.writeFileSync(FILE,JSON.stringify(db,rep))}catch{}return RU?Promise.race([pushUp(),new Promise(r=>setTimeout(r,20000))]).catch(()=>{}):Promise.resolve()};
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
const addTx=(u,amount,note,ref)=>{bal(u);u.wv=(u.wv||0)+1;fast();(db.glog=db.glog||[]).unshift({t:Date.now(),u:u.name,id:u.id,a:amount,n:String(note||'').slice(0,70)});if(db.glog.length>150)db.glog.length=150;u.stx=[{t:Date.now(),amount,note,ref},...(u.stx||[])].slice(0,100)};
const credit=(u,a,note,ref)=>{u.sal=bal(u)+a;addTx(u,a,note,ref)};
const dat=u=>({...(u.data||{}),saldo:bal(u),tx:u.stx||[]});
const nc=s=>String(s||'').toUpperCase().replace(/[^A-Z0-9]/g,''),fmt=s=>(nc(s).match(/.{1,4}/g)||[]).join('-');
const seen={};
const isOn=id=>seen[id]&&Date.now()-seen[id]<120000||[...clients].some(c=>c.u.id===id);

const pub=u=>({id:u.id,name:u.name,avatar:u.avatar,status:u.status,role:isRoot(u)?'root':isAdm(u)?'owner':RL.includes((u.data||{}).plan)?u.data.plan:'free',av:u.avatar?(u.avt||1):0});
const me=u=>({...pub(u),adm:isRoot(u)?'root':u.adm||'',email:u.email,bg:u.bg,wall:u.wall,fx:u.fx,sal:bal(u),data:dat(u)});
const srv=http.createServer((q,r)=>{
 const ip=String(q.headers['x-forwarded-for']||'').split(',')[0].trim()||q.socket.remoteAddress;hits[ip]=(hits[ip]||0)+1;
 if(q.method==='OPTIONS')return J(r,204,{});
 if(hits[ip]>600)return ER(r,429,'Terlalu banyak permintaan, tunggu sebentar');
 let b='',big=0;q.on('data',d=>{if(big)return;b+=d;if(b.length>4e6){big=1;b='';ER(r,413,'Ukuran file terlalu besar untuk dikirim')}});
 q.on('end',()=>{if(big)return;try{h(q,r,b?JSON.parse(b):{})}catch(e){console.error(e);ER(r,500,'Kesalahan di server')}})
});(async()=>{
 let localDb=null;try{localDb=JSON.parse(fs.readFileSync(FILE,'utf8'))}catch{}
 if(RU){let ok=0;for(let i=0;i<6&&!ok;i++){try{await loadUp();ok=1;console.log('Data dimuat dari Upstash')}catch(e){console.error('Gagal memuat Upstash (percobaan '+(i+1)+')',e.message);await new Promise(r=>setTimeout(r,2500*(i+1)))}}
  if(!ok){console.error('Upstash tidak bisa dimuat. Server berhenti agar data cloud tidak tertimpa data lama; Render akan menyalakan ulang otomatis.');process.exit(1)}loaded=true}
 if(localDb&&localDb.users){const w={};for(const id in localDb.users){const u=localDb.users[id];if(u&&u.sal!==undefined)w[id]=[u.sal,u.wv||0,u.stx]}reconcile(w,'file lokal');sv()}
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
const peff=id=>{const o=(db.prices||{})[id],b=PTAB[id];return o&&o.p>=0&&(!o.until||o.until>Date.now())?o.p:b.p};
const ptab=()=>{const r={};for(const id in PTAB){const o=(db.prices||{})[id];if(o&&o.p>=0&&(!o.until||o.until>Date.now()))r[id]={p:o.p,was:PTAB[id].p,note:o.note||'',until:o.until||0}}return r};
const alog=(u,a,d)=>{db.alog=db.alog||[];db.alog.unshift({t:Date.now(),by:u.name,a,d:String(d||'').slice(0,140)});if(db.alog.length>150)db.alog.length=150;sv()};
/* verifikasi langkah ke-2: password akun dicek ulang di server */
const vpw=(u,vp)=>{const n=Date.now();if(u.vl&&u.vl>n)return'Verifikasi dikunci. Coba lagi '+Math.ceil((u.vl-n)/6e4)+' menit lagi';if(typeof vp!=='string'||!vp||ph(vp,u.salt)!==u.ph){u.vf=(u.vf||0)+1;if(u.vf>=5){u.vf=0;u.vl=n+10*6e4;return'Password salah 5 kali. Verifikasi dikunci 10 menit'}return'Password salah ('+u.vf+'/5)'}u.vf=0;return null};
const isBanned=u=>{if(!u.ban)return false;if(u.bu&&u.bu<Date.now()){u.ban=0;u.bu=0;u.st=0;sv();return false}return true};
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
 {const v=adm?null:k==='text'?mod(b):(nm&&BAD.test(nm)?['ban','Nama file berisi konten judi/ilegal']:null);if(v){if(v[0]==='ban')u.ban=1;else if((u.st=(u.st||0)+1)>=3)u.ban=1;
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
  else if(op===2&&c.r==='live'&&c.host)hostBin(pl)}}catch(e){console.error('ws',e.message)}});
 const gone=()=>{if(clients.delete(c)&&c.r==='live')liveLeft(c)};sock.on('close',gone);sock.on('error',gone)});
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
 const hp=sha('astra-v1|'+up),pid=CODES[hp];if(pid){if(db.used[hp])return[409,{e:'Kode ini sudah dipakai.'}];db.used[hp]=u.id;return done({kind:'plan',plan:pid,days:null})}
 const c=db.codes[nc(code)];if(!c)return[400,{e:'Kode tidak valid.'}];
 if(c.used)return[409,{e:'Kode ini sudah dipakai dan tidak bisa dipakai lagi.'}];
 if(c.exp&&c.exp<n)return[410,{e:'Kode ini sudah kedaluwarsa.'}];
 c.used={u:u.id,t:n};
 if(c.t==='saldo'){credit(u,c.a,'Redeem kode saldo');return done({kind:'saldo',amount:c.a})}
 return done({kind:'plan',plan:c.plan,days:c.days})}
function h(q,r,b){
 const[p,qs]=q.url.split('?'),P=new URLSearchParams(qs||''),S=p.split('/').filter(Boolean),c=S[1],m=q.method;
 if(S[0]!=='api')return ER(r,404,'Tidak ditemukan');
 if(c==='ping')return J(r,200,{ok:1,v:11});
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
  const t=cr.randomBytes(24).toString('hex');db.tok[t]={id:u.id,ep:u.ep,c:Date.now()};sv();return J(r,200,{token:t,user:me(u),data:dat(u)});
 }
 if(c==='avatar'&&m==='GET'){const x=db.users[S[2]],mm=x&&x.avatar&&/^data:(image\/[\w.+-]+);base64,(.+)$/.exec(x.avatar);if(!mm)return ER(r,404,'Tidak ada foto');r.writeHead(200,{'content-type':mm[1],'cache-control':'public,max-age=86400','access-control-allow-origin':'*'});return r.end(Buffer.from(mm[2],'base64'))}
 const k=db.tok[(q.headers.authorization||'').slice(7)],u=k&&db.users[k.id];
 if(!u||k.ep!==u.ep)return ER(r,401,'Sesi habis, silakan masuk lagi');
  seen[u.id]=Date.now();
 if(db.maint&&!isAdm(u)&&c!=='hb')return ER(r,503,'Server sedang pemeliharaan: '+db.maint);
 if(c==='hb')return J(r,200,{ok:1,maint:db.maint||'',pr:ptab(),bal:bal(u),tx:u.stx||[],live:lpub()||null,gift:u.gift||null,ban:isBanned(u)?(u.bu||1):0});
 if(c==='gift'&&S[2]==='ack'&&m==='POST'){u.gift=null;sv();return J(r,200,{ok:1})}
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
   say('🔴 '+u.name+' sedang LIVE: "'+title+'". Ketuk tombol LIVE untuk menonton!');alog(u,'live mulai',title);
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
  if(S[2]==='buyplan'&&m==='POST'){const id=String(b.plan),pl=PTAB[id];if(!pl)return ER(r,400,'Paket tidak valid');const price=peff(id);
   if(bal(u)<price)return ER(r,400,'Saldo tidak cukup');const ref='PL'+Date.now().toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();
   u.sal=bal(u)-price;addTx(u,-price,'Beli '+pl.n,ref);sv();return J(r,200,{ok:1,ref,bal:u.sal,tx:u.stx,days:pl.d,price,name:pl.n})}
  if(S[2]==='giftrole'&&m==='POST'){
   const n=Date.now();u.tr=(u.tr||[]).filter(t=>n-t<6e4);if(u.tr.length>=5)return ER(r,429,'Terlalu sering, tunggu sebentar');
   {const ve=vpw(u,b.vp);if(ve)return ER(r,403,ve)}
   const t=db.users[String(b.to)],pl0=PTAB[String(b.plan)];if(!t||t.id===u.id)return ER(r,404,'Penerima tidak ditemukan');if(!pl0)return ER(r,400,'Paket tidak valid');if(isAdm(t))return ER(r,400,'Akun Owner tidak bisa dihadiahi role');const pl={...pl0,p:peff(String(b.plan))};
   if(bal(u)<pl.p)return ER(r,400,'Saldo tidak cukup');
   const ref='RL'+n.toString(36).toUpperCase()+cr.randomBytes(2).toString('hex').toUpperCase();u.tr.push(n);
   u.sal=bal(u)-pl.p;addTx(u,-pl.p,'Hadiah role '+pl.n+' untuk '+t.name+' (ID '+t.id+')',ref);t.gift={plan:String(b.plan),days:pl.d,from:u.name,t:n};sv();
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
   t.ban=b.on?1:0;t.bu=b.on&&dur?Date.now()+dur*6e4:0;if(!b.on)t.st=0;sv();
   alog(u,b.on?'ban':'buka ban',t.name+' (ID '+t.id+')'+(b.on?(dur?' · '+dur+' menit':' · permanen')+(why?' · '+why:''):''));
   say(b.on?'🛡️ '+t.name+' di-BAN dari komunitas oleh Owner'+(dur?' selama '+(dur>=1440?Math.round(dur/1440)+' hari':dur>=60?Math.round(dur/60)+' jam':dur+' menit'):' (permanen)')+(why?'. Alasan: '+why:'.'):'✅ Ban '+t.name+' dibuka oleh Owner.');
   return J(r,200,{ok:1,ban:!!t.ban})}
  if(a==='mute'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa di-mute');const dur=Math.max(0,Math.floor(+b.dur)||0);t.mute=dur?Date.now()+dur*6e4:0;sv();alog(u,dur?'mute':'buka mute',t.name+' (ID '+t.id+')'+(dur?' · '+dur+' menit':''));return J(r,200,{ok:1,mute:t.mute})}
  if(a==='saldo'){if(!t)return ER(r,404,'ID tidak ditemukan');const v=Math.trunc(+b.amount);if(!v||Math.abs(v)>1e9)return ER(r,400,'Nominal tidak valid');const d=v<0?-Math.min(-v,bal(t)):v;if(!d)return ER(r,400,'Saldo pengguna sudah 0');
   if(d>0)credit(t,d,String(b.note||'Penambahan saldo oleh Owner').slice(0,50));else{t.sal=bal(t)+d;addTx(t,d,String(b.note||'Pengurangan saldo oleh Owner').slice(0,50))}sv();alog(u,'saldo',t.name+' (ID '+t.id+') '+(d>0?'+':'')+d);return J(r,200,{ok:1,bal:t.sal})}
  if(a==='plan'){if(!t)return ER(r,404,'ID tidak ditemukan');const p=String(b.plan||'');if(p!=='free'&&!RL.includes(p))return ER(r,400,'Paket tidak valid');const days=Math.max(0,Math.min(3650,Math.floor(+b.days)||0));t.gift={plan:p,days,from:'Owner',t:Date.now(),rev:p==='free'};sv();alog(u,p==='free'?'cabut premium':'beri premium',t.name+' (ID '+t.id+') '+p+(days?' '+days+' hari':' unlimited'));return J(r,200,{ok:1})}
  if(a==='user/del'){if(!t)return ER(r,404,'ID tidak ditemukan');if(isRoot(t))return ER(r,403,'Akun Owner tidak bisa dihapus');alog(u,'hapus akun',t.name+' (ID '+t.id+')');delete db.users[t.id];delete db.mail[t.email];clients.forEach(c=>{if(c.u.id===t.id){try{c.sock.destroy()}catch{}}});sv();return J(r,200,{ok:1})}
  if(a==='users'){const qq=String(b.q||'').trim().toLowerCase(),off=Math.max(0,+b.off||0),lim=Math.min(40,+b.lim||25),n=Date.now();
   let L=Object.values(db.users);if(qq)L=L.filter(x=>x.id===qq||(x.name||'').toLowerCase().includes(qq)||(x.email||'').includes(qq));
   if(b.f==='ban')L=L.filter(x=>isBanned(x));else if(b.f==='on')L=L.filter(x=>isOn(x.id));else if(b.f==='adm')L=L.filter(x=>isAdm(x));
   if(b.sort==='sal')L.sort((x,y)=>bal(y)-bal(x));else L.sort((x,y)=>(isOn(y.id)-isOn(x.id))||((y.cr||0)-(x.cr||0)));
   return J(r,200,{total:L.length,list:L.slice(off,off+lim).map(x=>({id:x.id,name:x.name,email:x.email,role:pub(x).role,adm:x.adm||'',plan:(x.data||{}).plan||'free',pexp:(x.data||{}).planExp||0,sal:bal(x),ban:isBanned(x),bu:x.bu||0,mute:x.mute>n?x.mute:0,on:isOn(x.id),seen:seen[x.id]||0,cr:x.cr||0,av:pub(x).av}))})}
  if(a==='stats'){const us=Object.values(db.users),n=Date.now(),cs=Object.values(db.codes);
   return J(r,200,{users:us.length,online:us.filter(x=>isOn(x.id)).length,banned:us.filter(x=>isBanned(x)).length,muted:us.filter(x=>x.mute>n).length,admins:us.filter(x=>isAdm(x)).length,saldo:us.reduce((a,x)=>a+bal(x),0),com:db.com.length,codes:{free:cs.filter(c=>!c.used&&!(c.exp&&c.exp<n)).length,used:cs.filter(c=>c.used).length},live:lpub()||null,lastLive:db.lastLive||null,slow:db.slow||0,lock:!!db.lock,pin:db.pin||null,maint:db.maint||'',nomedia:!!db.nomedia,novoice:!!db.novoice,regClosed:!!db.regClosed,words:(db.words||[]).length,up:Math.round(process.uptime()),mem:Math.round(process.memoryUsage().rss/1e6),gifts:GIFTS.length})}
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
  if(a==='prices')return J(r,200,{base:Object.fromEntries(Object.entries(PTAB).map(([k,v])=>[k,{p:v.p,n:v.n,d:v.d}])),pr:ptab(),raw:db.prices||{}})
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
 if(c==='sync'&&m==='POST'){if(JSON.stringify(b.data||{}).length>6e5)return ER(r,413,'Data sinkron terlalu besar');bal(u);const d={...(b.data||{})};delete d.saldo;delete d.tx;u.data=d;sv();return J(r,200,{ok:1})}
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
  let L;if(c==='com')L=db.com;else{const o=S[2];if(!db.users[o])return ER(r,404,'ID tidak ditemukan');if(!u.fr.includes(o))return ER(r,403,'Tambahkan dulu sebagai teman untuk chat pribadi');L=db.dm[[u.id,o].sort().join('-')]||=[]}
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
