import{_ as u,r as e,o as i,c as d,a as s,b as n,d as a,w as t,e as k}from"./app-CqYFEabH.js";const m="/PolarDB-for-PostgreSQL/assets/essd-storage-grow-QKfeQwnZ.png",b="/PolarDB-for-PostgreSQL/assets/essd-storage-online-grow-6Cak6wu9.png",g="/PolarDB-for-PostgreSQL/assets/essd-storage-grow-complete-CDUTckAw.png",h={},_={id:"共享存储在线扩容",tabindex:"-1"},S={class:"header-anchor",href:"#共享存储在线扩容"},f={href:"https://developer.aliyun.com/live/250669"},v=s("p",null,"在使用数据库时，随着数据量的逐渐增大，不可避免需要对数据库所使用的存储空间进行扩容。由于 PolarDB for PostgreSQL 基于共享存储与分布式文件系统 PFS 的架构设计，与安装部署时类似，在扩容时，需要在以下三个层面分别进行操作：",-1),P={class:"table-of-contents"},E=s("p",null,"本文将指导您分别在以上三个层面上分别完成扩容操作，以实现不停止数据库实例的动态扩容。",-1),w=s("h2",{id:"块存储层扩容",tabindex:"-1"},[s("a",{class:"header-anchor",href:"#块存储层扩容"},[s("span",null,"块存储层扩容")])],-1),x=s("code",null,"lsblk",-1),B=k(`<p>另外，为保证后续扩容步骤的成功，请以 10GB 为单位进行扩容。</p><p>本示例中，在扩容之前，已有一个 20GB 的 ESSD 云盘多重挂载在两台 ECS 上。在这两台 ECS 上运行 <code>lsblk</code>，可以看到 ESSD 云盘共享存储对应的块设备 <code>nvme1n1</code> 目前的物理空间为 20GB。</p><div class="language-bash" data-ext="sh" data-title="sh"><pre class="language-bash"><code>$ lsblk
NAME        MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
nvme0n1     <span class="token number">259</span>:0    <span class="token number">0</span>  40G  <span class="token number">0</span> disk
└─nvme0n1p1 <span class="token number">259</span>:1    <span class="token number">0</span>  40G  <span class="token number">0</span> part /etc/hosts
nvme1n1     <span class="token number">259</span>:2    <span class="token number">0</span>  20G  <span class="token number">0</span> disk
</code></pre></div><p>接下来对这块 ESSD 云盘进行扩容。在阿里云 ESSD 云盘的管理页面上，点击 <strong>云盘扩容</strong>：</p><p><img src="`+m+'" alt="essd-storage-grow"></p><p>进入到云盘扩容界面以后，可以看到该云盘已被两台 ECS 实例多重挂载。填写扩容后的容量，然后点击确认扩容，把 20GB 的云盘扩容为 40GB：</p><p><img src="'+b+'" alt="essd-storage-online-grow"></p><p>扩容成功后，将会看到如下提示：</p><p><img src="'+g+`" alt="essd-storage-grow-complete"></p><p>此时，两台 ECS 上运行 <code>lsblk</code>，可以看到 ESSD 对应块设备 <code>nvme1n1</code> 的物理空间已经变为 40GB：</p><div class="language-bash" data-ext="sh" data-title="sh"><pre class="language-bash"><code>$ lsblk
NAME        MAJ:MIN RM SIZE RO TYPE MOUNTPOINT
nvme0n1     <span class="token number">259</span>:0    <span class="token number">0</span>  40G  <span class="token number">0</span> disk
└─nvme0n1p1 <span class="token number">259</span>:1    <span class="token number">0</span>  40G  <span class="token number">0</span> part /etc/hosts
nvme1n1     <span class="token number">259</span>:2    <span class="token number">0</span>  40G  <span class="token number">0</span> disk
</code></pre></div><p>至此，块存储层面的扩容就完成了。</p><h2 id="文件系统层扩容" tabindex="-1"><a class="header-anchor" href="#文件系统层扩容"><span>文件系统层扩容</span></a></h2><p>在物理块设备完成扩容以后，接下来需要使用 PFS 文件系统提供的工具，对块设备上扩大后的物理空间进行格式化，以完成文件系统层面的扩容。</p><p>在能够访问共享存储的 <strong>任意一台主机上</strong> 运行 PFS 的 <code>growfs</code> 命令，其中：</p><ul><li><code>-o</code> 表示共享存储扩容前的空间（以 10GB 为单位）</li><li><code>-n</code> 表示共享存储扩容后的空间（以 10GB 为单位）</li></ul><p>本例将共享存储从 20GB 扩容至 40GB，所以参数分别填写 <code>2</code> 和 <code>4</code>：</p><div class="language-bash" data-ext="sh" data-title="sh"><pre class="language-bash"><code>$ <span class="token function">sudo</span> pfs <span class="token parameter variable">-C</span> disk growfs <span class="token parameter variable">-o</span> <span class="token number">2</span> <span class="token parameter variable">-n</span> <span class="token number">4</span> nvme1n1

<span class="token punctuation">..</span>.

Init chunk <span class="token number">2</span>
                metaset        <span class="token number">2</span>/1: sectbda      0x500001000, npage       <span class="token number">80</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2560</span>, oid range <span class="token punctuation">[</span>    <span class="token number">2000</span>,     2a00<span class="token punctuation">)</span>
                metaset        <span class="token number">2</span>/2: sectbda      0x500051000, npage       <span class="token number">64</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2048</span>, oid range <span class="token punctuation">[</span>    <span class="token number">1000</span>,     <span class="token number">1800</span><span class="token punctuation">)</span>
                metaset        <span class="token number">2</span>/3: sectbda      0x500091000, npage       <span class="token number">64</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2048</span>, oid range <span class="token punctuation">[</span>    <span class="token number">1000</span>,     <span class="token number">1800</span><span class="token punctuation">)</span>

Init chunk <span class="token number">3</span>
                metaset        <span class="token number">3</span>/1: sectbda      0x780001000, npage       <span class="token number">80</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2560</span>, oid range <span class="token punctuation">[</span>    <span class="token number">3000</span>,     3a00<span class="token punctuation">)</span>
                metaset        <span class="token number">3</span>/2: sectbda      0x780051000, npage       <span class="token number">64</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2048</span>, oid range <span class="token punctuation">[</span>    <span class="token number">1800</span>,     <span class="token number">2000</span><span class="token punctuation">)</span>
                metaset        <span class="token number">3</span>/3: sectbda      0x780091000, npage       <span class="token number">64</span>, objsize  <span class="token number">128</span>, nobj <span class="token number">2048</span>, oid range <span class="token punctuation">[</span>    <span class="token number">1800</span>,     <span class="token number">2000</span><span class="token punctuation">)</span>

pfs growfs succeeds<span class="token operator">!</span>
</code></pre></div><p>如果看到上述输出，说明文件系统层面的扩容已经完成。</p><h2 id="数据库实例层扩容" tabindex="-1"><a class="header-anchor" href="#数据库实例层扩容"><span>数据库实例层扩容</span></a></h2><p>最后，在数据库实例层，扩容需要做的工作是执行 SQL 函数来通知每个实例上已经挂载到共享存储的 PFSD (PolarFS Daemon) 守护进程，告知共享存储上的新空间已经可以被使用了。需要注意的是，数据库实例集群中的 <strong>所有</strong> PFSD 都需要被通知到，并且需要 <strong>先通知所有 Replica 节点上的 PFSD，最后通知 Primary 节点上的 PFSD</strong>。这意味着我们需要在 <strong>每一个</strong> PolarDB-PG 节点上执行一次通知 PFSD 的 SQL 函数，并且 <strong>Replica 节点在先，Primary 节点在后</strong>。</p><p>数据库实例层通知 PFSD 的扩容函数实现在 PolarDB-PG 的 <code>polar_vfs</code> 插件中，所以首先需要在 <strong>Primary 节点</strong> 上创建 <code>polar_vfs</code> 插件。在创建插件的过程中，会在 Primary 节点和所有 Replica 节点上注册好 <code>polar_vfs_disk_expansion</code> 这个 SQL 函数。</p><div class="language-sql" data-ext="sql" data-title="sql"><pre class="language-sql"><code><span class="token keyword">CREATE</span> EXTENSION <span class="token keyword">IF</span> <span class="token operator">NOT</span> <span class="token keyword">EXISTS</span> polar_vfs<span class="token punctuation">;</span>
</code></pre></div><p>接下来，依次在所有的 Replica 节点上，最后到 Primary 节点上分别执行这个 SQL 函数。其中函数的参数名为块设备名：</p><div class="language-sql" data-ext="sql" data-title="sql"><pre class="language-sql"><code><span class="token keyword">SELECT</span> polar_vfs_disk_expansion<span class="token punctuation">(</span><span class="token string">&#39;nvme1n1&#39;</span><span class="token punctuation">)</span><span class="token punctuation">;</span>
</code></pre></div><p>执行完毕后，数据库实例层面的扩容也就完成了。此时，新的存储空间已经能够被数据库使用了。</p>`,26);function D(p,G){const l=e("Badge"),c=e("ArticleInfo"),o=e("router-link"),r=e("RouteLink");return i(),d("div",null,[s("h1",_,[s("a",S,[s("span",null,[n("共享存储在线扩容 "),s("a",f,[a(l,{type:"tip",text:"视频",vertical:"top"})])])])]),a(c,{frontmatter:p.$frontmatter},null,8,["frontmatter"]),v,s("nav",P,[s("ul",null,[s("li",null,[a(o,{to:"#块存储层扩容"},{default:t(()=>[n("块存储层扩容")]),_:1})]),s("li",null,[a(o,{to:"#文件系统层扩容"},{default:t(()=>[n("文件系统层扩容")]),_:1})]),s("li",null,[a(o,{to:"#数据库实例层扩容"},{default:t(()=>[n("数据库实例层扩容")]),_:1})])])]),E,w,s("p",null,[n("首先需要进行的是块存储层面上的扩容。不管使用哪种类型的共享存储，存储层面扩容最终需要达成的目的是：在能够访问共享存储的主机上运行 "),x,n(" 命令，显示存储块设备的物理空间变大。由于不同类型的共享存储有不同的扩容方式，本文以 "),a(r,{to:"/deploying/storage-aliyun-essd.html"},{default:t(()=>[n("阿里云 ECS + ESSD 云盘共享存储")]),_:1}),n(" 为例演示如何进行存储层面的扩容。")]),B])}const T=u(h,[["render",D],["__file","grow-storage.html.vue"]]),y=JSON.parse('{"path":"/operation/grow-storage.html","title":"共享存储在线扩容","lang":"en-US","frontmatter":{"author":"棠羽","date":"2022/10/12","minute":15},"headers":[{"level":2,"title":"块存储层扩容","slug":"块存储层扩容","link":"#块存储层扩容","children":[]},{"level":2,"title":"文件系统层扩容","slug":"文件系统层扩容","link":"#文件系统层扩容","children":[]},{"level":2,"title":"数据库实例层扩容","slug":"数据库实例层扩容","link":"#数据库实例层扩容","children":[]}],"git":{"updatedTime":1729232520000},"filePathRelative":"operation/grow-storage.md"}');export{T as comp,y as data};