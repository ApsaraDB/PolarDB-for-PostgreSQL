import{_ as r,r as l,o as i,c as u,d as n,a,w as e,b as s,e as d}from"./app-e5d03054.js";const k={},v=a("h1",{id:"基于-pfs-文件系统部署",tabindex:"-1"},[a("a",{class:"header-anchor",href:"#基于-pfs-文件系统部署","aria-hidden":"true"},"#"),s(" 基于 PFS 文件系统部署")],-1),m=a("p",null,"本文将指导您在分布式文件系统 PolarDB File System（PFS）上编译部署 PolarDB，适用于已经在共享存储上格式化并挂载 PFS 文件系统的计算节点。",-1),b={class:"table-of-contents"},_=d(`<h2 id="读写节点部署" tabindex="-1"><a class="header-anchor" href="#读写节点部署" aria-hidden="true">#</a> 读写节点部署</h2><p>初始化读写节点的本地数据目录 <code>~/primary/</code>：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/initdb <span class="token parameter variable">-D</span> <span class="token environment constant">$HOME</span>/primary
</code></pre></div><p>在共享存储的 <code>/nvme1n1/shared_data/</code> 路径上创建共享数据目录，然后使用 <code>polar-initdb.sh</code> 脚本初始化共享数据目录：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token comment"># 使用 pfs 创建共享数据目录</span>
<span class="token function">sudo</span> pfs <span class="token parameter variable">-C</span> disk <span class="token function">mkdir</span> /nvme1n1/shared_data
<span class="token comment"># 初始化 db 的本地和共享数据目录</span>
<span class="token function">sudo</span> <span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/polar-initdb.sh <span class="token punctuation">\\</span>
    <span class="token environment constant">$HOME</span>/primary/ /nvme1n1/shared_data/
</code></pre></div><p>编辑读写节点的配置。打开 <code>~/primary/postgresql.conf</code>，增加配置项：</p><div class="language-ini line-numbers-mode" data-ext="ini"><pre class="language-ini"><code><span class="token key attr-name">port</span><span class="token punctuation">=</span><span class="token value attr-value">5432</span>
<span class="token key attr-name">polar_hostid</span><span class="token punctuation">=</span><span class="token value attr-value">1</span>
<span class="token key attr-name">polar_enable_shared_storage_mode</span><span class="token punctuation">=</span><span class="token value attr-value">on</span>
<span class="token key attr-name">polar_disk_name</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">nvme1n1</span>&#39;</span>
<span class="token key attr-name">polar_datadir</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">/nvme1n1/shared_data/</span>&#39;</span>
<span class="token key attr-name">polar_vfs.localfs_mode</span><span class="token punctuation">=</span><span class="token value attr-value">off</span>
<span class="token key attr-name">shared_preload_libraries</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">$libdir/polar_vfs,$libdir/polar_worker</span>&#39;</span>
<span class="token key attr-name">polar_storage_cluster_name</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">disk</span>&#39;</span>
<span class="token key attr-name">logging_collector</span><span class="token punctuation">=</span><span class="token value attr-value">on</span>
<span class="token key attr-name">log_line_prefix</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">%p\\t%r\\t%u\\t%m\\t</span>&#39;</span>
<span class="token key attr-name">log_directory</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">pg_log</span>&#39;</span>
<span class="token key attr-name">listen_addresses</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">*</span>&#39;</span>
<span class="token key attr-name">max_connections</span><span class="token punctuation">=</span><span class="token value attr-value">1000</span>
<span class="token key attr-name">synchronous_standby_names</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">replica1</span>&#39;</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>编辑读写节点的客户端认证文件 <code>~/primary/pg_hba.conf</code>，增加以下配置项，允许只读节点进行物理复制：</p><div class="language-ini" data-ext="ini"><pre class="language-ini"><code>host	replication	postgres	0.0.0.0/0	trust
</code></pre></div><p>最后，启动读写节点：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start <span class="token parameter variable">-D</span> <span class="token environment constant">$HOME</span>/primary
</code></pre></div><p>检查读写节点能否正常运行：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/psql <span class="token punctuation">\\</span>
    <span class="token parameter variable">-p</span> <span class="token number">5432</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-d</span> postgres <span class="token punctuation">\\</span>
    <span class="token parameter variable">-c</span> <span class="token string">&#39;SELECT version();&#39;</span>
            version
--------------------------------
 PostgreSQL <span class="token number">11.9</span> <span class="token punctuation">(</span>POLARDB <span class="token number">11.9</span><span class="token punctuation">)</span>
<span class="token punctuation">(</span><span class="token number">1</span> row<span class="token punctuation">)</span>
</code></pre></div><p>在读写节点上，为对应的只读节点创建相应的复制槽，用于只读节点的物理复制：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/psql <span class="token punctuation">\\</span>
    <span class="token parameter variable">-p</span> <span class="token number">5432</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-d</span> postgres <span class="token punctuation">\\</span>
    <span class="token parameter variable">-c</span> <span class="token string">&quot;SELECT pg_create_physical_replication_slot(&#39;replica1&#39;);&quot;</span>
 pg_create_physical_replication_slot
-------------------------------------
 <span class="token punctuation">(</span>replica1,<span class="token punctuation">)</span>
<span class="token punctuation">(</span><span class="token number">1</span> row<span class="token punctuation">)</span>
</code></pre></div><h2 id="只读节点部署" tabindex="-1"><a class="header-anchor" href="#只读节点部署" aria-hidden="true">#</a> 只读节点部署</h2><p>在只读节点本地磁盘的 <code>~/replica1</code> 路径上创建一个空目录，然后通过 <code>polar-replica-initdb.sh</code> 脚本使用共享存储上的数据目录来初始化只读节点的本地目录。初始化后的本地目录中没有默认配置文件，所以还需要使用 <code>initdb</code> 创建一个临时的本地目录模板，然后将所有的默认配置文件拷贝到只读节点的本地目录下：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token function">mkdir</span> <span class="token parameter variable">-m</span> 0700 <span class="token environment constant">$HOME</span>/replica1
<span class="token function">sudo</span> ~/tmp_basedir_polardb_pg_1100_bld/bin/polar-replica-initdb.sh <span class="token punctuation">\\</span>
    /nvme1n1/shared_data/ <span class="token environment constant">$HOME</span>/replica1/

<span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/initdb <span class="token parameter variable">-D</span> /tmp/replica1
<span class="token function">cp</span> /tmp/replica1/*.conf <span class="token environment constant">$HOME</span>/replica1/
</code></pre></div><p>编辑只读节点的配置。打开 <code>~/replica1/postgresql.conf</code>，增加配置项：</p><div class="language-ini line-numbers-mode" data-ext="ini"><pre class="language-ini"><code><span class="token key attr-name">port</span><span class="token punctuation">=</span><span class="token value attr-value">5433</span>
<span class="token key attr-name">polar_hostid</span><span class="token punctuation">=</span><span class="token value attr-value">2</span>
<span class="token key attr-name">polar_enable_shared_storage_mode</span><span class="token punctuation">=</span><span class="token value attr-value">on</span>
<span class="token key attr-name">polar_disk_name</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">nvme1n1</span>&#39;</span>
<span class="token key attr-name">polar_datadir</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">/nvme1n1/shared_data/</span>&#39;</span>
<span class="token key attr-name">polar_vfs.localfs_mode</span><span class="token punctuation">=</span><span class="token value attr-value">off</span>
<span class="token key attr-name">shared_preload_libraries</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">$libdir/polar_vfs,$libdir/polar_worker</span>&#39;</span>
<span class="token key attr-name">polar_storage_cluster_name</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">disk</span>&#39;</span>
<span class="token key attr-name">logging_collector</span><span class="token punctuation">=</span><span class="token value attr-value">on</span>
<span class="token key attr-name">log_line_prefix</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">%p\\t%r\\t%u\\t%m\\t</span>&#39;</span>
<span class="token key attr-name">log_directory</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">pg_log</span>&#39;</span>
<span class="token key attr-name">listen_addresses</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">*</span>&#39;</span>
<span class="token key attr-name">max_connections</span><span class="token punctuation">=</span><span class="token value attr-value">1000</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>创建只读节点的复制配置文件 <code>~/replica1/recovery.conf</code>，增加读写节点的连接信息，以及复制槽名称：</p><div class="language-ini" data-ext="ini"><pre class="language-ini"><code><span class="token key attr-name">polar_replica</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">on</span>&#39;</span>
<span class="token key attr-name">recovery_target_timeline</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">latest</span>&#39;</span>
<span class="token key attr-name">primary_slot_name</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">replica1</span>&#39;</span>
<span class="token key attr-name">primary_conninfo</span><span class="token punctuation">=</span><span class="token value attr-value">&#39;<span class="token inner-value">host=[读写节点所在IP] port=5432 user=postgres dbname=postgres application_name=replica1</span>&#39;</span>
</code></pre></div><p>最后，启动只读节点：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/pg_ctl start <span class="token parameter variable">-D</span> <span class="token environment constant">$HOME</span>/replica1
</code></pre></div><p>检查只读节点能否正常运行：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/psql <span class="token punctuation">\\</span>
    <span class="token parameter variable">-p</span> <span class="token number">5433</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-d</span> postgres <span class="token punctuation">\\</span>
    <span class="token parameter variable">-c</span> <span class="token string">&#39;SELECT version();&#39;</span>
            version
--------------------------------
 PostgreSQL <span class="token number">11.9</span> <span class="token punctuation">(</span>POLARDB <span class="token number">11.9</span><span class="token punctuation">)</span>
<span class="token punctuation">(</span><span class="token number">1</span> row<span class="token punctuation">)</span>
</code></pre></div><h2 id="集群检查和测试" tabindex="-1"><a class="header-anchor" href="#集群检查和测试" aria-hidden="true">#</a> 集群检查和测试</h2><p>部署完成后，需要进行实例检查和测试，确保读写节点可正常写入数据、只读节点可以正常读取。</p><p>登录 <strong>读写节点</strong>，创建测试表并插入样例数据：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/psql <span class="token parameter variable">-q</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-p</span> <span class="token number">5432</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-d</span> postgres <span class="token punctuation">\\</span>
    <span class="token parameter variable">-c</span> <span class="token string">&quot;CREATE TABLE t (t1 INT PRIMARY KEY, t2 INT); INSERT INTO t VALUES (1, 1),(2, 3),(3, 3);&quot;</span>
</code></pre></div><p>登录 <strong>只读节点</strong>，查询刚刚插入的样例数据：</p><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token environment constant">$HOME</span>/tmp_basedir_polardb_pg_1100_bld/bin/psql <span class="token parameter variable">-q</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-p</span> <span class="token number">5433</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-d</span> postgres <span class="token punctuation">\\</span>
    <span class="token parameter variable">-c</span> <span class="token string">&quot;SELECT * FROM t;&quot;</span>
 t1 <span class="token operator">|</span> t2
----+----
  <span class="token number">1</span> <span class="token operator">|</span>  <span class="token number">1</span>
  <span class="token number">2</span> <span class="token operator">|</span>  <span class="token number">3</span>
  <span class="token number">3</span> <span class="token operator">|</span>  <span class="token number">3</span>
<span class="token punctuation">(</span><span class="token number">3</span> rows<span class="token punctuation">)</span>
</code></pre></div><p>在读写节点上插入的数据对只读节点可见，这意味着基于共享存储的 PolarDB 计算节点集群搭建成功。</p><hr><h2 id="常见运维步骤" tabindex="-1"><a class="header-anchor" href="#常见运维步骤" aria-hidden="true">#</a> 常见运维步骤</h2>`,35);function g(o,h){const c=l("ArticleInfo"),t=l("router-link"),p=l("RouterLink");return i(),u("div",null,[v,n(c,{frontmatter:o.$frontmatter},null,8,["frontmatter"]),m,a("nav",b,[a("ul",null,[a("li",null,[n(t,{to:"#读写节点部署"},{default:e(()=>[s("读写节点部署")]),_:1})]),a("li",null,[n(t,{to:"#只读节点部署"},{default:e(()=>[s("只读节点部署")]),_:1})]),a("li",null,[n(t,{to:"#集群检查和测试"},{default:e(()=>[s("集群检查和测试")]),_:1})]),a("li",null,[n(t,{to:"#常见运维步骤"},{default:e(()=>[s("常见运维步骤")]),_:1})])])]),_,a("ul",null,[a("li",null,[n(p,{to:"/zh/operation/backup-and-restore.html"},{default:e(()=>[s("备份恢复")]),_:1})]),a("li",null,[n(p,{to:"/zh/operation/grow-storage.html"},{default:e(()=>[s("共享存储在线扩容")]),_:1})]),a("li",null,[n(p,{to:"/zh/operation/scale-out.html"},{default:e(()=>[s("计算节点扩缩容")]),_:1})]),a("li",null,[n(p,{to:"/zh/operation/ro-online-promote.html"},{default:e(()=>[s("只读节点在线 Promote")]),_:1})])])])}const y=r(k,[["render",g],["__file","db-pfs.html.vue"]]);export{y as default};
