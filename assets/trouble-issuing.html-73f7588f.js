import{_ as s,o as n,c as a,e as t}from"./app-e5d03054.js";const o={},p=t(`<h1 id="问题报告" tabindex="-1"><a class="header-anchor" href="#问题报告" aria-hidden="true">#</a> 问题报告</h1><p>如果在运行 PolarDB for PostgreSQL 的过程中出现问题，请提供数据库的日志与机器的配置信息以方便定位问题。</p><p>通过 <code>polar_stat_env</code> 插件可以轻松获取数据库所在主机的硬件配置：</p><div class="language-sql" data-ext="sql"><pre class="language-sql"><code><span class="token operator">=</span><span class="token operator">&gt;</span> <span class="token keyword">CREATE</span> EXTENSION polar_stat_env<span class="token punctuation">;</span>
<span class="token operator">=</span><span class="token operator">&gt;</span> <span class="token keyword">SELECT</span> polar_stat_env<span class="token punctuation">(</span><span class="token punctuation">)</span><span class="token punctuation">;</span>
                           polar_stat_env
<span class="token comment">--------------------------------------------------------------------</span>
 {                                                                 <span class="token operator">+</span>
   <span class="token string">&quot;CPU&quot;</span>: {                                                        <span class="token operator">+</span>
     <span class="token string">&quot;Architecture&quot;</span>: <span class="token string">&quot;x86_64&quot;</span><span class="token punctuation">,</span>                                     <span class="token operator">+</span>
     <span class="token string">&quot;Model Name&quot;</span>: <span class="token string">&quot;Intel(R) Xeon(R) Platinum 8369B CPU @ 2.70GHz&quot;</span><span class="token punctuation">,</span><span class="token operator">+</span>
     <span class="token string">&quot;CPU Cores&quot;</span>: <span class="token string">&quot;8&quot;</span><span class="token punctuation">,</span>                                             <span class="token operator">+</span>
     <span class="token string">&quot;CPU Thread Per Cores&quot;</span>: <span class="token string">&quot;2&quot;</span><span class="token punctuation">,</span>                                  <span class="token operator">+</span>
     <span class="token string">&quot;CPU Core Per Socket&quot;</span>: <span class="token string">&quot;4&quot;</span><span class="token punctuation">,</span>                                   <span class="token operator">+</span>
     <span class="token string">&quot;NUMA Nodes&quot;</span>: <span class="token string">&quot;1&quot;</span><span class="token punctuation">,</span>                                            <span class="token operator">+</span>
     <span class="token string">&quot;L1d cache&quot;</span>: <span class="token string">&quot;192 KiB (4 instances)&quot;</span><span class="token punctuation">,</span>                         <span class="token operator">+</span>
     <span class="token string">&quot;L1i cache&quot;</span>: <span class="token string">&quot;128 KiB (4 instances)&quot;</span><span class="token punctuation">,</span>                         <span class="token operator">+</span>
     <span class="token string">&quot;L2 cache&quot;</span>: <span class="token string">&quot;5 MiB (4 instances)&quot;</span><span class="token punctuation">,</span>                            <span class="token operator">+</span>
     <span class="token string">&quot;L3 cache&quot;</span>: <span class="token string">&quot;48 MiB (1 instance)&quot;</span>                             <span class="token operator">+</span>
   }<span class="token punctuation">,</span>                                                              <span class="token operator">+</span>
   <span class="token string">&quot;Memory&quot;</span>: {                                                     <span class="token operator">+</span>
     <span class="token string">&quot;Memory Total (GB)&quot;</span>: <span class="token string">&quot;14&quot;</span><span class="token punctuation">,</span>                                    <span class="token operator">+</span>
     <span class="token string">&quot;HugePage Size (MB)&quot;</span>: <span class="token string">&quot;2&quot;</span><span class="token punctuation">,</span>                                    <span class="token operator">+</span>
     <span class="token string">&quot;HugePage Total Size (GB)&quot;</span>: <span class="token string">&quot;0&quot;</span>                               <span class="token operator">+</span>
   }<span class="token punctuation">,</span>                                                              <span class="token operator">+</span>
   <span class="token string">&quot;OS Params&quot;</span>: {                                                  <span class="token operator">+</span>
     <span class="token string">&quot;OS&quot;</span>: <span class="token string">&quot;5.10.134-16.1.al8.x86_64&quot;</span><span class="token punctuation">,</span>                             <span class="token operator">+</span>
     <span class="token string">&quot;Swappiness(1-100)&quot;</span>: <span class="token string">&quot;0&quot;</span><span class="token punctuation">,</span>                                     <span class="token operator">+</span>
     <span class="token string">&quot;Vfs Cache Pressure(0-1000)&quot;</span>: <span class="token string">&quot;100&quot;</span><span class="token punctuation">,</span>                          <span class="token operator">+</span>
     <span class="token string">&quot;Min Free KBytes(KB)&quot;</span>: <span class="token string">&quot;67584&quot;</span>                                <span class="token operator">+</span>
   }                                                               <span class="token operator">+</span>
 }
<span class="token punctuation">(</span><span class="token number">1</span> <span class="token keyword">row</span><span class="token punctuation">)</span>
</code></pre></div>`,4),e=[p];function c(r,u){return n(),a("div",null,e)}const k=s(o,[["render",c],["__file","trouble-issuing.html.vue"]]);export{k as default};
