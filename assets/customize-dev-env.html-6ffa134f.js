import{_ as r,r as o,o as c,c as d,a as s,b as n,d as a,w as l,e as p}from"./app-e5d03054.js";const m={},v=s("h1",{id:"定制开发环境",tabindex:"-1"},[s("a",{class:"header-anchor",href:"#定制开发环境","aria-hidden":"true"},"#"),n(" 定制开发环境")],-1),u=s("h2",{id:"自行构建开发镜像",tabindex:"-1"},[s("a",{class:"header-anchor",href:"#自行构建开发镜像","aria-hidden":"true"},"#"),n(" 自行构建开发镜像")],-1),b={href:"https://hub.docker.com/r/polardb/polardb_pg_devel/tags",target:"_blank",rel:"noopener noreferrer"},k=s("code",null,"polardb/polardb_pg_devel",-1),g=s("code",null,"linux/amd64",-1),h=s("code",null,"linux/arm64",-1),_={href:"https://hub.docker.com/_/ubuntu/tags",target:"_blank",rel:"noopener noreferrer"},f=s("code",null,"ubuntu:20.04",-1),S=p(`<details class="custom-container details"><div class="language-docker line-numbers-mode" data-ext="docker"><pre class="language-docker"><code><span class="token instruction"><span class="token keyword">FROM</span> ubuntu:20.04</span>
<span class="token instruction"><span class="token keyword">LABEL</span> maintainer=<span class="token string">&quot;mrdrivingduck@gmail.com&quot;</span></span>
<span class="token instruction"><span class="token keyword">CMD</span> bash</span>

<span class="token comment"># Timezone problem</span>
<span class="token instruction"><span class="token keyword">ENV</span> TZ=Asia/Shanghai</span>
<span class="token instruction"><span class="token keyword">RUN</span> ln -snf /usr/share/zoneinfo/<span class="token variable">$TZ</span> /etc/localtime &amp;&amp; echo <span class="token variable">$TZ</span> &gt; /etc/timezone</span>

<span class="token comment"># Upgrade softwares</span>
<span class="token instruction"><span class="token keyword">RUN</span> apt update -y &amp;&amp; <span class="token operator">\\</span>
    apt upgrade -y &amp;&amp; <span class="token operator">\\</span>
    apt clean -y</span>

<span class="token comment"># GCC (force to 9) and LLVM (force to 11)</span>
<span class="token instruction"><span class="token keyword">RUN</span> apt install -y <span class="token operator">\\</span>
        gcc-9 <span class="token operator">\\</span>
        g++-9 <span class="token operator">\\</span>
        llvm-11-dev <span class="token operator">\\</span>
        clang-11 <span class="token operator">\\</span>
        make <span class="token operator">\\</span>
        gdb <span class="token operator">\\</span>
        pkg-config <span class="token operator">\\</span>
        locales &amp;&amp; <span class="token operator">\\</span>
    update-alternatives --install <span class="token operator">\\</span>
        /usr/bin/gcc gcc /usr/bin/gcc-9 60 --slave <span class="token operator">\\</span>
        /usr/bin/g++ g++ /usr/bin/g++-9 &amp;&amp; <span class="token operator">\\</span>
    update-alternatives --install <span class="token operator">\\</span>
        /usr/bin/llvm-config llvm-config /usr/bin/llvm-config-11 60 --slave <span class="token operator">\\</span>
        /usr/bin/clang++ clang++ /usr/bin/clang++-11 --slave <span class="token operator">\\</span>
        /usr/bin/clang clang /usr/bin/clang-11 &amp;&amp; <span class="token operator">\\</span>
    apt clean -y</span>

<span class="token comment"># Generate locale</span>
<span class="token instruction"><span class="token keyword">RUN</span> sed -i <span class="token string">&#39;/en_US.UTF-8/s/^# //g&#39;</span> /etc/locale.gen &amp;&amp; <span class="token operator">\\</span>
    sed -i <span class="token string">&#39;/zh_CN.UTF-8/s/^# //g&#39;</span> /etc/locale.gen &amp;&amp; <span class="token operator">\\</span>
    locale-gen</span>

<span class="token comment"># Dependencies</span>
<span class="token instruction"><span class="token keyword">RUN</span> apt install -y <span class="token operator">\\</span>
        libicu-dev <span class="token operator">\\</span>
        bison <span class="token operator">\\</span>
        flex <span class="token operator">\\</span>
        python3-dev <span class="token operator">\\</span>
        libreadline-dev <span class="token operator">\\</span>
        libgss-dev <span class="token operator">\\</span>
        libssl-dev <span class="token operator">\\</span>
        libpam0g-dev <span class="token operator">\\</span>
        libxml2-dev <span class="token operator">\\</span>
        libxslt1-dev <span class="token operator">\\</span>
        libldap2-dev <span class="token operator">\\</span>
        uuid-dev <span class="token operator">\\</span>
        liblz4-dev <span class="token operator">\\</span>
        libkrb5-dev <span class="token operator">\\</span>
        gettext <span class="token operator">\\</span>
        libxerces-c-dev <span class="token operator">\\</span>
        tcl-dev <span class="token operator">\\</span>
        libperl-dev <span class="token operator">\\</span>
        libipc-run-perl <span class="token operator">\\</span>
        libaio-dev <span class="token operator">\\</span>
        libfuse-dev &amp;&amp; <span class="token operator">\\</span>
    apt clean -y</span>

<span class="token comment"># Tools</span>
<span class="token instruction"><span class="token keyword">RUN</span> apt install -y <span class="token operator">\\</span>
        iproute2 <span class="token operator">\\</span>
        wget <span class="token operator">\\</span>
        ccache <span class="token operator">\\</span>
        sudo <span class="token operator">\\</span>
        vim <span class="token operator">\\</span>
        git <span class="token operator">\\</span>
        cmake &amp;&amp; <span class="token operator">\\</span>
    apt clean -y</span>

<span class="token comment"># set to empty if GitHub is not barriered</span>
<span class="token comment"># ENV GITHUB_PROXY=https://ghproxy.com/</span>
<span class="token instruction"><span class="token keyword">ENV</span> GITHUB_PROXY=</span>

<span class="token instruction"><span class="token keyword">ENV</span> ZLOG_VERSION=1.2.14</span>
<span class="token instruction"><span class="token keyword">ENV</span> PFSD_VERSION=pfsd4pg-release-1.2.42-20220419</span>

<span class="token comment"># install dependencies from GitHub mirror</span>
<span class="token instruction"><span class="token keyword">RUN</span> cd /usr/local &amp;&amp; <span class="token operator">\\</span>
    <span class="token comment"># zlog for PFSD</span>
    wget --no-verbose --no-check-certificate <span class="token string">&quot;\${GITHUB_PROXY}https://github.com/HardySimpson/zlog/archive/refs/tags/\${ZLOG_VERSION}.tar.gz&quot;</span> &amp;&amp; <span class="token operator">\\</span>
    <span class="token comment"># PFSD</span>
    wget --no-verbose --no-check-certificate <span class="token string">&quot;\${GITHUB_PROXY}https://github.com/ApsaraDB/PolarDB-FileSystem/archive/refs/tags/\${PFSD_VERSION}.tar.gz&quot;</span> &amp;&amp; <span class="token operator">\\</span>
    <span class="token comment"># unzip and install zlog</span>
    gzip -d <span class="token variable">$ZLOG_VERSION</span>.tar.gz &amp;&amp; <span class="token operator">\\</span>
    tar xpf <span class="token variable">$ZLOG_VERSION</span>.tar &amp;&amp; <span class="token operator">\\</span>
    cd zlog-<span class="token variable">$ZLOG_VERSION</span> &amp;&amp; <span class="token operator">\\</span>
    make &amp;&amp; make install &amp;&amp; <span class="token operator">\\</span>
    echo <span class="token string">&#39;/usr/local/lib&#39;</span> &gt;&gt; /etc/ld.so.conf &amp;&amp; ldconfig &amp;&amp; <span class="token operator">\\</span>
    cd .. &amp;&amp; <span class="token operator">\\</span>
    rm -rf <span class="token variable">$ZLOG_VERSION</span>* &amp;&amp; <span class="token operator">\\</span>
    rm -rf zlog-<span class="token variable">$ZLOG_VERSION</span> &amp;&amp; <span class="token operator">\\</span>
    <span class="token comment"># unzip and install PFSD</span>
    gzip -d <span class="token variable">$PFSD_VERSION</span>.tar.gz &amp;&amp; <span class="token operator">\\</span>
    tar xpf <span class="token variable">$PFSD_VERSION</span>.tar &amp;&amp; <span class="token operator">\\</span>
    cd PolarDB-FileSystem-<span class="token variable">$PFSD_VERSION</span> &amp;&amp; <span class="token operator">\\</span>
    sed -i <span class="token string">&#39;s/-march=native //&#39;</span> CMakeLists.txt &amp;&amp; <span class="token operator">\\</span>
    ./autobuild.sh &amp;&amp; ./install.sh &amp;&amp; <span class="token operator">\\</span>
    cd .. &amp;&amp; <span class="token operator">\\</span>
    rm -rf <span class="token variable">$PFSD_VERSION</span>* &amp;&amp; <span class="token operator">\\</span>
    rm -rf PolarDB-FileSystem-<span class="token variable">$PFSD_VERSION</span>*</span>

<span class="token comment"># create default user</span>
<span class="token instruction"><span class="token keyword">ENV</span> USER_NAME=postgres</span>
<span class="token instruction"><span class="token keyword">RUN</span> echo <span class="token string">&quot;create default user&quot;</span> &amp;&amp; <span class="token operator">\\</span>
    groupadd -r <span class="token variable">$USER_NAME</span> &amp;&amp; <span class="token operator">\\</span>
    useradd -ms /bin/bash -g <span class="token variable">$USER_NAME</span> <span class="token variable">$USER_NAME</span> -p <span class="token string">&#39;&#39;</span> &amp;&amp; <span class="token operator">\\</span>
    usermod -aG sudo <span class="token variable">$USER_NAME</span></span>

<span class="token comment"># modify conf</span>
<span class="token instruction"><span class="token keyword">RUN</span> echo <span class="token string">&quot;modify conf&quot;</span> &amp;&amp; <span class="token operator">\\</span>
    mkdir -p /var/log/pfs &amp;&amp; chown <span class="token variable">$USER_NAME</span> /var/log/pfs &amp;&amp; <span class="token operator">\\</span>
    mkdir -p /var/run/pfs &amp;&amp; chown <span class="token variable">$USER_NAME</span> /var/run/pfs &amp;&amp; <span class="token operator">\\</span>
    mkdir -p /var/run/pfsd &amp;&amp; chown <span class="token variable">$USER_NAME</span> /var/run/pfsd &amp;&amp; <span class="token operator">\\</span>
    mkdir -p /dev/shm/pfsd &amp;&amp; chown <span class="token variable">$USER_NAME</span> /dev/shm/pfsd &amp;&amp; <span class="token operator">\\</span>
    touch /var/run/pfsd/.pfsd &amp;&amp; <span class="token operator">\\</span>
    echo <span class="token string">&quot;ulimit -c unlimited&quot;</span> &gt;&gt; /home/postgres/.bashrc &amp;&amp; <span class="token operator">\\</span>
    echo <span class="token string">&quot;export PGHOST=127.0.0.1&quot;</span> &gt;&gt; /home/postgres/.bashrc &amp;&amp; <span class="token operator">\\</span>
    echo <span class="token string">&quot;alias pg=&#39;psql -h /home/postgres/tmp_master_dir_polardb_pg_1100_bld/&#39;&quot;</span> &gt;&gt; /home/postgres/.bashrc</span>

<span class="token instruction"><span class="token keyword">ENV</span> PATH=<span class="token string">&quot;/home/postgres/tmp_basedir_polardb_pg_1100_bld/bin:$PATH&quot;</span></span>
<span class="token instruction"><span class="token keyword">WORKDIR</span> /home/<span class="token variable">$USER_NAME</span></span>
<span class="token instruction"><span class="token keyword">USER</span> <span class="token variable">$USER_NAME</span></span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div></details><p>将上述内容复制到一个文件内（假设文件名为 <code>Dockerfile-PolarDB</code>）后，使用如下命令构建镜像：</p><div class="custom-container tip"><p class="custom-container-title">TIP</p><p>💡 请在下面的高亮行中按需替换 <code>&lt;image_name&gt;</code> 内的 Docker 镜像名称</p></div><div class="language-bash" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> build <span class="token parameter variable">--network</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">-t</span> <span class="token operator">&lt;</span>image_name<span class="token operator">&gt;</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">-f</span> Dockerfile-PolarDB <span class="token builtin class-name">.</span>
</code></pre><div class="highlight-lines"><br><div class="highlight-line"> </div><br></div></div><h2 id="从干净的系统开始搭建开发环境" tabindex="-1"><a class="header-anchor" href="#从干净的系统开始搭建开发环境" aria-hidden="true">#</a> 从干净的系统开始搭建开发环境</h2><p>该方式假设您从一台具有 root 权限的干净的 CentOS 7 操作系统上从零开始，可以是：</p><ul><li>安装 CentOS 7 的物理机/虚拟机</li><li>从 CentOS 7 官方 Docker 镜像 <code>centos:centos7</code> 上启动的 Docker 容器</li></ul><h3 id="建立非-root-用户" tabindex="-1"><a class="header-anchor" href="#建立非-root-用户" aria-hidden="true">#</a> 建立非 root 用户</h3><p>PolarDB for PostgreSQL 需要以非 root 用户运行。以下步骤能够帮助您创建一个名为 <code>postgres</code> 的用户组和一个名为 <code>postgres</code> 的用户。</p><div class="custom-container tip"><p class="custom-container-title">TIP</p><p>如果您已经有了一个非 root 用户，但名称不是 <code>postgres:postgres</code>，可以忽略该步骤；但请注意在后续示例步骤中将命令中用户相关的信息替换为您自己的用户组名与用户名。</p></div><p>下面的命令能够创建用户组 <code>postgres</code> 和用户 <code>postgres</code>，并为该用户赋予 sudo 和工作目录的权限。需要以 root 用户执行这些命令。</p><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token comment"># install sudo</span>
yum <span class="token function">install</span> <span class="token parameter variable">-y</span> <span class="token function">sudo</span>
<span class="token comment"># create user and group</span>
<span class="token function">groupadd</span> <span class="token parameter variable">-r</span> postgres
<span class="token function">useradd</span> <span class="token parameter variable">-m</span> <span class="token parameter variable">-g</span> postgres postgres <span class="token parameter variable">-p</span> <span class="token string">&#39;&#39;</span>
<span class="token function">usermod</span> <span class="token parameter variable">-aG</span> wheel postgres
<span class="token comment"># make postgres as sudoer</span>
<span class="token function">chmod</span> u+w /etc/sudoers
<span class="token builtin class-name">echo</span> <span class="token string">&#39;postgres ALL=(ALL) NOPASSWD: ALL&#39;</span> <span class="token operator">&gt;&gt;</span> /etc/sudoers
<span class="token function">chmod</span> u-w /etc/sudoers
<span class="token comment"># grant access to home directory</span>
<span class="token function">chown</span> <span class="token parameter variable">-R</span> postgres:postgres /home/postgres/
<span class="token builtin class-name">echo</span> <span class="token string">&#39;source /etc/bashrc&#39;</span> <span class="token operator">&gt;&gt;</span> /home/postgres/.bashrc
<span class="token comment"># for su postgres</span>
<span class="token function">sed</span> <span class="token parameter variable">-i</span> <span class="token string">&#39;s/4096/unlimited/g&#39;</span> /etc/security/limits.d/20-nproc.conf
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>接下来，切换到 <code>postgres</code> 用户，就可以进行后续的步骤了：</p><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">su</span> postgres
<span class="token builtin class-name">source</span> /etc/bashrc
<span class="token builtin class-name">cd</span> ~
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="依赖安装" tabindex="-1"><a class="header-anchor" href="#依赖安装" aria-hidden="true">#</a> 依赖安装</h3><p>在 PolarDB for PostgreSQL 源码库根目录的 <code>deps/</code> 子目录下，放置了在各个 Linux 发行版上编译安装 PolarDB 和 PFS 需要运行的所有依赖。因此，首先需要克隆 PolarDB 的源码库。</p>`,16),E={href:"https://github.com/ApsaraDB/PolarDB-for-PostgreSQL",target:"_blank",rel:"noopener noreferrer"},N=s("code",null,"POLARDB_11_STABLE",-1),P={href:"https://gitee.com/mirrors/PolarDB-for-PostgreSQL",target:"_blank",rel:"noopener noreferrer"},y=s("div",{class:"language-bash","data-ext":"sh"},[s("pre",{class:"language-bash"},[s("code",null,[s("span",{class:"token function"},"sudo"),n(" yum "),s("span",{class:"token function"},"install"),n(),s("span",{class:"token parameter variable"},"-y"),n(),s("span",{class:"token function"},"git"),n(`
`),s("span",{class:"token function"},"git"),n(" clone "),s("span",{class:"token parameter variable"},"-b"),n(` POLARDB_11_STABLE https://github.com/ApsaraDB/PolarDB-for-PostgreSQL.git
`)])])],-1),R=s("div",{class:"language-bash","data-ext":"sh"},[s("pre",{class:"language-bash"},[s("code",null,[s("span",{class:"token function"},"sudo"),n(" yum "),s("span",{class:"token function"},"install"),n(),s("span",{class:"token parameter variable"},"-y"),n(),s("span",{class:"token function"},"git"),n(`
`),s("span",{class:"token function"},"git"),n(" clone "),s("span",{class:"token parameter variable"},"-b"),n(` POLARDB_11_STABLE https://gitee.com/mirrors/PolarDB-for-PostgreSQL
`)])])],-1),D=p(`<p>源码下载完毕后，使用 <code>sudo</code> 执行 <code>deps/</code> 目录下的相应脚本 <code>deps-***.sh</code> 自动完成所有的依赖安装。比如：</p><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token builtin class-name">cd</span> PolarDB-for-PostgreSQL
<span class="token function">sudo</span> ./deps/deps-centos7.sh
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div>`,2);function w(L,O){const e=o("ExternalLinkIcon"),i=o("CodeGroupItem"),t=o("CodeGroup");return c(),d("div",null,[v,u,s("p",null,[n("DockerHub 上已有构建完毕的开发镜像 "),s("a",b,[k,a(e)]),n(" 可供直接使用（支持 "),g,n(" 和 "),h,n(" 两种架构）。")]),s("p",null,[n("另外，我们也提供了构建上述开发镜像的 Dockerfile，从 "),s("a",_,[n("Ubuntu 官方镜像"),a(e)]),n(),f,n(" 开始构建出一个安装完所有开发和运行时依赖的镜像，您可以根据自己的需要在 Dockerfile 中添加更多依赖。以下是手动构建镜像的 Dockerfile 及方法：")]),S,s("p",null,[n("PolarDB for PostgreSQL 的代码托管于 "),s("a",E,[n("GitHub"),a(e)]),n(" 上，稳定分支为 "),N,n("。如果因网络原因不能稳定访问 GitHub，则可以访问 "),s("a",P,[n("Gitee 国内镜像"),a(e)]),n("。")]),a(t,null,{default:l(()=>[a(i,{title:"GitHub"},{default:l(()=>[y]),_:1}),a(i,{title:"Gitee 国内镜像"},{default:l(()=>[R]),_:1})]),_:1}),D])}const B=r(m,[["render",w],["__file","customize-dev-env.html.vue"]]);export{B as default};
