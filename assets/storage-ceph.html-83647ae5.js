import{_ as s,r as e,o as i,c as l,a as r,b as a,d as c,w as p,e as d}from"./app-e5d03054.js";const t={},o=d(`<h1 id="ceph-共享存储" tabindex="-1"><a class="header-anchor" href="#ceph-共享存储" aria-hidden="true">#</a> Ceph 共享存储</h1><p>Ceph 是一个统一的分布式存储系统，由于它可以提供较好的性能、可靠性和可扩展性，被广泛的应用在存储领域。Ceph 搭建需要 2 台及以上的物理机/虚拟机实现存储共享与数据备份，本教程以 3 台虚拟机环境为例，介绍基于 ceph 共享存储的实例构建方法。大体如下：</p><ol><li>获取在同一网段的虚拟机三台，互相之间配置 ssh 免密登录，用作 ceph 密钥与配置信息的同步；</li><li>在主节点启动 mon 进程，查看状态，并复制配置文件至其余各个节点，完成 mon 启动；</li><li>在三个环境中启动 osd 进程配置存储盘，并在主节点环境启动 mgr 进程、rgw 进程；</li><li>创建存储池与 rbd 块设备镜像，并对创建好的镜像在各个节点进行映射即可实现块设备的共享；</li><li>对块设备进行 PolarFS 的格式化与 PolarDB 的部署。</li></ol><div class="custom-container warning"><p class="custom-container-title">注意</p><p>操作系统版本要求 CentOS 7.5 及以上。以下步骤在 CentOS 7.5 上通过测试。</p></div><h2 id="环境准备" tabindex="-1"><a class="header-anchor" href="#环境准备" aria-hidden="true">#</a> 环境准备</h2><p>使用的虚拟机环境如下：</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>IP                  hostname
192.168.1.173       ceph001
192.168.1.174       ceph002
192.168.1.175       ceph003
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="安装-docker" tabindex="-1"><a class="header-anchor" href="#安装-docker" aria-hidden="true">#</a> 安装 docker</h3><div class="custom-container tip"><p class="custom-container-title">提示</p><p>本教程使用阿里云镜像站提供的 docker 包。</p></div><h4 id="安装-docker-依赖包" tabindex="-1"><a class="header-anchor" href="#安装-docker-依赖包" aria-hidden="true">#</a> 安装 docker 依赖包</h4><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code>yum <span class="token function">install</span> <span class="token parameter variable">-y</span> yum-utils device-mapper-persistent-data lvm2
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h4 id="安装并启动-docker" tabindex="-1"><a class="header-anchor" href="#安装并启动-docker" aria-hidden="true">#</a> 安装并启动 docker</h4><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code>yum-config-manager --add-repo http://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo
yum makecache
yum <span class="token function">install</span> <span class="token parameter variable">-y</span> docker-ce

systemctl start <span class="token function">docker</span>
systemctl <span class="token builtin class-name">enable</span> <span class="token function">docker</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="检查是否安装成功" tabindex="-1"><a class="header-anchor" href="#检查是否安装成功" aria-hidden="true">#</a> 检查是否安装成功</h4><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run hello-world
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h3 id="配置-ssh-免密登录" tabindex="-1"><a class="header-anchor" href="#配置-ssh-免密登录" aria-hidden="true">#</a> 配置 ssh 免密登录</h3><h4 id="密钥的生成与拷贝" tabindex="-1"><a class="header-anchor" href="#密钥的生成与拷贝" aria-hidden="true">#</a> 密钥的生成与拷贝</h4><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code>ssh-keygen
ssh-copy-id <span class="token parameter variable">-i</span> /root/.ssh/id_rsa.pub    root@ceph001
ssh-copy-id <span class="token parameter variable">-i</span> /root/.ssh/id_rsa.pub    root@ceph002
ssh-copy-id <span class="token parameter variable">-i</span> /root/.ssh/id_rsa.pub    root@ceph003
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h4 id="检查是否配置成功" tabindex="-1"><a class="header-anchor" href="#检查是否配置成功" aria-hidden="true">#</a> 检查是否配置成功</h4><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">ssh</span> root@ceph003
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h3 id="下载-ceph-daemon" tabindex="-1"><a class="header-anchor" href="#下载-ceph-daemon" aria-hidden="true">#</a> 下载 ceph daemon</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> pull ceph/daemon
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h2 id="mon-部署" tabindex="-1"><a class="header-anchor" href="#mon-部署" aria-hidden="true">#</a> mon 部署</h2><h3 id="ceph001-上-mon-进程启动" tabindex="-1"><a class="header-anchor" href="#ceph001-上-mon-进程启动" aria-hidden="true">#</a> ceph001 上 mon 进程启动</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">MON_IP</span><span class="token operator">=</span><span class="token number">192.168</span>.1.173 <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CEPH_PUBLIC_NETWORK</span><span class="token operator">=</span><span class="token number">192.168</span>.1.0/24 <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>mon01 <span class="token punctuation">\\</span>
    ceph/daemon mon
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>根据实际网络环境修改 IP、子网掩码位数。</p></div><h3 id="查看容器状态" tabindex="-1"><a class="header-anchor" href="#查看容器状态" aria-hidden="true">#</a> 查看容器状态</h3><div class="language-console line-numbers-mode" data-ext="console"><pre class="language-console"><code>$ docker exec mon01 ceph -s
cluster:
    id:     937ccded-3483-4245-9f61-e6ef0dbd85ca
    health: HEALTH_OK

services:
    mon: 1 daemons, quorum ceph001 (age 26m)
    mgr: no daemons active
    osd: 0 osds: 0 up, 0 in

data:
    pools:   0 pools, 0 pgs
    objects: 0 objects, 0 B
    usage:   0 B used, 0 B / 0 B avail
    pgs:
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>如果遇到 <code>mon is allowing insecure global_id reclaim</code> 的报错，使用以下命令解决。</p></div><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> <span class="token builtin class-name">exec</span> mon01 ceph config <span class="token builtin class-name">set</span> mon auth_allow_insecure_global_id_reclaim <span class="token boolean">false</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div></div></div><h3 id="生成必须的-keyring" tabindex="-1"><a class="header-anchor" href="#生成必须的-keyring" aria-hidden="true">#</a> 生成必须的 keyring</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> <span class="token builtin class-name">exec</span> mon01 ceph auth get client.bootstrap-osd <span class="token parameter variable">-o</span> /var/lib/ceph/bootstrap-osd/ceph.keyring
<span class="token function">docker</span> <span class="token builtin class-name">exec</span> mon01 ceph auth get client.bootstrap-rgw <span class="token parameter variable">-o</span> /var/lib/ceph/bootstrap-rgw/ceph.keyring
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="配置文件同步" tabindex="-1"><a class="header-anchor" href="#配置文件同步" aria-hidden="true">#</a> 配置文件同步</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">ssh</span> root@ceph002 <span class="token function">mkdir</span> <span class="token parameter variable">-p</span> /var/lib/ceph
<span class="token function">scp</span> <span class="token parameter variable">-r</span> /etc/ceph root@ceph002:/etc
<span class="token function">scp</span> <span class="token parameter variable">-r</span> /var/lib/ceph/bootstrap* root@ceph002:/var/lib/ceph
<span class="token function">ssh</span> root@ceph003 <span class="token function">mkdir</span> <span class="token parameter variable">-p</span> /var/lib/ceph
<span class="token function">scp</span> <span class="token parameter variable">-r</span> /etc/ceph root@ceph003:/etc
<span class="token function">scp</span> <span class="token parameter variable">-r</span> /var/lib/ceph/bootstrap* root@ceph003:/var/lib/ceph
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="在-ceph002-与-ceph003-中启动-mon" tabindex="-1"><a class="header-anchor" href="#在-ceph002-与-ceph003-中启动-mon" aria-hidden="true">#</a> 在 ceph002 与 ceph003 中启动 mon</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">MON_IP</span><span class="token operator">=</span><span class="token number">192.168</span>.1.174 <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CEPH_PUBLIC_NETWORK</span><span class="token operator">=</span><span class="token number">192.168</span>.1.0/24 <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>mon02 <span class="token punctuation">\\</span>
    ceph/daemon mon

<span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">MON_IP</span><span class="token operator">=</span><span class="token number">192.168</span>.1.175 <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CEPH_PUBLIC_NETWORK</span><span class="token operator">=</span><span class="token number">192.168</span>.1.0/24 <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>mon03 <span class="token punctuation">\\</span>
    ceph/daemon mon
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="查看当前集群状态" tabindex="-1"><a class="header-anchor" href="#查看当前集群状态" aria-hidden="true">#</a> 查看当前集群状态</h3><div class="language-console line-numbers-mode" data-ext="console"><pre class="language-console"><code>$ docker exec mon01 ceph -s
cluster:
    id:     937ccded-3483-4245-9f61-e6ef0dbd85ca
    health: HEALTH_OK

services:
    mon: 3 daemons, quorum ceph001,ceph002,ceph003 (age 35s)
    mgr: no daemons active
    osd: 0 osds: 0 up, 0 in

data:
    pools:   0 pools, 0 pgs
    objects: 0 objects, 0 B
    usage:   0 B used, 0 B / 0 B avail
    pgs:
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>从 mon 节点信息查看是否有添加在另外两个节点创建的 mon 添加进来。</p></div><h2 id="osd-部署" tabindex="-1"><a class="header-anchor" href="#osd-部署" aria-hidden="true">#</a> osd 部署</h2><h3 id="osd-准备阶段" tabindex="-1"><a class="header-anchor" href="#osd-准备阶段" aria-hidden="true">#</a> osd 准备阶段</h3><div class="custom-container tip"><p class="custom-container-title">提示</p><p>本环境的虚拟机只有一个 <code>/dev/vdb</code> 磁盘可用，因此为每个虚拟机只创建了一个 osd 节点。</p></div><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run <span class="token parameter variable">--rm</span> <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token parameter variable">--ipc</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /run/lock/lvm:/run/lock/lvm:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/run/udev/:/var/run/udev/:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /dev:/dev <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /run/lvm/:/run/lvm/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/log/ceph/:/var/log/ceph/:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">--entrypoint</span><span class="token operator">=</span>ceph-volume <span class="token punctuation">\\</span>
    docker.io/ceph/daemon <span class="token punctuation">\\</span>
    <span class="token parameter variable">--cluster</span> ceph lvm prepare <span class="token parameter variable">--bluestore</span> <span class="token parameter variable">--data</span> /dev/vdb
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>以上命令在三个节点都是一样的，只需要根据磁盘名称进行修改调整即可。</p></div><h3 id="osd-激活阶段" tabindex="-1"><a class="header-anchor" href="#osd-激活阶段" aria-hidden="true">#</a> osd 激活阶段</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token parameter variable">--pid</span><span class="token operator">=</span>host <span class="token parameter variable">--ipc</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /dev:/dev <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/localtime:/etc/ localtime:ro <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph:/var/lib/ceph:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/run/ceph:/var/run/ceph:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/run/udev/:/var/run/udev/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/log/ceph:/var/log/ceph:z <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /run/lvm/:/run/lvm/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CLUSTER</span><span class="token operator">=</span>ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CEPH_DAEMON</span><span class="token operator">=</span>OSD_CEPH_VOLUME_ACTIVATE <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CONTAINER_IMAGE</span><span class="token operator">=</span>docker.io/ceph/daemon <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">OSD_ID</span><span class="token operator">=</span><span class="token number">0</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>ceph-osd-0 <span class="token punctuation">\\</span>
    docker.io/ceph/daemon
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>各个节点需要修改 OSD_ID 与 name 属性，OSD_ID 是从编号 0 递增的，其余节点为 OSD_ID=1、OSD_ID=2。</p></div><h3 id="查看集群状态" tabindex="-1"><a class="header-anchor" href="#查看集群状态" aria-hidden="true">#</a> 查看集群状态</h3><div class="language-console line-numbers-mode" data-ext="console"><pre class="language-console"><code>$ docker exec mon01 ceph -s
cluster:
    id:     e430d054-dda8-43f1-9cda-c0881b782e17
    health: HEALTH_WARN
            no active mgr

services:
    mon: 3 daemons, quorum ceph001,ceph002,ceph003 (age 44m)
    mgr: no daemons active
    osd: 3 osds: 3 up (since 7m), 3 in (since     13m)

data:
    pools:   0 pools, 0 pgs
    objects: 0 objects, 0 B
    usage:   0 B used, 0 B / 0 B avail
    pgs:
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="mgr、mds、rgw-部署" tabindex="-1"><a class="header-anchor" href="#mgr、mds、rgw-部署" aria-hidden="true">#</a> mgr、mds、rgw 部署</h2><p>以下命令均在 ceph001 进行：</p><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>ceph-mgr-0 <span class="token punctuation">\\</span>
    ceph/daemon mgr

<span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">-e</span> <span class="token assign-left variable">CEPHFS_CREATE</span><span class="token operator">=</span><span class="token number">1</span> <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>ceph-mds-0 <span class="token punctuation">\\</span>
    ceph/daemon mds

<span class="token function">docker</span> run <span class="token parameter variable">-d</span> <span class="token parameter variable">--net</span><span class="token operator">=</span>host <span class="token punctuation">\\</span>
    <span class="token parameter variable">--privileged</span><span class="token operator">=</span>true <span class="token punctuation">\\</span>
    --security-opt <span class="token assign-left variable">seccomp</span><span class="token operator">=</span>unconfined <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /var/lib/ceph/:/var/lib/ceph/ <span class="token punctuation">\\</span>
    <span class="token parameter variable">-v</span> /etc/ceph:/etc/ceph <span class="token punctuation">\\</span>
    <span class="token parameter variable">--name</span><span class="token operator">=</span>ceph-rgw-0 <span class="token punctuation">\\</span>
    ceph/daemon rgw
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><p>查看集群状态：</p><div class="language-console line-numbers-mode" data-ext="console"><pre class="language-console"><code>docker exec mon01 ceph -s
cluster:
    id:     e430d054-dda8-43f1-9cda-c0881b782e17
    health: HEALTH_OK

services:
    mon: 3 daemons, quorum ceph001,ceph002,ceph003 (age 92m)
    mgr: ceph001(active, since 25m)
    mds: 1/1 daemons up
    osd: 3 osds: 3 up (since 54m), 3 in (since    60m)
    rgw: 1 daemon active (1 hosts, 1 zones)

data:
    volumes: 1/1 healthy
    pools:   7 pools, 145 pgs
    objects: 243 objects, 7.2 KiB
    usage:   50 MiB used, 2.9 TiB / 2.9 TiB avail
    pgs:     145 active+clean
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h2 id="rbd-块设备创建" tabindex="-1"><a class="header-anchor" href="#rbd-块设备创建" aria-hidden="true">#</a> rbd 块设备创建</h2><div class="custom-container tip"><p class="custom-container-title">提示</p><p>以下命令均在容器 mon01 中进行。</p></div><h3 id="存储池的创建" tabindex="-1"><a class="header-anchor" href="#存储池的创建" aria-hidden="true">#</a> 存储池的创建</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code><span class="token function">docker</span> <span class="token builtin class-name">exec</span> <span class="token parameter variable">-it</span> mon01 <span class="token function">bash</span>
ceph osd pool create rbd_polar
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="创建镜像文件并查看信息" tabindex="-1"><a class="header-anchor" href="#创建镜像文件并查看信息" aria-hidden="true">#</a> 创建镜像文件并查看信息</h3><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code>rbd create <span class="token parameter variable">--size</span> <span class="token number">512000</span> rbd_polar/image02
rbd info rbd_polar/image02

rbd image <span class="token string">&#39;image02&#39;</span><span class="token builtin class-name">:</span>
size <span class="token number">500</span> GiB <span class="token keyword">in</span> <span class="token number">128000</span> objects
order <span class="token number">22</span> <span class="token punctuation">(</span><span class="token number">4</span> MiB objects<span class="token punctuation">)</span>
snapshot_count: <span class="token number">0</span>
id: 13b97b252c5d
block_name_prefix: rbd_data.13b97b252c5d
format: <span class="token number">2</span>
features: layering, exclusive-lock, object-map, fast-diff, deep-flatten
op_features:
flags:
create_timestamp: Thu Oct <span class="token number">28</span> 06:18:07 <span class="token number">2021</span>
access_timestamp: Thu Oct <span class="token number">28</span> 06:18:07 <span class="token number">2021</span>
modify_timestamp: Thu Oct <span class="token number">28</span> 06:18:07 <span class="token number">2021</span>
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><h3 id="映射镜像文件" tabindex="-1"><a class="header-anchor" href="#映射镜像文件" aria-hidden="true">#</a> 映射镜像文件</h3><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>modprobe rbd # 加载内核模块，在主机上执行
rbd map rbd_polar/image02

rbd: sysfs write failed
RBD image feature set mismatch. You can disable features unsupported by the kernel with &quot;rbd feature disable rbd_polar/image02 object-map fast-diff deep-flatten&quot;.
In some cases useful info is found in syslog -  try &quot;dmesg | tail&quot;.
rbd: map failed: (6) No such device or address
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>某些特性内核不支持，需要关闭才可以映射成功。如下进行：关闭 rbd 不支持特性，重新映射镜像，并查看映射列表。</p></div><div class="language-bash line-numbers-mode" data-ext="sh"><pre class="language-bash"><code>rbd feature disable rbd_polar/image02 object-map fast-diff deep-flatten
rbd map rbd_polar/image02
rbd device list

<span class="token function">id</span>  pool       namespace  image    snap  device
<span class="token number">0</span>   rbd_polar             image01  -     /dev/  rbd0
<span class="token number">1</span>   rbd_polar             image02  -     /dev/  rbd1
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container tip"><p class="custom-container-title">提示</p><p>此处我已经先映射了一个 image01，所以有两条信息。</p></div><h3 id="查看块设备" tabindex="-1"><a class="header-anchor" href="#查看块设备" aria-hidden="true">#</a> 查看块设备</h3><p>回到容器外，进行操作。查看系统中的块设备：</p><div class="language-text line-numbers-mode" data-ext="text"><pre class="language-text"><code>lsblk

NAME                                                               MAJ:MIN RM  SIZE RO TYPE  MOUNTPOINT
vda                                                                253:0    0  500G  0 disk
└─vda1                                                             253:1    0  500G  0 part /
vdb                                                                253:16   0 1000G  0 disk
└─ceph--7eefe77f--c618--4477--a1ed--b4f44520dfc 2-osd--block--bced3ff1--42b9--43e1--8f63--e853b  ce41435
                                                                    252:0    0 1000G  0 lvm
rbd0                                                               251:0    0  100G  0 disk
rbd1                                                               251:16   0  500G  0 disk
</code></pre><div class="line-numbers" aria-hidden="true"><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div><div class="line-number"></div></div></div><div class="custom-container warning"><p class="custom-container-title">注意</p><p>块设备镜像需要在各个节点都进行映射才可以在本地环境中通过 <code>lsblk</code> 命令查看到，否则不显示。ceph002 与 ceph003 上映射命令与上述一致。</p></div><hr><h2 id="准备分布式文件系统" tabindex="-1"><a class="header-anchor" href="#准备分布式文件系统" aria-hidden="true">#</a> 准备分布式文件系统</h2>`,71);function v(u,m){const n=e("RouterLink");return i(),l("div",null,[o,r("p",null,[a("参阅 "),c(n,{to:"/zh/deploying/fs-pfs.html"},{default:p(()=>[a("格式化并挂载 PFS")]),_:1}),a("。")])])}const h=s(t,[["render",v],["__file","storage-ceph.html.vue"]]);export{h as default};
