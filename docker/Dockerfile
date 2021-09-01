FROM centos:latest

USER root
RUN rm -f /etc/yum.repos.d/CentOS-Linux-Media.repo 
RUN yum install --enablerepo=* -y \
	which \
	sudo \
	passwd \
	wget \
	git \
	perl \
	perl-devel \
	perl-libintl \
	perl-IPC-Run \
	perl-Time-HiRes \
	perl-Test-Simple \
	perl-ExtUtils-Embed \
	perl-ExtUtils-MakeMaker \
	perl-Text-Unidecode \
	pam-devel \
	openldap-devel \
	tcl-devel \
	libffi \
	libffi-devel \
	openssl \
	openssl-devel \
	apr apr-devel apr-util apr-util-devel pigz \
	&& yum install gcc gcc-c++ libstdc++ libstdc++-devel binutils -y \
	&& yum install glibc glibc-devel glibc-all-langpacks libieee1284 -y \
   	&& yum install -y --enablerepo=* \
	bison \
	flex \
	libzstd-devel libzstd \
	zstd \
	protobuf-devel \
	readline-devel \
	libxml2-devel \
	libxslt-devel \
	zlib-devel \
	bzip2-devel \
	lz4-devel \
	snappy-devel \
	python36-devel \
	openssh-clients \
	openssh-server \
	initscripts \
	vim \
	&& yum clean all
# install cmake from source code
RUN wget http://www.cmake.org/files/v2.8/cmake-2.8.10.2.tar.gz \
	&& tar -zxvf cmake-2.8.10.2.tar.gz \
	&& cd cmake-2.8.10.2 \
	&& ./bootstrap \
	&& gmake -j \
	&& gmake install \
	&& echo -e "r00t\nr00t" | passwd root \
	&& rm -f /run/nologin \
	&& useradd -m postgres && echo -e "postgres\npostgres" | passwd postgres \
	&& echo "postgres	ALL=(ALL)	ALL" >> /etc/sudoers 

USER postgres
WORKDIR /home/postgres
# set up authorized key for fast access, download source code from github and set up environment variables
RUN ssh-keygen -q -t rsa -f ~/.ssh/id_rsa -N '' \
	&& cp ~/.ssh/id_rsa.pub ~/.ssh/authorized_keys \
	&& git clone https://github.com/alibaba/PolarDB-for-PostgreSQL.git \
	&& echo -e "export PATH=\"\$HOME/polardb/polardbhome/bin:\$PATH\"\nexport LD_LIBRARY_PATH=\"\$HOME/polardb/polardbhome/lib:\$LD_LIBRARY_PATH\"" >> ~/.bashrc

# set up PolarDB-for-PostgreSQL
USER root
RUN ssh-keygen -q -t rsa -b 2048 -f /etc/ssh/ssh_host_rsa_key -N '' \
        && ssh-keygen -q -t ecdsa -f /etc/ssh/ssh_host_ecdsa_key -N '' \
        && ssh-keygen -t dsa -f /etc/ssh/ssh_host_ed25519_key -N '' \
	&& echo -e "Port 22\nPermitRootLogin yes" >> /etc/ssh/sshd_config \
	&& echo -e "StrictHostKeyChecking no" >> /etc/ssh/ssh_config \
        && sudo su -l root -c "/usr/sbin/sshd -D &" \
	&& sudo su -l postgres -c "/home/postgres/PolarDB-for-PostgreSQL/onekey.sh all" \
 	&& echo "host	all	all	0.0.0.0/0	trust" >> /home/postgres/polardb/data/dn_master/pg_hba.conf

# add script to run after initdb
RUN echo "sudo su -l root -c \"/usr/sbin/sshd -D &\" \
	&& sudo su -l postgres -c \"pgxc_ctl -c /home/postgres/polardb/polardb_paxos.conf start all\" \
	&& tail -f /dev/null" > /start.sh \
	&& chmod +x /start.sh

# add health check script 
RUN echo -e "sudo su -l postgres -c \"psql -p 10001 -d postgres -c 'select 1;'\"" > /healthcheck.sh \
	&& chmod +x /healthcheck.sh


ENTRYPOINT ["sh", "/start.sh"]

HEALTHCHECK --interval=4s --start-period=6s CMD /healthcheck.sh

