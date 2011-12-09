Summary: iPlant Job Execution Framework
Name: iplant-jobexec
Version: 0.2.0
Release: 6
Epoch: 0
Group: Applications
BuildRoot: %{_tmppath}/%{name}-%{version}-buildroot
License: BSD
Provides: iplant-jobexec
Requires: node >= v0.2.2
Requires: iplant-node-launch >= 0.0.1-5
Requires: iplant-nodejs-libs
Source0: %{name}-%{version}.tar.gz

%description
iPlant Job Execution Framework

%prep
%setup -q
mkdir -p $RPM_BUILD_ROOT
mkdir -p $RPM_BUILD_ROOT/usr/local/lib/node/jobexec
mkdir -p $RPM_BUILD_ROOT/usr/local/lib/node/jobexec/conf
mkdir -p $RPM_BUILD_ROOT/usr/local/bin
mkdir -p $RPM_BUILD_ROOT/var/log/iplant-jobexec/
mkdir -p $RPM_BUILD_ROOT/etc/init.d/
mkdir -p $RPM_BUILD_ROOT/etc/logrotate.d/

%build

%install
cp src/*.js $RPM_BUILD_ROOT/usr/local/lib/node/jobexec/
cp -r src/types $RPM_BUILD_ROOT/usr/local/lib/node/jobexec/
cp conf/service.conf $RPM_BUILD_ROOT/etc/iplant-jobexec.conf
cp conf/logrotate.conf $RPM_BUILD_ROOT/etc/logrotate.d/iplant-jobexec
install -m755 src/iplant-jobexec $RPM_BUILD_ROOT/etc/init.d/

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(0764,iplant,iplant)
%attr(0775,iplant,iplant) /usr/local/lib/node/jobexec
%config %attr(0644,root,root) /etc/iplant-jobexec.conf
%config %attr(0644,root,root) /etc/logrotate.d/iplant-jobexec
%attr(0755,root,root) /etc/init.d/iplant-jobexec
