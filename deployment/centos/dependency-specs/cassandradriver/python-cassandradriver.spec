%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Name:           python-cassandradriver
Version:        1.0.0b7
Release:        2%{?dist}
Group:          Development/Libraries
License:        UNKNOWN
Source0:        cassandra-driver-%{version}.tar.gz
BuildRoot:      %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires:  python-devel, python-setuptools,
Provides:       %{name} = %{version}-%{release}
Summary:        Python driver for Cassandra
Requires:       python-blist
Requires:       python-futures
Requires:       python-scales


%define debug_package %{nil}

%description
DataStax Python Driver for Apache Cassandra (Beta)

%prep
%setup -q -n cassandra-driver-%{version}



%build
%{__python} setup.py  build


%install
rm -rf %{buildroot}
%{__python} setup.py install -O1  --root %{buildroot}

#rm -rf %{buildroot}/usr/lib
#rm -rf %{buildroot}/usr/src



%clean
rm -rf %{buildroot}


%files
%defattr(644,root,root,755)
#%{python_sitelib}/*
/usr/*

%changelog
* Sun Dec 13 2013 Max Mazur <mmaxur@mirantis.com>
 - For Havanna  (fuel 4.0)
