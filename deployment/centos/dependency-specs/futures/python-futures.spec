%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print get_python_lib(1)")}

Name:           python-futures
Version:        2.1.5
Release:        1%{?dist}
Group:          Development/Libraries
Source0:        futures-%{version}.tar.gz
BuildRoot:      %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)
BuildRequires:  python-devel, python-setuptools,
Provides:       %{name} = %{version}-%{release}
Summary:        Backport of the concurrent.futures package from Python 3.2
License:        BSD
BuildArch:      noarch



%define debug_package %{nil}

%description
Backport of the concurrent.futures package from Python 3.2

%prep
%setup -q -n futures-%{version}



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
