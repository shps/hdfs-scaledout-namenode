maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"
description      "Installs/Configures the Chef server and Ironfan"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.1"

%w{ ubuntu debian }.each do |os|
  supports os
end

depends 'java'

recipe "chefserver::default", "Installs and configures a Chef Server, Knife Client, and Ironfan"
