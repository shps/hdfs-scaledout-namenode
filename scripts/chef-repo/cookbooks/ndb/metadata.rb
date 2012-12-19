maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"
description      "Installs/Configures mysql-cluster and defines resources for managing authorization"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.1"

%w{ ubuntu debian }.each do |os|
  supports os
end

depends "kthfsagent"

recipe "ndb::default", "Installs and configures MySQL Cluster"

