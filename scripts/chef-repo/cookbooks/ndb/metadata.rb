name             "mysql cluster"
maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"
description      "Installs/Configures mysql-cluster and defines resources for managing authorization"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.1"
recipe            "ndb::mgmd", "Installs a MySQL Cluster management server (ndb_mgmd)"
recipe            "ndb::ndbd", "Installs a MySQL Cluster data node (ndbd)"
recipe            "ndb::mysqld", "Installs a MySQL Server connected to the MySQL Cluster (mysqld)"

%w{ ubuntu debian }.each do |os|
  supports os
end

# attribute "mysql/password",
#   :display_name => "MySQL Server Password for 'kthfs' user",
#   :description => "Password for the 'kthfs' user",
#   :default => "kthfs"


