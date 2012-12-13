maintainer       "Jim Dowling"
maintainer_email "jdowling@kth.se"
license          "GPL 2.0"
description      "Installs/Configures the KTHFS agent"
long_description IO.read(File.join(File.dirname(__FILE__), 'README.md'))
version          "0.1"

%w{ ubuntu debian }.each do |os|
  supports os
end

recipe "kthfsagent::default", "Installs and configures the KTHFS agent"
