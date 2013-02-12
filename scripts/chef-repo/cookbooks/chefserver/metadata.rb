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

attribute "chef/server_url",
  :display_name => "Chef server URL",
  :description => "URL fo rthe Chef Server",
  :default => "http://localhost:4000"

attribute "aws/zone",
  :display_name => "AWS Zone",
  :description => "Amazon Web Services Zone.",
  :default => "eu-west-1"

attribute "aws/instance_type",
  :display_name => "AWS Instance",
  :description => "AWS Instance Type: 'instance' or 'ebs'",
  :default => "ebs"

attribute "aws/image_id",
  :display_name => "AWS image id",
  :description => "AWS Image Id",
  :default => "ami-ffcdce8b"

attribute "aws/bootstrap_distro",
  :display_name => "AWS Bootstrap distro",
  :description => "AWS bootstrap distro",
  :default => "ubuntu11.04-ironfan"
