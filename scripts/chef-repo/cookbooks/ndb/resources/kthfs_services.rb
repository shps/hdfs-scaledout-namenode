actions :install_ndbd, :install_mgmd, :install_mysqld

attribute :ini_file, :kind_of => String, :name_attribute => true
attribute :node_id, :kind_of => Integer, :required => true

