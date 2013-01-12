include 'kthfs'

ndb_kthfs_services "#{node[:ndb][:kthfs_services]}" do
 node_id node[:mysql][:id]
 action :install_mysqld
end
