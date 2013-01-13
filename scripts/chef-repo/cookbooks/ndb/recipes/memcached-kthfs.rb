include_recipe "kthfs"

ndb_kthfs_services "#{node[:ndb][:kthfs_services]}" do
 node_id node[:memcached][:id]
 action :install_memcached
end
