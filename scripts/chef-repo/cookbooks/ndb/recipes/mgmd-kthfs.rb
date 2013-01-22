include_recipe "ndb::kthfs"

ndb_kthfs_services node[:ndb][:kthfs_services] do
 node_id node[:mgm][:id]
 action :install_mgmd
end
