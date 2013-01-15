include 'kthfs'
#include_recipe "collectd"
include "collectd"

ndb_kthfs_services "#{node[:ndb][:kthfs_services]}" do
 node_id node[:mysql][:id]
 action :install_mysqld
end

# TODO -COLLECTD - conf file update
# collectd_plugin "dbi" do
#   options :data_dir=>"/var/lib/collectd/rrd"
# end
 
