ndb_waiter "#{node[:mysql][:base_dir]}/bin/ndb_waiter" do
#  action :wait_until_cluster_ready
  action :nothing
end


