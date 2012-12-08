
include_recipe "ndb"

for script in node[:mgm][:scripts]
  template "#{node[:ndb][:base_dir]}/scripts/#{script}" do
    source "#{script}.erb"
    owner "root"
    group "root"
    mode 0644
    variables({
       :ndb_dir => node[:ndb][:base_dir],
       :mysql_dir => node[:mysql][:base_dir]
    })
  end
end 



