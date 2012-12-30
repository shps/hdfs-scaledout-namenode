for install_gem in node[:chefclient][:gems]

  cookbook_file "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem" do
    source "#{install_gem}.gem"
    owner node[:kthfs][:user]
    group node[:kthfs][:user]
    mode 0755
    action :create_if_missing
  end

  gem_package "#{install_gem}" do
    source "#{Chef::Config[:file_cache_path]}/#{install_gem}.gem"
    action :install
  end

end 


