#
# Cookbook Name:: kthfs
# Recipe:: default
#
# Copyright 2013, KTH
#
# All rights reserved - Do Not Redistribute
#


# Download tarball
 
remote_file "/tmp/#{node[:kthfs][:tarfile]}" do
	source "#{node[:kthfs][:url]}"
	owner "#{node[:kthfs][:user]}"
	mode "0644"
end

# Create dir
directory node[:kthfs][:dir] do
	recursive true
	owner "root"
	mode "0644"
	action :create
end

# Extract kthfs-demo
bash 'extract-kthfs' do
	user "root"
	code <<-EOH
		tar -xf /tmp/#{node[:kthfs][:tarfile]} -C #{node[:kthfs][:dir]}
	EOH
end
