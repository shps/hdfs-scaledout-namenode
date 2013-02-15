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


template "#{node[:kthfs][:dir]}/hadoop/etc/hadoop/hdfs-site.xml" do
  source "hdfs-site.xml.erb"
  owner "root"
  group "root"
  mode "755"
end

template "#{node[:kthfs][:dir]}/hadoop/etc/hadoop/core-site.xml" do
  source "core-site.xml.erb"
  owner "root"
  group "root"
  mode "755"
end


%w(namenode-format start-nn stop-nn start-dn stop-dn).each do |file|
  template "#{node[:kthfs][:dir]}/hadoop/sbin/scripts/#{file}.sh" do
    source "#{file}.sh.erb"
    owner "root"
    group "root"
    mode "755"
  end
end

bash 'format-nn' do
	user "root"
	code <<-EOH
		#{node[:kthfs][:dir]}/hadoop/sbin/scripts/namenode-format.sh
	EOH
# not_if "test -d #{node[:kthfs][:dir]}/hadoop/tmp/dfs/name/current/"
end

bash 'start-nn' do
	user "root"
	code <<-EOH
		#{node[:kthfs][:dir]}/hadoop/sbin/scripts/start-nn.sh
	EOH
end

bash 'start-dn' do
	user "root"
	code <<-EOH
		#{node[:kthfs][:dir]}/hadoop/sbin/scripts/start-dn.sh
	EOH
end
