#require 'fileutils'
libpath = File.expand_path '../libraries', __FILE__
require File.join(libpath, 'inifile')

kthfs_dir = File.dirname(node[:ndb][:kthfs_services])

directory "#{kthfs_dir}" do
  owner node[:ndb][:user]
  group node[:ndb][:user]
  mode "755"
  action :create
  recursive true
end

file node[:ndb][:kthfs_services] do
  owner "root"
  group "root"
  mode 00755
  action :create_if_missing
end

