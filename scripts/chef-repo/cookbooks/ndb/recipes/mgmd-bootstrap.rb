require 'fileutils'
libpath = File.expand_path '../../libraries', __FILE__
require File.join(libpath, 'inifile')

ndb_kthfs_services node[:ndb][:kthfs_services] do
 node_id node[:mgm][:id]
 action :install_mgmd
end
