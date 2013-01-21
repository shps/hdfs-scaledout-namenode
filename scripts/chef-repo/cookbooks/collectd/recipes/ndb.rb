
  bash "update_collectd_ndb" do
   code <<-EOF
     args = "#\n"
     args << "# plugins for ndb\n" 
     args << "#\n"
     args << "LoadPlugin dbi\n"
     args << "<Plugin dbi>\n"
     args << "  <Query "free_dm">\n"
     args << "    Statement "SELECT node_id, total FROM ndbinfo.memoryusage where memory_type LIKE 'Data memory'"\n"
     args << "      # Use with MySQL 5.0.0 or later\n"
     args << "      MinVersion 50000\n"
     args << "    <Result>\n"
     args << "      Type "gauge"\n"
     args << "      InstancePrefix "free_data_memory"\n"
     args << "      InstancesFrom "node_id"\n"
     args << "      ValuesFrom "total"\n"
     args << "    </Result>\n"
     args << "  </Query>\n"
     args << "  <Query "free_im">\n"
     args << "    Statement "SELECT node_id, total FROM ndbinfo.memoryusage where memory_type LIKE 'Index memory'"\n"
     args << "      # Use with MySQL 5.0.0 or later\n"
     args << "      MinVersion 50000\n"
     args << "    <Result>\n"
     args << "      Type "gauge"\n"
     args << "      InstancePrefix "free_index_memory"\n"
     args << "      InstancesFrom "node_id"\n"
     args << "      ValuesFrom "total"\n"
     args << "    </Result>\n"
     args << "  </Query>\n"
     args << "\n"
     args << "  <Database "ndbinfo">\n"
     args << "    Driver "mysql"\n"
     args << "    DriverOption "host" "???.sics.se"\n"
     args << "    DriverOption "username" "kthfs"\n"
     args << "    DriverOption "password" "kthfs"\n"
     args << "    DriverOption "dbname" "ndbinfo"\n"
     args << "    SelectDB "ndbinfo"\n"
     args << "    Query "free_dm"\n"
     args << "    Query "free_im"\n"
     args << "  </Database>\n"
     args << "</Plugin>\n"

     args << ""
     echo #{args} >> node[:ndb][:collectd_conf] 
    EOF
    not_if { `grep ndb node[:ndb][:collectd_conf]` }
  end


# Need to restart collectd daemon

service "collectd" do
  action [:restart]
end

