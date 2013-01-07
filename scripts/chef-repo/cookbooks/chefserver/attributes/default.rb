# Default values for configuration parameters
default[:chef][:user] = "kthfs"
default[:chef][:password] = "kthfs"
default[:chef][:base_dir] = "/var/lib/chefserver"
default[:chef][:port] = 4000
default[:chef][:client] = "webui"
default[:chef][:org] = "kth"

default[:aws][:user] = "jdowling"
default[:aws][:key_id] = "AKIAJRR5Z45K4ZSIFYWQ"
default[:aws][:secret] = "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"

default[:chef][:gems] = %w{ ruby-shadow-2.1.4 addressable-2.3.2 amqp-0.9.8 builder-3.1.4 bundler-1.2.3 bunny-0.6.0 bunny-0.8.0 chef-10.16.4 chef-expander-10.16.4 chef-server-api-10.16.4 chef-server-webui-10.16.4 chef-solr-10.16.4 coderay-1.0.8 daemons-1.1.9 dep_selector-0.0.8 em-http-request-1.0.3 erubis-2.7.0 eventmachine-1.0.0 excon-0.16.10 extlib-0.9.15 fast_xs-0.8.0 fog-1.8.0 formatador-0.2.4 gorillib-0.5.0 haml-3.1.7 highline-1.6.15 ipaddress-0.8.0 json-1.7.5 merb-assets-1.1.3 merb-core-1.1.3 merb-haml-1.1.3 merb-helpers-1.1.3 merb-param-protection-1.1.3 mime-types-1.19 minitest-4.3.3 mixlib-authentication-1.3.0 mixlib-cli-1.2.2 mixlib-config-1.1.2 mixlib-log-1.4.1 moneta-0.7.1 net-ssh-2.6.2 net-ssh-gateway-1.1.0 net-ssh-multi-1.1 nokogiri-1.5.6 ohai-6.14.0 polyglot-0.3.3 rack-1.4.1 rake-10.0.3 rbvmomi-1.6.0 rdoc-3.12 rest-client-1.6.7 ruby-openid-2.2.2 ruby-shadow-2.1.4 systemu-2.5.2 thin-1.5.0 treetop-1.4.12 trollop-2.0 uuidtools-2.1.3 yajl-ruby-0.7.9 ironfan-4.7.1 }

#default[:chef][:gems] = %w{ chef-10.16.4 chef-server-api-10.16.4 chef-server-webui-10.16.4 chef-solr-10.16.4 }

default[:ironfan][:gems] = %w{ rake-10.0.3 i18n-0.6.1 multi_json-1.5.0 activesupport-3.2.10 builder-3.1.4 activemodel-3.2.10 addressable-2.3.2 archive-tar-minitar-0.5.2 bunny-0.8.0 erubis-2.7.0 highline-1.6.15 json-1.7.5 mixlib-log-1.4.1 mixlib-authentication-1.3.0 mixlib-cli-1.2.2 mixlib-config-1.1.2 mixlib-shellout-1.1.0 moneta-0.7.1 net-ssh-2.6.2 net-ssh-gateway-1.1.0 net-ssh-multi-1.1 ipaddress-0.8.0 systemu-2.5.2 yajl-ruby-1.1.0 ohai-6.14.0 mime-types-1.19 rest-client-1.6.7 polyglot-0.3.3 treetop-1.4.12 uuidtools-2.1.3 chef-10.16.4 hashie-1.2.0 chozo-0.3.0 minitar-0.5.4 facter-1.6.17 timers-1.0.2 celluloid-0.12.4 multipart-post-1.1.5 faraday-0.8.4 net-http-persistent-2.8 ridley-0.6.2 solve-0.4.1 thor-0.16.0 ffi-1.2.0 childprocess-0.3.6 log4r-1.1.10 net-scp-1.0.4 vagrant-1.0.5 berkshelf-1.1.1 bundler-1.2.3 coderay-1.0.8 configliere-0.4.18 diff-lcs-1.1.3 gherkin-2.11.5 cucumber-1.2.1 excon-0.16.10 formatador-0.2.4 nokogiri-1.5.6 ruby-hmac-0.4.0 fog-1.8.0 git-1.2.5 gorillib-0.5.0 posix-spawn-0.3.6 grit-2.5.0 listen-0.7.0 lumberjack-1.0.2 method_source-0.8.1 slop-3.3.3 pry-0.9.10 guard-1.6.1 guard-chef-0.0.2 guard-cucumber-1.3.0 spoon-0.0.1 guard-process-1.0.5 ironfan-4.7.1 rdoc-3.12 jeweler-1.8.4 redcarpet-2.2.2 rspec-core-2.12.2 rspec-expectations-2.12.1 rspec-mocks-2.12.1 rspec-2.12.0 ruby_gntp-0.3.4 yard-0.8.3 knife-ec2-0.6.2 }
