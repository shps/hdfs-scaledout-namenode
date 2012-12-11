require "fog"

def exec_commands_in_server(server, cmds)
  results = server.ssh(cmds) do |stdout, stderr|
    $stdout.puts stdout unless stdout.empty?
    $stderr.puts stderr unless stderr.empty?
  end

  unless results.all? { |res| res.status == 0 }
    puts "\nWhoops! something went wrong! :("
    exit 1
  end

  results
end

conn = Fog::Compute.new(provider: 'AWS', region: 'sa-east-1')

puts "* Bootstrapping ec2 instance..."
server = conn.servers.bootstrap(
  key_name: "augusto",
  username: "ubuntu",
  flavor_id: "t1.micro",
  image_id: "ami-2e845d33" # Ubuntu 12.04 64-bit Canonical
)

puts "* Starting Chef Server install..."

create_chef_rb = <<CMD
(
cat <<'EOP'
file_cache_path "/tmp/chef-solo"
cookbook_path "/tmp/chef-solo/cookbooks"
EOP
) > /etc/chef/solo.rb
CMD

create_chef_json = <<CMD
(
cat <<'EOP'
{
  "chef_server": {
    "server_url": "http://localhost:4000"
  },
  "run_list": [ "recipe[chef-server::rubygems-install]", "recipe[chef-server::apache-proxy]" ]
}
EOP
) > chef.json
CMD

exec_commands_in_server(server, [
  "sudo apt-get update",
  "sudo apt-get upgrade -y",
  "sudo apt-get install -y build-essential ruby1.9.3",
  "sudo gem install chef --no-ri --no-rdoc",
  "sudo mkdir /etc/chef",
  "sudo bash -c '#{create_chef_rb}'",
  "sudo bash -c '#{create_chef_json}'",
  #"sudo chef-solo -c /etc/chef/solo.rb -j chef.json -r http://s3.amazonaws.com/chef-solo/bootstrap-latest.tar.gz"
  # ^^ broken in ubuntu 12.04. Had to update java cookbook.
  "sudo chef-solo -c /etc/chef/solo.rb -j chef.json -r http://restorando-ops.s3.amazonaws.com/bootstrap-chef.tgz",
  # This hack shouldn't be necessary, but chef-server cookbook is currently broken in ubuntu 12.04.
  "sudo ln -s /usr/local/bin/chef-solr /usr/bin/chef-solr",
  "sudo ln -s /usr/local/bin/chef-expander /usr/bin/chef-expander",
  "sudo ln -s /usr/local/bin/chef-server /usr/bin/chef-server",
  "sudo ln -s /usr/local/bin/chef-client /usr/bin/chef-client",
  "sudo ln -s /usr/local/bin/chef-solo /usr/bin/chef-solo",
  "sudo /etc/init.d/chef-solr start",
  "sudo /etc/init.d/chef-expander start",
  "sudo /etc/init.d/chef-server start"
])

sleep 5
puts "\nChef Server should be running. Phew! :)"

puts "\n* Configuring kife..."

exec_commands_in_server(server, [
  "mkdir ~/.chef",
  "sudo cp /etc/chef/{validation.pem,webui.pem} ~/.chef",
  "sudo knife configure -i --defaults -r $HOME/chef-repo",
  "sudo chown -R $USER ~/.chef",
  "knife client list"
])

puts "\n* Creating knife desktop client..."

client_name = ENV["USER"]
exec_commands_in_server(server, [
  "knife client create #{client_name} -d -a -f desktop_client.pem",
  "knife client show #{client_name}"
])
client_key = server.ssh("cat desktop_client.pem").first.stdout

puts
puts "Your knife client name is: #{client_name}"
puts "Your knife client key is:"
puts client_key

validation_key = server.ssh("sudo cat /etc/chef/validation.pem").first.stdout
puts "\nYour validation key is:"
puts validation_key

puts "\nExample knife.rb:"
knife_rb = %q{
current_dir = File.dirname(__FILE__)
log_level                :info
log_location             STDOUT
node_name                "CLIENT_NAME"
client_key               "#{current_dir}/CLIENT_NAME.pem"
validation_client_name   "chef-validator"
validation_key           "#{current_dir}/validation.pem"
chef_server_url          "https://SERVER_NAME"
cache_type               'BasicFile'
cache_options( :path => "#{ENV['HOME']}/.chef/checksums" )
cookbook_path            ["#{current_dir}/../cookbooks"]
}.gsub("SERVER_NAME", server.dns_name).gsub("CLIENT_NAME", client_name)
puts knife_rb

puts
puts "Server instance ID: #{server.id}"
puts "Server public DNS: #{server.dns_name}"

exit 0
