bash "install_chef_server" do
user "#{node[:chef][:user]}"
ignore_failure true
code <<-EOF

# Update aptitude and install ruby dependencies
sudo apt-get update -y
sudo /usr/bin/apt-get install -y -q build-essential openssl libreadline6 libreadline6-dev curl git-core zlib1g zlib1g-dev libssl-dev libyaml-dev libsqlite3-dev sqlite3 libxml2-dev libxslt-dev autoconf libc6-dev ncurses-dev automake libtool bison subversion

# install some other needed packages
sudo apt-get -y -q update
sudo apt-get -y -q install curl
sudo apt-get -y -q install build-essential
sudo apt-get -y -q install couchdb
sudo apt-get -y -q install zlib1g-dev
sudo apt-get -y -q install libxml2-dev
sudo apt-get -y -q install nginx
sudo apt-get -y -q install openjdk-7-jre-headless
sudo apt-get -y -q install opscode-keyring
sudo apt-get -y -q install libgecode-dev
sudo apt-get -y -q install rabbitmq-server
`java -version 2> /dev/null` && sudo apt-get -y -q install openjdk-6-jdk


# install rvm multi-user
# http://beginrescueend.com/rvm/install/
if [ ! -e ~/usr/local/rvm/scripts/rvm ]
then
  sudo bash -s stable < <(curl \
    -s https://raw.github.com/wayneeseguin/rvm/master/binscripts/rvm-installer)
fi

# Manage some user roles - allow chef and default user to install gems
sudo useradd -m chef -s /bin/bash
sudo usermod -a -G admin chef
sudo usermod -a -G rvm chef
sudo usermod -a -G rvm `whoami`

# Copy a sudoers file that allows chef to run sudo without password
sudo cp sudoers /etc/sudoers

# This su creates a new login session for the active user, allowing the recent
# user modifications to count when rvm install is invoked.
#
#   - Put RVM into mixed mode (separate rubies and gemsets per user)
#   - Install 1.9.3 as default user
sudo su -l $USER -c "rvm user all; rvm install 1.9.3; rvm use 1.9.3 --default"

# Make sure the scripts are executable by chef
# chmod a+x /home/ubuntu/install-chef-server/*.sh;
# sudo chown -R chef /home/ubuntu/install-chef-server;

# Install 1.9.2 as default as chef user (using same login trick as before)
sudo su - chef -l -c "rvm install 1.9.2; rvm use 1.9.2 --default"

# Run the remainder of the chef-server install as chef
# sudo su - chef -l -c "cd /home/ubuntu/install-chef-server; 
#                      ./install-chef-server.sh"

EOF
end

bash "install_chef_server" do
user "chef"
ignore_failure true
code <<-EOF

cd /home/ubuntu/install-chef-server

PATH=${PATH}:/usr/local/sbin:/usr/sbin:/sbin:${GEM_PATH}
echo $PATH

STARTDIR=`pwd`

# read the config file
. conf/config

# add the opscode repo
echo "deb http://apt.opscode.com/ `lsb_release -cs`-0.10 main" | \
  sudo tee /etc/apt/sources.list.d/opscode.list > /dev/null
# and their key
sudo mkdir -p /etc/apt/trusted.gpg.d
if [ ! "`gpg --list-keys | grep 83EF826A`" ]
then
  EXITSTATUS=2
  while [ ${EXITSTATUS} == 2 ]
  do
    gpg --keyserver keys.gnupg.net --recv-keys 83EF826A
    EXITSTATUS=$?
  done
fi
gpg --export packages@opscode.com | \
  sudo tee /etc/apt/trusted.gpg.d/opscode-keyring.gpg > /dev/null

# RabbitMQ repo
echo "deb http://www.rabbitmq.com/debian/ testing main" | \
  sudo tee /etc/apt/sources.list.d/rabbit.list > /dev/null
if [ ! "`sudo apt-key list | grep Rabbit`" ]
then
  cd /tmp
  wget http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
  sudo apt-key add rabbitmq-signing-key-public.asc
fi

# configure rabbit (if it's not already done)
[ "`sudo rabbitmqctl list_vhosts | grep chef`" ] \
  || sudo rabbitmqctl add_vhost /chef
[ "`sudo rabbitmqctl list_users | grep chef`" ] \
  || sudo rabbitmqctl add_user chef testing
sudo rabbitmqctl set_permissions -p /chef chef ".*" ".*" ".*"
# we also like the rabbit webui management thing
sudo rabbitmq-plugins enable rabbitmq_management
sudo service rabbitmq-server restart

# install the chef gems (if we don't already have them)
for gem in chef-server chef-server-api chef-solr chef-server-webui 
do
  if [ ! "`gem list | grep \"${gem} \"`" ]
  then
    gem install ${gem} --no-ri --no-rdoc
  fi
done

# install the chef config file
sudo mkdir -p /etc/chef
sudo chown -R `whoami` /etc/chef
[ ${WEBUI_PASSWORD} ] || WEBUI_PASSWORD='password'
[ ${SERVERNAME} ] || SERVERNAME=`ip -f inet -o addr | grep eth0 \
  | tr -s ' ' ' ' | cut -d ' ' -f 4 | cut -d '/' -f 1`
cat ${STARTDIR}/files/server.rb | sed "s:SERVERNAME:${SERVERNAME}:" \
  | sed "s:PASSWORD:${WEBUI_PASSWORD}:" \
  | sudo tee /etc/chef/server.rb > /dev/null

# run the solr installer
# NOTE: THIS WILL NUKE ANY EXISTING CHEF SOLR CONFIGURATION AND DATA
sudo mkdir -p /var/chef
sudo chown -R `whoami` /var/chef
chef-solr-installer -f

# we do this so we don't have to run as root
sudo mkdir -p /var/log/chef
sudo chown -R `whoami` /var/log/chef

# setup the services
[ ${CHEF_SERVER_USER} ] || CHEF_SERVER_USER=`whoami`
# the chef gems supply some upstart scripts, but they run everything as root
# we'd rather run as whatever chef user we're using
for file in `find /usr/local/rvm/ | grep debian/etc/init/ | grep -v client`
do
  outfile=`basename ${file}`
  service=${outfile%.conf}

# horrendous sed monster to make these jobs run as our user 
  cat ${file} | \
    sed "s:    :  :g" | \
    sed "s:test -x .* || \(.*\):su - ${CHEF_SERVER_USER} -c \"which ${service}\" || \1:" | \
    sed "s:exec /usr/bin/${service} \(.*\):script\n  su - ${CHEF_SERVER_USER} -c \"${service} \1\"\nend script:" | \
    sudo tee /etc/init/${outfile} > /dev/null

# symlinking here means we get tab-complete in 'service foo start'-type stuff
# (among other things, I'm sure)
  sudo ln -sf /lib/init/upstart-job /etc/init.d/${service}
# actually start the thing
  sudo service ${service} start 2> /dev/null || sudo service ${service} restart
done

# set up the nginx vhosts to proxy this stuff
#cd ${STARTDIR}/files/nginx
#for file in `ls`
#do
#  NAME=`echo ${file} | tr "[:lower:]" "[:upper:]"`NAME

## @OrganizedGang explained this indirect reference voodoo to me
#  REPLACEMENT=${!NAME}
#  [ ${REPLACEMENT} ] || REPLACEMENT=${file}
#  cat ${file} | sed "s:${NAME}:${REPLACEMENT}:" |\
#    sudo tee /etc/nginx/sites-available/${REPLACEMENT} > /dev/null
#done

cd ${STARTDIR}
for line in `cat conf/vhosts`
do
  UPSTREAM=`echo ${line} | cut -d ':' -f 1`
  PORT=`echo ${line} | cut -d ':' -f 2`
  SERVERNAME=`echo ${line} | cut -d ':' -f 3`
  cat files/nginx/vhost.template |\
    sed "s:UPSTREAM:${UPSTREAM}:" |\
    sed "s:PORT:${PORT}:" |\
    sed "s:SERVERNAME:${SERVERNAME}:" |\
    sudo tee /etc/nginx/sites-available/${SERVERNAME} > /dev/null
  [ ${PORT} == "4040" ] && WEBUI="http://${SERVERNAME}"
  [ ${PORT} == "4000" ] && CHEFSERVER="http://${SERVERNAME}:4000"
  sudo ln -s /etc/nginx/sites-available/${SERVERNAME} /etc/nginx/sites-enabled
done

sudo service nginx restart

# end

echo

echo "Chef-server is at ${CHEFSERVER}"
echo "Chef WebUI is at ${WEBUI}"
echo "WebUI login: admin/${WEBUI_PASSWORD}"


EOF
end
