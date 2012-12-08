#!/bin/sh 


USERID=`id | sed -e 's/).*//; s/^.*(//;'`
if [ "X$USERID" != "Xroot" ]; then
 if [ "X$USERID" = "Xroot" ]; then
   echo ""
   echo "You started cluster as user: 'root'."
   echo "You should start cluster as user: 'root'."
   echo "If you continue, you will change ownership of database files"
   echo "from 'root' to 'root'."
# TODO: return -2
 else
   echo ""
   echo "You started the cluster as user: '$USERID'."
   echo "You should start the cluster as user: 'root'."
   echo "If you continue, you will change ownership of database files"
   echo "from 'root' to '$USERID'."
# TODO: return -2
 fi
 
 echo ""
start_as_wrong_user() 
{
  echo -n "Do you really want to start the cluster as user 'root'? y/n/h(help) "
  read ACCEPT
  case $ACCEPT in
   y | Y)
      ;;
   n | N)
      echo ""
      echo "Bye.."
      echo ""
      exit 1
      ;;
    *)
      echo ""
      echo -n "Please enter 'y' or 'n'." 
      start_as_wrong_user
      ;;
   esac
}
start_as_wrong_user


fi  

echo "Test if a mysql server is already running on this host."

MYSQL_SOCKET=`<%= @ndb_dir %>/scripts/get-mysqld-1-socket.sh`
<%= @mysql_dir %>/bin/mysqladmin -S $MYSQL_SOCKET -s -u root ping 
# Don't redirect error, as this will give a '0' return result &> /dev/null
if [ $? -eq 0 ] ; then
 echo "A MySQL Server is already running at socket . Not starting another MySQL Server at this socket."
 exit 1
fi

<%= @mysql_dir %>/bin/mysqld --defaults-file=<%= @ndb_dir %>/mysql_1/my-7-2-8.cnf 1> <%= @ndb_dir %>/logs/mysql-stdout-1.log 2>&1 &
RES=`echo $?`
exit $RES

