#!/usr/bin/php
<?php 
if ($argc != 5) {
   die("Usage: $argv[0] database startCountAt numDirs numFiles\n\n");
}
$database=$argv[1]
$inodeId=$argv[2];
$numDirs=$argv[3];
$numFilesPerDir=$argv[4];
echo "Writing $numDirs directories each containing $numFilesPerDir files\n";


function get_random_string($valid_chars, $length)
{
     $random_string = "";
    $num_valid_chars = strlen($valid_chars);
    for ($i = 0; $i < $length; $i++)
    {
        $random_pick = mt_rand(1, $num_valid_chars);
        $random_char = $valid_chars[$random_pick-1];
        $random_string .= $random_char;
    }
    return $random_string;
}

function get_its_row($parentId,$id,$isDir,$permission) {
      $name=get_random_string("abcdefghijklmnopqrstuvwxyzABCDEF", 10);
      $t = time();
      $row=$id.",'".$name."',".$parentId.",".$isDir.",1,".$t.",".$t.",'".$permission."',0,0,NULL,NULL,NULL,NULL,NULL,281474976710672,NULL,NULL,NULL,NULL" ;
      return $row;
}
function get_bi_row($id,$bid) {
     $numBytes=30;
      $t = time();
      $row=$bid.",0,".$id.",".$numBytes.",".$t.",1,0,".$t.",-1,0";
      $row.="##\n";
      return $row;
}

function get_triplets_row($bid,$storageId) {
     $numBytes=30;
      $row=$bid.",0,".$storageId;
      $row.="##\n";
      return $row;
}

$user="root";
$password="";
$host="127.0.0.1:3306";
$connection=mysql_connect($host,$user,$password);
@mysql_select_db($database) or die( "Unable to select database");
$query="SELECT storageId from DatanodeInfo limit 1";
$result=mysql_query($query);
echo "Number of storageIds found in DatanodeInfo table (should be 1): ".mysql_num_rows($result)."\n";
$storageId=mysql_result($result,0,"storageId");

$query="SELECT permission from INodeTableSimple limit 1";
$result=mysql_query($query);
$permission=mysql_result($result,0,"permission");
#$permission=get_random_string("abcdefghijklmnopqrstuvwxyzABCDEF", 20);

$rootId=0;
$numBytes=0;
$n=0;

for ($d = 0; $d < $numDirs; $d++) 
{ 
  $inodeId++;
  $pid = $inodeId;
  $r=get_its_row($rootId,$inodeId,"1",$permission);
  $n+=1;
  $inodeInsert="INSERT INTO INodeTableSimple values (".$r."),";
  $blockInsert="INSERT INTO BlockInfo values ";
  $tripletInsert="INSERT INTO triplets values ";
  for ($f = 0; $f < $numFilesPerDir; $f++) 
    {
      // INodeTableSimple
      $inodeId++;
      $its=get_its_row($pid,$inodeId,"0",$permission);
      $inodeInsert.="(".$its.")";
      if ($f != ($numFilesPerDir-1)) {
       $inodeInsert.=",";
      }
      // BlockInfo
      $bid=$inodeId;
      $bi=get_bi_row($inodeId,$bid);
      $blockInsert.="(".$bi.")";
      if ($f != ($numFilesPerDir-1)) {
       $blockInsert.=",";
      }

      // triplets
      $ti=get_triplets_row($bid,$storageId);
      $tripletInsert.="(".$bi.")";
      if ($f != ($numFilesPerDir-1)) {
       $tripletInsert.=",";
      }

      // DatanodeInfo
      // asssume already exists. StorageId passed as input parameter
     $n+=1;
   }
   echo "$n\n";
   #echo "Executing: $inodeInsert";
   mysql_query($inodeInsert);
   mysql_query($blockInsert);
   mysql_query($tripletInsert);
   sleep(1);
}

echo "Finished.\n";


mysql_close();
?> 
