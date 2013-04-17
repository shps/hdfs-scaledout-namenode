#!/usr/bin/php

<?php 
if ($argc != 4) {
   die("Usage: $argv[0] startCountAt numDirs numFiles\n\n");
}
$inodeId=$argv[1];
$numDirs=$argv[2];
$numFilesPerDir=$argv[3];
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
      $name=get_random_string("abcdefghijklmnopqrstuvwxyzABCDEF", 5);
  #echo "Adding name: $name \n";
      $t = time();

      #$row=$id.",".$name.",".$parentId.",".$isDir.",1,".$t.",".$t.",".$permission.",0,0,0,\N,\N,\N,0,281474976710672,0,\N,\N,\N" ;
      $row=$id.",".$name.",".$parentId.",\N,1,".$t.",".$t.",".$permission.",0,0,\N,\N,\N,\N,\N,281474976710672,\N,\N,\N,\N" ;
      $row.="##\n";
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

$storageId=get_random_string("abcdefghijklmnopqrstuvwxyzABCDEF", 30);
$permission=get_random_string("abcdefghijklmnopqrstuvwxyzABCDEF", 20);



#$numDirLevels=1;

$f1 = "/tmp/INodeTableSimple.dat";
$f2 = "/tmp/BlockInfo.dat";
$f3 = "/tmp/triplets.dat";

// Open the file for appended writing 
$fp1 = fopen($f1, "w") or die("Couldn't open $f1 for writing!"); 
$fp2 = fopen($f2, "w") or die("Couldn't open $f2 for writing!"); 
$fp3 = fopen($f3, "w") or die("Couldn't open $f3 for writing!"); 

$rootId=0;
$numBytes=0;
$n=0;

for ($d = 0; $d < $numDirs; $d++) 
{ 
//  $pid = rand();
  $inodeId++;
  $pid = $inodeId;
  $r=get_its_row($rootId,$inodeId,"1",$permission);
  $numBytes += fwrite($fp1, $r) or die("Couldn't write values to $f1!"); 
  $n+=1;
 // Files are stored in Leaf nodes in Filesystem
  for ($f = 0; $f < $numFilesPerDir; $f++) 
    {
      // INodeTableSimple
      $inodeId++;
      $its=get_its_row($pid,$inodeId,"0",$permission);
      $numBytes += fwrite($fp1, $its) or die("Couldn't write values to $f1!"); 

      // BlockInfo
      $bid=$inodeId;
      $bi=get_bi_row($inodeId,$bid);
      $numBytes += fwrite($fp2, $bi) or die("Couldn't write values to $f2!"); 

      // triplets
      $ti=get_triplets_row($bid,$storageId);
      $numBytes += fwrite($fp3, $ti) or die("Couldn't write values to $f3!"); 

      // DatanodeInfo
      // asssume already exists. StorageId passed as input parameter
     $n+=1;
   }
}

fclose($fp1); 
fclose($fp2); 
fclose($fp3); 
echo "Wrote $n rows containing $numBytes bytes to files successfully!\n"; 
echo "Now run from mysql client:\n";
#echo "LOAD DATA INFILE '".$f1."' INTO TABLE INodeTableSimple FIELDS TERMINATED BY ','  LINES TERMINATED BY '##\\n' (@var4) SET isDir= CAST(@var4 AS UNSIGNED), (@var11) SET isUnderConstruction= CAST(@var11 AS UNSIGNED), (@var15) SET isClosedFile= CAST(@var15 AS UNSIGNED), (@var17) SET isDirWithQuota= CAST(@var17 AS UNSIGNED);\n";
echo "LOAD DATA INFILE '".$f1."' INTO TABLE INodeTableSimple FIELDS TERMINATED BY ','  LINES TERMINATED BY '##\\n';\n";
echo "LOAD DATA INFILE '".$f2."' INTO TABLE BlockInfo FIELDS TERMINATED BY ','  LINES TERMINATED BY '##\\n';\n";
echo "LOAD DATA INFILE '".$f3."' INTO TABLE triplets FIELDS TERMINATED BY ','  LINES TERMINATED BY '##\\n';\n";

?> 
