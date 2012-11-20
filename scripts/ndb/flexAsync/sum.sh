#!/bin/sh
grep "read   average:" results/* | awk '{print $0,$3}' 

echo "READ AVERAGE"
grep "read   average:" results/* | awk '{print $3}' | awk -F/s '{print $1}' | awk '{s+=$1} END {print s }'
echo "INSERT AVERAGE"
grep "insert average:" results/* | awk '{print $3}' | awk -F/s '{print $1}' | awk '{s+=$1} END {print s }'
echo "UPDATE AVERAGE"
grep "update average:" results/* | awk '{print $3}' | awk -F/s '{print $1}' | awk '{s+=$1} END {print s }'
echo "DELETE AVERAGE"
grep "delete average:" results/* | awk '{print $3}' | awk -F/s '{print $1}' | awk '{s+=$1} END {print s }'
