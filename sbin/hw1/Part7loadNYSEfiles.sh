for i in `ls ./datas/NYSE/*[A-Z].csv`;  do
   mongoimport -d INFO7250 -c NYSE $i --type=csv --headerline  ;
   #echo $i
done
