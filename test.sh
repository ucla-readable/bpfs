
echo $0
echo $1

if [[ "$1" == "r" ]]
then
	echo "Welcome"
else
	echo "Good Bye."
fi

while read line
do
	echo $line
done < last_log
