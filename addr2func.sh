echo "Stripping..."
cut -c -17 trace/dcache_trace.out > trace/strip_trace
echo "Stripped..."
echo "Converting addresses to function names..."
while read line
do
	addr2line -e bpfs -f $line
done < trace/strip_trace > trace/addr2func
echo "Conversion done..."
echo "Eliminating noise..."
while read line
do
        read line2
        if [ $line != "??" ]
        then
                echo $line
        fi
done < trace/addr2func > trace/func_list
sort trace/func_list > trace/func_list_sort
echo "Eliminated noise."
echo "Counting uniq lines..."
uniq -c trace/func_list_sort > trace/func_count
echo "Counting done..."
echo "Sorting..."
sort -r -g trace/func_count > trace/func_count_sort
