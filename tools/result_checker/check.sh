# Checks if the results files are correct (with the first 2 million rows).
# Meant to be run from the root directory.
cd ./results || exit
list="max_avg distancias dos_mas_rapidos tres_escalas"
for i in $list; do
    echo "Test $i"
    ls -t *$i.txt | head -1 | xargs cat | diff -q <(sort ../tools/result_checker/$i.txt) <(sort -)
    if [ $? -eq 0 ]; then
        echo "OK"
    else
        echo "ERROR"
    fi
done
