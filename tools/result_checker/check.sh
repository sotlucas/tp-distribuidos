# Checks if the results files are correct (with the first 2 million rows).
# Meant to be run from the root directory.
cd ./results || exit
list="max_avg distancias dos_mas_rapidos tres_escalas"
for i in $list; do
    filename=$(ls -t *$i.txt | head -1)
    echo "Testing $filename"
    diff <(sort ../tools/result_checker/"$i".txt) <(sort "$filename")
    echo "---"
done
