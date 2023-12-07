# Checks if the results files are correct (with the first 2 million rows).
# Meant to be run from the root directory.
# Usage: ./tools/result_checker/check.sh

# Get all client ids
client_ids=$(ls -d ./results/client_* | cut -d '_' -f 2)

# Do the check for each client}
for client_id in $client_ids
do
    echo "Checking client $client_id..."
    # Create temp directory
    mkdir -p ./tools/result_checker/temp/client_$client_id

    cd ./results/client_$client_id || exit

    result_types="max_avg distancias tres_escalas"
    for result_type in $result_types
    do
        filename=$(ls -t *$result_type.txt | head -1)
        echo "Checking client_$client_id/$filename..."
        sudo sort -o ../../tools/result_checker/temp/client_$client_id/$filename $filename
        python3 ../../tools/result_checker/result_checker.py ../../tools/result_checker/temp/client_$client_id/$filename ../../tools/result_checker/$result_type.txt $result_type
        echo "---"
    done

    result_types="dos_mas_rapidos"
    for result_type in $result_types
    do
        filename=$(ls -t *$result_type.txt | head -1)
        echo "Checking client_$client_id/$filename..."
        # Sorts by the second column (origin of the route), then by the third column (destination of the route), then by the fourth column (duration of the route)
        sudo sort -t ',' -k 2,2 -k 3,3 -k 4,4 -o ../../tools/result_checker/temp/client_$client_id/$filename $filename
        python3 ../../tools/result_checker/result_checker.py ../../tools/result_checker/temp/client_$client_id/$filename ../../tools/result_checker/$result_type.txt $result_type
        echo "---"
    done

  rm -rf ./tools/result_checker/temp/client_$client_id
  cd ../../ || exit
done







