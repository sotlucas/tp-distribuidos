# Script to change the file of flights to be processed. It creates a symbolic link to the file in the temp folder.
# It needs to be run from the root folder of the project.
# Usage: ./change_file.sh <file_name>
# Example: ./change_file.sh temp/archivo_500.csv
ln -sfnr $1 temp/archivo.csv