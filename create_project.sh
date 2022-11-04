name=$1

if [ -d $name ]
    then
    echo "[ERROR] Directory \"$name\" already exists!"
    exit 1
fi

echo "Creating a directory: $name"

mkdir -p $name

cd $name

cp ../src/_analysis_functions.py ./
cp ../src/_ensemble_analysis.py ./
cp ../src/_user_functions.py ./
cp ../src/_user_functions_test_set.py ./

cp ../src/submit.sh ./

touch NOTES.md


echo "Directory $name created successfully!"