name=$1

if [ -d name ]
    then
    echo "[ERROR] Directory already exists!"
    exit 1
fi

mkdir -p $name

cd $name

cp ../src/_analysis_functions.py ./
cp ../src/_ensemble_analysis.py ./
cp ../src/_user_functions.py ./
cp ../src/_user_functions_test_set.py ./

cp ../src/submit.sh ./

touch NOTES.md

echo "Directory created successfully."


