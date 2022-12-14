DIRECTORY_NAME=$1

if [ -d $DIRECTORY_NAME ]
    then
    echo "[ERROR] Directory \"$DIRECTORY_NAME\" already exists!"
    exit 1
fi

echo "Creating a directory: $DIRECTORY_NAME"

mkdir -p $DIRECTORY_NAME

cd $DIRECTORY_NAME

cp ../src/_user_functions_test_set.py ./
cp ../src/submit.sh ./

mv _user_functions_test_set.py _user_functions.py

sed -i -e "s/brightness_temperature/$DIRECTORY_NAME/" submit.sh
sed -i -e "s/PLACEHOLDER_JOBNAME/$DIRECTORY_NAME/" submit.sh

touch NOTES.md

echo "Directory $DIRECTORY_NAME created successfully!"