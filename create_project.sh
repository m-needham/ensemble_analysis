DIRECTORY_NAME=$1

if [ -d $DIRECTORY_NAME ]
    then
    echo "[ERROR] Directory \"$DIRECTORY_NAME\" already exists!"
    exit 1
fi

echo "Creating a directory: $DIRECTORY_NAME"

mkdir -p $DIRECTORY_NAME

cd $DIRECTORY_NAME

cp ../src/_user_functions.py ./
cp ../src/submit.sh ./

sed -i -e "s/brightness_temperature/$DIRECTORY_NAME/" submit.sh

touch NOTES.md

echo "Directory $DIRECTORY_NAME created successfully!"