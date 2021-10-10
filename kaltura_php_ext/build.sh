rm -rf ./build
mkdir -p ./build
cd ./build
cp ../config.m4 .
cp -r ../php_versions/ php_versions/
cp ../*.c .
cp ../*.h .
phpize
./configure --enable-kaltura
make