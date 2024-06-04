# Denum
## An Effective and Efficient Log Compressor by Parsing Numbers



##### Dataset

Loghub: 

https://github.com/logpai/loghub

download these datasets and copy them into Logs/{logname}/{logname}.log

### Compress

###  - C++ implementation

##### Dependencies

python >= 3.7.3

regex = 2012.1.8

gcc >= 9.4.0

PCRE2 = 10.34


##### 1. Compile


`g++ -O3 -std=c++17 -o denum_compress denum_compress.cpp -lboost_iostreams -lpthread -lpcre2-8`

#### 2. Execution

Assume the chunksize is set to 100000, and the target log file is Logs/HDFS/HDFS.log

`denum_compress HDFS 100000`

### - Python implementation

Assume the target log file is Logs/HDFS/HDFS.log

1. `cd Denum_Package`

2. `python3 compress.py HDFS`

### Decompress



1. `cd Denum_Package`

2. `python3 decompress.py HDFS`