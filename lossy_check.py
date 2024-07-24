
# file_name1='Mac_result/Mac.log'
# file_name2= '../Benchmakr_datasets/Mac/Mac_1.log'


file_name1='Logs/Apache/Apache.log'
file_name2= 'decompress_output/Apache/1/DecompressedApache.log'

# file_name1='Hadoop_result/1/Hadoop.log'
# file_name2= '../Benchmakr_datasets/Hadoop/Hadoop.log'

# file_name1='Proxifier_result/Proxifier.log'
# file_name2= '../Benchmakr_datasets/Proxifier/Proxifier.log'

# file_name1='OpenSSH_result/1/OpenSSH.log'
# file_name2= '../Benchmakr_datasets/OpenSSH/OpenSSH.log'

# file_name1= '../Benchmakr_datasets/Mac/Mac_1.log'
# file_name2= '../Decompressed_results/Mac_result/Mac.log'

# file_name1= '../Decompressed_results/Linux_result/Linux.log'
# file_name2= '../Benchmakr_datasets/Linux/Linux_1.log'

# file_name1='HPC_result/1/HPC.log'
# file_name2= '../Benchmakr_datasets/HPC/HPC.log'
#
# file_name1='OpenStack_result/1/OpenStack.log'
# file_name2= '../Benchmakr_datasets/OpenStack/OpenStack.log'

# file_name1='HDFS_result/1/HDFS.log'
# file_name2= '../Benchmakr_datasets/HDFS/HDFS__.log'

with open(file_name2,mode='r',encoding='utf-8') as file:
    origin=file.readlines()
with open(file_name1,mode='r',encoding='utf-8') as file:
    result=file.readlines()

index=0
count=0
for re,o in zip(result,origin):
    re=re.replace('\n',' ')
    re=re.replace(' ','')
    o=o.replace('\n',' ')
    o=o.replace(' ','')

    if re !=o:
        print('pos '+str(index))
        print('Line: '+str(count)+ '    failed match')
        print("result = "+re)
        print("origin = " +o)
        print("\n")
        count += 1
    index+=1
if count !=0:
    print("Unsuccessful Match num in total ==  "+str(count))
else:
    print("The decompressed file is the same as the original file.\n \n The whole compression&decompression process is Lossless!")