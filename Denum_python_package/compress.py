import os
import time
import argparse
import Denum_simplel as LZ

# 定义所有的设置
setting = {
    'Apache': {
        'dataset_name': 'Apache',
        'input_path': '../Logs/Apache/Apache.log',

    },
    'OpenSSH': {
        'dataset_name': 'OpenSSH',
        'input_path': '../Logs/OpenSSH/OpenSSH.log',

    },
    'Linux': {
        'dataset_name': 'Linux',
        'input_path': '../Logs/Linux/Linux.log',

    },
    'Proxifier': {
        'dataset_name': 'Proxifier',
        'input_path': '../Logs/Proxifier/Proxifier.log',

    },
    'Zookeeper': {
        'dataset_name': 'Zookeeper',
        'input_path': '../Logs/Zookeeper/Zookeeper.log',

    },
    'Mac': {
        'dataset_name': 'Mac',
        'input_path': '../Logs/Mac/Mac.log',

    },
    'HDFS': {
        'dataset_name': 'HDFS',
        'input_path': '../Logs/HDFS/HDFS.log',

    },
    'Android': {
        'dataset_name': 'Android',
        'input_path': '../Logs/Android/Android.log',

    },
    'BGL': {
        'dataset_name': 'BGL',
        'input_path': '../Logs/BGL/BGL.log',

    },
    'HPC': {
        'dataset_name': 'HPC',
        'input_path': '../Logs/HPC/HPC.log',

    },
    'Spark': {
        'dataset_name': 'Spark',
        'input_path': '../Logs/Spark/Spark.log',

    },
    'Hadoop': {
        'dataset_name': 'Hadoop',
        'input_path': '../Logs/Hadoop/Hadoop.log',

    },
    'HealthApp': {
        'dataset_name': 'HealthApp',
        'input_path': '../Logs/HealthApp/HealthApp.log',

    },
    'OpenStack': {
        'dataset_name': 'OpenStack',
        'input_path': '../Logs/OpenStack/OpenStack.log',

    },
    'Windows': {
        'dataset_name': 'Windows',
        'input_path': '../Logs/Windows/Windows.log',

    },
    'Thunderbird': {
        'dataset_name': 'Thunderbird',
        'input_path': '../Logs/Thunderbird/Thunderbird.log',

    },
}

def main():

    parser = argparse.ArgumentParser(description="Compress log files based on setting name.")
    parser.add_argument("setting_name", help="The name of the setting to be applied.")
    args = parser.parse_args()


    applied_setting = setting.get(args.setting_name)
    if not applied_setting:
        print(f"Error: Setting '{args.setting_name}' not found.")
        return


    compressor = LZ.dataloader(applied_setting)


    compressor.compress()

if __name__ == "__main__":
    main()