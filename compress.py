import os
import time
import argparse
import Denum_simplel as LZ

# 定义所有的设置
setting = {
    'Apache': {
        'dataset_name': 'Apache',
        'input_path': '../Logs/Apache/Apache.log',
        'logformat': '\[<Time>\] \[<Level>\] <Content>',
        'digit_header': [],
        'caldelta': [],
    },
    'OpenSSH': {
        'dataset_name': 'OpenSSH',
        'input_path': '../Logs/OpenSSH/OpenSSH.log',
        'logformat': '<Date> <Day> <Time> <Component> sshd\[<Pid>\]: <Content>',
        'digit_header': [],
        'caldelta': [],
    },
    'Linux': {
        'dataset_name': 'Linux',
        'input_path': '../Logs/Linux/Linux.log',
        'logformat': '<Month> <Date> <Time> <Level> <Component>(\[<PID>\])?: <Content>',
        'digit_header': [],
        'caldelta': ['Time', 'PID'],
    },
    'Proxifier': {
        'dataset_name': 'Proxifier',
        'input_path': '../Logs/Proxifier/Proxifier.log',
        'logformat': '\[<Time>\] <Program> - <Content>',
        'digit_header': ['PID'],
        'caldelta': ['Time'],
    },
    'Zookeeper': {
        'dataset_name': 'Zookeeper',
        'input_path': '../Logs/Zookeeper/Zookeeper.log',
        'logformat': '<Date> <Time> - <Level>  \[<Node>:<Component>@<Id>\] - <Content>',
        'digit_header': [],
        'caldelta': ['Time'],
    },
    'Mac': {
        'dataset_name': 'Mac',
        'input_path': '../Logs/Mac/Mac.log',
        'logformat': '<Month>  <Date> <Time> <User> <Component>\[<PID>\]( \(<Address>\))?: <Content>',
        'digit_header': [],
        'caldelta': ['Time'],
    },
    'HDFS': {
        'dataset_name': 'HDFS',
        'input_path': '../Logs/HDFS/HDFS.log',
        'logformat': '<Date> <Time> <Pid> <Level> <Component>: <Content>',
        'digit_header': [],
        'caldelta': ['Timestamp'],
    },
    'Android': {
        'dataset_name': 'Android',
        'input_path': '../Logs/Android/Android.log',
        'logformat': '<Date> <Time>  <Pid>  <Tid> <Level> <Component>: <Content>',
        'digit_header': [],
        'caldelta': ['Date', 'Time'],
    },
    'BGL': {
        'dataset_name': 'BGL',
        'input_path': '../Logs/BGL/BGL.log',
        'logformat': '<Label> <Timestamp> <Date> <Node> <Time> <Content>',
        'digit_header': [],
        'caldelta': ['Timestamp', 'Date', 'Time'],
    },
    'HPC': {
        'dataset_name': 'HPC',
        'input_path': '../Logs/HPC/HPC.log',
        'logformat': '<LogId> <Node> <Component> <State> <Time> <Flag> <Content>',
        'digit_header': ['LogId', 'Flag', 'Time'],
        'caldelta': ['LogId', 'Time'],
    },
    'Spark': {
        'dataset_name': 'Spark',
        'input_path': '../Logs/Spark/Spark.log',
        'logformat': '<Date> <Time> <Level> <Component>: <Content>',
        'digit_header': [],
        'caldelta': ['Date', 'Time'],
    },
    'Hadoop': {
        'dataset_name': 'Hadoop',
        'input_path': '../Logs/Hadoop/Hadoop.log',
        'logformat': '<Date> <Time> <Level> \[<Process>\] <Component>: <Content>',
        'digit_header': [],
        'caldelta': ['Date', 'Time'],
    },
    'HealthApp': {
        'dataset_name': 'HealthApp',
        'input_path': '../Logs/HealthApp/HealthApp.log',
        'logformat': '<Time>\|<Component>\|<Pid>\|<Content>',
        'digit_header': [],
        'caldelta': [],
    },
    'OpenStack': {
        'dataset_name': 'OpenStack',
        'input_path': '../Logs/OpenStack/OpenStack.log',
        'logformat': '<Logrecord> <Date> <Time> <Pid> <Level> <Component> \[(req-<ADDR2> )?(<ADDR3> )?(<ADDR4> )?<ADDR1>\] <Content>',
        'digit_header': ['PID'],
        'caldelta': ['Time'],
    },
    'Windows': {
        'dataset_name': 'Windows',
        'input_path': '../Logs/Windows/Windows.log',
        'logformat': '<Date> <Time>, <Level>                  <Component>    <Content>',
        'digit_header': [],
        'caldelta': ['Time'],
    },
    'Thunderbird': {
        'dataset_name': 'Thunderbird',
        'input_path': '../Logs/Thunderbird/Thunderbird.log',
        'logformat': '<Label> <Timestamp> <Date> <User> <Month> <Day> <Time> <Location> <Component>(\[<PID>\])?: <Content>',
        'digit_header': [],
        'caldelta': ['Timestamp'],
    },
}

def main():
    # 使用argparse解析命令行参数
    parser = argparse.ArgumentParser(description="Compress log files based on setting name.")
    parser.add_argument("setting_name", help="The name of the setting to be applied.")
    args = parser.parse_args()

    # 获取应用的设置
    applied_setting = setting.get(args.setting_name)
    if not applied_setting:
        print(f"Error: Setting '{args.setting_name}' not found.")
        return

    # 初始化压缩器
    compressor = LZ.dataloader(applied_setting)

    # 执行压缩
    compressor.compress()

if __name__ == "__main__":
    main()