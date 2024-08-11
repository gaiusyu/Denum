import glob
import random
import shutil
import tarfile
import time
from collections import defaultdict
from datetime import datetime
from collections import Counter
import pyppmd
import regex as re
import pandas as pd
import os
import copy
from itertools import zip_longest

from multiprocessing import Pool


class dataloader():

    def __init__(self,dataset_information):
 
        self.path=dataset_information['input_path']
        self.logname=dataset_information['dataset_name']
 
        self.logheader=[]
        self.logcontent=[]
        self.LZMA_list=[]
        self.PPMD_list=[]

    def load_data(self,chunkID):
        headers, regex = self.generate_logformat_regex(self.logformat)
        self.logheader,self.logcontent = self.log_to_dataframe(self.path, regex, headers,self.caldelta,chunkID)

    def generate_logformat_regex(self, logformat):
        """ Function to generate regular expression to split log messages
        """
        headers = []
        splitters = re.split(r'(<[^<>]+>)', logformat)
        regex = ''
        for k in range(len(splitters)):
            if k % 2 == 0:
                splitter = re.sub(' ', '\\\s+', splitters[k])
                regex += splitter
            else:
                header = splitters[k].strip('<').strip('>')
                if header in self.digit_headers:
                    regex += '(?P<%s>\d+)' % header
                else:
                    regex += '(?P<%s>.*?)' % header
                headers.append(header)
        regex = re.compile('^' + regex + '$')
        return headers, regex

    def log_to_dataframe(self, log_file, regex, headers,caldelta,chunkID):
        """ Function to transform log file to dataframe
        """
        log_headers=[]
        log_content=[]
        linecount = 0
        delta_temp=defaultdict(list)
        last_num=defaultdict(int)

        with open(log_file, 'r', encoding="ISO-8859-1") as fil:
            for line in fil.readlines():
                    linecount += 1
                    header_list = ''
                    try:
                        match = re.search(regex, line, timeout=0.5)
                        for header in headers:
                            if header in caldelta:
                                matched_time=match.group(header)
                                timenumbers = re.findall(r'\d+', matched_time)
                                if header not in last_num.keys():
                                    last_num[header]=int(''.join(timenumbers))
                                    modified_line = re.sub(r'\d+', '<*>', matched_time)
                                    with open('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/'+caldelta+'.txt','w', encoding="ISO-8859-1") as file:
                                        file.write(modified_line+'\n'+str(''.join(timenumbers)))
                                else:
                                    timenumbers=''.join(timenumbers)
                                    delta=int(timenumbers)-int(last_num[header])
                                    last_num[header]=int(timenumbers)
                                    delta_temp[header].append(delta)
                                header_list = header_list+"<"+header+">"
                                continue
                            if header != 'Content':
                                header_list=header_list+(match.group(header))
                        log_content.append(match.group('Content'))
                        log_headers.append(header_list)
                    except Exception as e:

                        log_headers.append('')
                        log_content.append(line.strip())
                        pass
            for key in delta_temp:
                self.store_content_with_ids(delta_temp[key], key, type='number', chunkID=chunkID)

                #
                # with open('../Output/' + self.logname+'/'+str(chunkID) + '/PPMd/'+key+'delta.bin', 'wb') as file:
                #     for t in delta_temp[key]:
                #         file.write(elastic_encoder(t))
        log_headers,num1 = self.replace_numbers_and_save(log_headers,chunkID,'header')
        log_content,num2 = self.replace_numbers_and_save(log_content,chunkID,'content')
        num1.extend(num2)
        ids_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname+ 'nums.bin'
        with open(ids_file_path, 'ab') as ids_file:
            for id in num1:
                ids_file.write(elastic_encoder(int(id)))
        #self.store_content_with_ids(num1,'nums',type='number',chunkID=chunkID)
        return log_headers,log_content

    def store_content_with_ids(self,input, output,type,chunkID,compressor):
        content_to_id = {}
        id_to_content = {}
        id_counter = 1
        ids_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+self.logname+str(output)+'ids.bin'  # 这里替换为你想要存储ID的文件路径
        if type=='number':
            mapping_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname + str(
                output) + 'mapping.bin'
        else:
            mapping_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/'+compressor+'/'+self.logname+str(output)+'mapping.txt'  # 这里替换为你想要存储ID和内容映射的文件路径
        id_list = []
        for line in input:
                if isinstance(line,str):
                    line = line.strip()
                if line != None:
                    if line not in content_to_id:
                        content_id = id_counter
                        content_to_id[line] = content_id
                        id_to_content[content_id] = line
                        id_counter += 1
                    else:
                        content_id = content_to_id[line]
                    id_list.append(content_id)
                else:
                    id_list.append(0)
        if type=='number':

            with open(ids_file_path, 'ab') as ids_file, \
                        open(mapping_file_path, 'ab') as mapping_file:
                    for content, content_id in content_to_id.items():
                        mapping_file.write(elastic_encoder(int(content)))
                    for id in id_list:
                        ids_file.write(elastic_encoder(id))
        else:
            with open(ids_file_path, 'ab') as ids_file, \
                    open(mapping_file_path, 'a', encoding="ISO-8859-1") as mapping_file:
                for content, content_id in content_to_id.items():
                    mapping_file.write(f'{content}\n')
                for id in id_list:
                    ids_file.write(elastic_encoder(id))




    def store_numlist_with_ids(self,input, output,type,chunkID):
        content_to_id = {}
        id_to_content = {}
        id_counter = 1
        ids_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+self.logname+str(output)+'ids.bin'  # 这里替换为你想要存储ID的文件路径
        if type=='number':
            mapping_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + self.logname + str(
                output) + 'mapping.bin'
        else:
            mapping_file_path = '../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/'+self.logname+str(output)+'mapping.txt'  # 这里替换为你想要存储ID和内容映射的文件路径
        id_list = []
        for line in input:
                line=' '.join(line)
                if isinstance(line,str):
                    line = line.strip()
                if line != None:
                    if line not in content_to_id:
                        content_id = id_counter
                        content_to_id[line] = content_id
                        id_to_content[content_id] = line
                        id_counter += 1
                    else:
                        content_id = content_to_id[line]
                    id_list.append(content_id)
                else:
                    id_list.append(0)
        if type=='number':

            with open(ids_file_path, 'ab') as ids_file, \
                        open(mapping_file_path, 'ab') as mapping_file:
                    for content, content_id in content_to_id.items():
                        mapping_file.write(elastic_encoder(int(content)))
                    for id in id_list:
                        ids_file.write(elastic_encoder(id))
        else:
            with open(ids_file_path, 'ab') as ids_file, \
                    open(mapping_file_path, 'a', encoding="ISO-8859-1") as mapping_file:
                for content, content_id in content_to_id.items():
                    mapping_file.write(f'{content}\n')
                for id in id_list:
                    ids_file.write(elastic_encoder(id))
        # else:
        #     with open(ids_file_path, 'ab') as ids_file, \
        #             open(mapping_file_path, 'a', encoding='utf-8') as mapping_file:
        #             for content, content_id in content_to_id.items():
        #                 mapping_file.write(f'{elastic_encoder(int(content))}\n')
        #             for id in id_list:
        #                 ids_file.write(elastic_encoder(id))



    def kernel_compress(self,chunkID):
        create_tar('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd','../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/temp.tar')
        compress_tar_with_ppmd('../Output/'+self.logname+'/'+str(chunkID)+'/PPMd/temp.tar', '../Output/'+self.logname+'/'+str(chunkID)+'/temp.ppmd')
        achieved_size_ppmd = get_file_size('../Output/'+self.logname+'/'+str(chunkID)+'/temp.ppmd')
        General7z_compress_lzma('../Output/'+self.logname+'/'+str(chunkID)+'/lzma')
        achieved_size_lzma = get_file_size('../Output/'+self.logname+'/'+str(chunkID)+'/lzma/'+'temp.tar.'+'xz')

        General7z_compress_bzip2('../Output/' + self.logname + '/' + str(chunkID) + '/bzip2')
        achieved_size_bzip2 = get_file_size(
            '../Output/' + self.logname + '/' + str(chunkID) + '/bzip2/' + 'temp.tar.' + 'bz2')

        totalsize=achieved_size_lzma+achieved_size_bzip2
        print('achieved size = '+str(totalsize))

        return totalsize

    def kernel_decompress(self,chunkID,type):
        file_path='../Output/'+self.logname+'/'+str(chunkID)+'/lzma/temp.tar.'+type
        decompress_path='../decompress_output/'+self.logname+'/'+str(chunkID)
        create_and_empty_directory(decompress_path)
        if type=='xz':
            with tarfile.open(file_path) as tar:
                tar.extractall(path=decompress_path)


    def process_chunk(self,chunkID, chunk_data, logname):

        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/PPMd')
        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/lzma')
        create_and_empty_directory('../Output/' + logname + '/' + str(chunkID) + '/bzip2')
        Denum_logs = self.replace_numbers_and_save_by_order_binary(chunk_data, chunkID, 'all')
        Denum_logs = self.variable_extract(Denum_logs, chunkID)
        self.store_content_with_ids(Denum_logs, "all", type='str', chunkID=chunkID, compressor='lzma')
        achieved_size = self.kernel_compress(chunkID)
        return achieved_size

    def compress(self):
        chunkID = 1
        size_total = 0
        achieved_size = 0
        time0 = time.perf_counter()

        # Read the entire log file
        log_file_path = '/home/cyy-test/C++project/Logs/' + self.logname + '/' + self.logname + '.log'
        if not os.path.exists(log_file_path):
            print(f'Log file {log_file_path} does not exist.')
            return

        with open(log_file_path, 'r', encoding="ISO-8859-1") as fil:
            print("what")
            logs = fil.readlines()
        
        # Split logs into chunks of 100000 lines
        chunks = [logs[i:i + 100000] for i in range(0, len(logs), 100000)]

        # Use multiprocessing to process chunks in parallel
        with Pool(processes=4) as pool:
            results = pool.starmap(self.process_chunk, [(i + 1, chunk, self.logname) for i, chunk in enumerate(chunks)])

        # Accumulate results from all chunks
        for  achieved_size_chunk in results:
            achieved_size += achieved_size_chunk

        size_total = get_file_size(log_file_path)
        compression_ratio = size_total / achieved_size
        print('CR = ' + str(compression_ratio))
        
        time1 = time.perf_counter()
        timecost = time1 - time0
        CS = size_total / (1024 * 1024) / timecost
        print('CS = ' + str(CS) + ' MB/S')

    def replace_numbers_and_save_by_order_binary(self,input_file,chunkID,type):

            grouped, dict = self.replace_and_group(input_file)
            modified_lines=grouped

            for key in dict:
                label=key[1:-1]
                # if label=='N' or label=='I':
                #     label='general'

                ids_file_path = '../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + "_"+label+ '_.bin'
                with open(ids_file_path, 'ab') as ids_file:
                        if label=='N' or label=='I' :
                            for id in dict[key]:
                                ids_file.write(elastic_encoder(int(id)))
                        else:
                            save=delta_transform(dict[key])
                            for id in save:
                                ids_file.write(elastic_encoder(int(id)))

                # self.store_content_with_ids(numbers, "all"+type+str(order),type='number',chunkID=chunkID)
            return modified_lines
    #
    # def delimeter_mining(self,logs):
    #     temp=logs.copy()
    #     random.shuffle(temp)
    #     lenth=[]
    #     sample=[]
    #     for log in temp:
    #         if len(log) not in lenth:
    #             lenth.append(len(log))
    #             sample.append(log)
    #         else:
    #             continue
    #         if len(lenth)>=10:
    #             break
    #     delimeters=self.find_special_chars_with_high_freq(sample)
    #
    #     return delimeters
    #
    # def find_special_chars_with_high_freq(self,str_list, freq_threshold=10):
    #     candidate=[',',' ','|',';','[',']','(',')']
    #     char_counter = Counter()
    #     for s in str_list:
    #         for char in s.strip():
    #             if char in candidate:
    #                 char_counter[char] += 1
    #
    #     result = [char for char, count in char_counter.items() if count > freq_threshold]
    #
    #     return result
    #
    # def delimeter_mining(self, logs):
    #     temp = logs.copy()
    #     random.shuffle(temp)
    #     lenth = set()
    #     sample = []
    #     for log in temp:
    #         log_len = len(log)
    #         if log_len not in lenth:
    #             lenth.add(log_len)
    #             sample.append(log)
    #         if len(lenth) >= 10:
    #             break
    #     delimiters = self.find_special_chars_with_high_freq(sample)
    #
    #     return delimiters

    def find_special_chars_with_high_freq(self, str_list, freq_threshold=10):
        candidates = [',', ' ', '|', ';', '[', ']', '(', ')']
        char_counter = Counter()
        for s in str_list:
            stripped_s = s.strip()
            filtered_chars = [char for char in stripped_s if char in candidates]
            char_counter.update(filtered_chars)

        result = [char for char, count in char_counter.items() if count > freq_threshold]

        return result
    def store_processed_logs(self,logs,chunkID):
        ids_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/lzma/processed' + self.logname + 'template.bin'
        dict=defaultdict(list)
        id_dict={}
        v_id=[]
        id=0
        delimeter=self.delimeter_mining(logs)
        for log in logs:
            split = self.split_by_multiple_delimiters(delimeter, log.strip())
            lenth=len(split)
            tag=self.count_special_characters(delimeter,log,lenth)
            if tag not in id_dict:
                id_dict[tag]=id
                id+=1
            label=id_dict[tag]
            dict[tag].append(split)
            v_id.append(label)
        with open(ids_file_path,'wb') as file:
            for id in v_id:
                file.write(elastic_encoder(int(id)))
        self.split_and_save_template(dict,chunkID)


    def variable_extract(self,logs,chunkID):

        varibale_set=[]
        modified_lines=[]
        digit_pattern = re.compile(r'\d')
        regex_pattern, delimiters = self.delimeter_mining(logs)
        for log in logs:
            modified_line=''
            split = self.split_by_multiple_delimiters(regex_pattern, log.strip())
            for word in split:
                if digit_pattern.search(word):
                    modified_line=modified_line+'<*>'
                    varibale_set.append(word)
                else:
                    modified_line=modified_line+word
            modified_lines.append(modified_line)
        self.store_content_with_ids(varibale_set,'variableset','str',chunkID,compressor='lzma')
        return modified_lines

    # def variable_extract(self, logs, chunkID):
    #     variable_set = []  # 使用集合以改善性能和自动去重
    #     modified_lines = []
    #     regex_pattern, delimiters = self.delimeter_mining(logs)
    #     digit_pattern = re.compile(r'\d')  # 预编译数字正则表达式
    #
    #     for log in logs:
    #         modified_line_parts = []  # 使用列表来收集字符串
    #         split = self.split_by_multiple_delimiters(regex_pattern, log.strip())
    #
    #         for word in split:
    #             if digit_pattern.search(word):  # 使用预编译的正则表达式
    #                 modified_line_parts.append('<*>')
    #                 if word not in variable_set:
    #                     variable_set.append(word)
    #                 variable_set.add(word)  # 使用 add 代替 append 来添加到集合
    #             else:
    #                 modified_line_parts.append(word)
    #
    #         modified_lines.append(''.join(modified_line_parts))
    #
    #     # 现在variable_set是一个集合，需要转换为列表
    #     self.store_content_with_ids(list(variable_set), 'variableset', 'str', chunkID, compressor='lzma')
    #     return modified_lines

    def delimeter_mining(self, logs):
        temp = logs.copy()
        random.shuffle(temp)
        lengths = set()
        sample = []
        for log in temp:
            log_len = len(log)
            if log_len not in lengths:
                lengths.add(log_len)
                sample.append(log)
            if len(lengths) >= 10:
                break
        delimiters = self.find_special_chars_with_high_freq(sample)
        regex_pattern = re.compile('(' + '|'.join(re.escape(delimiter) for delimiter in delimiters) + ')')
        return regex_pattern, delimiters

    def split_by_multiple_delimiters(self, regex_pattern, string_to_split):
        return regex_pattern.split(string_to_split)


    def count_special_characters(self,a, b,lenth):

        result = ""
        for char in a:
            count = b.count(char)
            if count > 0:
                result += str(count) + char
        return result

    def split_and_save_template(self,dict,chunkID):
        template_list=[]
        template_id=0
        var_dict={}
        var_id=0
        for key in dict:
            sub_id=0
            template=''
            group=self.split_and_group_by_space(dict[key])
            for lis in group:
                if len(set(lis))==1:
                    template=template+str(lis[0])
                else:
                    template=template+'<*>'
                    var_dict,var_id=self.store_variable(lis,var_dict,var_id,str(template_id)+'-'+str(sub_id),chunkID)
                sub_id+=1
            template_list.append(template)
            template_id+=1
        self.store_file(var_dict.keys(), chunkID, 'var_dict','lzma')
        self.store_file(template_list,chunkID,'template','lzma')


    def store_variable(self,var_list,var_dict,var_id,tag,chunkID):
        id_list=[]
        for var in var_list:
            if var not in var_dict:
                var_dict[var]=var_id
                var_id+=1
            id_list.append(var_dict[var])
        ids_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/lzma/processed' + self.logname +tag+ 'ids.bin'
        with open(ids_file_path,'wb') as file:
            for id in id_list:
                file.write(elastic_encoder(id))
        return var_dict,var_id

    def store_file(self,list,chunkID,tag,compressor):
        dict_file_path = '../Output/' + self.logname + '/' + str(
            chunkID) + '/'+compressor+'/processed' + self.logname +str(tag)+ '.txt'
        with open(dict_file_path,'w',encoding="ISO-8859-1") as file:
            for line in list:
                file.write(line)
                file.write('\n')


    def split_and_group_by_space(self,input_list):
        grouped = list(zip(*(s for s in input_list)))
        grouped = [list(group) for group in grouped]

        return grouped

    # def replace_and_group(self,lst):
    #     patterns = defaultdict(list)
    #     replaced = []
    #     # Define a replacement function
    #     def find_timestamp_and_combine(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<T>'].append(int(combined_number))
    #         return '<T>'
    #     def find_timestamp_and_combine2(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<TT>'].append(int(combined_number))
    #         return '<TT>'
    #     def find_timestamp_and_combine3(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<TTT>'].append(int(combined_number))
    #         return '<TTT>'
    #     def find_IP_and_combine(match):
    #         num = match.group()
    #         numbers = re.findall(r'\d+', num)
    #         combined_number = int(''.join(numbers)) # 将三个数字连接起来  # 转换为整数并添加到列表中
    #         patterns['<I>'].append(int(combined_number))
    #         return '<I>'
    #     def replace_with_pattern(match):
    #         num = match.group()
    #         alpha=['a','b','c','d','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
    #         pattern = f"<{len(num)}N{num[0]}>"
    #
    #         if len(num)==1 :
    #             pattern = f"<{alpha[len(num)]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         if len(num)==3 and num[0]=='0':
    #             pattern = f"<{alpha[len(num)]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         if len(num)>=15:
    #             return num
    #
    #         if len(num)>=4:
    #             pattern = f"<{alpha[len(num)]}{alpha[int(num[0])]}>"
    #             patterns[pattern].append(num)
    #             return pattern
    #
    #         else:
    #             patterns['<*>'].append(num)
    #             return '<n>'
    #
    #     for item in lst:
    #
    #         if self.logname=='BGL':
    #             replaced_item = re.sub(r'(\d{4})\.(\d{2})\.(\d{2})', find_IP_and_combine, item)
    #             replaced_item = re.sub(r'(\d{4})-(\d{2})-(\d{2})', find_timestamp_and_combine2, replaced_item)
    #             replaced_item = re.sub(r'(\d{2})\.(\d{2})\.(\d{2})', find_timestamp_and_combine, replaced_item)
    #             replaced_item = re.sub(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])', replace_with_pattern, replaced_item)
    #         else:
    #             replaced_item = re.sub(r'(\d+)\.(\d+)\.(\d+)\.(\d+)', find_IP_and_combine, item)
    #             replaced_item = re.sub(r'(\d+):(\d+):(\d+)\.(\d+)', find_timestamp_and_combine2, replaced_item)
    #             replaced_item = re.sub(r'(\d+):(\d+):(\d+)', find_timestamp_and_combine, replaced_item)
    #             replaced_item = re.sub(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])', replace_with_pattern, replaced_item)
    #         replaced.append(replaced_item)
    #
    #     # for key in patterns:
    #     #     if '<4N' in key:
    #     #         print(key)
    #     #         print(len(set(patterns[key])))
    #
    #     return replaced, dict(patterns)

    def replace_and_group(self, lst):
        patterns = defaultdict(list)
        replaced = []
        ip_pattern = re.compile(r'(\d+)\.(\d+)\.(\d+)\.(\d+)')
        timestamp_pattern_2 = re.compile(r'(\d+):(\d+):(\d+)\.(\d+)')
        timestamp_pattern_4 = re.compile(r'(\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+)')
        timestamp_pattern_3 = re.compile(r'(\d+):(\d+)')
        float_pattern = re.compile(r'(\d+)\.(\d+)')
        timestamp_pattern_1 = re.compile(r'(\d+):(\d+):(\d+)')
        num_pattern = re.compile(r'(?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9])')
        alpha = 'abcdefghijklmnopqrstuvwxyz'

        def find_and_combine(pattern_key, match):
            numbers = re.findall(r'\d+', match.group())
            combined_number = int(''.join(numbers))
            patterns[pattern_key].append(combined_number)
            return pattern_key

        def find_and_combineIP(pattern_key,  match):
            # 找出所有的数字
            numbers = re.findall(r'\d+', match.group())
            # 确保每个数字都是三位数，不足三位在前面补零
            padded_numbers = [num.zfill(3) for num in numbers]
            # 将这些数字组合成一个长数字
            combined_number = int(''.join(padded_numbers))
            # 将组合好的数字加入到对应的键下的列表中
            patterns[pattern_key].append(combined_number)
            return pattern_key

        def replace_with_pattern(match):
            num = match.group()
            if len(num) == 2 :
                pattern = f"<{alpha[len(num)-1]}>"
            elif len(num) == 3 :
                pattern = f"<{alpha[len(num)-1]}>"

            elif len(num) >= 15:
                return num
            elif len(num) >= 4:
                pattern = f"<{alpha[len(num)-1]}{alpha[int(num[0])-1]}>"
            else:
                pattern = f"<{alpha[len(num)-1]}>"
            patterns[pattern].append(num)
            return pattern

        for item in lst:
            if self.logname == 'BGL':
                #replaced_item = ip_pattern.sub(lambda m: find_and_combineIP('<I>', m), item)
                replaced_item = timestamp_pattern_4.sub(lambda m: find_and_combine('<TT>', m), item)
                replaced_item = timestamp_pattern_1.sub(lambda m: find_and_combine('<T>', m), replaced_item)
                replaced_item = num_pattern.sub(replace_with_pattern, replaced_item)
            else:
                replaced_item = ip_pattern.sub(lambda m: find_and_combineIP('<I>', m), item)
                replaced_item = timestamp_pattern_2.sub(lambda m: find_and_combine('<TT>', m), replaced_item)
                replaced_item = timestamp_pattern_1.sub(lambda m: find_and_combine('<T>', m), replaced_item)
                replaced_item = timestamp_pattern_3.sub(lambda m: find_and_combine('<TT>', m), replaced_item)
                #replaced_item = float_pattern.sub(lambda m: find_and_combine('<F>', m), replaced_item)
                replaced_item = num_pattern.sub(replace_with_pattern, replaced_item)
            replaced.append(replaced_item)

        return replaced, dict(patterns)

    def replace_numbers_and_save(self,input_file,chunkID,type):
        numbers_collection = {}
        modified_lines = []
        num_dict= {}
        num_ID=0
        for line in input_file:
                current_numbers = re.findall(r'\d+', line)
                modified_line = re.sub(r'\d+', '<*>', line)
                pattern = r'\B\S*<\*>\S*\B'
                matches = re.findall(pattern, modified_line)
                for i in matches:
                    if i not in numbers_collection:
                        numbers_collection[i]=[]
                        num_dict[i]=num_ID
                        num_ID+=1
                    placeholder_num=i.count('<*>')
                    save_nums = current_numbers[:placeholder_num]
                    current_numbers=current_numbers[placeholder_num:]
                    numbers_collection[i].append(save_nums)
                modified_lines.append(modified_line)
        with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/numformat.txt', 'w',
                      encoding="ISO-8859-1") as file:
                for key in numbers_collection.keys():
                    file.write(key)
                    file.write('\n')
        count = 0
        for key in numbers_collection.keys():
            num_list = numbers_collection[key]
            if key == '<>':
                self.store_numlist_with_ids(num_list,str(count),'0',chunkID)
                count += 1
            else:
                num_list=list(map(list, zip(*num_list)))
                sub = 0
                for nl in num_list:
                    with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/' + str(count) +'_'+str(sub)+ '.bin', 'wb') as file:
                        for num in nl:
                            file.write(elastic_encoder(int(num)))
                    sub+=1
                count += 1
            #
            #
            # if key == '<*>:<*>:<*>':
            #     num_list = numbers_collection[key]
            #     num_list=delta_transform(num_list)
            #     with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/'+str(count)+ '.bin','wb') as file:
            #         for num in num_list:
            #             file.write(elastic_encoder(num))
            #     count+=1
            # else:
            #     num_list=numbers_collection[key]
            #     with open('../Output/' + self.logname + '/' + str(chunkID) + '/lzma/'+str(count)+ '.txt','w',encoding="ISO-8859-1") as file:
            #         for num in num_list:
            #             file.write(num)
            #             file.write('\n')
            #     count+=1
        return modified_lines


    def split_and_store(self,inputlist,tag,chunkID):
        lines = inputlist
        min_blocks = min(len(line.split()) for line in lines if line.strip()) + 1
        adict=defaultdict(list)
        for line in lines:
            if not line.strip():
                continue
            parts = line.split()
            for i in range(min_blocks - 1):
                adict[i].append(parts[i])

            adict[min_blocks].append(' '.join(parts[min_blocks - 1:]))

        num=0
        for key in adict:
            temp=adict[key]
            self.store_content_with_ids(temp, tag+str(num),type='str',chunkID=chunkID)
            num+=1


    def decompress(self):
        chunkID=1
        while True:
            template_list=[]
            chunk_file_path = '../Output/'+self.logname+'/'+str(chunkID)
            if os.path.exists(chunk_file_path):
                #self.kernel_decompress(chunkID=chunkID,type='xz')
                print("1")
            else:
                break

            template_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'allmapping.txt'
            variable_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'variablesetmapping.txt'
            variable_id_path = '../decompress_output/' + self.logname + '/' + str(
                chunkID) + '/' + self.logname + 'variablesetids.bin'
            template_id_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/'+self.logname+'allids.bin'
            out_put_path='../decompress_output/'+self.logname+'/'+str(chunkID)+'/Decompressed'+self.logname+'.log'
            directory_to_search = '../decompress_output/'+self.logname+'/'+str(chunkID)+'/'
            pattern = '_*_.bin'
            matching_files = self.find_files(directory_to_search, pattern)

            with open(template_path,'r',encoding="ISO-8859-1") as file:
                templates=file.readlines()
            with open(variable_path,'r',encoding="ISO-8859-1") as file:
                variables_dict=file.readlines()
            with open(template_id_path,'rb') as bfile:
                binary=bfile.read()
                index_list = elastic_decoder_bytes(binary)
                a = set(index_list )
            with open(variable_id_path,'rb') as bfile:
                binary=bfile.read()
                v_index_list = elastic_decoder_bytes(binary)

            for id in index_list:
                template_list.append(templates[id-1])
            replacements=[]
            for id in v_index_list:
                replacements.append(variables_dict[id-1])
            replacement_iter = iter(replacements)
            no_num_logs=self.replace_placeholders('<*>',template_list,replacement_iter)

            alpha = 'abcdefghijklmnopqrstuvwxyz'
            num_pattern = rf'_([a-zA-Z]+)_\.bin'
            for file in matching_files:

                    # 使用正则表达式搜索
                    match = re.search(num_pattern, file)
                    t_id=match.group(1)

                    if t_id == 'I':
                        ph = '<I>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements = id_list
                        replacement_iter = iter(replacements)
                        no_num_logs = self.replace_placeholders( ph, no_num_logs, replacement_iter)
                    if t_id == 'a' or t_id == 'b':
                        ph='<'+str(t_id)+'>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements = id_list
                        replacement_iter = iter(replacements)

                        no_num_logs = self.replace_placeholders( ph, no_num_logs, replacement_iter)


                    else:
                        ph='<'+str(t_id)+'>'
                        with open(file, 'rb') as bfile:
                            binary = bfile.read()
                            id_list = elastic_decoder_bytes(binary)
                        replacements=delta_transform_inverse(id_list)
                        replacement_iter = iter(replacements)
                        print(ph)
                        no_num_logs=self.replace_placeholders(ph,no_num_logs,replacement_iter)


            with open(out_put_path,'w',encoding="ISO-8859-1") as output_file:
                for log in no_num_logs:
                    output_file.write(log)
            chunkID += 1

    def replace_placeholders(self,placeholder,str_list, replacement_iter):

        replaced_list = []
        for s in str_list:
            new_s = s
            while placeholder in new_s:
                try:
                    replacement = next(replacement_iter)
                    new_s = new_s.replace(placeholder, str(replacement).strip(), 1)  # 只替换第一个匹配项
                except StopIteration:
                    raise ValueError("Compressed file is damaged: Not enough replacement variables to replace all "+placeholder)
            replaced_list.append(new_s)

        return replaced_list
    def extract_pos_number(self,filename):
        pattern = r'Apache(\d+)-(\d+)ids\.bin'
        match = re.search(pattern, filename)
        if match:
            return int(match.group(2))
        else:
            return float('inf')





    def find_files(self,directory, pattern):
        search_pattern = os.path.join(directory, pattern)
        file_list = glob.glob(search_pattern)
        return file_list




    def header_decompress(self,mapping_path,id_path):
        with open(id_path, 'rb') as id_file:
            ids_b=id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(mapping_path, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping=mapping_file.readlines()

        header=[]
        for id in ids:
            id=id-1
            header.append(mapping[id])
        return header

    def content_decompress(self, mapping_path, id_path):
        with open(id_path, 'rb') as id_file:
            ids_b = id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(mapping_path, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping = mapping_file.readlines()

        content = []
        for id in ids:
            id=id-1
            content.append(mapping[id])

        return content

    def number_padding(self,header,content,num_mapping,num_ids):
        lenth=len(header)
        header.extend(content)
        logs=header
        with open(num_ids, 'rb') as id_file:
            ids_b = id_file.read()
            ids = elastic_decoder_bytes(ids_b)
        with open(num_mapping, 'r', encoding="ISO-8859-1") as mapping_file:
            mapping = mapping_file.readlines()

        nums = []
        for id in ids:
            id=id-1
            nums.append(mapping[id])

        replacements = iter(nums)


        decompressed_logs = [
            re.sub(r"<\*>", lambda match: next(replacements).strip(), template)
            for template in logs
        ]

        try:
            count=0
            while True:
                a=next(replacements)
                print(str(a))
                count+=1
        except Exception as e:
            print('count =='+str(count))
        header=decompressed_logs[:lenth]
        content=decompressed_logs[lenth:]
        decompressed_logs=[str1.strip() +' '+ str2 for str1, str2 in zip(header, content)]
        return decompressed_logs





def delta_transform(num_list):
        initial=int(num_list[0])
        new_list=[initial]
        last=initial
        for item in num_list[1:]:
            delta=int(item)-int(last)
            new_list.append(delta)
            last=item
        return new_list

def delta_transform_inverse(num_list):
        initial=num_list[0]
        new_list=[initial]
        last=initial
        for item in num_list[1:]:
            inverse=int(item)+int(last)
            new_list.append(inverse)
            last=inverse
        return new_list

def get_file_size(file_path):
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        return file_size
    else:
        return 0


def get_folder_size(folder_path):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(folder_path):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            total_size += os.path.getsize(file_path)

    return total_size



def ppmd_compress_file(input_filename, compressed_filename, level=5, mem_size=16 << 20):
    with open(input_filename, 'rb') as input_file:
        data_to_compress = input_file.read()

    encoder = pyppmd.Ppmd8Encoder(level, mem_size)
    compressed_data = encoder.encode(data_to_compress)

    with open(compressed_filename, 'wb') as compressed_file:
        compressed_file.write(compressed_data)

def ppmd_decompress_file(compressed_filename, decompressed_filename, level=5, mem_size=16 << 20):
    with open(compressed_filename, 'rb') as compressed_file:
        compressed_data = compressed_file.read()

    decoder = pyppmd.Ppmd8Decoder(level, mem_size)
    decompressed_data = decoder.decode(compressed_data)

    with open(decompressed_filename, 'wb') as decompressed_file:
        decompressed_file.write(decompressed_data)


def create_tar(directory_path, tar_archive_name):
    with tarfile.open(tar_archive_name, 'w') as tar:
        for root, _, files in os.walk(directory_path):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, start=directory_path)
                tar.add(full_path, arcname=arcname)


def compress_tar_with_ppmd(tar_archive_name, ppmd_archive_name, max_order=6, mem_size=16 << 20):
    with open(tar_archive_name, 'rb') as f_in:
        data_to_compress = f_in.read()

    encoder = pyppmd.Ppmd8Encoder(max_order, mem_size)
    compressed_data = encoder.encode(data_to_compress)

    with open(ppmd_archive_name, 'wb') as f_out:
        f_out.write(compressed_data)


def General7z_compress_lzma(dir):

    file_list=[]

    for root, dirs, files in os.walk(dir):
        for file in files:
            filename = os.path.join(root, file)
            file_list.append(filename)
    tarall = tarfile.open(os.path.join("{}/temp.tar.{}".format(dir, 'xz')), \
                              "w:{}".format('xz'))
    for idx, filepath in enumerate(file_list, 1):
            tarall.add(filepath, arcname=os.path.basename(filepath))
    tarall.close()


def General7z_compress_bzip2(dir):

    file_list=[]

    for root, dirs, files in os.walk(dir):
        for file in files:
            filename = os.path.join(root, file)
            file_list.append(filename)
    tarall = tarfile.open(os.path.join("{}/temp.tar.{}".format(dir, 'bz2')), \
                              "w:{}".format('bz2'))
    for idx, filepath in enumerate(file_list, 1):
            tarall.add(filepath, arcname=os.path.basename(filepath))
    tarall.close()

def create_and_empty_directory(directory_path):
    try:
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
        for item in os.listdir(directory_path):
            item_path = os.path.join(directory_path, item)
            if os.path.isfile(item_path):
                os.unlink(item_path)
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)
        print(f"Directory clean succeed: {directory_path}")
    except Exception as e:
        print(f"Directory clean failed: {e}")

#
# def zigzag_encoder(num: int):
#     return (num << 1) ^ (num >> 31)
#
#
# def zigzag_decoder(num: int):
#     return (num >> 1) ^ -(num & 1)
#
#
# def elastic_encoder(num: int):
#     # TODO: there're some bugs in elastic encoder
#     buffer = b''
#     cur = zigzag_encoder(num)
#     for i in range(4):
#         if (cur & (~0x7f)) == 0:
#             buffer += cur.to_bytes(1, "little")
#             # ret = i + 1
#             break
#         else:
#             buffer += ((cur & 0x7f) | 0x80).to_bytes(1, 'little')
#             cur = cur >> 7
#     return buffer
#
# def elastic_decoder_bytes(binary_bytes):
#     num_list=[]
#     num_byte=bytes()
#     for byt in binary_bytes:
#         num = int(byt)
#         byt = bytes([byt])
#         if num <128:
#             num_byte += byt
#             decode_num = int(elastic_decoder(num_byte))
#             num_list.append(decode_num)
#             num_byte=bytes()
#         else:
#             num_byte += byt
#     return num_list
#
#
# def elastic_decoder(num):
#
#     ret = 0
#     offset = 0
#     i = 0
#
#     while i < 5:
#         cur = num[i]
#         if (cur & (0x80) != 0x80):
#             ret |= (cur << offset)
#             i += 1
#             break
#         else:
#             ret |= ((cur & 0x7f) << offset)
#         i += 1
#         offset += 7
#
#     decode_num = zigzag_decoder(ret)
#     return decode_num
#

def zigzag_encoder(num: int):
    return (num << 1) ^ (num >> 63)


def zigzag_decoder(num: int):
    return (num >> 1) ^ -(num & 1)

def elastic_encoder(num: int):
    buffer = b''
    cur = zigzag_encoder(num)
    while True:
        # 如果当前剩余的数字在7位内，则直接输出
        if cur < 0x80:
            buffer += cur.to_bytes(1, "little")
            break
        else:
            # 输出当前的7位，并设置最高位为1
            buffer += ((cur & 0x7f) | 0x80).to_bytes(1, 'little')
            cur >>= 7
    return buffer

def elastic_decoder_bytes(binary_bytes):
    num_list = []
    num_byte = bytes()
    for byt in binary_bytes:
        num_byte += bytes([byt])
        # 如果最高位为0，表示当前数字结束
        if byt < 128:
            decode_num = elastic_decoder(num_byte)
            num_list.append(decode_num)
            num_byte = bytes()
    return num_list

def elastic_decoder(num_bytes):
    ret = 0
    offset = 0
    for i in range(len(num_bytes)):
        cur = num_bytes[i]
        ret |= ((cur & 0x7f) << offset)
        if cur & 0x80 == 0:
            break
        offset += 7
    return zigzag_decoder(ret)


class ExampleClass:
    def replace_and_group(self, lst):
        patterns = defaultdict(list)
        replaced = []
        entity_id = 1  # 初始ID号

        # 生成带有ID的实体替换模式
        def generate_entity_pattern():
            nonlocal entity_id
            pattern = f"<E{entity_id}>"
            entity_id += 1
            return pattern

        def replace_with_pattern(match):
            num = match.group()
            before = match.string[:match.start()]
            after = match.string[match.end():]
            if len(num) >= 4:
                pattern = f"<{len(num)}N{num[0]}>"
                patterns[pattern].append(num)
                return pattern
            elif before and after and before[-1].isalpha() and after[0].isalpha():
                entity_pattern = generate_entity_pattern()
                patterns[entity_pattern].append(num)
                return entity_pattern
            else:
                patterns['<*>'].append(num)
                return '<*>'
        for item in lst:
            replaced_item = re.sub(r'\d+', replace_with_pattern, item)
            replaced.append(replaced_item)

        return replaced, dict(patterns)