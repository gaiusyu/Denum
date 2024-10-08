/*
Denum C++ implemention.

Part One: elastic encoder/decoder

Part two: Pure number/Tokens containing only numbers and special characters processing

Part three: Numeric variable processing

Part four: Block compression implementation

Part five: Main function

*/


#define PCRE2_CODE_UNIT_WIDTH 8
#include <iostream>
#include <unordered_set>
#include <iostream>
#include <string>
#include <vector>
#include <regex>
#include <unordered_map>
#include <list>
#include <fstream>
#include <sys/stat.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <unordered_set>
#include <pcre2.h>
#include <stdexcept>
#include <future>
#include <filesystem>
#include <cstdlib>
#include <algorithm>
#include <queue>
#include <cmath>

std::queue<std::string> lines;
std::mutex mtx;



/*
Part One: elastic encoder/decoder

METHODS:
zigzag_encode()
zigzag_decode()
elastic_encode()
elastic_decode()
elastic_decode_bytes()

DESCRIPTIONS: Elastic encoder/decoder, which is proposed by Wei in LogReducer. LogShrink also incoperated this techinology

*/


int64_t zigzag_encode(int64_t num) {
    return (num << 1) ^ (num >> 63);
}

int64_t zigzag_decode(int64_t num) {
    return (num >> 1) ^ -(num & 1);
}

std::vector<unsigned char> elastic_encode(int64_t num) {
    std::vector<unsigned char> buffer;
    uint64_t cur = zigzag_encode(num);
    while (true) {
        if (cur < 0x80) {
            buffer.push_back(static_cast<unsigned char>(cur));
            break;
        } else {
            buffer.push_back(static_cast<unsigned char>((cur & 0x7F) | 0x80));
            cur >>= 7;
        }
    }
    return buffer;
}

int64_t elastic_decode(const std::vector<unsigned char>& num_bytes) {
    int64_t ret = 0;
    int offset = 0;
    for (auto cur : num_bytes) {
        ret |= (static_cast<int64_t>(cur & 0x7F) << offset);
        if ((cur & 0x80) == 0) {
            break;
        }
        offset += 7;
    }
    return zigzag_decode(ret);
}

std::vector<int64_t> elastic_decode_bytes(const std::vector<unsigned char>& binary_bytes) {
    std::vector<int64_t> num_list;
    std::vector<unsigned char> num_byte;
    for (auto byt : binary_bytes) {
        num_byte.push_back(byt);
        if (byt < 128) {
            int64_t decode_num = elastic_decode(num_byte);
            num_list.push_back(decode_num);
            num_byte.clear();
        }
    }
    return num_list;
}


/*
Part two: Pure number/Tokens containing only numbers and special characters processing

METHODS:
compile_num()
compile_pattern()
replace_and_group()
    process_with_pattern()

DESCRIPTIONS: Since Numeric variable processing requires word split first, we have sperated it from these two types of numberic token parsing.
Both compile_num() and compile_pattern() are designed to compile regular expression. replace_and_group() calls process_with_pattern() to find 
pure numbers/tokens containing only numbers and special characters processing in logs and use corresponding tags to replace them, output a 
replaced log list, a map storing tags and corresponding numbers。
*/


struct RegexPattern {
    std::vector<std::string> patterns;
    std::vector<std::string> substitutions;
};

// LogProcessor class
class LogProcessor {
public:
    std::string logname;
    std::unordered_map<std::string, RegexPattern> regex_map;
    std::vector<pcre2_code *> compiled_patterns; // Used to store the compiled regular expression.
    pcre2_code *re_num;
    // Constructor.
    LogProcessor(const std::string &name) : logname(name) {
        // Predefined regular expressions and replacement symbols for different log names. You can modified these based on your systems.
        compile_num(&re_num, R"((?<![a-zA-Z0-9])\d+(?![a-zA-Z0-9]))");
        regex_map["Android"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Apache"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2}) (\d+):(\d+):(\d+))"}, {"<I>", "<T>"} };
        regex_map["BGL"] = { {R"((\d+)-(\d+)-(\d+)-(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+))",R"((\d+)\.(\d+)\.(\d+))"}, {"<E>", "<T>", "<F>"} };
        regex_map["Hadoop"] = { {R"((\d+)\-(\d+)\-(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<D>", "<T>"} };
        regex_map["HDFS"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+):(\d+):(\d+),(\d+))"}, {"<I>", "<T>"} };
        regex_map["HealthApp"] = { { R"((\d+):(\d+):(\d+):(\d+))"}, {"<T>"} };
        regex_map["HPC"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Linux"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+)-(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<I>", "<T>"} };
        regex_map["Mac"] = { {R"((\d+)-(\d+)-(\d+)-(\d+))", R"((\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<D>", "<T>"} };
        regex_map["OpenSSH"] = { {R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)",R"(sshd\[(\d+)\]:)"}, {"<I>", "<T>", "<S>"} };
        regex_map["OpenStack"] = { { R"(\.(\d+)-(\d+)-(\d+)_(\d+):(\d+):(\d+))",R"((\d+)-(\d+)-(\d+).(\d+):(\d+):(\d+)\.(\d+))"}, { "<D>", "<T>"} };
        regex_map["Proxifier"] = { { R"((\d+)\.(\d+) (\d+):(\d+):(\d+)(?:\.(\d+))?)"}, {"<T>"} };
        regex_map["Spark"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))", R"((\d{2})\/(\d{2})\/(\d{2}) (\d+):(\d+):(\d+))",R"((\d+)\.(\d{1}) MB)",R"((\d+)\.(\d{1}) KB)",R"((\d+)\.(\d{1}) GB)",R"((\d+)\.(\d{1}) B)"}, {"<I>", "<T>", "<M>", "<K>", "<G>", "<B>"} };
        regex_map["Thunderbird"] = { { R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+):(\d+):(\d+))",R"((\d{4}})\.(\d+)\.(\d+))",R"(\[(\d+)\]:)"}, {"<I>","<T>","<A>","<B>"}};
        regex_map["Windows"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
        regex_map["Zookeeper"] = { {  R"((\d+)\.(\d+)\.(\d+)\.(\d+))",R"((\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+),(\d+))", R"((\d+):(\d+):(\d+))"}, {"<I>","<T>","<D>"} };
        // Compile the regular expression based on the log name.
        if (regex_map.find(logname) != regex_map.end()) {
            const auto &patterns = regex_map[logname].patterns;
            for (const auto &pattern : patterns) {
                pcre2_code *re = compile_pattern(pattern.c_str());
                compiled_patterns.push_back(re);
            }
        } else {
            throw std::runtime_error("Unknown logname");
        }
    }

    // Destructor.
    ~LogProcessor() {
        // Release the compiled regular expression.
        for (auto re : compiled_patterns) {
            pcre2_code_free(re);
        }
        pcre2_code_free(re_num);
    }
    /*
    replace_and_group() : parse pure numbers and tokens containing only numbers and special characters 

    input: log list

    output: replaced log list, a unordered_map to store tag and corresponding numbers

    */

    std::pair<std::vector<std::string>, std::unordered_map<std::string, std::list<int64_t>>> replace_and_group(const std::vector<std::string> &lst) {
        std::unordered_map<std::string, std::list<int64_t>> patterns;
        std::vector<std::string> replaced;

        const std::string alpha = "abcdefghijklmnopqrstuvwxyz";
        const auto &substitutions = regex_map[logname].substitutions;

        for (auto &item : lst) {
            std::string result = item;
            for (size_t i = 0; i < compiled_patterns.size(); ++i) {
                result = process_with_pattern(result, compiled_patterns[i], substitutions[i], patterns);
            }
            result = process_with_pattern(result, re_num, alpha, patterns, true); 
            replaced.push_back(result);
        }
        
        return {replaced, patterns};
    }

private:
    pcre2_code* compile_pattern(const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        pcre2_code *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
        return re;
    }
    void compile_num(pcre2_code **re, const char *pattern) {
        int errornumber;
        PCRE2_SIZE erroroffset;
        *re = pcre2_compile((PCRE2_SPTR)pattern, PCRE2_ZERO_TERMINATED, 0, &errornumber, &erroroffset, nullptr);
        if (*re == nullptr) {
            throw std::runtime_error("Regex compilation failed");
        }
    }

    std::string process_with_pattern(const std::string &input, pcre2_code *re, const std::string &substitution, std::unordered_map<std::string, std::list<int64_t>> &patterns, bool is_num = false) {
        std::string result;
        PCRE2_SIZE last_pos = 0;

        PCRE2_SPTR subject = (PCRE2_SPTR)input.c_str();
        size_t subject_length = strlen((char *)subject);

        pcre2_match_data *match_data = pcre2_match_data_create_from_pattern(re, nullptr);

        int rc;
        while ((rc = pcre2_match(re, subject, subject_length, last_pos, 0, match_data, nullptr)) > 0) {
            PCRE2_SIZE *ovector = pcre2_get_ovector_pointer(match_data);

            if (is_num) {
                std::string num = input.substr(ovector[0], ovector[1] - ovector[0]);
                size_t len = num.length();
                if (len < 15) {
                    std::string pattern_key = "<" + std::string(1, substitution[len - 1]) + (len >= 4 ? std::string(1, substitution[num[0] - '0']) : "") + ">";
                    patterns[pattern_key].push_back(std::stoll(num));
                    result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
                } else {
                    result += input.substr(last_pos, ovector[0] - last_pos) + num;
                }
            } else {
                std::string match = input.substr(ovector[0], ovector[1] - ovector[0]);
                std::string num_str;
                for (char c : match) {
                    if (std::isdigit(c)) {
                        num_str += c;
                    }
                }
                std::string pattern_key = substitution;
                patterns[pattern_key].push_back(std::stoll(num_str));
                result += input.substr(last_pos, ovector[0] - last_pos) + pattern_key;
            }
            last_pos = ovector[1];
        }
        result += input.substr(last_pos);

        pcre2_match_data_free(match_data); // Release the memory used for regex matching.

        return result;
    }
};


/*
Part Three: Numeric variable processing

METHODS:
varaible_extract()
    delimiter_mining() 
        find_special_chars_with_high_freq()
    split_by_multiple_delimiters()
store_content_with_id()
regex_escape()


DESCRIPTIONS: Since Numeric variable processing requires word split first, we have sperated it from the other two types of numberic token processing.
The input of varaible_extract() is the replaced log list output by replace_and_group, this part of codes is related to parse numberic variable. delimiter_mining()
calls find_special_chars_with_high_freq() to determine what delimeters should be used in word split. split_by_multiple_delimiters() conduct word split. 
store_content_with_id() is a implementation of Dict-IDs manner storeage.

*/


class DenumLogProcessor {
private:
    std::string logname;

public:
    DenumLogProcessor(std::string name) : logname(name) {}
    /*
    variable_extract() : parse numberic variables and save extracted numberic variables to files

    input: replaced log list (output of replace_and_group())

    output: logs without numbers
    */
    std::vector<std::string> variable_extract(const std::vector<std::string>& logs, const std::string& chunkID) {
        std::vector<std::string> modified_lines;
        std::vector<std::string> variable_set;
        std::regex digit_pattern("\\d");
        std::regex regex_pattern;
        std::vector<std::string> delimiters;
        std::tie(regex_pattern, delimiters) = delimeter_mining(logs);
        std::string modified_line;
        std::vector<std::string> split;
        for (const auto& log : logs) {
            modified_line = "";
            split = split_by_multiple_delimiters(regex_pattern, log,true);
            for (const auto& word : split) {
                if (std::regex_search(word, digit_pattern)) {
                    modified_line += "<*>";
                    variable_set.push_back(word);
                } else {
                    modified_line += word;
                }
            }
            modified_lines.push_back(modified_line);
        }
        store_content_with_ids(variable_set, "variableset", chunkID, "lzma");
        return modified_lines;
    }
    
    void ensure_directory_exists(const std::string& dir) {
        struct stat buffer;
        if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
            #ifdef _WIN32
            _mkdir(dir.c_str());  
            #else
            mkdir(dir.c_str(), 0777);  
            #endif
        }
    }


    void store_content_with_ids(const std::vector<std::string>& input, const std::string& output, const std::string& chunkID, const std::string& compressor) {
    std::unordered_map<std::string, int> content_to_id;
    std::unordered_map<int, std::string> id_to_content;
    int id_counter = 1;
    std::vector<int> id_list;
    std::string id_dir = "output/" + logname + "/" + chunkID + "/";
    std::string ids_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "ids.bin";
    std::string mapping_file_path = "output/" + logname + "/" + chunkID + "/" + logname + output + "mapping.txt";
    ensure_directory_exists(id_dir);

    for (const auto& line : input) {
        if (line.empty()) continue;
        if (content_to_id.find(line) == content_to_id.end()) {
            content_to_id[line] = id_counter;
            id_to_content[id_counter] = line;
            id_counter++;
        }
        id_list.push_back(content_to_id[line]);
    }

    // Create an ordered vector to store the contents of id_to_content.
    std::vector<std::pair<int, std::string>> sorted_content;
    for (const auto& pair : id_to_content) {
        sorted_content.push_back(pair);
    }

    // Sort by id_counter.
    std::sort(sorted_content.begin(), sorted_content.end(), [](const auto& a, const auto& b) {
        return a.first < b.first;
    });

    std::ofstream ids_file(ids_file_path, std::ios::binary);
    std::ofstream mapping_file(mapping_file_path);

    // Now write to mapping_file in the sorted order.
    for (const auto& pair : sorted_content) {
        mapping_file << pair.second << "\n";
    }

    for (int id : id_list) {
        auto encoded = elastic_encode(id);
        ids_file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
    }

    ids_file.close();
    mapping_file.close();
}

    std::string regex_escape(const std::string& pattern) {
            // List of special characters that need to be escaped.
            static const std::string special_chars = R"([-[\]{}()*+?.\\^$|])";

            // Construct the escaped pattern.
            std::string escaped_pattern;
            for (char c : pattern) {
                if (special_chars.find(c) != std::string::npos) {
                    escaped_pattern += '\\'; // Add escape characters.
                }
                escaped_pattern += c;
            }

            return escaped_pattern;
        }

    std::tuple<std::regex, std::vector<std::string>> delimeter_mining(const std::vector<std::string>& logs) {
        std::vector<std::string> temp = logs;
        std::random_shuffle(temp.begin(), temp.end());
        std::unordered_set<size_t> lengths;
        std::vector<std::string> sample;
        size_t iteration_count = 0;
        for (const auto& log : temp) {
            size_t log_len = log.size();
            if (lengths.find(log_len) == lengths.end()) {
                lengths.insert(log_len);
                sample.push_back(log);
            }
            iteration_count++;
            if (lengths.size() >= 10 || iteration_count >= 200) {
                break;
            }
        }
        std::vector<std::string> delimiters = find_special_chars_with_high_freq(sample);
        if (delimiters.empty()) {
            throw std::runtime_error("No delimiters found. Cannot create a valid regex pattern.");
        }

        std::string pattern_str = "(";
        for (const auto& delimiter : delimiters) {
            if (!delimiter.empty()) {
                pattern_str += regex_escape(delimiter) + "|";
            }
        }
        if (pattern_str.back() == '|') {
            pattern_str.pop_back(); // Remove the trailing "|".
        }
        pattern_str += ")";

        if (pattern_str == "()") {
            throw std::runtime_error("Invalid regex pattern: " + pattern_str);
        }
        return std::make_tuple(std::regex(pattern_str), delimiters);
    }

    std::vector<std::string> split_by_multiple_delimiters(const std::regex& pattern, const std::string& str, bool include_delimiters) {
        std::vector<std::string> result;
        auto words_begin = std::sregex_iterator(str.begin(), str.end(), pattern);
        auto words_end = std::sregex_iterator();

        size_t last_pos = 0;
        for (std::sregex_iterator iter = words_begin; iter != words_end; ++iter) {
            std::smatch match = *iter;
            size_t current_pos = match.position();
            if (current_pos > last_pos) {
                result.push_back(str.substr(last_pos, current_pos - last_pos));
            }
            if (include_delimiters) {
                result.push_back(match.str());
            }
            last_pos = current_pos + match.length();
        }

        if (last_pos < str.length()) {
            result.push_back(str.substr(last_pos));
        }

        return result;
    }


    std::vector<std::string> find_special_chars_with_high_freq(const std::vector<std::string>& str_list, size_t freq_threshold = 10) {
        std::vector<char> candidates = {',', ' ', '|', ';', '[', ']', '(', ')', '_', '/'};
        std::unordered_map<char, size_t> char_counter;
        for (const auto& s : str_list) {
            for (char c : s) {
                if (std::find(candidates.begin(), candidates.end(), c) != candidates.end()) {
                    char_counter[c]++;
                }
            }
        }

        std::vector<std::string> result;
        for (const auto& pair : char_counter) {
            if (pair.second > freq_threshold) {
                result.push_back(std::string(1, pair.first));
            }
        }

        // If the result is empty, add a space character.
        if (result.empty()) {
            result.push_back(" ");
        }

        return result;
    }
};
/*
Part Four: Block compression implementation

METHODS:
ensure_directory_exists
sanitize_filename
delta_transform
compressDirectory
processLogBlock

DESCRIPTIONS: processLogBlock is the "main" function, which is designed to compress each log block. 
log_processor.replace_and_group(block); is to parse Pure numbers & Tokens containing only***.
std::vector<std::string> modified_logs = denum_processor.variable_extract(final_output, std::to_string(block_id)); is to parse Numeric variables.
denum_processor.store_content_with_ids(modified_logs, "all", std::to_string(block_id), "lzma"); is an implementation of String Processing, which uses
Dict-IDs manner to store logs without numbers. 


*/



void ensure_directory_exists(const std::string& dir) {
    struct stat buffer;
    if (stat(dir.c_str(), &buffer) != 0) { // Check if the directory exists.
        #ifdef _WIN32
        _mkdir(dir.c_str());  // Create directory on Windows system.
        #else
        mkdir(dir.c_str(), 0777);  // Create directory on Unix/Linux system.
        #endif
    }
}


std::string sanitize_filename(std::string filename) {
    std::replace(filename.begin(), filename.end(), '<', '_');  // Replace '<' with '_'.
    std::replace(filename.begin(), filename.end(), '>', '_');  // Replace '>' with '_'.
    return filename;
}

/*
delta_transform() : Calculate the difference between adjacent numbers.

input: number list

output: difference list
*/

std::list<int64_t> delta_transform(const std::list<int64_t>& num_list) {
    if (num_list.empty()) {
        return {}; // If the list is empty, return an empty list.
    }

    std::list<int64_t> new_list;

    auto it = num_list.begin();
    int64_t initial = *it;
    new_list.push_back(initial); // Add the initial element.
    int64_t last = initial;

    for (++it; it != num_list.end(); ++it) {
        int64_t delta = *it - last;
        new_list.push_back(delta);
        last = *it;
    }

    return new_list;
}


void compressDirectory(const std::string& output_dir, int block_id) {
    std::string directoryPath = output_dir + "/" + std::to_string(block_id);
    std::string command = "tar -cJf " + output_dir + "/compressed" + std::to_string(block_id) + ".xz " + directoryPath;
    int result = std::system(command.c_str());

    if (result != 0) {
        std::cerr << "Command failed with return code: " << result << std::endl;
    } else {
        std::cout << "Block " << block_id << " directory successfully compressed into compressed" << block_id << ".xz" << std::endl;
    }
}


void processLogBlock(const std::vector<std::string>& block, int block_id, const std::string& output_dir, LogProcessor& log_processor, DenumLogProcessor& denum_processor, std::map<int, std::vector<std::string>>& final_outputs, const std::string& output_logs) {
    auto [final_output, final_patterns] = log_processor.replace_and_group(block); // parse Pure numbers & Tokens containing only ***.
    std::string logname_dir = output_dir + "/" + std::to_string(block_id) + "/";
    ensure_directory_exists(logname_dir);
    std::vector<std::string> modified_logs = denum_processor.variable_extract(final_output, std::to_string(block_id)); // parse Numeric variables

    if (output_logs == "1") {
        denum_processor.store_content_with_ids(modified_logs, "all", std::to_string(block_id), "lzma"); // an implementation of String Processing, which uses Dict-IDs manner to store logs without numbers.
    }
    else if (output_logs == "2") {
        final_outputs[block_id] = modified_logs;
    }
    else if (output_logs == "3") {

        std::ofstream final_log_file(logname_dir +  "logswithoutnums.log");
        if (!final_log_file.is_open()) {
            std::cerr << "Unable to open final log file for writing: " << logname_dir +  "logswithoutnums.log" << std::endl;
            return;
        }

        for (const auto& log : modified_logs) {
                final_log_file << log << std::endl;
            }
        
        final_log_file.close();
    }


    // Collect the final_output with block_id
    // final_outputs[block_id] = final_output;
    // final_outputs[block_id] = modified_logs;
    // This part of codes is related to how to store these parsing results (numbers). 
    for (const auto &pair : final_patterns) {
        std::vector<unsigned char> encoded_buffer;
        std::string sanitized_filename = sanitize_filename(pair.first);
        std::string filename = logname_dir  + sanitized_filename + ".bin";
        std::ofstream file(filename, std::ios::out | std::ios::binary);
        std::list<int64_t>  transformed;
        // This part of codes can be modified based on your empirical kownledge on your datasets. 
        if (pair.first != "<I>" && pair.first != "<a>" && pair.first != "<b>"&& pair.first != "<c>") {
            // Perform delta_transform when there is no arthimtic relationship.
            transformed = delta_transform(pair.second);
        } else {
            transformed =pair.second;
        }
        for (const auto& num : transformed) {
            auto encoded = elastic_encode(num);
            file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());
        }
        file.close(); // Ensure the file is closed.
    }

    // Compress the output directory of the current block.
    compressDirectory(output_dir, block_id);
}

/*
Part Five: main function

argv[0]: logname

argv[1]: block size

argv[2]: mode, "1" represents default Denum, "2" is to generate logs without numbers for RQ3, "3" is Denum without string processing for RQ4

DESCRIPTIONS: Divide the input log into different log blocks, call processLogBlock for compression in multiprocessing manner, and output CR & CS, etc.
*/

int main(int argc, char* argv[]) { 
    if (argc < 4) {
        std::cerr << "Usage: " << argv[0] << " <logname>" << std::endl;
        return 1;
    }

    const std::string logname = argv[1];
    size_t BLOCK_SIZE;
    const std::string output_logs = argv[3];


    try {
        BLOCK_SIZE = std::stoul(argv[2]);
    } catch (const std::invalid_argument& e) {
        std::cerr << "Invalid block size argument: " << argv[2] << std::endl;
        return 1;
    } catch (const std::out_of_range& e) {
        std::cerr << "Block size value out of range: " << argv[2] << std::endl;
        return 1;
    }

    std::cout << "Block Size: " << BLOCK_SIZE << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    const std::string log_path = "Logs/" + logname + "/" + logname + ".log";
    
    const int num_threads = 4; // Adjust based on the number of CPU cores.
    std::vector<std::future<void>> futures;
    std::map<int, std::vector<std::string>> final_outputs;

    LogProcessor log_processor(logname);
    DenumLogProcessor denum_processor(logname);

    std::ifstream log_file(log_path);
    if (!log_file.is_open()) {
        std::cerr << "Unable to open log file: " << log_path << std::endl;
        return 1;
    }

    // Clean up and ensure the output directory exists.
    std::string command = "rm -rf output/" + logname + "/* ";
    int result = std::system(command.c_str());
    ensure_directory_exists("output/" + logname);

    std::vector<std::string> block;
    block.reserve(BLOCK_SIZE);
    std::string line;
    int block_index = 0;

    while (std::getline(log_file, line)) {
        block.push_back(line);
        if (block.size() == BLOCK_SIZE) {
            futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs), output_logs));
            block.clear();
            ++block_index;
        }
    }

    // Process any remaining log lines (if any).
    if (!block.empty()) {
        futures.push_back(std::async(std::launch::async, processLogBlock, block, block_index, "output/" + logname, std::ref(log_processor), std::ref(denum_processor), std::ref(final_outputs),output_logs));
    }

    // Wait for all threads to complete.
    for (auto& future : futures) {
        future.wait();
    }

    auto stop = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start);
    std::uintmax_t fileSize = std::filesystem::file_size(log_path);
    double dataSizeInMB = static_cast<double>(fileSize) / (1024.0 * 1024.0);
    double speedInMBPerSecond = dataSizeInMB / duration.count() * 1000;
    double totalSize = 0;
    double totalBytes = 0;
    for (int i = 0; i <= block_index; ++i) {
        std::string compressed_path = "output/" + logname + "/compressed" + std::to_string(i) + ".xz";
        std::uintmax_t achieved_fileSize = std::filesystem::file_size(compressed_path);
        double dataSizeInMB = static_cast<double>(achieved_fileSize) / (1024.0 * 1024.0);
        totalSize += dataSizeInMB;
        totalBytes += achieved_fileSize;
    }
    double CR = dataSizeInMB / totalSize;

    // Output time taken CR&CS, achieved size, keeping three decimal places.
    std::cout << "Replacement completed in " << duration.count() << " milliseconds." << std::endl;
    std::cout << "Compression speed: " << std::fixed << std::setprecision(3) << speedInMBPerSecond << " MB/s" << std::endl;
    std::cout << "Achieved size: " << totalBytes << " Bytes" << std::endl;
    std::cout << "Compression ratio: " << std::fixed << std::setprecision(3) << CR << std::endl;
    if (output_logs == "2") {
        std::ofstream final_log_file("output/" + logname + ".log");
        for (const auto& [block_id, output] : final_outputs) {
            std::size_t length = output.size();
            for (const auto& log : output) {
                final_log_file << log << std::endl;
            }
        }
        final_log_file.close();
    }
    return 0;
}