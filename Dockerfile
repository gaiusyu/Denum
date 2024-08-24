# 基于官方 C++ 开发镜像
FROM gcc:9.4

# 安装 Python 3.7.3
RUN apt-get update && apt-get install -y python3.7

# 安装 PCRE2 和 Boost 相关的依赖
RUN apt-get install -y libpcre2-dev libboost-iostreams-dev

# 将源代码复制到容器中
COPY denum_compress.cpp /app/
WORKDIR /app

# 编译代码
RUN g++ -O3 -std=c++17 -o denum_compress denum_compress.cpp -lboost_iostreams -lpthread -lpcre2-8

# 设置默认命令
ENTRYPOINT ["./denum_compress"]