# 查找所有可能需要修复的头文件
find . -name "*.h" -exec grep -l "uint64_t\|uint32_t\|uint16_t\|uint8_t" {} \; | grep -v "include.*cstdint" > files_to_fix.txt

# 批量在这些文件开头添加 #include <cstdint>
while read file; do
    if ! grep -q "#include <cstdint>" "$file"; then
        sed -i '1i#include <cstdint>' "$file"
        echo "Fixed: $file"
    fi
done < files_to_fix.txt

# 清理临时文件
rm files_to_fix.txt

# 重新编译
make shared_lib