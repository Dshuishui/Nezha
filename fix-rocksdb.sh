#!/bin/bash
# Fix missing <cstdint> includes for GCC 9+ compatibility (Ubuntu 20.04+)

find . -name "*.h" -exec grep -l "uint64_t\|uint32_t\|uint16_t\|uint8_t" {} \; | grep -v "include.*cstdint" > files_to_fix.txt

while read file; do
    if ! grep -q "#include <cstdint>" "$file"; then
        sed -i '1i#include <cstdint>' "$file"
        echo "Fixed: $file"
    fi
done < files_to_fix.txt

rm files_to_fix.txt

make shared_lib -j$(nproc)
