#!/bin/bash
# 多节点驱动脚本共用的判定。source 进来用，不单独执行。
#
# 为什么要抽出来：同一条判定此前在 failover.sh、recover.sh、lsmraft.sh 里各写了一遍
#     echo "$rep" | grep -qE 'races=0 err_lines=0' || fail=1
# 三份拷贝各自漂移，而它有两个毛病：
#
#  1. **不看节点还活着没有。** REPORT 行里本来就有 alive=，三份拷贝一个都没用它。
#     于是一个**静默死掉**的节点照样判过——而"静默死掉"正是这次快照改造要防的那种
#     失效（leader 内存被慢 follower 钉住，最后被 OOM 杀掉，日志里一行解释都没有）。
#  2. **子串匹配、且要求两个字段相邻。** `err_lines=0` 会前缀匹配 `err_lines=01`；
#     更现实的是在两个字段之间插一个新字段，整条判定就变成**永久假失败**——
#     而假失败和漏报一样糟，它会训练人去忽略这个闸门。
#
# two-node-rounds.sh 里的写法是对的（按字段取值再精确比较），这里把它变成共用的一份。

# gate_field <REPORT 行> <字段名>
# 按字段名取值。逐 token 比对而不是正则匹配：REPORT 行本来就是空格分隔的
# `名=值`，按 token 取值**在构造上**就不可能前缀匹配（`err_lines=0` 不会命中
# `err_lines=03`），也不依赖 `\<` 这种 GNU 扩展——BSD grep 上它不一定管用，
# 而这几个脚本既在 Linux 服务器上跑，也在 mac 上被本机自审调用。
gate_field() {
    local tok
    for tok in $1; do
        case "$tok" in
            "$2="*) printf '%s' "${tok#*=}"; return 0;;
        esac
    done
    return 1
}

# gate_report_ok <REPORT 行> <期望 alive：yes|no|any> <标签>
# 合格条件：alive 符合预期、races=0、err_lines=0。
# 空行（SSH 超时或节点脚本自己挂了）一律不合格——拿不到判据不等于判据通过。
# 返回 0 = 合格；不合格时把原因打出来。
gate_report_ok() {
    local rep=$1 want_alive=$2 label=$3
    if [ -z "$rep" ]; then
        echo "    [闸门] $label: 拿不到 REPORT（SSH 超时或节点脚本失败）——不算通过"
        return 1
    fi
    local alive races err
    alive=$(gate_field "$rep" alive)
    races=$(gate_field "$rep" races)
    err=$(gate_field "$rep" err_lines)
    local bad=0
    if [ "$want_alive" != any ] && [ "$alive" != "$want_alive" ]; then
        echo "    [闸门] $label: alive=${alive}，期望 ${want_alive}（节点在本轮结束时的存活状态不对）"
        bad=1
    fi
    if [ "$races" != 0 ]; then
        echo "    [闸门] $label: races=$races"
        bad=1
    fi
    if [ "$err" != 0 ]; then
        echo "    [闸门] $label: err_lines=$err"
        bad=1
    fi
    # 字段本身缺失也算不合格：REPORT 的格式变了而判定没跟上，同样是判据失效。
    if [ -z "$alive" ] || [ -z "$races" ] || [ -z "$err" ]; then
        echo "    [闸门] $label: REPORT 行里缺字段（alive=$alive races=$races err_lines=${err}）"
        bad=1
    fi
    return $bad
}
