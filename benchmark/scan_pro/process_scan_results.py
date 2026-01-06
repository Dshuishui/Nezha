#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Scan测试结果处理脚本
使用IQR方法去除异常值，并选取最接近平均值的实际测试数据
"""

import sys
import numpy as np


def read_data(filepath):
    """读取测试结果文件，解析数据行"""
    data = []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        # 跳过空行、标题行、汇总信息行
        if not line or line.startswith('测试编号') or line.startswith('汇总') or \
           '次测试' in line or '测试完成时间' in line or '测试参数' in line:
            continue
        
        parts = line.split(',')
        if len(parts) >= 10:
            try:
                test_id = int(parts[0])
                elapsed_time = float(parts[1])
                throughput = float(parts[2])
                avg_latency = float(parts[3])
                total_requests = int(parts[4])
                success_requests = int(parts[5])
                scan_count = int(parts[6])
                client_threads = int(parts[7])
                value_size = int(parts[8])
                data_size_mb = float(parts[9])
                
                data.append({
                    'test_id': test_id,
                    'elapsed_time': elapsed_time,
                    'throughput': throughput,
                    'avg_latency': avg_latency,
                    'total_requests': total_requests,
                    'success_requests': success_requests,
                    'scan_count': scan_count,
                    'client_threads': client_threads,
                    'value_size': value_size,
                    'data_size_mb': data_size_mb
                })
            except (ValueError, IndexError):
                continue
    
    return data


def remove_outliers_iqr(data, key='throughput'):
    """使用IQR方法去除异常值"""
    values = [d[key] for d in data]
    
    q1 = np.percentile(values, 25)
    q3 = np.percentile(values, 75)
    iqr = q3 - q1
    
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    filtered_data = [d for d in data if lower_bound <= d[key] <= upper_bound]
    removed_count = len(data) - len(filtered_data)
    
    return filtered_data, {
        'q1': q1,
        'q3': q3,
        'iqr': iqr,
        'lower_bound': lower_bound,
        'upper_bound': upper_bound,
        'removed_count': removed_count
    }


def find_closest_to_mean(data, key='throughput'):
    """找到最接近平均值的数据行"""
    values = [d[key] for d in data]
    mean_value = np.mean(values)
    
    closest_idx = 0
    min_diff = abs(data[0][key] - mean_value)
    
    for i, d in enumerate(data):
        diff = abs(d[key] - mean_value)
        if diff < min_diff:
            min_diff = diff
            closest_idx = i
    
    return data[closest_idx], mean_value


def process_scan_results(filepath):
    """处理scan测试结果的主函数"""
    # 1. 读取数据
    data = read_data(filepath)
    if not data:
        print("错误：未能读取到有效数据")
        return None
    
    print(f"=" * 60)
    print(f"Scan测试结果分析")
    print(f"=" * 60)
    print(f"\n【原始数据统计】")
    print(f"  总测试次数: {len(data)}")
    
    throughputs = [d['throughput'] for d in data]
    print(f"  吞吐量范围: {min(throughputs):.4f} ~ {max(throughputs):.4f} MB/s")
    print(f"  吞吐量平均值: {np.mean(throughputs):.4f} MB/s")
    print(f"  吞吐量中位数: {np.median(throughputs):.4f} MB/s")
    print(f"  吞吐量标准差: {np.std(throughputs):.4f} MB/s")
    
    # 2. 使用IQR方法去除异常值
    filtered_data, iqr_info = remove_outliers_iqr(data, 'throughput')
    
    print(f"\n【IQR异常值检测】")
    print(f"  Q1 (25%): {iqr_info['q1']:.4f} MB/s")
    print(f"  Q3 (75%): {iqr_info['q3']:.4f} MB/s")
    print(f"  IQR: {iqr_info['iqr']:.4f} MB/s")
    print(f"  有效范围: [{iqr_info['lower_bound']:.4f}, {iqr_info['upper_bound']:.4f}] MB/s")
    print(f"  剔除异常值数量: {iqr_info['removed_count']}")
    print(f"  剩余有效数据: {len(filtered_data)}")
    
    if not filtered_data:
        print("错误：去除异常值后无有效数据")
        return None
    
    # 3. 计算去除异常值后的统计
    filtered_throughputs = [d['throughput'] for d in filtered_data]
    print(f"\n【去除异常值后统计】")
    print(f"  吞吐量范围: {min(filtered_throughputs):.4f} ~ {max(filtered_throughputs):.4f} MB/s")
    print(f"  吞吐量平均值: {np.mean(filtered_throughputs):.4f} MB/s")
    print(f"  吞吐量中位数: {np.median(filtered_throughputs):.4f} MB/s")
    print(f"  吞吐量标准差: {np.std(filtered_throughputs):.4f} MB/s")
    
    # 4. 找到最接近平均值的实际数据行
    # closest_data, mean_throughput = find_closest_to_mean(filtered_data, 'throughput')
    # 修改后：找最接近中位数的
    median_throughput = np.median([d['throughput'] for d in filtered_data])
    closest_data = min(filtered_data, key=lambda d: abs(d['throughput'] - median_throughput))
    
    # print(f"\n【最终选取结果】")
    # print(f"  计算得到的平均吞吐量: {mean_throughput:.4f} MB/s")
    # print(f"  选取的测试编号: {closest_data['test_id']}")
    # print(f"  选取的吞吐量: {closest_data['throughput']:.4f} MB/s")
    # print(f"  选取的平均时延: {closest_data['avg_latency']:.4f} ms")
    # print(f"  与平均值的差距: {abs(closest_data['throughput'] - mean_throughput):.4f} MB/s")
    print(f"\n【最终选取结果】")
    print(f"  计算得到的中位数吞吐量: {median_throughput:.4f} MB/s")  # 改这里
    print(f"  选取的测试编号: {closest_data['test_id']}")
    print(f"  选取的吞吐量: {closest_data['throughput']:.4f} MB/s")
    print(f"  选取的平均时延: {closest_data['avg_latency']:.4f} ms")
    print(f"  与中位数的差距: {abs(closest_data['throughput'] - median_throughput):.4f} MB/s")  # 改这里
    
    print(f"\n{'=' * 60}")
    print(f"【结论】")
    print(f"  推荐使用的吞吐量: {closest_data['throughput']:.4f} MB/s")
    print(f"  推荐使用的时延: {closest_data['avg_latency']:.4f} ms")
    print(f"{'=' * 60}")
    
    return {
        'throughput': closest_data['throughput'],
        'latency': closest_data['avg_latency'],
        'test_id': closest_data['test_id'],
        # 'mean_throughput': mean_throughput,
        'original_count': len(data),
        'filtered_count': len(filtered_data),
        'removed_count': iqr_info['removed_count']
    }


def main():
    if len(sys.argv) < 2:
        print("用法: python process_scan_results.py <结果文件路径>")
        print("示例: python process_scan_results.py scan_benchmark_results.txt")
        sys.exit(1)
    
    filepath = sys.argv[1]
    
    try:
        result = process_scan_results(filepath)
        if result:
            print(f"\n程序执行完成")
    except FileNotFoundError:
        print(f"错误：文件不存在 - {filepath}")
        sys.exit(1)
    except Exception as e:
        print(f"错误：处理过程中出现异常 - {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()