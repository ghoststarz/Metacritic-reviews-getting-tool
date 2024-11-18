#!/user/bin/env python3
# -*- coding: utf-8 -*-
import os
import pandas as pd


def split_reviews(file_path):
    # 读取Excel文件
    df = pd.read_excel(file_path)

    # 筛选出POSITIVE和NEGATIVE的行
    positive_df = df[df['Review Type'] == 'POSITIVE']
    negative_df = df[df['Review Type'] == 'NEGATIVE']

    # 构建输出文件路径
    base_name = os.path.splitext(file_path)[0]  # 去除文件扩展名
    positive_file = f"{base_name}_positive.xlsx"
    negative_file = f"{base_name}_negative.xlsx"

    # 保存分割后的文件
    positive_df.to_excel(positive_file, index=False, engine='openpyxl')
    negative_df.to_excel(negative_file, index=False, engine='openpyxl')
    print(f"文件已处理: {file_path}")
    print(f"保存正面评价到: {positive_file}")
    print(f"保存负面评价到: {negative_file}")


def process_directory(folder_path):
    for root, _, files in os.walk(folder_path):
        for file in files:
            # 检查文件是否为Excel文件
            if file.endswith('.xlsx') or file.endswith('.xls'):
                file_path = os.path.join(root, file)
                try:
                    # 对文件进行POSITIVE和NEGATIVE分割
                    split_reviews(file_path)
                except Exception as e:
                    print(f"处理文件时出错: {file_path}, 错误信息: {e}")


# 设置需要处理的文件夹路径
folder_path = 'gamelist'  # 请将此路径替换为包含Excel文件的文件夹路径
process_directory(folder_path)

