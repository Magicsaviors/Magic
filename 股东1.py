import pandas as pd
import numpy as np
from collections import defaultdict
import multiprocessing as mp
import tempfile
import os
import re

def contains_special_characters(text):
    if pd.isna(text) or text.isspace() or text == "":  # 检查是否为空值或空白
        return True
    if isinstance(text, str):
        # 检查是否全是特殊字符*、＊、？、/、_、&、#
        if re.fullmatch(r'[*＊？?/_&#]+', text):
            return True
        # 检查是否是仅包含1到2个中文字符
        if re.fullmatch(r'[\u4e00-\u9fff]{1}', text):  # 匹配1到2个连续的中文字符
            return True
        # 检查明显的时间格式，包括 "2002年"、"6月"、"3日"、"1982/3/8"
        if re.search(r'(\d{1,4}年)|(\d{1,2}月)|(\d{1,2}日)|(\d{1,4}/\d{1,2}/\d{1,2})', text):
            return True
        # 检查是否纯小写英文字母或纯数字
        if text.islower() and text.isalpha():
            return True
        if text.isdigit():
            return True
    return False


def split_data(input_file, columns_to_use):
    data = pd.read_csv(input_file, usecols=columns_to_use, encoding='utf-8')
    
    # 分割数据
    clean_data = data[data['name'].apply(contains_special_characters)]
    process_data = data[~data['name'].apply(contains_special_characters)]
    
    # 保存分割后的数据
    clean_file = r"C:/Users/Lenovo/Desktop/clean_data1.csv"
    process_file = r"C:/Users/Lenovo/Desktop/data_to_process1.csv"
    
    clean_data.to_csv(clean_file, index=False, encoding='utf-8-sig')
    process_data.to_csv(process_file, index=False, encoding='utf-8-sig')
    
    print(f"包含特殊字符的数据已保存到: {clean_file}")
    print(f"需要处理的数据已保存到: {process_file}")
    
    return process_file

def preprocess_data(data):
    company_shareholders = defaultdict(list)
    company_info = {}
    for _, row in data.iterrows():
        # 处理 capital_percent 列，去掉百分号并转换为浮点数
        capital_percent = row['capital_percent']
        if isinstance(capital_percent, str) and '%' in capital_percent:
            capital_percent = float(capital_percent.replace('%', '')) / 100.0
        else:
            capital_percent = float(capital_percent)  # 确保是浮点数
        
        company_shareholders[row['key']].append((row['name'], capital_percent))
        company_info[row['key']] = {
            '成立日期': row['成立日期'],
            '核准日期': row['核准日期']
        }
    
    return company_shareholders, company_info

def process_company(company, company_shareholders, processed_chains):
    """
    处理每个公司以构建股东链，从第一个股东开始，排除公司本身。
    如果公司的股东是自己，则链只包含该公司。如果股东是NaN或空字符串，则不包括在链中。
    如果公司的股东为空，链将为空，最终股东将为空，级别为0。
    """
    if company in processed_chains:
        return company, processed_chains[company]
    
    chain = []
    visited = set()

    shareholders = company_shareholders.get(company)
    if not shareholders or pd.isna(shareholders[0][0]) or shareholders[0][0] == '':
        # 如果股东为空或NaN，链、最终股东和级别都为空或零
        processed_chains[company] = {
            'chain': [],
            'final_shareholder': None,
            'chain_length_minus_one': 0
        }
        return company, processed_chains[company]

    # 获取第一个股东
    first_shareholder = shareholders[0][0]

    if first_shareholder == company:
        # 如果第一个股东是公司自己，结束链
        chain.append(company)
        processed_chains[company] = {
            'chain': chain,
            'final_shareholder': company,
            'chain_length_minus_one': 1  # 如果公司是自己的股东，链长度应为1
        }
        return company, processed_chains[company]

    current_company = first_shareholder
    while current_company:
        if current_company in visited or current_company == company:  # 处理循环引用或自我引用
            chain.append(current_company)
            break
        visited.add(current_company)
        chain.append(current_company)
        
        shareholders = company_shareholders.get(current_company)
        if not shareholders or pd.isna(shareholders[0][0]) or shareholders[0][0] == '':
            break
        
        next_company = shareholders[0][0]  # 取第一个股东

        if next_company == current_company:
            chain.append(next_company)
            break

        current_company = next_company

    processed_chains[company] = {
        'chain': chain,
        'final_shareholder': chain[-1] if chain else None,
        'chain_length_minus_one': len(chain)  # 修正链长度计算
    }
    return company, processed_chains[company]




def main():
    columns_to_use = ['key', 'name', 'capital_percent', '成立日期', '核准日期']
    input_file = r"C:/Users/Lenovo/Desktop/combined_file1.csv"
    
    # 分割数据并获取需要处理的数据文件路径
    process_file = split_data(input_file, columns_to_use)
    
    # 读取需要处理的数据
    data = pd.read_csv(process_file, encoding='utf-8')

    # 处理 capital_percent 列，去掉百分号并转换为浮点数
    data['capital_percent'] = pd.to_numeric(data['capital_percent'].str.rstrip('%'), errors='coerce') / 100.0
    data['key'] = data['key'].astype('category')
    data['name'] = data['name'].astype('category')
    data['成立日期'] = pd.to_datetime(data['成立日期'], format='%Y/%m/%d', errors='coerce')
    data['核准日期'] = pd.to_datetime(data['核准日期'], format='%Y/%m/%d', errors='coerce')

    company_shareholders, company_info = preprocess_data(data)

    unique_companies = set(data['key'])

    manager = mp.Manager()
    processed_chains = manager.dict()

    with mp.Pool() as pool:
        results = pool.starmap(process_company, [(company, company_shareholders, processed_chains) for company in unique_companies])

    # 将结果转换为DataFrame，并添加新的列
    result_data = [
        {
            'company': company,
            'shareholder_chain': result['chain'],
            'final_shareholder': result['final_shareholder'],
            'chain_length_minus_one': result['chain_length_minus_one'],
            '成立日期': company_info[company]['成立日期'],
            '核准日期': company_info[company]['核准日期']
        } 
        for company, result in results
    ]
    result_df = pd.DataFrame(result_data)

    # 使用临时文件并重命名
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv', mode='w', encoding='utf-8-sig')
    result_df.to_csv(temp_file.name, index=False, date_format='%Y/%m/%d')
    temp_file.close()

    output_file = r"C:/Users/Lenovo/Desktop/shareholder_chains1.csv"
    if os.path.exists(output_file):
        os.remove(output_file)
    os.rename(temp_file.name, output_file)

    print(f"股东链数据已保存到 {output_file}")

if __name__ == "__main__":
    main()
