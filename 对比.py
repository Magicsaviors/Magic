import pandas as pd

# 定义文件路径
file_path_new = r'C:/Users/Lenovo/Desktop/shareholder_chains1.csv'
file_path_old = r'C:/Users/Lenovo/Desktop/shareholder_chains.csv'
output_file_path = r'C:/Users/Lenovo/Desktop/shareholder_chains_with_changes1.csv'

# 读取 CSV 文件到 DataFrame
shareholder_chains1 = pd.read_csv(file_path_new)
shareholder_chains = pd.read_csv(file_path_old)

# 合并两个 DataFrame，根据 'company' 列进行外连接
merged_df = pd.merge(shareholder_chains1, shareholder_chains, on='company', how='left', suffixes=('', '_old'))

# 比较 'chain_length_minus_one' 列，新增 'chain_changed' 列
# 对 'company' 相同但 'chain_length_minus_one' 变化的情况标记为 1，或对 company 不同（即链条为空）的情况标记为 1
merged_df['chain_changed'] = ((merged_df['chain_length_minus_one'] != merged_df['chain_length_minus_one_old']) | 
                              (merged_df['chain_length_minus_one_old'].isna())).astype(int)

# 删除不需要的旧数据列
columns_to_drop = [col for col in merged_df.columns if col.endswith('_old')]
cleaned_df = merged_df.drop(columns=columns_to_drop)

# 将带有 'chain_changed' 列的最终数据保存到新文件
cleaned_df.to_csv(output_file_path, index=False)

print("文件处理完毕，已保存到", output_file_path)
