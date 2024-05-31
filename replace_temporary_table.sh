import re

# 读取HQL脚本文件
def read_hql_script(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# 移除HQL脚本中的注释
def remove_comments(script):
    return re.sub(r"--.*$", "", script, flags=re.MULTILINE)

# 查找所有的临时表名
def find_temp_tables(script):
    pattern = r"create.*?(table|view)\s+(?:if\s+not\s+exists)?\s*(.*?)\s+AS"
    return re.findall(pattern, script, flags=re.IGNORECASE)

# 查找目标表名
def find_target_table(script):
    pattern = r"insert\s+overwrite\s+table\s+`?(?:\w+)`?\.`?(\w+)`?\s+partition"
    return re.findall(pattern, script, flags=re.IGNORECASE)

# 临时表映射成规范的表名
def mapping_temp_tables(temp_tables, target_table):
    counter = 0
    table_mapping={} 
    for table in temp_tables:
        formatted_table = f"tmp.tmp_{target_table}_{counter:02d}"+"_${env.suffix}"  
        table_mapping[table]=formatted_table
        counter += 1 
    return table_mapping

# 在脚本中替换临时表名
def replace_temp_tables_in_script(script, mapping): 
    print("\n替换临时表名:")
    # 根据键的长度从大到小对 mapping 进行排序
    sorted_mapping = dict(sorted(mapping.items(), key=lambda item: len(item[0]), reverse=True))
    #遍历排序后的 sorted_mapping 并执行替换操作。这样可以确保更长的表名（如 test_01_${YYYY}_1）会优先替换，避免被较短的表名（如 test_01_${YYYY}）覆盖
    for temp_table,formatted_table in sorted_mapping.items(): 
        print(f"{temp_table} -> {formatted_table}")
        script = script.replace(temp_table, formatted_table) 
        # script = re.sub(re.escape(temp_table), formatted_table, script)  
    return script

# 写入格式化后的脚本到新文件
def write_new_script(output_file_path, formatted_script):
    with open(output_file_path, 'w') as file:
        file.write(formatted_script)

# 主函数
def main(): 
    input_file_path = '/usr/file/input.sql'
    output_file_path = '/usr/file/output.sql' 
    
    try:
        hql_script = read_hql_script(input_file_path)
        cleaned_script = remove_comments(hql_script) 
        temp_tables = [item[1] for item in find_temp_tables(cleaned_script)]
        print(f"临时表名: {temp_tables}") 
        target_table = find_target_table(cleaned_script)[0] if find_target_table(cleaned_script) else None
        print(f"\n目标表名: {target_table}")
        mapping_temp_table = mapping_temp_tables(temp_tables, target_table)
        formatted_script=replace_temp_tables_in_script(hql_script,mapping_temp_table)
        write_new_script(output_file_path, formatted_script)
        print(f"脚本文件 '{output_file_path}' 已被更新。")
    except Exception as e:
        print(f"处理过程中发生错误: {e}")

if __name__ == "__main__":
    main()
