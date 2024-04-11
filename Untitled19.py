#!/usr/bin/env python
# coding: utf-8

# In[3]:


# 导入必要的模块
import csv
import os
import pymysql
import pandas as pd
import datetime
import time
from tqdm import tqdm
import chardet

def connect_to_database():
    return pymysql.connect(host='###.##.##.###', user='####', password='######', database='####')

# 文件夹路径
folder_path = r'Z:\power_plant_data'

# 创建数据库连接
connection = connect_to_database()
cursor = connection.cursor()

# 使用 os.walk() 遍历文件夹
for foldername, subfolders, filenames in os.walk(folder_path):
    for filename in filenames:
        # 提取信息
        parts = foldername.split('\\')
        power_plant_name = parts[-4]
        unit_number = parts[-3]
        num_type = parts[-2]
        date = parts[-1]
        file_name = filename
        power_plant_id = power_plant_name[:3]

        # 构造插入语句
        insert_query = f"""
            INSERT INTO Oech_Analyse_power_plant_documentation
            (power_plant_id, power_plant_name, unit_number, num_type, date, file_name)
            VALUES
            ('{power_plant_id}', '{power_plant_name}', '{unit_number}', '{num_type}', '{date}','{file_name}')
            ON DUPLICATE KEY UPDATE date = '{date}',file_name = '{file_name}';
        """

        # 执行插入
        cursor.execute(insert_query)
        
# 提交更改并关闭连接
connection.commit()
cursor.close()
connection.close()

def insert_data_into_database(cursor, table_name, data_frame):
    existing_columns = get_table_columns(cursor, table_name)
    valid_columns = [col for col in data_frame.columns if col in existing_columns]
    #筛选不含空值的列
    valid_columns_not_null = [col for col in valid_columns if data_frame[col].notnull().any()]

    # 生成插入SQL语句
    insert_sql = f"INSERT INTO {table_name} ({', '.join(valid_columns)}) VALUES ({', '.join(['%s'] * len(valid_columns))}) ON DUPLICATE KEY UPDATE {', '.join([f'`{col}` = VALUES(`{col}`)' for col in valid_columns_not_null])};"
    
    values = [tuple(str(row[col]) for col in valid_columns) for index, row in data_frame.iterrows()]
    
    # 执行插入操作
    cursor.executemany(insert_sql, values)

def get_table_columns(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return [column[0] for column in cursor.fetchall()]

def main():
    time_start = time.time()  # 记录开始时间

    # 连接到数据库
    conn = connect_to_database()

    # 查询配置表
    with conn.cursor() as c1:
        c1.execute('SELECT * FROM Oech_Analyse_configure')
        columns = [column[0] for column in c1.description]
        df_config = pd.DataFrame(c1.fetchall(), columns=columns)

    # 查询电厂文档表
    with conn.cursor() as c2:
        c2.execute('SELECT Power_plant_name, Unit_number, num_type, date, file_name FROM Oech_Analyse_power_plant_documentation WHERE is_write != "是" OR is_write IS NULL')
        table_data1 = c2.fetchall()

    # 构造文件路径列表
    full_paths = [os.path.join('Z:/power_plant_data', *row[:-1], row[-1]).replace('\\', '/') for row in table_data1]
    table_data2 = [row + (path,) for row, path in zip(table_data1, full_paths)]

    # 构建列名替换字典
    header_replacement_dict = {value: column for column, values in df_config.items() for value in values}

    with conn.cursor() as cursor:
        # 遍历文件数据列表
        for a in tqdm(table_data2, desc='处理文件', unit='文件'):
            try:
                with open(a[-1], 'r', encoding='utf-8') as csvfile:
                    reader = csv.reader(csvfile)
                    # 读取CSV文件中的数据，去除第1行和第3行
                    rows = [row for index, row in enumerate(reader) if index not in [0, 2]]
            
                columns2 = rows[0]
                data_rows = [row[:-1] for row in rows[1:]]
                new_data = pd.DataFrame(data_rows, columns=columns2)

                # 获取 time 列的第1行第2行和最后一行数据
                start_time = new_data.iloc[:, 0].iloc[0]
                end_time = new_data.iloc[:, 0].iloc[-1]
                second_time = new_data.iloc[:, 0].iloc[1]
            
                # 转换字符串为 datetime 类型，使用正确的格式化字符串
                start_time = datetime.datetime.strptime(start_time.strip(), "%Y/%m/%d %H:%M:%S")
                second_time = datetime.datetime.strptime(second_time.strip(), "%Y/%m/%d %H:%M:%S")
                end_time = datetime.datetime.strptime(end_time.strip(), "%Y/%m/%d %H:%M:%S")
            
                # 计算时间差值
                data_granularity = second_time - start_time
                data_granularity = format(data_granularity)[-10:]

                # 列名替换
                new_data.columns = [header_replacement_dict[col] if col in header_replacement_dict else col for col in new_data.columns]
                
                new_data['power_plant_id'] = a[0][:3]
                new_data['power_plant_name'] = a[0]
                new_data['unit_number'] = a[1]
                new_data['num_type'] = a[2]
                new_data['data_granularity'] = data_granularity

                # 去除重复列
                valid_data = new_data.loc[:, ~new_data.columns.duplicated()]

                # 遍历需要插入的表名列表
                for table_name in ['Oech_Analyse_data_analysis_data', 'Oech_Analyse_gt_cso_temp', 'Oech_Analyse_fuel_gas_control', 'Oech_Analyse_shell_metal_t_brg', 'Oech_Analyse_cass_cpfm_max', 'Oech_Analyse_bop', 'Oech_Analyse_cass_gtc', 'Oech_Analyse_cass_cpfm_p_peak_value', 'Oech_Analyse_cass_cpfm_p_peak_freq', 'Oech_Analyse_cass_cpfm_acc_peak', 'Oech_Analyse_gt_cso_ref_eoh']:
                    insert_data_into_database(cursor, table_name, valid_data)

                # 更新文件记录为已写入
                update_sql = f"UPDATE Oech_Analyse_power_plant_documentation SET is_write = '是' WHERE Power_plant_name = '{a[0]}' AND Unit_number = '{a[1]}' AND file_name = '{a[4]}';"
                cursor.execute(update_sql)
                conn.commit()

            except FileNotFoundError:
                tqdm.write(f"文件未找到: {a[-1]} 跳过下一个文件。")
                continue

            except Exception as e:
                tqdm.write(f"处理文件时出错 {a[-1]}: {e}")
                continue
    # 创建数据库连接
    connection = connect_to_database()
    cursor = connection.cursor()

    # 构造插入语句
    insert_query = f"""
        INSERT INTO Oech_Analyse_data_granularity (
  date_part,
  power_plant_id,
  unit_number,
  num_type,
  min_time_part,
  max_time_part,
  data_granularity
) 
SELECT 
  date_part,
  power_plant_id,
  unit_number,
  num_type,
  min_time_part,
  max_time_part,
  data_granularity
FROM 
(
  WITH RankedData AS (
    SELECT DISTINCT
      DATE(time) AS date_part,
      TIME(time) AS time_part,
      power_plant_id,
      unit_number,
      num_type,
      data_granularity,   
      ROW_NUMBER() OVER (PARTITION BY DATE(time), power_plant_id, unit_number, num_type, data_granularity ORDER BY TIME(time)) AS rn_asc,
      ROW_NUMBER() OVER (PARTITION BY DATE(time), power_plant_id, unit_number, num_type, data_granularity ORDER BY TIME(time) DESC) AS rn_desc
    FROM Oech_Analyse_data_analysis_data
  )
  SELECT 
    power_plant_id,
    unit_number,
    num_type,
    date_part,
    MIN(CASE WHEN rn_asc = 1 THEN time_part END) AS min_time_part,
    MAX(CASE WHEN rn_desc = 1 THEN time_part END) AS max_time_part,
    data_granularity
  FROM RankedData
  GROUP BY
    date_part,
    power_plant_id,
    unit_number,
    num_type,
    data_granularity
) AS Result
ON DUPLICATE KEY UPDATE
  min_time_part = Result.min_time_part,
  max_time_part = Result.max_time_part,
  data_granularity = Result.data_granularity;"""

    # 执行插入
    cursor.execute(insert_query)

    # 提交更改并关闭连接
    connection.commit()
    cursor.close()
    connection.close()

    time_end = time.time()  # 记录结束时间
    time_sum = time_end - time_start  # 计算的时间差为程序的执行时间，单位为秒/s
    print(f"执行时间: {time_sum} 秒")   # 输出执行时间


# 主程序入口
if __name__ == "__main__":
    main()

print('异常数据检查处理')

time1 = time.time()  # 记录开始时间

# 创建数据库连接
connection = connect_to_database()
cursor = connection.cursor()

# 执行SQL语句
sql_queries = [
    "UPDATE Oech_Analyse_data_analysis_data SET gt_actld = gt_load WHERE gt_actld IS NULL;",
    "UPDATE Oech_Analyse_gt_cso_ref_eoh SET bpref = exref WHERE bpref IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_press = fg_p_spcv_out WHERE fg_press IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET ma_fcv = mb_fcv WHERE ma_fcv IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET ma_fcv_dp = mb_fcv_dp WHERE ma_fcv_dp IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET ma_mani_p = mb_mani_p WHERE ma_mani_p IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_ma_flow = fg_mb_flow WHERE fg_ma_flow IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_ma_filter_dp = fg_mb_filter_dp WHERE fg_ma_filter_dp IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_lhv_calorie_meter_a = fg_lhv_calorie_meter_b WHERE fg_lhv_calorie_meter_a IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_density_calorie_metera = fg_density_calorie_meterb WHERE fg_density_calorie_metera IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_co2_calorie_metera = fg_co2_calorie_meterb WHERE fg_co2_calorie_metera IS NULL;",
    "UPDATE Oech_Analyse_fuel_gas_control SET fg_n2_calorie_metera = fg_n2_calorie_meterb WHERE fg_n2_calorie_metera IS NULL;",
    "UPDATE Oech_Analyse_shell_metal_t_brg SET thrust_brg_metal_t_gtb = thrust_brg_metal_t_geneb WHERE thrust_brg_metal_t_gtb IS NULL;",
    "UPDATE Oech_Analyse_bop SET tca_outlet_water_t = tca_outlet_water_p WHERE tca_outlet_water_t IS NULL;"
]

# 依次执行每个 SQL 查询
for query in tqdm(sql_queries):
    cursor.execute(query)

# 提交更改并关闭连接
connection.commit()
cursor.close()
connection.close()

time2 = time.time()  # 记录结束时间
time_sum = time2 - time1  # 计算的时间差为程序的执行时间，单位为秒/s
print(f"执行时间: {time_sum} 秒")   # 输出执行时间


time.sleep(10)

os.system("taskkill /f /im cmd.exe") # 关闭cmd窗口


# In[ ]:




