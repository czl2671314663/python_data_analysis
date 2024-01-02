#!/usr/bin/env python
# coding: utf-8

# In[3]:


import csv
import os
import pymysql
import pandas as pd

def connect_to_database():
    return pymysql.connect(host='172.16.50.150', user='root', password='fz@2023&', database='input')

def get_table_columns(cursor, table_name):
    cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    return [column[0] for column in cursor.fetchall()]

def insert_data_into_database(cursor, table_name, data_frame):
    existing_columns = get_table_columns(cursor, table_name)
    valid_columns = [col for col in data_frame.columns if col in existing_columns]
    valid_columns_not_null = [col for col in valid_columns if data_frame[col].notnull().any()]

    insert_sql = f"INSERT INTO {table_name} ({', '.join(valid_columns)}) VALUES ({', '.join(['%s'] * len(valid_columns))}) ON DUPLICATE KEY UPDATE {', '.join([f'`{col}` = VALUES(`{col}`)' for col in valid_columns_not_null])};"
    
    values = [tuple(str(row[col]) for col in valid_columns) for index, row in data_frame.iterrows()]
    
    cursor.executemany(insert_sql, values)

def main():
    conn = connect_to_database()

    # 查询配置表
    with conn.cursor() as c1:
        c1.execute('SELECT * FROM Oech_Analyse_configure')
        columns = [column[0] for column in c1.description]
        df_config = pd.DataFrame(c1.fetchall(), columns=columns)

    # 查询电厂文档表
    with conn.cursor() as c2:
        c2.execute('SELECT Power_plant_name, Unit_number, num_type, file_name FROM Oech_Analyse_power_plant_documentation WHERE is_write != "是" OR is_write IS NULL')
        table_data1 = c2.fetchall()

    full_paths = [os.path.join('D:/power_plant_data', *row[:-1], row[-1]).replace('\\', '/') for row in table_data1]
    table_data2 = [row + (path,) for row, path in zip(table_data1, full_paths)]

    header_replacement_dict = {value: column for column, values in df_config.items() for value in values}

    with conn.cursor() as cursor:
        for a in table_data2:
            with open(a[-1], 'r', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                rows = [row for index, row in enumerate(reader) if index not in [0, 2]]

            columns2 = rows[0]
            data_rows = [row[:-1] for row in rows[1:]]
            new_data = pd.DataFrame(data_rows, columns=columns2)

            new_data.columns = [header_replacement_dict[col] if col in header_replacement_dict else col for col in new_data.columns]

            new_data['power_plant_name'] = a[0]
            new_data['unit_number'] = a[1]

            valid_data = new_data.loc[:, ~new_data.columns.duplicated()]

            for table_name in ['Oech_Analyse_data_analysis_data', 'Oech_Analyse_gt_cso_temp', 'Oech_Analyse_fuel_gas_control', 'Oech_Analyse_shell_metal_t_brg', 'Oech_Analyse_cass_cpfm_max', 'Oech_Analyse_bop', 'Oech_Analyse_cass_gtc', 'Oech_Analyse_cass_cpfm_p_peak_value', 'Oech_Analyse_cass_cpfm_p_peak_freq', 'Oech_Analyse_cass_cpfm_acc_peak', 'Oech_Analyse_gt_cso_ref_eoh']:
                insert_data_into_database(cursor, table_name, valid_data)

            update_sql = f"UPDATE Oech_Analyse_power_plant_documentation SET is_write = '是' WHERE Power_plant_name = '{a[0]}' AND Unit_number = '{a[1]}' AND file_name = '{a[3]}';"
            cursor.execute(update_sql)
            conn.commit()

    conn.close()

if __name__ == "__main__":
    main()
    


# In[ ]:




