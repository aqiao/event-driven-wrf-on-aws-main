import subprocess as sp
import pandas as pd
import xarray as xr
import numpy as np
import wrf
import os
from zConfig import record_dir, date_now_str


def run_cmd(cmd):
    sp.run(cmd, shell='True')


def clean_idx(data_dir):
    run_cmd(f"rm -f {data_dir}/*.idx")


def export_log(msg, file_path):
    run_cmd(f"echo '{msg}' >> {file_path}")

def load_WT_locat(locat_file):
    df_locat = pd.read_excel(locat_file)
    turbines_names = df_locat['turbines'].values
    turbines_lons = xr.DataArray(df_locat['lons'].values, dims='id', coords={'id': turbines_names})
    turbines_lats = xr.DataArray(df_locat['lats'].values, dims='id', coords={'id': turbines_names})
    turbines_height = xr.DataArray(df_locat['WS_height'].values, dims='id', coords={'id': turbines_names})
    return turbines_names, turbines_lons, turbines_lats, turbines_height

def mkdir_dir_notexist(data_dir):
    if os.path.exists(data_dir) == False:
        print(f"新建 {data_dir}")
        os.makedirs(data_dir)
    else:
        print(f'{data_dir}已存在')

def export_excel(ds, model_name, refdate, output_dir):
    '''适用于EC、GFS、Pg、Merge'''
    data_vars = list(ds.data_vars)
    for id in ds.id.values:
        df_output = ds.sel(id=id).to_pandas()[data_vars]
        output_name = f"{model_name}_{id}_{refdate}.xlsx"
        output_path = os.path.join(output_dir, output_name)
        df_output.to_excel(output_path)

def export_excel_mean(ds, model_name, refdate, output_dir):
    '''适用于EC、GFS、Pg、Merge'''
    data_vars = list(ds.data_vars)
    df_output = ds.isel(id=slice(None, -1)).mean('id').to_pandas()[data_vars]  # 不平均tower
    output_name = f"{model_name}_{refdate}.xlsx"
    output_path = os.path.join(output_dir, output_name)
    df_output.to_excel(output_path)

def export_netcdf(ds, model_name, refdate, output_dir):
    '''适用于EC、GFS、Pg、Merge'''
    output_name = f"{model_name}_{refdate}.nc"
    output_path = os.path.join(output_dir, output_name)
    ds.to_netcdf(output_path)

def utc_to_bjt(ds):
    ds.coords['time'] = ds.coords['time'] + pd.Timedelta('8H')
    return ds

def add_vars_leadhour(ds, leadhour, vars_leadhour):
    for var_leadhour in vars_leadhour:
        var_new = f"{var_leadhour}_-{leadhour}h"
        ds[var_new] = ds[var_leadhour] - ds[var_leadhour].shift(time=4 * leadhour)
    return ds

def detect_files(file_path_list, model_name, all_files, min_valid_short, min_valid_mid, log_path, record_dir):
    global date_now_str

    file_path_exist_list = [os.path.exists(file_path) for file_path in file_path_list]
    N_file_exist = file_path_exist_list.count(True)
    record_dict = {
        'model': model_name,
        'all': all_files,
        'min_valid_short': min_valid_short,
        'min_valid_mid': min_valid_mid,
        'exist': N_file_exist,
        'valid_short': None,
        'valid_mid': None,
    }

    stop_flag = False
    if model_name == 'EC':
        if N_file_exist < min_valid_short:
            export_log(f"短期预报所需{model_name}文件数量不足", log_path)
            record_dict.update({'valid_short': 0, 'valid_mid': 0})
            stop_flag = True
        elif min_valid_short <= N_file_exist < min_valid_mid:
            export_log(f"中期预报所需{model_name}文件数量不足", log_path)
            record_dict.update({'valid_short': 1, 'valid_mid': 0})
        else:
            record_dict.update({'valid_short': 1, 'valid_mid': 1})
    else:
        if N_file_exist < min_valid_short:
            export_log(f"短期预报所需{model_name}文件数量不足", log_path)
            record_dict.update({'valid_short': 0})
            stop_flag = True
        else:
            record_dict.update({'valid_short': 1})

    record_exception(record_dict, record_dir)
    if stop_flag:
        raise (RuntimeError("所需文件缺失，中止程序"))
    else:
        return list(np.array(file_path_list)[file_path_exist_list])


def record_exception(record_dict, record_dir):
    global date_now_str

    file_path = os.path.join(record_dir, f"record_{date_now_str}.xlsx")
    column_names = ['all', 'min_valid_short', 'min_valid_mid', 'exist', 'valid_short', 'valid_mid']
    if not os.path.exists(file_path):
        df_empty = pd.DataFrame(None, index=['GFS', 'EC', 'Pg'], columns=column_names)
        df_empty.to_excel(file_path)
    df = pd.read_excel(file_path, index_col=0)
    df.loc[record_dict['model']] = [record_dict[column_name] for column_name in column_names]
    df.to_excel(file_path)