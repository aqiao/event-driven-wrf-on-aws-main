'''
压缩wrfout
插值
'''
#%%
import sys

import numpy as np
import pandas as pd
import xarray as xr
import wrf 
from netCDF4 import Dataset
from tqdm import tqdm
import os 
import time
import warnings
warnings.filterwarnings("ignore")
from zConfig import date_now_str, gfs_database_dir, gfs_excel_database_dir, gfs_cut_database_dir, gfs_netcdf_dir, gfs_varnames_path, gfs_log_path, locat_path, pred_length, target_height, record_dir
from zFunc import mkdir_dir_notexist, load_WT_locat, export_excel, export_netcdf, export_log, utc_to_bjt, add_vars_leadhour, detect_files
from zCal import interp_gfs_vars, cal_gfs_extend_vars

prefix = sys.argv[1]
s3_prefix = sys.argv[2]
local_gfs_database_dir = gfs_database_dir(prefix)
local_gfs_excel_database_dir = gfs_excel_database_dir(prefix)
local_gfs_cut_database_dir = gfs_cut_database_dir(prefix)
local_gfs_netcdf_dir = gfs_netcdf_dir(prefix)
local_gfs_varnames_path = gfs_varnames_path(prefix)
local_gfs_log_path = gfs_log_path(prefix)
local_locat_path = locat_path(prefix)
local_record_dir= record_dir(prefix)

def get_varnames(file_path):
    df_varnames = pd.read_excel(file_path)
    var_surface_names = df_varnames['surface'].dropna().to_list()
    var_sigma_names = df_varnames['sigma'].dropna().to_list()
    return var_surface_names, var_sigma_names

def yield_gfs_file_path_list():
    global date_now_str, gfs_dir
    # 预先给出的文件列表
    datetime_range = pd.date_range(f'{date_now_str} 13:00:00', periods=pred_length, freq='1H') # 丢弃前一天
    file_name_list = [time.strftime("wrfout_d02_%Y-%m-%d_%H:00:00") for time in datetime_range]
    file_path_list = [os.path.join(gfs_dir, file_name) for file_name in file_name_list] # 预先给出，不依赖实际wrfout生成情况
    return file_path_list

def pick_surface(wrfin, var_surface_names):
    var_surface_dict = {}
    for var_surface_name in var_surface_names:
        var_surface = wrf.getvar(wrfin, var_surface_name, timeidx=wrf.ALL_TIMES, method='cat')
        if var_surface.ndim == 2: # 拓展维度，应对插值问题
            time = pd.Timestamp(var_surface.Time.values)
            var_surface = var_surface.expand_dims(dim={'Time': [time]}, axis=0)
        del var_surface.attrs['projection'] # 删除信息，否则不能保存
        var_surface_dict[var_surface_name] = var_surface
    return var_surface_dict

def pick_sigma(wrfin, var_sigma_names, target_height=300):
    # 确定小于300的最高sigma层
    zindex = get_gfs_zindex(wrfin, target_height)

    # sigma层变量
    var_sigma_le_300m_dict = {}
    for var_sigma_name in var_sigma_names:
        var_sigma = wrf.getvar(wrfin, var_sigma_name, timeidx=wrf.ALL_TIMES, method='cat')
        var_sigma_le_300m = var_sigma.sel(bottom_top=slice(0, zindex))
        if var_sigma_le_300m.ndim == 3:
            time = pd.Timestamp(var_sigma.Time.values)
            var_sigma_le_300m = var_sigma_le_300m.expand_dims(dim={'Time': [time]}, axis=0)
        del var_sigma_le_300m.attrs['projection']
        var_sigma_le_300m_dict[var_sigma_name] = var_sigma_le_300m
    return var_sigma_le_300m_dict

def get_gfs_zindex(wrf_list, target_height):
    z_agl = wrf.getvar(wrf_list, 'height_agl', timeidx=wrf.ALL_TIMES, method='cat') # 模式高度AGL
    zindex_list = []
    try:
        for time in z_agl.Time.to_index():
            for i in z_agl.sel(Time=time).bottom_top.values:
                if z_agl.sel(bottom_top=i).min() >= target_height:
                    zindex_list.append(i)
                    break
        zindex = np.max(zindex_list)
    except ValueError: # 只有一个时刻的情况
        for i in z_agl.bottom_top.values:
            if z_agl.sel(bottom_top=i).min() >= target_height:
                zindex = i
                break
    return zindex

def export_cut(var_surface_dict, var_sigma_le_300m_dict, output_path):
    var_dict = var_surface_dict.copy()
    var_dict.update(var_sigma_le_300m_dict)
    ds = xr.Dataset(var_dict)
    ds.to_netcdf(output_path)

def cut_gfs(file_path):
    global local_gfs_varnames_path, target_height, gfs_cut_dir

    var_surface_names, var_sigma_names = get_varnames(local_gfs_varnames_path)
    wrfin = Dataset(file_path)
    var_surface_dict = pick_surface(wrfin, var_surface_names)
    var_sigma_le_300m_dict = pick_sigma(wrfin, var_sigma_names, target_height)

    output_name = "cut_" + os.path.basename(file_path)
    output_path = os.path.join(gfs_cut_dir, output_name)

    export_cut(var_surface_dict, var_sigma_le_300m_dict, output_path)

# -------------------------------------------------------------------------
export_log("-"*40, local_gfs_log_path)
export_log(f"**** {date_now_str} ****", local_gfs_log_path)
date_gfs_start = (pd.Timestamp(date_now_str) - pd.Timedelta('1D')).strftime('%Y%m%d')

gfs_dir = os.path.join(local_gfs_database_dir, date_gfs_start)
gfs_cut_dir = os.path.join(local_gfs_cut_database_dir, date_now_str)
gfs_excel_dir = os.path.join(local_gfs_excel_database_dir, date_now_str)

mkdir_dir_notexist(gfs_cut_dir)
mkdir_dir_notexist(gfs_excel_dir)

### 变量压缩
# 待读取的文件列表
file_path_list = yield_gfs_file_path_list()
file_path_list = detect_files(file_path_list, 'GFS', 100, 27, None, local_gfs_log_path, local_record_dir)

# for i in tqdm(range(len(file_path_list)-1)):
#     file_path_current, file_path_next = file_path_list[i], file_path_list[i+1]
#     search_count = 0
#     while os.path.exists(file_path_next) == False:
#         search_count += 1
#         time.sleep(wait_time)
#         if search_count >= search_max: raise(RuntimeError('超过最大搜索轮数'))
#     cut_gfs(file_path_current, var_surface_names, var_sigma_names, target_height, gfs_cut_dir)

for file_path in tqdm(file_path_list):
    cut_gfs(file_path)

export_log(f"**** wrfout压缩完成 ****", local_gfs_log_path)
export_log("-"*40, local_gfs_log_path)

# %%
### 变量插值（合并之后）
# 风机高度及位置信息
turbines_info = load_WT_locat(locat_path)
select_levels = np.unique(turbines_info[-1])

ds_final_list = []
for file_name in tqdm(sorted(os.listdir(gfs_cut_dir))):
    # export_log(f'-'*40, log_path)
    # export_log(f'> 当前{file_name}', gfs_log_path)
    file_path = os.path.join(gfs_cut_dir, file_name)
    ds = xr.open_dataset(file_path)

    ds_interp = interp_gfs_vars(ds, turbines_info, select_levels, local_gfs_log_path, cal_static=True)
    ds_final = cal_gfs_extend_vars(ds_interp, drop_var=False)
    ds_final_list.append(ds_final)

ds_concat = xr.concat(ds_final_list, dim='time')
ds_concat = utc_to_bjt(ds_concat)
ds_concat = add_vars_leadhour(ds_concat, 1, ['T2', 'tk', 'PSFC', 'pressure'])
ds_concat = add_vars_leadhour(ds_concat, 3, ['T2', 'tk', 'PSFC', 'pressure'])

export_excel(ds_concat, 'GFS', date_now_str, gfs_excel_dir)
export_netcdf(ds_concat, 'GFS', date_now_str, gfs_netcdf_dir)
# script += f"\naws s3 cp ../slurm-${{SLURM_JOB_ID}}.out {output}/logs/\n"
# script += f"\naws s3 cp /fsx/{zone}/post {output}/post/ --recursive \n"
export_log(f"**** 开始上传后处理结果文件到S3 ****", local_gfs_log_path)
os.system(f"aws s3 cp {prefix}/post {s3_prefix} --recursive")
export_log(f"**** 处理和导出完成 ****", local_gfs_log_path)