import numpy as np
import pandas as pd
import xarray as xr
import os 
import wrf
from glob import glob
from geocat.comp import rcm2rgrid, rcm2points
from zFunc import export_log

def interp_ec_vars(ds, turbines_info, log_path):
    turbines_names, turbines_lons, turbines_lats, turbines_height = turbines_info

    # 维度信息
    ds_lat_reverse = ds.sortby('latitude', ascending=True) 
    lons, lats = np.meshgrid(ds_lat_reverse['longitude'], ds_lat_reverse['latitude'])
    
    # 地理插值
    var_interp_dict = {}
    for var_name in ds.data_vars:
        # export_log(f"> 正在插值EC_{var_name}", log_path)
        data_prepare = ds[var_name]
        data_interp2d = interp2d_wrfout(data_prepare, lons, lats, turbines_lons, turbines_lats, turbines_names)
        # export_log(f"Max: {np.max(data_interp2d).values:.4f}", log_path)
        var_interp_dict[var_name] = data_interp2d
    ds_interp = xr.Dataset(var_interp_dict)
    return ds_interp

def cal_ec_extend_vars(ds_interp, log_path, drop_var=False):
    # 风速风向、GFS的空气密度
    ds_final = ds_interp.copy()
    # export_log(f"> 正在计算风速风向", log_path)
    ds_final['ws100'], ds_final['wd100'] = cal_WS_WD(ds_final['u100'], ds_final['v100'])
    if drop_var == True:
        ds_final = ds_final.drop_vars(['u100', 'v100'])
    return ds_final

def interp_gfs_vars(ds, turbines_info, select_levels, log_path, cal_static=False):
    turbines_names, turbines_lons, turbines_lats, turbines_height = turbines_info

    # 维度信息
    ds = ds.rename({'Time': 'time'})
    lats, lons = ds['XLAT'], ds['XLONG']

    # 垂直坐标
    z_agl = ds['z'] - ds['ter']

    # 计算稳定度
    if cal_static == True:
        # export_log(f"> 正在计算静力稳定度", log_path)
        ds['ss'] = cal_static_stability(ds['pressure'], ds['tk'])
        # export_log(f"Max: {np.max(ds['ss']).values:.4f}", log_path) 
    
    # 垂直+地理插值
    var_interp2d_dict = {}
    for var_name in ds.data_vars:
        # export_log(f"> 正在插值GFS_{var_name}", log_path)
        data_prepare = ds[var_name]
        data_interplevel = interplevel_wrfout(data_prepare, z_agl, select_levels)
        data_interp2d = interp2d_wrfout(data_interplevel, lons, lats, turbines_lons, turbines_lats, turbines_names, select_levels)
        # export_log(f"Max: {np.max(data_interp2d).values:.4f}", log_path)
        var_interp2d_dict[var_name] = data_interp2d

    # 风机高度点插值
    var_interp_dict = {}
    for var_name, data_interp2d in var_interp2d_dict.items():
        data_interp3d = interp3dpoint_wrfout(data_interp2d, turbines_height)
        var_interp_dict[var_name] = data_interp3d
    ds_interp = xr.Dataset(var_interp_dict)
    return ds_interp

def cal_gfs_extend_vars(ds_interp, drop_var=False):
    # 风速风向、GFS的空气密度
    ds_final = ds_interp.copy()
    # export_log(f"> 正在计算风速风向", log_path)
    ds_final['ws'], ds_final['wd'] = cal_WS_WD(ds_final['ua'], ds_final['va'])
    ds_final['ws10'], ds_final['wd10'] = cal_WS_WD(ds_final['U10'], ds_final['V10'])
    # export_log(f"> 正在计算空气密度", log_path)
    ds_final['air_density'] = cal_air_density(ds_final['pressure'], ds_final['tk'], ds_final['QVAPOR'])
    if drop_var == True:
        ds_final = ds_final.drop_vars(['ua', 'va', 'U10', 'V10', 'z'])
    return ds_final

def interplevel_wrfout(da_input, z_agl, target_level):
    '''对300m以下原始bottom_top进行风机高度插值'''
    if 'bottom_top' in da_input.dims:
        da_interp_level = wrf.interplevel(da_input, z_agl, target_level, squeeze=False)
        return da_interp_level
    else:
        return da_input

def interp2d_wrfout(da_input, source_lon2d, source_lat2d, target_lons, target_lats, target_names, select_levels=None):
    if 'level' in da_input.dims:
        data_interp = xr.DataArray(np.nan, dims=['time','level','id'], coords=[da_input.time, select_levels, target_names])
        for level in select_levels:
            data_input = da_input.sel(level=level)
            data_interp.loc[dict(level=level)] = rcm2points(source_lat2d, source_lon2d, data_input, target_lats, target_lons)
    else:
        data_interp = xr.DataArray(np.nan, dims=['time','id'], coords=[da_input.time, target_names])
        data_interp.loc[:] = rcm2points(source_lat2d, source_lon2d, da_input, target_lats, target_lons)
    return data_interp

def interp3dpoint_wrfout(da_input, target_level):
    '''每台风机的3d插值，3D插值之前必须完成2D插值'''
    if 'level' in da_input.dims:
        da_interp3d = da_input.isel(level=0).copy()
        for id in da_input.id:
            da_interp3d.loc[dict(id=id)] = da_input.sel(id=id, level=target_level.sel(id=id))
        da_interp3d.coords['level'] = target_level
        return da_interp3d
    else:
        return da_input

def cal_WS_WD(u, v):
    from metpy.calc import wind_speed, wind_direction
    from metpy.units import units
    ws, wd = u.copy(), u.copy()
    u_input = u.values * units('m/s')
    v_input = v.values * units('m/s')
    ws[:], wd[:] = wind_speed(u_input, v_input), wind_direction(u_input, v_input)
    return ws, wd

def cal_air_density(pres, t, mixing_ratio):
    from metpy.calc import density
    from metpy.units import units
    rho = pres.copy()
    pres_input = pres * units('hPa')
    t_input = t * units('degK')
    mixing_ratio_input = mixing_ratio * units('kg/kg')
    rho[:] = density(pres_input, t_input, mixing_ratio_input)
    return rho

def cal_potential_temperature(pres, t):
    from metpy.calc import potential_temperature
    from metpy.units import units
    th = pres.copy()
    pres_input = pres * units('hPa')
    t_input = t * units('degK')
    th[:] = potential_temperature(pres_input, t_input)
    return th

def cal_static_stability(pres, t, vertical_dim=1):
    from metpy.calc import static_stability
    from metpy.units import units
    ss = pres.copy()
    pres_input = pres * units('hPa')
    t_input = t * units('degK')
    ss[:] = static_stability(pres_input, t_input, vertical_dim)
    return ss