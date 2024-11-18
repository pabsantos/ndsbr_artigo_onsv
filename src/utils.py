import pandas as pd
import geopandas as gpd
import numpy as np

def load_nds(path: str) -> pd.DataFrame:
    return pd.read_parquet(path)

def load_vias(url: str) -> gpd.GeoDataFrame:
    cols = ['CHAVE', 'HIERARQUIA','geometry']
    vias = gpd.read_file(url, encoding='latin1')
    return vias[cols].to_crs('31982')

def load_radares(path: str) -> gpd.GeoDataFrame:
    radar = gpd.read_file(path)
    return radar.to_crs('31982').dropna()

def nds_to_geo(nds_sample: pd.DataFrame) -> gpd.GeoDataFrame:
    ndsbr_geo = gpd.GeoDataFrame(
        nds_sample,
        geometry=gpd.points_from_xy(nds_sample['long'], nds_sample['lat']),
        crs='4674'
    )
    return ndsbr_geo.to_crs('31982')

def nds_filter_speed(nds_geo: gpd.GeoDataFrame, speed: int) -> pd.DataFrame:
    nds_valid_speed = nds_geo.query(f'spd_kmh > {speed}')
    return nds_valid_speed

def add_v85_to_vias(nds_geo: gpd.GeoDataFrame, vias: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    ndsbr_nearest = gpd.sjoin_nearest(
       nds_geo,
       vias,
       how='left',
       max_distance=20
    )
    v85_spd = ndsbr_nearest.groupby('CHAVE')['spd_kmh'].quantile(0.85)
    v85_spd = v85_spd.reset_index()
    vias_v85 = pd.merge(vias, v85_spd, how='left', on='CHAVE')
    vias_v85_valid = vias_v85.dropna(subset='spd_kmh')
    return vias_v85_valid

def add_radares_to_vias(radares: gpd.GeoDataFrame, vias: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    radar_vias = gpd.sjoin_nearest(
        radares,
        vias[['CHAVE', 'geometry']],
        how='left',
        max_distance=20
    ).dropna()

    radar_vias_count = radar_vias['CHAVE'].value_counts().to_frame().reset_index()

    radar_vias_count['presenca_radar'] = radar_vias_count['count'].apply(
        lambda x: True if x >= 1 else False
    )

    vias_radares = pd.merge(
        vias,
        radar_vias_count,
        how='left',
        on='CHAVE'
    )

    vias_radares['presenca_radar'] = vias_radares['presenca_radar'].fillna(False)
    vias_radares['count'] = vias_radares['count'].fillna(0)
    return vias_radares
