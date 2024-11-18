from src import utils
import pandas as pd

def main() -> None:
    nds_path = 'data/ndsbr_full.parquet'
    ndsbr_sample = utils.load_nds(nds_path)

    url_vias = 'https://ippuc.org.br/geodownloads/SHAPES_SIRGAS/EIXO_RUA_SIRGAS.zip'
    vias = utils.load_vias(url_vias)

    radares_path = 'data/radar.geojson'
    radares = utils.load_radares(radares_path)

    ndsbr_geo = utils.nds_to_geo(ndsbr_sample)
    ndsbr_geo = utils.nds_filter_speed(ndsbr_geo, 5)

    vias_v85 = utils.add_v85_to_vias(ndsbr_geo, vias)
    vias_radares = utils.add_radares_to_vias(radares, vias_v85)

    #print(vias_radares.query('presenca_radar == True').head())
    vias_radares.to_file('data/vias_estudo.geojson')

if __name__ == '__main__':
    main()
