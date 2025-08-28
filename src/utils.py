import requests
import numpy as np
from rasterstats import zonal_stats

def sample_raster(grid, raster_path, stat='mean'):
    """Amostra raster nas células do grid"""
    try:
        stats = zonal_stats(grid['geometry'], raster_path, stats=[stat])
        return [s[stat] for s in stats]
    except:
        return [np.nan] * len(grid)

def download_worldpop(bbox, output_path):
    """Download de dados WorldPop (exemplo)"""
    pass