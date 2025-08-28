import geopandas as gpd
from sqlalchemy import create_engine
import numpy as np

def extract_cost_features(grid, engine, cost_per_km=50000):
    """Extrai features de custo de conexão"""
    print("   🔌 Calculando distância à rede...")
    grid['dist_to_grid_km'] = np.nan 
    
    print("   💰 Calculando custo de conexão...")
    grid['connection_cost_brl'] = grid['dist_to_grid_km'] * cost_per_km
    
    return grid