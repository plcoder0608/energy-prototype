import geopandas as gpd
from sqlalchemy import create_engine
import numpy as np

def extract_impact_features(grid, engine):
    """Extrai features de impacto ambiental"""
    print("   🏞️  Calculando distância a UCs...")
    try:
        ucs = gpd.read_postgis(
            "SELECT * FROM unidades_conservacao", 
            engine, 
            geom_col='geometry'
        )
        ucs_union = ucs.unary_union
        grid['dist_to_uc_km'] = grid['centroid'].distance(ucs_union) / 1000
    except:
        print("   ⚠️  UCs não encontradas - pulando")
        grid['dist_to_uc_km'] = np.nan
    
    print("   👥 Calculando densidade populacional...")
    grid['pop_density'] = np.nan
    
    return grid