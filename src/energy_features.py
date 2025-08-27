# scripts/energy_features.py
import geopandas as gpd
import requests
from sqlalchemy import create_engine
import numpy as np

def get_solar_nasa_power(lon, lat):
    """Obtém irradiação solar da NASA POWER API"""
    try:
        url = "https://power.larc.nasa.gov/api/temporal/climatology/point"
        params = {
            "parameters": "ALLSKY_SFC_SW_DWN",
            "community": "RE",
            "longitude": lon,
            "latitude": lat,
            "format": "JSON"
        }
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        return data['properties']['parameter']['ALLSKY_SFC_SW_DWN']['annual']
    except Exception as e:
        print(f"   ⚠️  Erro NASA POWER: {e}")
        return None

def extract_energy_features(grid, engine, sample_size=100):
    """Extrai features de energia solar e eólica"""
    
    # SOLAR: Usar dados do Atlas Solar existente
    print("   ☀️  Processando solar (ANNUAL)...")
    
    try:
        # Carregar dados solares com a coluna correta ANNUAL
        solar_data = gpd.read_postgis(
            "SELECT ANNUAL, geometry FROM atlas_solar_utm", 
            engine, 
            geom_col='geometry'
        )
        
        print(f"   📊 Dados solares carregados: {len(solar_data)} polígonos")
        print(f"   📈 Média de irradiação: {solar_data['ANNUAL'].mean():.1f} kWh/m²/dia")
        
        # Cruzamento espacial para obter irradiação solar
        print("   🔄 Fazendo cruzamento espacial...")
        grid_solar = gpd.sjoin(grid, solar_data, how='left', predicate='intersects')
        
        # Calcular média por célula
        solar_means = grid_solar.groupby('cell_id')['ANNUAL'].mean()
        grid['solar_irradiance'] = grid['cell_id'].map(solar_means)
        
        print(f"   ✅ {grid['solar_irradiance'].notna().sum()} células com dados solares")
        
    except Exception as e:
        print(f"   ❌ Erro no processamento solar: {e}")
        grid['solar_irradiance'] = np.nan
    
    # EÓLICA: Placeholder (precisa do Atlas Eólico)
    print("   💨 Processando eólica...")
    grid['wind_potential'] = np.nan
    
    # NASA POWER para amostra (apenas se necessário)
    missing_solar = grid['solar_irradiance'].isna().sum()
    if missing_solar > 0:
        print(f"   🛰️  NASA POWER para {missing_solar} células sem dados...")
        missing_cells = grid[grid['solar_irradiance'].isna()].sample(min(sample_size, missing_solar))
        
        for idx, row in missing_cells.iterrows():
            centroid = row['centroid']
            # Converter UTM para WGS84 (lat/lon) para NASA POWER
            from pyproj import Transformer
            transformer = Transformer.from_crs("EPSG:32724", "EPSG:4326", always_xy=True)
            lon, lat = transformer.transform(centroid.x, centroid.y)
            
            irrad = get_solar_nasa_power(lon, lat)
            if irrad:
                grid.loc[idx, 'solar_irradiance'] = irrad
        
        print(f"   ✅ {grid['solar_irradiance'].notna().sum()} células com dados após NASA POWER")
    
    return grid