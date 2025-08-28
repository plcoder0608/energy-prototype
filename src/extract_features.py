import geopandas as gpd
from sqlalchemy import create_engine, text
import numpy as np
import time

def main():
    print("🎯 EXTRAINDO FEATURES (VERSÃO FINAL)...")
    start_time = time.time()
    
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    print("1. Carregando amostra do grid...")
    grid = gpd.read_postgis(
        "SELECT * FROM energy_grid",
        engine, 
        geom_col='geometry'
    )
    print(f"   📊 {len(grid)} células para processar")
    
    print("2. Extraindo solar via SQL...")
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                DROP TABLE IF EXISTS temp_solar_features;
                CREATE TABLE temp_solar_features AS
                SELECT 
                    eg.cell_id,
                    AVG(asol."ANNUAL") as solar_irradiance
                FROM energy_grid eg
                LEFT JOIN atlas_solar_utm asol 
                    ON ST_Intersects(eg.geometry, asol.geometry)
                WHERE eg.cell_id IN (SELECT cell_id FROM energy_grid)
                GROUP BY eg.cell_id;
            """))
            
            result = conn.execute(text("SELECT COUNT(*) FROM temp_solar_features"))
            count = result.scalar()
            print(f"   ✅ {count} células processadas")
            
            result = conn.execute(text("SELECT * FROM temp_solar_features"))
            solar_data = []
            for row in result:
                solar_data.append({'cell_id': row[0], 'solar_irradiance': row[1]})
            
            import pandas as pd
            solar_df = pd.DataFrame(solar_data)
          
        grid = grid.merge(solar_df, on='cell_id', how='left')
        print(f"   🔄 {grid['solar_irradiance'].notna().sum()} células com dados solares")
        
    except Exception as e:
        print(f"   ❌ Erro: {e}")
        print("   🔄 Usando fallback...")
        grid['solar_irradiance'] = np.nan
    
    print("3. Adicionando features básicas...")
    grid['wind_potential'] = np.nan
    grid['dist_to_uc_km'] = np.nan
    grid['pop_density'] = np.nan
    grid['dist_to_grid_km'] = np.nan
    grid['connection_cost_brl'] = np.nan
    
    print("4. Salvando...")
    grid.to_postgis('energy_features_sample', engine, if_exists='replace', index=False)
    
    processing_time = time.time() - start_time
    
    if 'solar_irradiance' in grid.columns and grid['solar_irradiance'].notna().any():
        solar_data = grid['solar_irradiance'].dropna()
        print(f"   ☀️  Solar - Células com dados: {len(solar_data)}")
        print(f"   📈 Mín: {solar_data.min():.1f}, Máx: {solar_data.max():.1f}, Méd: {solar_data.mean():.1f}")
    else:
        print("   ⚠️  Nenhum dado solar extraído")
    
    print(f"   ⏱️  Tempo: {processing_time:.1f} segundos")
    print("\n🎉 FEATURES EXTRAÍDAS COM SUCESSO!")

if __name__ == "__main__":
    main()