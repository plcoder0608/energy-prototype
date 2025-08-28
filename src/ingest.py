import geopandas as gpd
from sqlalchemy import create_engine
import os
from datetime import datetime

DB_CONFIG = {
    "host": "localhost",
    "database": "energy",
    "user": "postgres",
    "password": "senha",
    "port": "5432"
}

SHP_PATH = "data/solar/atlas_solar/shp/GLOBAL_HORIZONTAL/global_horizontal_means.shp"
TABLE_NAME = "atlas_solar_global_horizontal"

def ingest_atlas_solar():
    """Ingere dados do Atlas Solar para o PostGIS"""
    
    print("🚀 Iniciando ingestão do Atlas Solar para PostGIS...")
    
    try:
        engine = create_engine(f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        with engine.connect() as conn:
            print("✅ Conexão com PostgreSQL bem-sucedida!")
    except Exception as e:
        print(f"❌ Erro na conexão: {e}")
        return False

    try:
        print("📂 Carregando shapefile...")
        gdf = gpd.read_file(SHP_PATH)
        print(f"✅ Shapefile carregado: {len(gdf)} registros")
        print(f"📊 Colunas: {list(gdf.columns)}")
        print(f"🎯 CRS: {gdf.crs}")
        print(f"📈 Amostra de dados:\n{gdf.head(2)}")
        
    except Exception as e:
        print(f"❌ Erro ao carregar shapefile: {e}")
        return False

    try:
        gdf['ingestion_date'] = datetime.now()
        gdf['data_source'] = 'INPE_LABREN'
        gdf['unidade'] = 'kWh/m²/dia'
        
        if 'mean' in gdf.columns:
            gdf = gdf.rename(columns={'mean': 'irradiacao_media'})
        
        print("✅ Dados preparados para ingestão")
        
    except Exception as e:
        print(f"❌ Erro no preparo dos dados: {e}")
        return False

    try:
        print("💾 Salvando no PostgreSQL...")
        gdf.to_postgis(
            name=TABLE_NAME,
            con=engine,
            if_exists="replace", 
            index=False,
            chunksize=10000 
        )
        print(f"✅ Dados salvos na tabela '{TABLE_NAME}'!")
        
    except Exception as e:
        print(f"❌ Erro ao salvar no PostgreSQL: {e}")
        return False

    try:
        with engine.connect() as conn:
            from sqlalchemy import text

            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME};"))
            count = result.scalar()
            print(f"✅ Verificação: {count} registros na tabela '{TABLE_NAME}'")
        
            result = conn.execute(text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{TABLE_NAME}';
            """))
            print("\n📋 Estrutura da tabela:")
            for row in result:
                print(f"  - {row[0]}: {row[1]}")
                
    except Exception as e:
        print(f"⚠️  Erro na verificação: {e}")
        return False

    print("🎉 Ingestão concluída com sucesso!")
    return True

if __name__ == "__main__":
    ingest_atlas_solar()