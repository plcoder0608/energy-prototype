# preprocess_fixed.py
import geopandas as gpd
from sqlalchemy import create_engine, text
from pyproj import CRS
import os

def main():
    print("🚀 INICIANDO PRÉ-PROCESSAMENTO...")
    
    # 1. Conectar ao banco
    print("1. Conectando ao PostgreSQL...")
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    # 2. Carregar dados solares
    print("2. Carregando dados solares...")
    gdf_solar = gpd.read_postgis("SELECT * FROM atlas_solar_global_horizontal", engine, geom_col='geometry')
    print(f"   ✅ {len(gdf_solar)} registros carregados")
    
    # 3. Definir CRS UTM (versão simplificada)
    print("3. Definindo CRS UTM...")
    utm_crs = CRS.from_epsg(32724)  # UTM 24S - Brasil
    print(f"   🎯 CRS: {utm_crs}")
    
    # 4. Reprojetar
    print("4. Reprojetando para UTM...")
    gdf_solar_utm = gdf_solar.to_crs(utm_crs)
    print("   ✅ Reprojeção concluída")
    
    # 5. Salvar no banco
    print("5. Salvando dados reprojetados...")
    gdf_solar_utm.to_postgis('atlas_solar_utm', engine, if_exists='replace', index=False)
    print("   ✅ Dados salvos no PostGIS")
    
    # 6. Criar área de estudo
    print("6. Criando área de estudo...")
    area_of_interest = gdf_solar_utm.unary_union.convex_hull
    area_gdf = gpd.GeoDataFrame({'name': ['study_area']}, 
                               geometry=[area_of_interest], 
                               crs=utm_crs)
    
    area_gdf.to_postgis('study_area', engine, if_exists='replace', index=False)
    print(f"   📐 Área de estudo: {area_of_interest.area:,.0f} m²")
    print(f"   📏 Aproximadamente: {area_of_interest.area / 1e6:,.0f} km²")
    print("   ✅ Área de estudo salva")
    
    # 7. Verificação final
    print("7. Verificando...")
    with engine.connect() as conn:
        # Verificar dados UTM
        result = conn.execute(text("SELECT COUNT(*) FROM atlas_solar_utm"))
        count_utm = result.scalar()
        print(f"   📊 {count_utm:,} registros na tabela UTM")
        
        # Verificar área de estudo
        result = conn.execute(text("SELECT ST_Area(geometry) FROM study_area"))
        area_m2 = result.scalar()
        print(f"   🗺️  Área de estudo: {area_m2:,.0f} m²")
    
    print("\n🎉 PRÉ-PROCESSAMENTO ESPACIAL CONCLUÍDO COM SUCESSO!")
    print("   ✅ Dados reprojetados para UTM")
    print("   ✅ Área de estudo definida")
    print("   ✅ Tudo pronto para análise!")

if __name__ == "__main__":
    main()