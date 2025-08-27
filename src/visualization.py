import geopandas as gpd
from sqlalchemy import create_engine
import contextily as ctx
import matplotlib.pyplot as plt
import folium
import os
import time

def visualize_score(table_name='final_scores'):
    """
    Lê a tabela de scores do banco de dados e gera mapas de visualização.
    """
    print("🎨 INICIANDO A VISUALIZAÇÃO DOS RESULTADOS...")
    
    # Conexão com o banco de dados
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    try:
        # 1. Carregar o GeoDataFrame com os scores finais
        print(f"1. Lendo scores da tabela '{table_name}'...")
        scores_gdf = gpd.read_postgis(f"SELECT * FROM {table_name}", engine, geom_col='geometry')
        print(f"   ✅ {len(scores_gdf)} células carregadas.")

        # 2. Gerar Heatmap Estático (PNG)
        print("2. Gerando heatmap estático...")
        fig, ax = plt.subplots(1, 1, figsize=(10, 10))
        scores_gdf.to_crs(epsg=3857).plot(
            column='score_norm',
            cmap='RdYlGn',
            scheme='quantiles',
            k=6,
            legend=True,
            ax=ax
        )
        ctx.add_basemap(ax, source=ctx.providers.Esri.WorldImagery)
        plt.title('Mapa de Viabilidade de Energia Solar', fontsize=16)
        output_path_static = '../outputs/maps/viabilidade_heatmap.png'
        plt.savefig(output_path_static, dpi=300, bbox_inches='tight')
        print(f"   ✅ Heatmap estático salvo em: {output_path_static}")
        
        # 3. Gerar Mapa Interativo (HTML)
        print("3. Gerando mapa interativo...")
        start_time = time.time()
        
        # --- AQUI ESTÁ A MUDANÇA MAIS IMPORTANTE ---
        # Seleciona uma amostra aleatória de 5.000 células
        sample_size = 5000
        scores_for_folium = scores_gdf.sample(n=sample_size, random_state=42).to_crs(epsg=4326)
        print(f"   ✅ Usando amostra de {len(scores_for_folium)} células para o mapa interativo.")

        # Limpa os dados antes de calcular o centroide
        scores_for_folium = scores_for_folium.dropna(subset=['geometry'])
        scores_for_folium = scores_for_folium[scores_for_folium.is_valid].copy()
        
        center_lat = scores_for_folium.centroid.y.mean()
        center_lon = scores_for_folium.centroid.x.mean()

        m = folium.Map(location=[center_lat, center_lon], zoom_start=6)

        folium.Choropleth(
            geo_data=scores_for_folium.__geo_interface__,
            name='choropleth',
            data=scores_for_folium,
            columns=['cell_id', 'score_norm'],
            key_on='feature.properties.cell_id',
            fill_color='RdYlGn',
            legend_name='Score de Viabilidade',
            fill_opacity=0.7,
            line_opacity=0.2
        ).add_to(m)

        output_path_interactive = '../outputs/maps/viabilidade_mapa_amostra.html'
        m.save(output_path_interactive)
        
        end_time = time.time()
        print(f"   ✅ Mapa interativo salvo em: {output_path_interactive}")
        print(f"   ⏱️ Tempo de geração do mapa: {end_time - start_time:.1f} segundos")

    except Exception as e:
        print(f"❌ Ocorreu um erro durante a visualização: {e}")
        print("Certifique-se de que a tabela 'final_scores' existe e contém a coluna 'score_norm'.")

if __name__ == "__main__":
    visualize_score()