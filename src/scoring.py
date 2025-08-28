import pandas as pd
import numpy as np
import geopandas as gpd
from sqlalchemy import create_engine
import os

def minmax(s):
    """Normaliza uma série para o intervalo [0,1] usando Min-Max"""
    if s.isnull().all() or (s.max() - s.min()) == 0:
        return pd.Series(0.5, index=s.index)
    return (s - s.min()) / (s.max() - s.min())

def calculate_score(table_name='energy_features_sample'):
    """
    Lê o grid do banco de dados com as features, calcula o score
    e salva o resultado em uma nova tabela.
    """
    print("📊 INICIANDO CÁLCULO DO SCORE...")
    
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    try:
        print(f"1. Lendo dados da tabela '{table_name}'...")
        grid = gpd.read_postgis(f"SELECT * FROM {table_name}", engine, geom_col='geometry')
        print(f"   ✅ {len(grid)} células carregadas.")
        
        numeric_cols = ['solar_irradiance', 'dist_to_uc_km', 'connection_cost_brl']
        for col in numeric_cols:
            if col in grid.columns:
                grid[col] = pd.to_numeric(grid[col], errors='coerce')
        
        solar_col = 'solar_irradiance'
        if solar_col in grid.columns and grid[solar_col].notna().any():
            grid['energy_n'] = minmax(grid[solar_col])
        else:
            print(f"   Aviso: Coluna '{solar_col}' não encontrada ou vazia. Usando placeholder.")
            grid['energy_n'] = 0.5

        impact_col = 'dist_to_uc_km'
        if impact_col in grid.columns and grid[impact_col].notna().any():
            grid['impact_n'] = 1 - minmax(grid[impact_col])
        else:
            grid['impact_n'] = 0.5

        cost_col = 'connection_cost_brl'
        if cost_col in grid.columns and grid[cost_col].notna().any():
            grid['connection_cost_n'] = minmax(grid[cost_col])
        else:
            grid['connection_cost_n'] = 0.5
        
        alpha, beta, gamma = 0.6, 0.25, 0.15
        print("2. Calculando o score...")
        grid['score'] = (
            alpha * grid['energy_n'] -
            beta * grid['impact_n'] -
            gamma * grid['connection_cost_n']
        )
        grid['score_norm'] = minmax(grid['score'])
        print("   ✅ Score calculado com sucesso.")

        final_table_name = 'final_scores'
        print(f"3. Salvando resultados na tabela '{final_table_name}'...")
        grid.to_postgis(final_table_name, engine, if_exists='replace', index=False)
        print("   ✅ Resultados salvos.")
        
        print("\n🎉 SCORING CONCLUÍDO!")
        print(f"   Score médio: {grid['score_norm'].mean():.3f}")
        print(f"   Melhor célula: {grid['score_norm'].max():.3f}")
        print(f"   Pior célula: {grid['score_norm'].min():.3f}")

    except Exception as e:
        print(f"❌ Ocorreu um erro: {e}")
        print(f"Verifique se a tabela '{table_name}' existe e se os tipos de dados estão corretos.")
        
if __name__ == "__main__":
    calculate_score()