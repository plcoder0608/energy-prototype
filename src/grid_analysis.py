import geopandas as gpd
from shapely.geometry import box, Point
import numpy as np
from sqlalchemy import create_engine, text
from pyproj import CRS
import re

def make_grid(bounds, cell_size_m, crs):
    """Cria grid regular de células quadradas"""
    minx, miny, maxx, maxy = bounds
    xs = np.arange(minx, maxx, cell_size_m)
    ys = np.arange(miny, maxy, cell_size_m)
    
    polys = []
    cell_ids = []
    
    for i, x in enumerate(xs):
        for j, y in enumerate(ys):
            cell_poly = box(x, y, x + cell_size_m, y + cell_size_m)
            polys.append(cell_poly)
            cell_ids.append(f"cell_{i}_{j}")
    
    gdf = gpd.GeoDataFrame({'cell_id': cell_ids, 'geometry': polys}, crs=crs)
    
    gdf['centroid'] = gdf.geometry.centroid
    
    return gdf

def parse_bounds(extent_str):
    """Converte string BOX do PostGIS para tuple de coordenadas"""
    extent_str = extent_str.replace('BOX(', '').replace(')', '')
    
    parts = extent_str.split(',')
    
    coords1 = parts[0].strip().split()
    coords2 = parts[1].strip().split()
    
    minx = float(coords1[0])
    miny = float(coords1[1])
    maxx = float(coords2[0])
    maxy = float(coords2[1])
    
    return (minx, miny, maxx, maxy)

def create_energy_grid(cell_size_km=5):
    """Cria grid para análise de energia"""
    print("🔋 CRIANDO GRID PARA ANÁLISE DE ENERGIA...")
    
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    print("1. Obtendo extensão da área de estudo...")
    with engine.connect() as conn:
        result = conn.execute(text("SELECT ST_Extent(geometry) FROM study_area"))
        extent_str = result.scalar()
        print(f"   📐 Extensão: {extent_str}")
        
        bounds = parse_bounds(extent_str)
        print(f"   🎯 Bounds: {bounds}")
    
    utm_crs = CRS.from_epsg(32724)
    cell_size_m = cell_size_km * 1000
    
    print(f"2. Criando grid {cell_size_km}km x {cell_size_km}km...")
    grid_gdf = make_grid(bounds, cell_size_m, utm_crs)
    print(f"   ✅ {len(grid_gdf)} células criadas")
    
    grid_gdf['cell_size_km'] = cell_size_km
    grid_gdf['area_km2'] = grid_gdf.geometry.area / 1e6
    grid_gdf['centroid_lon'] = grid_gdf.centroid.x
    grid_gdf['centroid_lat'] = grid_gdf.centroid.y
    
    print("3. Salvando grid no PostgreSQL...")
    grid_gdf.to_postgis('energy_grid', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM energy_grid"))
        count = result.scalar()
        result = conn.execute(text("SELECT MIN(area_km2), MAX(area_km2) FROM energy_grid"))
        min_area, max_area = result.fetchone()
        
    print(f"   📊 {count} células salvas")
    print(f"   📏 Área por célula: {min_area:.1f} - {max_area:.1f} km²")
    
    print("4. Informações do grid:")
    print(f"   🎯 CRS: {utm_crs}")
    print(f"   📐 Extensão: {bounds}")
    print(f"   🔲 Tamanho da célula: {cell_size_km} km × {cell_size_km} km")
    print(f"   📦 Número de células: {len(grid_gdf)}")
    
    return grid_gdf

def create_energy_grid_simple(cell_size_km=5):
    """Versão simplificada - carrega bounds do GeoDataFrame"""
    print("🔋 CRIANDO GRID (VERSÃO SIMPLIFICADA)...")
    
    engine = create_engine('postgresql://postgres:senha@localhost:5432/energy')
    
    print("1. Carregando área de estudo...")
    study_area = gpd.read_postgis("SELECT * FROM study_area", engine, geom_col='geometry')
    bounds = study_area.total_bounds
    print(f"   🎯 Bounds: {bounds}")
    
    utm_crs = CRS.from_epsg(32724)
    cell_size_m = cell_size_km * 1000
    
    print(f"2. Criando grid {cell_size_km}km x {cell_size_km}km...")
    grid_gdf = make_grid(bounds, cell_size_m, utm_crs)
    print(f"   ✅ {len(grid_gdf)} células criadas")
    
    grid_gdf['cell_size_km'] = cell_size_km
    grid_gdf['area_km2'] = grid_gdf.geometry.area / 1e6
    grid_gdf['centroid_lon'] = grid_gdf.centroid.x
    grid_gdf['centroid_lat'] = grid_gdf.centroid.y
    
    grid_gdf.to_postgis('energy_grid', engine, if_exists='replace', index=False)
    print(f"   📊 {len(grid_gdf)} células salvas no banco")
    
    return grid_gdf

if __name__ == "__main__":
    try:
        grid = create_energy_grid(cell_size_km=5)
    except Exception as e:
        print(f"❌ Erro na versão principal: {e}")
        print("🔄 Tentando versão simplificada...")
        grid = create_energy_grid_simple(cell_size_km=5)
    
    print("\n🎉 GRID CRIADO COM SUCESSO!")
    print("   Pronto para análise de viabilidade energética!")