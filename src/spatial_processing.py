# scripts/spatial_processing.py
from pyproj import CRS
import geopandas as gpd
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
import numpy as np
import os

def get_utm_crs(gdf=None, epsg_code=32724):
    """
    Determina o CRS UTM - versão simplificada
    Se não fornecer um GeoDataFrame, usa EPSG padrão para Brasil
    """
    if gdf is not None:
        # Calcula o centroide para informação (mas usa CRS fixo)
        centroid = gdf.to_crs(4326).unary_union.centroid
        lon, lat = centroid.x, centroid.y
        print(f"📍 Centroide: {lon:.2f}, {lat:.2f}")
    
    # 🎯 CRS FIXO para Brasil - evita problemas com estimate_utm_crs
    utm_crs = CRS.from_epsg(epsg_code)  # UTM 24S para Brasil
    print(f"🎯 CRS UTM determinado: {utm_crs}")
    print(f"🗺️  Zona UTM: 24S (Brasil)")
    
    return utm_crs

def reproject_vector(gdf, target_crs):
    """Reprojetar vetor para CRS métrico"""
    print(f"🔄 Reprojetando vetor de {gdf.crs} para {target_crs}")
    return gdf.to_crs(target_crs)

def reproject_raster(input_path, output_path, target_crs):
    """Reprojetar raster para CRS métrico"""
    print(f"🔄 Reprojetando raster: {os.path.basename(input_path)}")
    
    with rasterio.open(input_path) as src:
        transform, width, height = calculate_default_transform(
            src.crs, target_crs, src.width, src.height, *src.bounds
        )
        kwargs = src.meta.copy()
        kwargs.update({
            'crs': target_crs,
            'transform': transform,
            'width': width,
            'height': height
        })

        with rasterio.open(output_path, 'w', **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=target_crs,
                    resampling=Resampling.nearest
                )
    
    print(f"✅ Raster reprojetado: {output_path}")

# Função de teste para verificar se o módulo está funcionando
def test_module():
    """Testa se o módulo está funcionando corretamente"""
    print("🧪 Testando spatial_processing module...")
    try:
        crs = get_utm_crs()
        print(f"✅ get_utm_crs() funciona: {crs}")
        
        # Teste simples de CRS
        test_crs = CRS.from_epsg(4326)
        print(f"✅ CRS básico funciona: {test_crs}")
        
        print("🎉 Módulo spatial_processing está funcionando!")
        return True
        
    except Exception as e:
        print(f"❌ Erro no módulo: {e}")
        return False

if __name__ == "__main__":
    test_module()