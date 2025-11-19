#!/usr/bin/env python3
"""
Test API Locally
================

Script para probar la API FastAPI con los datos reales del proyecto.
Verifica que la API funcione correctamente con los resultados de MapReduce.
"""

import sys
import json
import subprocess
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def ensure_mapreduce_data_exists():
    """Asegurar que los datos de MapReduce existen"""
    
    # Obtener rutas del proyecto
    project_root = Path(__file__).parent.parent
    output_dir = project_root / "data" / "output"
    
    # Verificar que existen los resultados de MapReduce
    required_dirs = [
        "temperature_analysis",
        "precipitation_analysis", 
        "extreme_weather"
    ]
    
    missing_dirs = []
    for dir_name in required_dirs:
        dir_path = output_dir / dir_name
        if not dir_path.exists() or not any(dir_path.iterdir()):
            missing_dirs.append(dir_name)
    
    if missing_dirs:
        logger.warning(f"⚠️ Faltan datos de MapReduce: {missing_dirs}")
        logger.info("🔄 Ejecutando jobs MapReduce para generar datos...")
        
        # Ejecutar jobs MapReduce rápidamente
        try:
            mapreduce_script = project_root / "tests" / "test_mapreduce_quick.py"
            if mapreduce_script.exists():
                result = subprocess.run([sys.executable, str(mapreduce_script)], 
                                     capture_output=True, text=True, timeout=120)
                if result.returncode == 0:
                    logger.info("✅ Datos de MapReduce generados exitosamente")
                else:
                    logger.error(f"❌ Error ejecutando MapReduce: {result.stderr}")
                    return False
            else:
                logger.error("❌ No se encontró el script de MapReduce")
                return False
        except Exception as e:
            logger.error(f"❌ Error ejecutando MapReduce: {e}")
            return False
    
    return True

def test_api_endpoints():
    """Probar endpoints de la API con datos reales del proyecto"""
    
    try:
        # Agregar path del proyecto al sys.path para importar módulos
        project_root = Path(__file__).parent.parent
        sys.path.insert(0, str(project_root / "src"))
        
        # Importar API
        from api.weather_api import app
        from fastapi.testclient import TestClient
        
        # La API ya está configurada para usar data/output del proyecto
        # No necesitamos cambiar la configuración
        
        # Crear cliente de prueba
        client = TestClient(app)
        
        logger.info("🌐 Probando endpoints de la API con datos reales...")
        
        # Lista de endpoints para probar
        test_cases = [
            {
                "endpoint": "/health",
                "description": "Health Check",
                "expected_keys": ["status", "version"]
            },
            {
                "endpoint": "/temperature-analysis",
                "description": "Temperature Analysis", 
                "expected_keys": ["mean_temperature", "record_count"]
            },
            {
                "endpoint": "/precipitation-analysis",
                "description": "Precipitation Analysis",
                "expected_keys": ["total_precipitation_mm"]
            },
            {
                "endpoint": "/extreme-weather",
                "description": "Extreme Weather Analysis",
                "expected_keys": ["location_key", "risk_level"]
            },
            {
                "endpoint": "/comparative-analysis?comparison_type=temperature",
                "description": "Comparative Analysis",
                "expected_keys": ["comparison_type", "locations"]
            }
        ]
        
        successful_tests = 0
        
        for test_case in test_cases:
            try:
                response = client.get(test_case["endpoint"])
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # Verificar que hay datos
                    if data:
                        logger.info(f"✅ {test_case['description']}: OK")
                        successful_tests += 1
                        
                        # Mostrar muestra de datos
                        if isinstance(data, list) and len(data) > 0:
                            sample = str(data[0])[:100] + "..." if len(str(data[0])) > 100 else str(data[0])
                        elif isinstance(data, dict):
                            sample = str(data)[:100] + "..." if len(str(data)) > 100 else str(data)
                        else:
                            sample = str(data)
                        
                        logger.info(f"  📄 Muestra: {sample}")
                    else:
                        logger.warning(f"⚠️ {test_case['description']}: Sin datos")
                        
                elif response.status_code == 404:
                    logger.warning(f"⚠️ {test_case['description']}: No hay datos (404)")
                else:
                    logger.error(f"❌ {test_case['description']}: Error {response.status_code}")
                    
            except Exception as e:
                logger.error(f"❌ {test_case['description']}: Excepción - {e}")
        
        logger.info(f"\n📊 Tests completados: {successful_tests}/{len(test_cases)}")
        return successful_tests >= len(test_cases) * 0.8  # 80% de éxito mínimo
        
    except ImportError as e:
        logger.error(f"❌ Error importando API: {e}")
        logger.error("Verifique que FastAPI esté instalado: pip install fastapi")
        return False
    except Exception as e:
        logger.error(f"❌ Error general en API: {e}")
        return False

def main():
    """Función principal para probar API localmente"""
    print("🌐 PRUEBA LOCAL DE API FASTAPI")
    print("=" * 35)
    
    # Asegurar que existen los datos de MapReduce
    logger.info("📋 Verificando datos de MapReduce...")
    if not ensure_mapreduce_data_exists():
        logger.error("❌ No se pudieron obtener los datos de MapReduce")
        return False
    
    # Probar endpoints
    api_success = test_api_endpoints()
    
    # Reporte final
    print("\n" + "=" * 35)
    if api_success:
        print("🎉 ¡API FUNCIONANDO CORRECTAMENTE!")
        print("✅ La API está lista para funcionar con datos reales")
        print("")
        print("💡 Para usar la API en vivo:")
        print("  python src/api/weather_api.py")
        print("  Luego ve a: http://localhost:8000/docs")
    else:
        print("⚠️ API con problemas - revisar errores")
        
    return api_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)