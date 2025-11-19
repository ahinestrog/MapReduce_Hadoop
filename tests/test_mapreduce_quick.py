#!/usr/bin/env python3
"""
Quick MapReduce Jobs Test
========================

Script para probar rápidamente los jobs MapReduce en modo local.
Perfecto para validar la lógica antes de usar en producción.
"""

import sys
import json
import tempfile
from pathlib import Path
import subprocess
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_data():
    """Crear datos de prueba mínimos"""
    test_records = [
        {
            "date": "2024-01-15",
            "location_key": "medellin_colombia", 
            "country": "Colombia",
            "climate_zone": "tropical_mountain",
            "coordinates": "6.25,-75.56",
            "temperature_2m_max": 28.5,
            "temperature_2m_min": 16.2,
            "temperature_2m_mean": 22.3,
            "precipitation_sum": 5.2,
            "windspeed_10m_max": 12.1,
            "humidity_2m_max": 85.0,
            "humidity_2m_min": 60.0,
            "pressure_msl_mean": 1015.2,
            "sunshine_duration": 8.5
        },
        {
            "date": "2024-01-16",
            "location_key": "miami_usa",
            "country": "USA", 
            "climate_zone": "tropical_subtropical",
            "coordinates": "25.76,-80.19",
            "temperature_2m_max": 32.1,
            "temperature_2m_min": 24.8,
            "temperature_2m_mean": 28.5,
            "precipitation_sum": 0.0,
            "windspeed_10m_max": 18.5,
            "humidity_2m_max": 78.0,
            "humidity_2m_min": 55.0,
            "pressure_msl_mean": 1012.8,
            "sunshine_duration": 10.2
        },
        {
            "date": "2024-01-17",
            "location_key": "sydney_australia",
            "country": "Australia",
            "climate_zone": "oceanic", 
            "coordinates": "-33.87,151.21",
            "temperature_2m_max": 45.2,  # Temperatura extrema para probar
            "temperature_2m_min": 28.1,
            "temperature_2m_mean": 36.7,
            "precipitation_sum": 65.5,   # Precipitación extrema para probar
            "windspeed_10m_max": 35.2,   # Viento extremo para probar
            "humidity_2m_max": 95.0,
            "humidity_2m_min": 75.0,
            "pressure_msl_mean": 1008.1,
            "sunshine_duration": 12.0
        }
    ]
    
    # Crear archivo temporal
    temp_file = tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False)
    for record in test_records:
        temp_file.write(json.dumps(record) + '\n')
    temp_file.close()
    
    return temp_file.name

def test_job(job_name, job_file, test_data_file):
    """Probar un job MapReduce específico"""
    logger.info(f"🔄 Probando {job_name}...")
    
    try:
        project_root = Path(__file__).parent.parent  # Subir dos niveles desde tests/
        job_path = project_root / "src" / "mapreduce" / job_file
        
        # Usar directorios de salida reales que la API espera
        output_dir_map = {
            "temperature_analysis_job.py": project_root / "data" / "output" / "temperature_analysis",
            "precipitation_analysis_job.py": project_root / "data" / "output" / "precipitation_analysis",
            "extreme_weather_job.py": project_root / "data" / "output" / "extreme_weather"
        }
        
        output_dir = output_dir_map[job_file]
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Comando para ejecutar job localmente
        cmd = [
            sys.executable, 
            str(job_path),
            "-r", "local",
            test_data_file,
            "--output-dir", str(output_dir)
        ]
        
        # Ejecutar job
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Verificar salida
            output_files = list(Path(output_dir).glob("part-*"))
            if output_files:
                # Leer y mostrar resultado
                with open(output_files[0], 'r') as f:
                    content = f.read().strip()
                    if content:
                        logger.info(f"✅ {job_name} funcionando")
                        
                        # Mostrar muestra del resultado
                        lines = content.split('\n')
                        logger.info(f"  📊 Generó {len(lines)} líneas de resultado")
                        if lines:
                            # Mostrar primera línea como muestra
                            first_result = lines[0]
                            if len(first_result) > 100:
                                logger.info(f"  📄 Muestra: {first_result[:100]}...")
                            else:
                                logger.info(f"  📄 Muestra: {first_result}")
                        return True
                    else:
                        logger.error(f"❌ {job_name}: Archivo de salida vacío")
                        return False
            else:
                logger.error(f"❌ {job_name}: No generó archivos de salida")
                return False
        else:
            logger.error(f"❌ {job_name} falló:")
            logger.error(f"  Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏱️ {job_name}: Timeout")
        return False
    except Exception as e:
        logger.error(f"❌ {job_name}: Error inesperado - {e}")
        return False

def main():
    """Función principal para pruebas rápidas"""
    print("🧪 PRUEBAS RÁPIDAS DE JOBS MAPREDUCE")
    print("=" * 40)
    
    # Usar datos reales en lugar de datos de prueba
    project_root = Path(__file__).parent.parent  # Subir un nivel desde tests/
    real_data_file = project_root / "data" / "input" / "unified_weather_data.jsonl"
    
    if not real_data_file.exists():
        logger.error(f"❌ No se encontró el dataset unificado: {real_data_file}")
        logger.info("💡 Ejecuta primero: python src/data_extraction/weather_extractor.py")
        return False
    
    logger.info(f"📋 Usando dataset real: {real_data_file}")
    logger.info(f"📊 Tamaño del archivo: {real_data_file.stat().st_size / 1024:.1f} KB")
    
    # Jobs a probar
    jobs = [
        ("Temperature Analysis", "temperature_analysis_job.py"),
        ("Precipitation Analysis", "precipitation_analysis_job.py"), 
        ("Extreme Weather", "extreme_weather_job.py")
    ]
    
    # Probar cada job
    successful_jobs = 0
    for job_name, job_file in jobs:
        if test_job(job_name, job_file, str(real_data_file)):
            successful_jobs += 1
    
    # Reporte final
    print("\n" + "=" * 40)
    print("📊 REPORTE DE PRUEBAS")
    print(f"✅ Jobs funcionando: {successful_jobs}/{len(jobs)}")
    
    if successful_jobs == len(jobs):
        print("🎉 ¡TODOS LOS JOBS FUNCIONAN!")
        print("✅ Todos los jobs MapReduce están funcionando correctamente")
    else:
        print("⚠️ Algunos jobs necesitan correcciones")
    
    return successful_jobs == len(jobs)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)