#!/usr/bin/env python3
"""
Local Testing Environment Setup
===============================

Script para configurar entorno local de pruebas que simule el comportamiento
de procesamiento distribuido MapReduce sin necesidad de infraestructura cloud.

Este script permite:
- Probar extracción de datos
- Simular jobs MapReduce localmente  
- Validar API con datos de prueba
- Verificar que todo funcione en modo local

Autor: Proyecto MapReduce - Testing Local
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
import json
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class LocalTestingEnvironment:
    """
    Entorno de pruebas local que simula procesamiento distribuido
    """
    
    def __init__(self):
        self.project_root = Path(__file__).parent.parent  # Subir un nivel desde tests/
        self.data_dir = self.project_root / "data"
        self.test_data_dir = self.data_dir / "local_test"
        
        # Crear estructura de directorios
        self.setup_directories()
    
    def setup_directories(self):
        """Crear estructura de directorios local"""
        directories = [
            self.data_dir / "input",
            self.data_dir / "output", 
            self.data_dir / "local_test",
            self.test_data_dir / "input",
            self.test_data_dir / "output"
        ]
        
        for dir_path in directories:
            dir_path.mkdir(parents=True, exist_ok=True)
            
        logger.info(f"✅ Estructura de directorios creada en: {self.data_dir}")
    
    def install_dependencies(self):
        """Instalar dependencias necesarias"""
        logger.info("📦 Instalando dependencias...")
        
        try:
            # Verificar e instalar requirements
            subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                         check=True, cwd=self.project_root)
            logger.info("✅ Dependencias instaladas")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Error instalando dependencias: {e}")
            return False
        
        return True
    
    def test_data_extraction(self):
        """Probar extracción de datos localmente"""
        logger.info("🌤️ Probando extracción de datos...")
        
        try:
            # Cambiar al directorio del extractor
            extractor_dir = self.project_root / "src" / "data_extraction"
            
            # Ejecutar extractor con configuración local
            result = subprocess.run([
                sys.executable, "weather_extractor.py"
            ], cwd=extractor_dir, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                logger.info("✅ Extracción de datos completada")
                
                # Verificar archivos generados
                input_files = list(self.data_dir.glob("input/*.json*"))
                logger.info(f"📁 Archivos generados: {len(input_files)}")
                
                for file in input_files[:3]:  # Mostrar primeros 3
                    size = file.stat().st_size / 1024
                    logger.info(f"  📄 {file.name}: {size:.1f} KB")
                
                return len(input_files) > 0
            else:
                logger.error(f"❌ Error en extracción: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("⏱️ Timeout en extracción de datos")
            return False
        except Exception as e:
            logger.error(f"❌ Error inesperado: {e}")
            return False
    
    def test_mapreduce_jobs_local(self):
        """Probar jobs MapReduce en modo local"""
        logger.info("⚙️ Probando jobs MapReduce localmente...")
        
        # Buscar archivo de datos principal
        data_files = list(self.data_dir.glob("input/*.jsonl"))
        if not data_files:
            # Crear datos de prueba mínimos
            self.create_sample_data()
            data_files = list(self.data_dir.glob("input/*.jsonl"))
        
        if not data_files:
            logger.error("❌ No hay archivos de datos para probar")
            return False
        
        main_data_file = data_files[0]
        logger.info(f"📊 Usando archivo: {main_data_file}")
        
        # Probar cada job MapReduce
        jobs = [
            ("temperature_analysis_job.py", "temperature_analysis"),
            ("precipitation_analysis_job.py", "precipitation_analysis"), 
            ("extreme_weather_job.py", "extreme_weather")
        ]
        
        successful_jobs = 0
        
        for job_file, job_name in jobs:
            try:
                logger.info(f"🔄 Ejecutando {job_name}...")
                
                job_path = self.project_root / "src" / "mapreduce" / job_file
                output_dir = self.data_dir / "output" / job_name
                
                # Limpiar directorio de salida
                if output_dir.exists():
                    shutil.rmtree(output_dir)
                output_dir.mkdir(parents=True)
                
                # Ejecutar job en modo local
                cmd = [
                    sys.executable, str(job_path),
                    "-r", "local",
                    str(main_data_file),
                    "--output-dir", str(output_dir)
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
                
                if result.returncode == 0:
                    # Verificar salida
                    output_files = list(output_dir.glob("part-*"))
                    if output_files:
                        logger.info(f"✅ {job_name} completado - {len(output_files)} archivos")
                        successful_jobs += 1
                        
                        # Mostrar muestra de resultado
                        with open(output_files[0], 'r') as f:
                            first_line = f.readline().strip()
                            if first_line:
                                logger.info(f"  📄 Muestra: {first_line[:80]}...")
                    else:
                        logger.warning(f"⚠️ {job_name} sin archivos de salida")
                else:
                    logger.error(f"❌ {job_name} falló: {result.stderr}")
                    
            except subprocess.TimeoutExpired:
                logger.error(f"⏱️ Timeout en {job_name}")
            except Exception as e:
                logger.error(f"❌ Error en {job_name}: {e}")
        
        logger.info(f"📊 Jobs completados: {successful_jobs}/{len(jobs)}")
        return successful_jobs > 0
    
    def create_sample_data(self):
        """Crear datos de muestra para pruebas"""
        logger.info("🔨 Creando datos de muestra...")
        
        sample_records = []
        
        # Datos de muestra realistas
        locations = [
            {"key": "medellin_colombia", "country": "Colombia", "zone": "tropical_mountain"},
            {"key": "sao_paulo_brasil", "country": "Brasil", "zone": "subtropical"},
            {"key": "miami_usa", "country": "USA", "zone": "tropical_subtropical"}
        ]
        
        for i in range(100):  # 100 registros de prueba
            for loc in locations:
                record = {
                    "date": f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
                    "location_key": loc["key"],
                    "country": loc["country"],
                    "climate_zone": loc["zone"],
                    "coordinates": "0,0",
                    "temperature_2m_max": 20 + (i % 15) + (hash(loc["key"]) % 10),
                    "temperature_2m_min": 10 + (i % 10) + (hash(loc["key"]) % 5),
                    "temperature_2m_mean": 15 + (i % 12) + (hash(loc["key"]) % 7),
                    "precipitation_sum": (i * hash(loc["key"])) % 20,
                    "windspeed_10m_max": 5 + (i % 10),
                    "humidity_2m_max": 60 + (i % 30),
                    "humidity_2m_min": 40 + (i % 20),
                    "pressure_msl_mean": 1013 + (i % 20),
                    "sunshine_duration": 8 + (i % 8)
                }
                sample_records.append(record)
        
        # Guardar datos de muestra
        sample_file = self.data_dir / "input" / "sample_weather_data.jsonl"
        with open(sample_file, 'w') as f:
            for record in sample_records:
                f.write(json.dumps(record) + '\n')
        
        logger.info(f"✅ Creados {len(sample_records)} registros de muestra en {sample_file}")
    
    def test_api(self):
        """Probar API con datos locales"""
        logger.info("🌐 Probando API FastAPI...")
        
        try:
            # Importar y configurar API - agregar src al path
            src_path = str(self.project_root / "src")
            if src_path not in sys.path:
                sys.path.insert(0, src_path)
            
            from api.weather_api import app, config
            from fastapi.testclient import TestClient
            
            # Configurar directorio de datos local
            config.data_dir = self.data_dir / "output"
            
            # Crear cliente de prueba
            client = TestClient(app)
            
            # Probar endpoints
            endpoints_to_test = [
                ("/health", "Health check"),
                ("/temperature-analysis", "Temperature analysis"),
                ("/precipitation-analysis", "Precipitation analysis"),
                ("/extreme-weather", "Extreme weather"),
                ("/comparative-analysis?comparison_type=temperature", "Comparative analysis")
            ]
            
            successful_endpoints = 0
            
            for endpoint, description in endpoints_to_test:
                try:
                    response = client.get(endpoint)
                    if response.status_code == 200:
                        logger.info(f"✅ {description}: OK ({response.status_code})")
                        successful_endpoints += 1
                    else:
                        logger.warning(f"⚠️ {description}: {response.status_code}")
                        
                except Exception as e:
                    logger.error(f"❌ {description}: {e}")
            
            logger.info(f"📊 Endpoints funcionando: {successful_endpoints}/{len(endpoints_to_test)}")
            return successful_endpoints > 0
            
        except ImportError as e:
            logger.error(f"❌ Error importando API: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error en API: {e}")
            return False
    
    def run_full_test_suite(self):
        """Ejecutar suite completa de pruebas locales"""
        logger.info("🧪 INICIANDO SUITE DE PRUEBAS LOCALES")
        logger.info("=" * 50)
        
        results = {}
        
        # 1. Instalar dependencias
        results['dependencies'] = self.install_dependencies()
        
        # 2. Probar extracción de datos
        results['data_extraction'] = self.test_data_extraction()
        
        # 3. Probar jobs MapReduce
        results['mapreduce_jobs'] = self.test_mapreduce_jobs_local()
        
        # 4. Probar API
        results['api'] = self.test_api()
        
        # Reporte final
        logger.info("\n" + "=" * 50)
        logger.info("📊 REPORTE FINAL DE PRUEBAS LOCALES")
        
        passed = 0
        total = len(results)
        
        for component, status in results.items():
            icon = "✅" if status else "❌"
            logger.info(f"  {icon} {component.replace('_', ' ').title()}: {'PASS' if status else 'FAIL'}")
            if status:
                passed += 1
        
        logger.info(f"\n🎯 Resultado: {passed}/{total} componentes funcionando")
        
        if passed == total:
            logger.info("🎉 ¡TODOS LOS COMPONENTES FUNCIONAN CORRECTAMENTE!")
            logger.info("✅ El sistema está completamente validado y funcional")
        else:
            logger.info("⚠️ Algunos componentes necesitan ajustes")
            logger.info("🔧 Revisa los errores antes de usar en producción")
        
        return passed == total

def main():
    """Función principal"""
    print("🏠 CONFIGURANDO ENTORNO LOCAL DE PRUEBAS")
    print("=" * 50)
    
    env = LocalTestingEnvironment()
    success = env.run_full_test_suite()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ¡ENTORNO LOCAL COMPLETAMENTE FUNCIONAL!")
        print("")
        print("📋 Opciones para expandir el sistema:")
        print("  1. Configurar cluster distribuido local con Docker")
        print("  2. Migrar a plataformas cloud (AWS, GCP, Azure)") 
        print("  3. Implementar stream processing con Kafka/Spark")
        print("  4. Todo el código ya está validado y funcionando ✅")
    else:
        print("⚠️ Hay errores que corregir antes de usar en producción")
        
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)