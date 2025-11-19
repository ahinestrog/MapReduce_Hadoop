#!/usr/bin/env python3
"""
Test Complete System
===================

Script maestro para probar todo el sistema MapReduce + Weather Analysis
- Extracción de datos
- Jobs MapReduce (local)
- API FastAPI
- Validación completa del sistema
"""

import sys
import subprocess
from pathlib import Path
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name, description):
    """Ejecutar un script Python y reportar el resultado"""
    
    # Path to the script in the tests directory
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        logger.error(f"❌ Script no encontrado: {script_name}")
        return False
    
    logger.info(f"\n🔄 Ejecutando: {description}")
    logger.info(f"   Script: {script_name}")
    
    try:
        
        # Ejecutar script
        result = subprocess.run(
            [sys.executable, str(script_path)],
            capture_output=True,
            text=True,
            timeout=300  # 5 minutos máximo
        )
        
        if result.returncode == 0:
            logger.info(f"✅ {description}: EXITOSO")
            
            # Mostrar últimas líneas de salida
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                last_lines = lines[-3:] if len(lines) > 3 else lines
                for line in last_lines:
                    if line.strip():
                        logger.info(f"   📄 {line.strip()}")
            
            return True
        else:
            logger.error(f"❌ {description}: FALLÓ")
            
            # Mostrar errores
            if result.stderr:
                error_lines = result.stderr.strip().split('\n')[-3:]
                for line in error_lines:
                    if line.strip():
                        logger.error(f"   💥 {line.strip()}")
            
            return False
            
    except subprocess.TimeoutExpired:
        logger.error(f"⏱️ {description}: TIMEOUT (>5 min)")
        return False
    except Exception as e:
        logger.error(f"❌ {description}: EXCEPCIÓN - {e}")
        return False

def check_dependencies():
    """Verificar dependencias básicas"""
    
    logger.info("🔍 Verificando dependencias...")
    
    required_packages = [
        'fastapi',
        'uvicorn',
        'requests',
        'mrjob'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package)
            logger.info(f"  ✅ {package}")
        except ImportError:
            missing_packages.append(package)
            logger.warning(f"  ⚠️ {package} - FALTANTE")
    
    if missing_packages:
        logger.warning("🚨 Instalar paquetes faltantes:")
        logger.warning(f"   pip install {' '.join(missing_packages)}")
        return False
    
    logger.info("✅ Todas las dependencias disponibles")
    return True

def main():
    """Función principal - ejecutar suite completa de pruebas"""
    
    print("🚀 SISTEMA COMPLETO DE PRUEBAS")
    print("🌦️ MapReduce + Weather Analysis")
    print("=" * 50)
    
    # Verificar dependencias
    if not check_dependencies():
        logger.error("❌ Dependencias faltantes - instalar primero")
        return False
    
    # Lista de pruebas a ejecutar
    test_suite = [
        {
            "script": "test_mapreduce_quick.py",
            "description": "Jobs MapReduce (Rápido)"
        },
        {
            "script": "test_api_local.py", 
            "description": "API FastAPI (Local)"
        },
        {
            "script": "setup_local_testing.py",
            "description": "Sistema Completo (Integración)"
        }
    ]
    
    # Ejecutar pruebas
    successful_tests = 0
    total_tests = len(test_suite)
    
    for test in test_suite:
        success = run_script(test["script"], test["description"])
        if success:
            successful_tests += 1
    
    # Reporte final
    print("\n" + "=" * 50)
    print(f"📊 REPORTE FINAL: {successful_tests}/{total_tests} pruebas exitosas")
    
    success_rate = (successful_tests / total_tests) * 100
    
    if success_rate >= 100:
        print("🎉 ¡SISTEMA COMPLETAMENTE FUNCIONAL!")
        print("✅ Todo el código está validado y listo")
        print("")
        print("🚀 Próximos pasos:")
        print("  1. Analizar resultados de los jobs MapReduce")
        print("  2. Explorar API interactiva en http://localhost:8000/docs")
        print("  3. Expandir con nuevas métricas o ubicaciones") 
        print("  4. Considerar migración a sistemas distribuidos")
        print("  5. Iniciar API: python src/api/weather_api.py")
        
    elif success_rate >= 70:
        print("✅ SISTEMA MAYORMENTE FUNCIONAL")
        print("⚠️ Algunas pruebas fallaron - revisar logs")
        
    else:
        print("❌ SISTEMA CON PROBLEMAS")
        print("🔧 Revisar errores antes de continuar")
    
    print(f"\n📈 Tasa de éxito: {success_rate:.1f}%")
    
    return success_rate >= 70

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)