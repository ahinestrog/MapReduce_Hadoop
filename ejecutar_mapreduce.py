#!/usr/bin/env python3
import subprocess
import sys
from pathlib import Path

def ejecutar_comando(comando, descripcion):
    print(f"\n{'='*50}")
    print(f"{descripcion}")
    print(f"{'='*50}")
    resultado = subprocess.run(comando, shell=True)
    if resultado.returncode != 0:
        print(f"Error en: {descripcion}")
        return False
    return True

def main():
    base_dir = Path(__file__).parent
    data_input = base_dir / "data" / "input" / "unified_weather_data.jsonl"
    data_output = base_dir / "data" / "output"
    
    print("Iniciando procesamiento MapReduce...")
    
    if not data_input.exists():
        print(f"Error: No existe {data_input}")
        print("Ejecuta primero: python src/extraccion/extractor_clima.py")
        return False
    
    jobs = [
        ("analisis_temperatura", "Análisis de Temperatura"),
        ("analisis_precipitacion", "Análisis de Precipitación"),
        ("analisis_clima_extremo", "Análisis de Clima Extremo")
    ]
    
    for job_name, descripcion in jobs:
        output_dir = data_output / job_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        comando = f"python src/mapreduce/{job_name}.py {data_input} --output-dir {output_dir}"
        if not ejecutar_comando(comando, descripcion):
            return False
    
    print("\n" + "="*50)
    print("Procesamiento completado exitosamente")
    print("="*50)
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
