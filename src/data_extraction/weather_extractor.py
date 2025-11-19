#!/usr/bin/env python3
"""
Weather Data Extractor for MapReduce Project
============================================

Extrae datos climáticos de la API Open-Meteo para múltiples ubicaciones.
Implementa tolerancia a fallos y manejo robusto de errores.

Características del Sistema Distribuido:
- Redundancia: Múltiples ubicaciones y períodos de datos
- Tolerancia a fallos: Retry automático y manejo de errores
- Escalabilidad: Procesamiento paralelo de múltiples ubicaciones

Autor: Proyecto MapReduce - Tópicos Especiales en Telemática
"""

import requests
import json
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
import random
import numpy as np
from typing import List, Dict, Tuple, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class WeatherDataExtractor:
    """
    Extractor de datos climáticos con características de sistemas distribuidos.
    """
    
    def __init__(self, data_dir: str = None):
        # Si no se especifica data_dir, usar la ruta relativa al proyecto
        if data_dir is None:
            # Desde src/data_extraction/ ir a ../../data/
            self.data_dir = Path(__file__).parent.parent.parent / "data"
        else:
            self.data_dir = Path(data_dir)
        
        # Crear directorios necesarios
        self.data_dir.mkdir(exist_ok=True)
        (self.data_dir / "input").mkdir(exist_ok=True)
        (self.data_dir / "output").mkdir(exist_ok=True)
        
        # Configuración de la API
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        self.max_retries = 3
        self.retry_delay = 2  # segundos
        
        # Ubicaciones estratégicas para análisis distribuido
        self.locations = {
            "medellin_colombia": {
                "latitude": 6.25,
                "longitude": -75.56,
                "timezone": "America/Bogota",
                "country": "Colombia",
                "climate_zone": "tropical_mountain"
            },
            "sao_paulo_brasil": {
                "latitude": -23.55,
                "longitude": -46.64,
                "timezone": "America/Sao_Paulo",
                "country": "Brasil",
                "climate_zone": "subtropical"
            },
            "buenos_aires_argentina": {
                "latitude": -34.61,
                "longitude": -58.38,
                "timezone": "America/Argentina/Buenos_Aires",
                "country": "Argentina",
                "climate_zone": "temperate"
            },
            "miami_usa": {
                "latitude": 25.76,
                "longitude": -80.19,
                "timezone": "America/New_York",
                "country": "USA",
                "climate_zone": "tropical_subtropical"
            },
            "ciudad_mexico": {
                "latitude": 19.43,
                "longitude": -99.13,
                "timezone": "America/Mexico_City",
                "country": "Mexico",
                "climate_zone": "tropical_highland"
            },
            "madrid_espana": {
                "latitude": 40.42,
                "longitude": -3.70,
                "timezone": "Europe/Madrid",
                "country": "Spain",
                "climate_zone": "continental_mediterranean"
            },
            "tokyo_japan": {
                "latitude": 35.68,
                "longitude": 139.69,
                "timezone": "Asia/Tokyo",
                "country": "Japan",
                "climate_zone": "humid_subtropical"
            },
            "sydney_australia": {
                "latitude": -33.87,
                "longitude": 151.21,
                "timezone": "Australia/Sydney",
                "country": "Australia",
                "climate_zone": "oceanic"
            }
        }
        
        # Parámetros climáticos básicos (probados que funcionan)
        self.weather_params = [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum"
        ]
    
    def _make_request_with_retry(self, url: str, params: dict, max_retries: int = None) -> Optional[dict]:
        """
        Realiza petición HTTP con retry automático - Tolerancia a fallos.
        """
        if max_retries is None:
            max_retries = self.max_retries
            
        for attempt in range(max_retries + 1):
            try:
                logger.info(f"Realizando petición (intento {attempt + 1}/{max_retries + 1})")
                # Debug: mostrar URL completa en el primer intento
                if attempt == 0:
                    test_response = requests.Request('GET', url, params=params)
                    prepared = test_response.prepare()
                    logger.info(f"🔍 URL completa: {prepared.url}")
                
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                if 'error' in data:
                    logger.error(f"Error en API: {data['error']}")
                    if attempt < max_retries:
                        time.sleep(self.retry_delay * (attempt + 1))
                        continue
                    return None
                
                return data
                
            except requests.exceptions.RequestException as e:
                logger.error(f"Error en petición HTTP: {e}")
                if attempt < max_retries:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
                    
            except json.JSONDecodeError as e:
                logger.error(f"Error decodificando JSON: {e}")
                if attempt < max_retries:
                    time.sleep(self.retry_delay * (attempt + 1))
                    continue
        
        logger.error(f"Falló después de {max_retries + 1} intentos")
        return None
    
    def extract_location_data(self, location_key: str, start_date: str, end_date: str) -> Optional[str]:
        """
        Extrae datos para una ubicación específica.
        """
        location = self.locations[location_key]
        
        params = {
            "latitude": location["latitude"],
            "longitude": location["longitude"],
            "start_date": start_date,
            "end_date": end_date,
            "daily": ",".join(self.weather_params),
            "timezone": location["timezone"]
        }
        
        logger.info(f"Extrayendo datos para {location_key} ({start_date} a {end_date})")
        
        data = self._make_request_with_retry(self.base_url, params)
        if not data:
            logger.error(f"No se pudieron obtener datos para {location_key}")
            return None
        
        # Procesar y enriquecer datos
        processed_data = self._process_location_data(data, location_key, location)
        
        # Guardar archivo individual en input/
        output_file = self.data_dir / "input" / f"weather_{location_key}_{start_date}_{end_date}.json"
        
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Datos guardados en: {output_file}")
            return str(output_file)
            
        except IOError as e:
            logger.error(f"Error guardando archivo {output_file}: {e}")
            return None
    
    def _process_location_data(self, raw_data: dict, location_key: str, location_info: dict) -> dict:
        """
        Procesa y enriquece los datos de una ubicación.
        """
        daily_data = raw_data.get("daily", {})
        dates = daily_data.get("time", [])
        
        # Estructura de datos enriquecida para MapReduce
        processed = {
            "metadata": {
                "location_key": location_key,
                "country": location_info["country"],
                "climate_zone": location_info["climate_zone"],
                "coordinates": {
                    "latitude": location_info["latitude"],
                    "longitude": location_info["longitude"]
                },
                "timezone": location_info["timezone"],
                "extraction_timestamp": datetime.now().isoformat(),
                "total_records": len(dates)
            },
            "daily_records": []
        }
        
        # Procesar cada día
        for i, date in enumerate(dates):
            record = {
                "date": date,
                "location_key": location_key,
                "country": location_info["country"],
                "climate_zone": location_info["climate_zone"],
                "coordinates": f"{location_info['latitude']},{location_info['longitude']}"
            }
            
            # Agregar métricas climáticas
            for param in self.weather_params:
                values = daily_data.get(param, [])
                record[param] = values[i] if i < len(values) else None
            
            # Calcular métricas derivadas
            record["temperature_range"] = self._calculate_temperature_range(record)
            record["comfort_index"] = self._calculate_comfort_index(record)
            
            processed["daily_records"].append(record)
        
        return processed
    
    def _calculate_temperature_range(self, record: dict) -> Optional[float]:
        """Calcula el rango de temperatura diario."""
        temp_max = record.get("temperature_2m_max")
        temp_min = record.get("temperature_2m_min")
        
        if temp_max is not None and temp_min is not None:
            return round(temp_max - temp_min, 2)
        return None
    
    def _calculate_comfort_index(self, record: dict) -> Optional[float]:
        """Calcula un índice de confort básico."""
        temp_mean = record.get("temperature_2m_mean")
        humidity_max = record.get("humidity_2m_max")
        windspeed = record.get("windspeed_10m_max")
        
        if all(v is not None for v in [temp_mean, humidity_max, windspeed]):
            # Índice simple basado en temperatura, humedad y viento
            comfort = (
                (1.0 if 18 <= temp_mean <= 26 else 0.5) *
                (1.0 if humidity_max <= 70 else 0.7) *
                (1.0 if windspeed <= 15 else 0.8)
            )
            return round(comfort, 3)
        return None
    
    def extract_all_locations_parallel(self, start_date: str, end_date: str, max_workers: int = 4) -> List[str]:
        """
        Extrae datos de todas las ubicaciones en paralelo - Escalabilidad.
        """
        logger.info(f"Iniciando extracción paralela con {max_workers} workers")
        logger.info(f"Período: {start_date} a {end_date}")
        logger.info(f"Ubicaciones: {len(self.locations)}")
        
        successful_files = []
        failed_locations = []
        
        # Procesamiento paralelo para escalabilidad
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Enviar tareas
            future_to_location = {
                executor.submit(self.extract_location_data, location_key, start_date, end_date): location_key
                for location_key in self.locations.keys()
            }
            
            # Recoger resultados
            for future in as_completed(future_to_location):
                location_key = future_to_location[future]
                try:
                    result = future.result()
                    if result:
                        successful_files.append(result)
                        logger.info(f"✅ Completado: {location_key}")
                    else:
                        failed_locations.append(location_key)
                        logger.error(f"❌ Falló: {location_key}")
                        
                except Exception as e:
                    failed_locations.append(location_key)
                    logger.error(f"❌ Excepción en {location_key}: {e}")
        
        # Reporte final
        logger.info(f"\n📊 REPORTE DE EXTRACCIÓN:")
        logger.info(f"   ✅ Exitosos: {len(successful_files)}")
        logger.info(f"   ❌ Fallidos: {len(failed_locations)}")
        
        if failed_locations:
            logger.warning(f"   Ubicaciones fallidas: {failed_locations}")
        
        return successful_files
    
    def create_unified_dataset(self, file_paths: List[str], output_name: str = "unified_weather_data") -> str:
        """
        Crea un dataset unificado para MapReduce - Formato local JSONL.
        """
        logger.info("Creando dataset unificado para MapReduce...")
        
        all_records = []
        total_records = 0
        
        for file_path in file_paths:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    records = data.get("daily_records", [])
                    all_records.extend(records)
                    total_records += len(records)
                    
            except Exception as e:
                logger.error(f"Error procesando {file_path}: {e}")
        
        # Crear archivo unificado en formato JSON Lines (óptimo para MapReduce)
        unified_file = self.data_dir / "input" / f"{output_name}.jsonl"
        
        try:
            with open(unified_file, 'w', encoding='utf-8') as f:
                for record in all_records:
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            
            logger.info(f"✅ Dataset unificado creado: {unified_file}")
            logger.info(f"   📊 Total de registros: {total_records}")
            logger.info(f"   📁 Tamaño: {unified_file.stat().st_size / 1024 / 1024:.2f} MB")
            
            return str(unified_file)
            
        except Exception as e:
            logger.error(f"Error creando dataset unificado: {e}")
            raise

def main():
    """
    Función principal para ejecutar la extracción de datos.
    """
    extractor = WeatherDataExtractor()
    
    # Configurar período de datos - 2022 (datos completamente disponibles)
    end_date = "2022-12-31"
    start_date = "2022-12-01"
    
    logger.info("🌤️  INICIANDO EXTRACCIÓN DE DATOS CLIMÁTICOS (MODO PRUEBA)")
    logger.info("=" * 60)
    logger.info(f"📅 Período: {start_date} a {end_date}")
    
    try:
        # Extraer datos de todas las ubicaciones
        successful_files = extractor.extract_all_locations_parallel(start_date, end_date)
        
        if not successful_files:
            logger.warning("⚠️  No se pudieron extraer datos de ninguna ubicación")
            logger.info("💡 Creando datos de muestra para continuar...")
            # Crear datos de muestra como fallback
            return
        
        # Crear dataset unificado
        unified_file = extractor.create_unified_dataset(successful_files)
        
        logger.info("\n🎉 EXTRACCIÓN COMPLETADA EXITOSAMENTE")
        logger.info(f"📁 Dataset principal: {unified_file}")
        logger.info("✅ Dataset preparado para procesamiento MapReduce local")
        
    except Exception as e:
        logger.error(f"Error en proceso principal: {e}")
        raise

if __name__ == "__main__":
    main()