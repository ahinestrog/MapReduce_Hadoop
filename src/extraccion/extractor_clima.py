import requests
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ExtractorClima:
    def __init__(self):
        self.data_dir = Path(__file__).parent.parent.parent / "data"
        (self.data_dir / "input").mkdir(parents=True, exist_ok=True)
        (self.data_dir / "output").mkdir(exist_ok=True)
        
        self.base_url = "https://archive-api.open-meteo.com/v1/archive"
        
        self.ubicaciones = {
            "medellin_colombia": {"latitude": 6.25, "longitude": -75.56, "timezone": "America/Bogota", "country": "Colombia", "climate_zone": "tropical_mountain"},
            "sao_paulo_brasil": {"latitude": -23.55, "longitude": -46.64, "timezone": "America/Sao_Paulo", "country": "Brasil", "climate_zone": "subtropical"},
            "buenos_aires_argentina": {"latitude": -34.61, "longitude": -58.38, "timezone": "America/Argentina/Buenos_Aires", "country": "Argentina", "climate_zone": "temperate"},
            "miami_usa": {"latitude": 25.76, "longitude": -80.19, "timezone": "America/New_York", "country": "USA", "climate_zone": "tropical_subtropical"},
            "ciudad_mexico": {"latitude": 19.43, "longitude": -99.13, "timezone": "America/Mexico_City", "country": "Mexico", "climate_zone": "tropical_highland"},
            "madrid_espana": {"latitude": 40.42, "longitude": -3.70, "timezone": "Europe/Madrid", "country": "Spain", "climate_zone": "continental_mediterranean"},
            "tokyo_japan": {"latitude": 35.68, "longitude": 139.69, "timezone": "Asia/Tokyo", "country": "Japan", "climate_zone": "humid_subtropical"},
            "sydney_australia": {"latitude": -33.87, "longitude": 151.21, "timezone": "Australia/Sydney", "country": "Australia", "climate_zone": "oceanic"}
        }
    
    def extraer_datos(self, ubicacion: str, config: dict, fecha_inicio: str, fecha_fin: str) -> dict:
        params = {
            'latitude': config['latitude'],
            'longitude': config['longitude'],
            'start_date': fecha_inicio,
            'end_date': fecha_fin,
            'daily': 'temperature_2m_max,temperature_2m_min,precipitation_sum',
            'timezone': config['timezone']
        }
        
        try:
            respuesta = requests.get(self.base_url, params=params, timeout=30)
            respuesta.raise_for_status()
            datos = respuesta.json()
            
            archivo = self.data_dir / "input" / f"weather_{ubicacion}_{fecha_inicio}_{fecha_fin}.json"
            with open(archivo, 'w') as f:
                json.dump(datos, f, indent=2)
            
            logger.info(f"Datos guardados: {ubicacion}")
            return {'ubicacion': ubicacion, 'estado': 'exitoso', 'archivo': str(archivo)}
        except Exception as e:
            logger.error(f"Error en {ubicacion}: {e}")
            return {'ubicacion': ubicacion, 'estado': 'error', 'error': str(e)}
    
    def crear_dataset_unificado(self) -> str:
        archivos = list(self.data_dir.glob("input/weather_*.json"))
        registros = []
        
        for archivo in archivos:
            try:
                with open(archivo, 'r') as f:
                    datos = json.load(f)
                
                nombre_archivo = archivo.stem
                partes = nombre_archivo.split('_')
                ubicacion = '_'.join(partes[1:-2])
                
                config = self.ubicaciones.get(ubicacion, {})
                fechas = datos.get('daily', {}).get('time', [])
                temp_max = datos.get('daily', {}).get('temperature_2m_max', [])
                temp_min = datos.get('daily', {}).get('temperature_2m_min', [])
                precip = datos.get('daily', {}).get('precipitation_sum', [])
                
                for i, fecha in enumerate(fechas):
                    registro = {
                        'location_key': ubicacion,
                        'country': config.get('country', 'Unknown'),
                        'climate_zone': config.get('climate_zone', 'unknown'),
                        'date': fecha,
                        'temperature_2m_max': temp_max[i] if i < len(temp_max) else None,
                        'temperature_2m_min': temp_min[i] if i < len(temp_min) else None,
                        'temperature_2m_mean': (temp_max[i] + temp_min[i]) / 2 if i < len(temp_max) and i < len(temp_min) else None,
                        'precipitation_sum': precip[i] if i < len(precip) else 0
                    }
                    registros.append(registro)
            except Exception as e:
                logger.error(f"Error procesando {archivo}: {e}")
        
        archivo_salida = self.data_dir / "input" / "unified_weather_data.jsonl"
        with open(archivo_salida, 'w') as f:
            for registro in registros:
                f.write(json.dumps(registro) + '\n')
        
        logger.info(f"Dataset unificado creado: {len(registros)} registros")
        return str(archivo_salida)
    
    def ejecutar(self, fecha_inicio: str = None, fecha_fin: str = None):
        if not fecha_inicio:
            fecha_fin_dt = datetime.now() - timedelta(days=7)
            fecha_inicio_dt = fecha_fin_dt - timedelta(days=30)
            fecha_inicio = fecha_inicio_dt.strftime('%Y-%m-%d')
            fecha_fin = fecha_fin_dt.strftime('%Y-%m-%d')
        
        logger.info(f"Extrayendo datos: {fecha_inicio} a {fecha_fin}")
        
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = []
            for ubicacion, config in self.ubicaciones.items():
                future = executor.submit(self.extraer_datos, ubicacion, config, fecha_inicio, fecha_fin)
                futures.append(future)
            
            resultados = [f.result() for f in futures]
        
        exitosos = sum(1 for r in resultados if r['estado'] == 'exitoso')
        logger.info(f"Completados: {exitosos}/{len(resultados)}")
        
        archivo_unificado = self.crear_dataset_unificado()
        logger.info(f"Dataset listo: {archivo_unificado}")

if __name__ == "__main__":
    extractor = ExtractorClima()
    extractor.ejecutar(fecha_inicio='2022-12-01', fecha_fin='2022-12-31')
