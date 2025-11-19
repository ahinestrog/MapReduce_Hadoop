#!/usr/bin/env python3
"""
Job MapReduce #1: Análisis de Temperaturas por Zona Climática
=============================================================

Calcula estadísticas de temperatura por zona climática:
- Temperatura promedio, mínima y máxima por zona
- Rango de temperaturas y variabilidad
- Distribución estacional

Características de Sistema Distribuido:
- Paralelismo: Map distribuye por zona climática
- Agregación: Reduce consolida estadísticas
- Escalabilidad: Procesa millones de registros
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import sys
from statistics import mean, stdev
from collections import defaultdict
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TemperatureAnalysisJob(MRJob):
    """
    MapReduce Job para análisis de temperaturas por zona climática.
    Implementa tolerancia a fallos y procesamiento distribuido.
    """

    def configure_args(self):
        """Configurar argumentos del job."""
        super().configure_args()
        self.add_passthru_arg('--analysis-type', default='full',
                            help='Tipo de análisis: full, seasonal, monthly')

    def mapper_extract_temperature_data(self, _, line):
        """
        MAPPER: Extrae y emite datos de temperatura por zona climática.
        
        Input: Línea JSON con datos climáticos
        Output: (zona_climática, datos_temperatura)
        """
        try:
            # Parsear datos JSON
            record = json.loads(line.strip())
            
            # Extraer campos necesarios
            climate_zone = record.get('climate_zone')
            date = record.get('date')
            temp_max = record.get('temperature_2m_max')
            temp_min = record.get('temperature_2m_min')
            temp_mean = record.get('temperature_2m_mean')
            country = record.get('country')
            
            # Validar datos básicos - Tolerancia a fallos
            if not all([climate_zone, date, temp_max is not None, temp_min is not None]):
                logger.warning(f"Registro incompleto ignorado: {record}")
                return
            
            # Calcular temperatura promedio si no está presente
            if temp_mean is None:
                temp_mean = (float(temp_max) + float(temp_min)) / 2.0
            
            # Extraer mes para análisis estacional
            try:
                month = int(date.split('-')[1])
                season = self._get_season(month)
            except (ValueError, IndexError):
                logger.warning(f"Fecha inválida: {date}")
                return
            
            # Emitir clave compuesta y datos
            key = {
                'climate_zone': climate_zone,
                'analysis_type': self.options.analysis_type,
                'season': season if self.options.analysis_type == 'seasonal' else 'all',
                'month': month if self.options.analysis_type == 'monthly' else 0
            }
            
            value = {
                'temp_max': float(temp_max),
                'temp_min': float(temp_min),
                'temp_mean': float(temp_mean),
                'date': date,
                'country': country,
                'temp_range': float(temp_max) - float(temp_min)
            }
            
            yield (json.dumps(key, sort_keys=True), json.dumps(value))
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
            # Continuar con siguiente registro - Tolerancia a fallos
        except Exception as e:
            logger.error(f"Error inesperado en mapper: {e}")
            # Continuar con siguiente registro

    def combiner_aggregate_partial(self, key, values):
        """
        COMBINER: Agrega parcialmente los datos para reducir tráfico de red.
        Optimización para sistemas distribuidos.
        """
        try:
            temp_max_list = []
            temp_min_list = []
            temp_mean_list = []
            temp_range_list = []
            countries = set()
            count = 0
            
            for value_json in values:
                value = json.loads(value_json)
                temp_max_list.append(value['temp_max'])
                temp_min_list.append(value['temp_min'])
                temp_mean_list.append(value['temp_mean'])
                temp_range_list.append(value['temp_range'])
                countries.add(value['country'])
                count += 1
            
            # Agregación parcial
            partial_stats = {
                'temp_max_values': temp_max_list,
                'temp_min_values': temp_min_list,
                'temp_mean_values': temp_mean_list,
                'temp_range_values': temp_range_list,
                'countries': list(countries),
                'record_count': count,
                'is_partial': True
            }
            
            yield (key, json.dumps(partial_stats))
            
        except Exception as e:
            logger.error(f"Error en combiner: {e}")
            # Re-emitir valores originales como fallback
            for value in values:
                yield (key, value)

    def reducer_calculate_statistics(self, key, values):
        """
        REDUCER: Calcula estadísticas finales por zona climática.
        
        Input: (zona_climática, [datos_temperatura, ...])
        Output: (zona_climática, estadísticas_completas)
        """
        try:
            key_data = json.loads(key)
            
            # Acumuladores para estadísticas
            all_temp_max = []
            all_temp_min = []
            all_temp_mean = []
            all_temp_range = []
            all_countries = set()
            total_records = 0
            
            # Procesar valores (pueden ser datos originales o parcialmente agregados)
            for value_json in values:
                value = json.loads(value_json)
                
                if value.get('is_partial', False):
                    # Datos del combiner
                    all_temp_max.extend(value['temp_max_values'])
                    all_temp_min.extend(value['temp_min_values'])
                    all_temp_mean.extend(value['temp_mean_values'])
                    all_temp_range.extend(value['temp_range_values'])
                    all_countries.update(value['countries'])
                    total_records += value['record_count']
                else:
                    # Datos originales del mapper
                    all_temp_max.append(value['temp_max'])
                    all_temp_min.append(value['temp_min'])
                    all_temp_mean.append(value['temp_mean'])
                    all_temp_range.append(value['temp_range'])
                    all_countries.add(value['country'])
                    total_records += 1
            
            # Calcular estadísticas si hay datos suficientes
            if len(all_temp_mean) < 1:
                logger.warning(f"Insuficientes datos para {key}")
                return
            
            # Estadísticas descriptivas
            stats = {
                'climate_zone': key_data['climate_zone'],
                'analysis_type': key_data['analysis_type'],
                'season': key_data['season'],
                'month': key_data['month'],
                'record_count': total_records,
                'countries': list(all_countries),
                
                # Estadísticas de temperatura
                'temperature_stats': {
                    'mean_temperature': round(mean(all_temp_mean), 2),
                    'max_temperature_overall': round(max(all_temp_max), 2),
                    'min_temperature_overall': round(min(all_temp_min), 2),
                    'avg_daily_max': round(mean(all_temp_max), 2),
                    'avg_daily_min': round(mean(all_temp_min), 2),
                    'avg_daily_range': round(mean(all_temp_range), 2),
                    'temperature_variability': round(stdev(all_temp_mean) if len(all_temp_mean) > 1 else 0, 2)
                },
                
                # Métricas de distribución
                'distribution': {
                    'q1_temp': round(sorted(all_temp_mean)[len(all_temp_mean)//4], 2),
                    'median_temp': round(sorted(all_temp_mean)[len(all_temp_mean)//2], 2),
                    'q3_temp': round(sorted(all_temp_mean)[3*len(all_temp_mean)//4], 2),
                    'extreme_temp_range': round(max(all_temp_max) - min(all_temp_min), 2)
                }
            }
            
            # Análisis de confort climático
            comfortable_days = sum(1 for temp in all_temp_mean if 18 <= temp <= 26)
            stats['comfort_analysis'] = {
                'comfortable_days': comfortable_days,
                'comfort_percentage': round((comfortable_days / len(all_temp_mean)) * 100, 2),
                'hot_days': sum(1 for temp in all_temp_mean if temp > 30),
                'cold_days': sum(1 for temp in all_temp_mean if temp < 10)
            }
            
            yield (key_data['climate_zone'], json.dumps(stats, ensure_ascii=False))
            
        except Exception as e:
            logger.error(f"Error en reducer: {e}")
            # Emitir error para debugging
            yield ("ERROR", json.dumps({"error": str(e), "key": key}))

    def _get_season(self, month):
        """Determina la estación basada en el mes (hemisferio norte)."""
        if month in [12, 1, 2]:
            return "winter"
        elif month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        else:
            return "autumn"

    def steps(self):
        """Definir los pasos del job MapReduce."""
        return [
            MRStep(
                mapper=self.mapper_extract_temperature_data,
                combiner=self.combiner_aggregate_partial,
                reducer=self.reducer_calculate_statistics
            )
        ]

if __name__ == '__main__':
    TemperatureAnalysisJob.run()