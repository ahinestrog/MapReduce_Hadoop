#!/usr/bin/env python3
"""
Job MapReduce #2: Análisis de Precipitación por País
====================================================

Analiza patrones de precipitación por país:
- Precipitación total y promedio por país
- Días lluviosos vs secos
- Estacionalidad de precipitación
- Comparación entre países

Características de Sistema Distribuido:
- Particionamiento por país para balance
- Agregación eficiente con combiner
- Tolerancia a datos faltantes
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import sys
from statistics import mean, median
from collections import defaultdict
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PrecipitationAnalysisJob(MRJob):
    """
    MapReduce Job para análisis de precipitación por país.
    """

    def configure_args(self):
        """Configurar argumentos del job."""
        super().configure_args()
        self.add_passthru_arg('--min-precipitation', type=float, default=1.0,
                            help='Precipitación mínima para considerar día lluvioso (mm)')

    def mapper_extract_precipitation_data(self, _, line):
        """
        MAPPER: Extrae datos de precipitación por país.
        
        Input: Línea JSON con datos climáticos
        Output: (país, datos_precipitación)
        """
        try:
            record = json.loads(line.strip())
            
            # Extraer campos
            country = record.get('country')
            date = record.get('date')
            precipitation = record.get('precipitation_sum')
            climate_zone = record.get('climate_zone')
            location_key = record.get('location_key')
            
            # Validación de datos
            if not all([country, date, precipitation is not None]):
                logger.warning(f"Registro incompleto para precipitación: {record}")
                return
            
            # Convertir a numérico y manejar valores nulos
            try:
                precip_value = float(precipitation) if precipitation else 0.0
            except (ValueError, TypeError):
                precip_value = 0.0
                logger.warning(f"Valor de precipitación inválido: {precipitation}")
            
            # Extraer información temporal
            try:
                year, month, day = date.split('-')
                year, month = int(year), int(month)
                season = self._get_season(month)
            except (ValueError, IndexError):
                logger.warning(f"Fecha inválida: {date}")
                return
            
            # Determinar si es día lluvioso
            is_rainy = precip_value >= self.options.min_precipitation
            
            # Crear datos para emitir
            value = {
                'precipitation': precip_value,
                'date': date,
                'year': year,
                'month': month,
                'season': season,
                'is_rainy': is_rainy,
                'climate_zone': climate_zone,
                'location_key': location_key
            }
            
            yield (country, json.dumps(value))
            
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
        except Exception as e:
            logger.error(f"Error inesperado en mapper precipitación: {e}")

    def combiner_aggregate_precipitation(self, country, values):
        """
        COMBINER: Agregación parcial por país para optimización.
        """
        try:
            precipitation_values = []
            rainy_days = 0
            total_days = 0
            seasonal_data = defaultdict(list)
            monthly_data = defaultdict(list)
            yearly_data = defaultdict(list)
            climate_zones = set()
            
            for value_json in values:
                data = json.loads(value_json)
                
                precipitation_values.append(data['precipitation'])
                if data['is_rainy']:
                    rainy_days += 1
                total_days += 1
                
                seasonal_data[data['season']].append(data['precipitation'])
                monthly_data[data['month']].append(data['precipitation'])
                yearly_data[data['year']].append(data['precipitation'])
                climate_zones.add(data['climate_zone'])
            
            # Agregar datos estacionales
            seasonal_stats = {}
            for season, values_season in seasonal_data.items():
                seasonal_stats[season] = {
                    'total_precip': sum(values_season),
                    'avg_precip': mean(values_season) if values_season else 0,
                    'days': len(values_season)
                }
            
            # Agregar datos mensuales
            monthly_stats = {}
            for month, values_month in monthly_data.items():
                monthly_stats[str(month)] = {
                    'total_precip': sum(values_month),
                    'avg_precip': mean(values_month) if values_month else 0,
                    'days': len(values_month)
                }
            
            # Agregar datos anuales
            yearly_stats = {}
            for year, values_year in yearly_data.items():
                yearly_stats[str(year)] = {
                    'total_precip': sum(values_year),
                    'avg_precip': mean(values_year) if values_year else 0,
                    'days': len(values_year)
                }
            
            partial_result = {
                'total_precipitation': sum(precipitation_values),
                'precipitation_values': precipitation_values,
                'rainy_days': rainy_days,
                'total_days': total_days,
                'seasonal_stats': seasonal_stats,
                'monthly_stats': monthly_stats,
                'yearly_stats': yearly_stats,
                'climate_zones': list(climate_zones),
                'is_partial': True
            }
            
            yield (country, json.dumps(partial_result))
            
        except Exception as e:
            logger.error(f"Error en combiner precipitación: {e}")
            # Re-emitir valores originales
            for value in values:
                yield (country, value)

    def reducer_calculate_precipitation_stats(self, country, values):
        """
        REDUCER: Calcula estadísticas finales de precipitación por país.
        """
        try:
            # Acumuladores
            all_precipitation = []
            total_rainy_days = 0
            total_days = 0
            all_seasonal_stats = defaultdict(lambda: defaultdict(list))
            all_monthly_stats = defaultdict(lambda: defaultdict(list))
            all_yearly_stats = defaultdict(lambda: defaultdict(list))
            all_climate_zones = set()
            
            # Procesar valores
            for value_json in values:
                data = json.loads(value_json)
                
                if data.get('is_partial', False):
                    # Datos del combiner
                    all_precipitation.extend(data['precipitation_values'])
                    total_rainy_days += data['rainy_days']
                    total_days += data['total_days']
                    all_climate_zones.update(data['climate_zones'])
                    
                    # Agregar estadísticas estacionales
                    for season, stats in data['seasonal_stats'].items():
                        all_seasonal_stats[season]['total_precip'].append(stats['total_precip'])
                        all_seasonal_stats[season]['days'].append(stats['days'])
                    
                    # Agregar estadísticas mensuales
                    for month, stats in data['monthly_stats'].items():
                        all_monthly_stats[month]['total_precip'].append(stats['total_precip'])
                        all_monthly_stats[month]['days'].append(stats['days'])
                    
                    # Agregar estadísticas anuales
                    for year, stats in data['yearly_stats'].items():
                        all_yearly_stats[year]['total_precip'].append(stats['total_precip'])
                        all_yearly_stats[year]['days'].append(stats['days'])
                        
                else:
                    # Datos originales del mapper
                    all_precipitation.append(data['precipitation'])
                    if data['is_rainy']:
                        total_rainy_days += 1
                    total_days += 1
                    all_climate_zones.add(data['climate_zone'])
                    
                    # Agregar a estadísticas
                    season = data['season']
                    month = str(data['month'])
                    year = str(data['year'])
                    
                    all_seasonal_stats[season]['total_precip'].append(data['precipitation'])
                    all_seasonal_stats[season]['days'].append(1)
                    all_monthly_stats[month]['total_precip'].append(data['precipitation'])
                    all_monthly_stats[month]['days'].append(1)
                    all_yearly_stats[year]['total_precip'].append(data['precipitation'])
                    all_yearly_stats[year]['days'].append(1)
            
            # Calcular estadísticas si hay datos suficientes
            if not all_precipitation:
                logger.warning(f"Sin datos de precipitación para {country}")
                return
            
            # Estadísticas básicas
            total_precip = sum(all_precipitation)
            avg_precip = mean(all_precipitation)
            median_precip = median(all_precipitation)
            max_precip = max(all_precipitation)
            
            # Estadísticas estacionales consolidadas
            seasonal_final = {}
            for season, data_lists in all_seasonal_stats.items():
                if data_lists['total_precip']:
                    seasonal_final[season] = {
                        'total_precipitation': sum(data_lists['total_precip']),
                        'avg_precipitation': mean(data_lists['total_precip']),
                        'total_days': sum(data_lists['days']),
                        'avg_daily_precip': sum(data_lists['total_precip']) / sum(data_lists['days'])
                    }
            
            # Estadísticas mensuales consolidadas
            monthly_final = {}
            for month, data_lists in all_monthly_stats.items():
                if data_lists['total_precip']:
                    monthly_final[month] = {
                        'total_precipitation': sum(data_lists['total_precip']),
                        'avg_precipitation': mean(data_lists['total_precip']),
                        'total_days': sum(data_lists['days']),
                        'avg_daily_precip': sum(data_lists['total_precip']) / sum(data_lists['days'])
                    }
            
            # Estadísticas anuales consolidadas
            yearly_final = {}
            for year, data_lists in all_yearly_stats.items():
                if data_lists['total_precip']:
                    yearly_final[year] = {
                        'total_precipitation': sum(data_lists['total_precip']),
                        'avg_precipitation': mean(data_lists['total_precip']),
                        'total_days': sum(data_lists['days']),
                        'avg_daily_precip': sum(data_lists['total_precip']) / sum(data_lists['days'])
                    }
            
            # Análisis de patrones
            dry_days = total_days - total_rainy_days
            rainy_percentage = (total_rainy_days / total_days) * 100 if total_days > 0 else 0
            
            # Clasificación de humedad
            if avg_precip > 5:
                humidity_class = "muy_humedo"
            elif avg_precip > 2:
                humidity_class = "humedo"
            elif avg_precip > 0.5:
                humidity_class = "moderado"
            else:
                humidity_class = "arido"
            
            # Resultado final
            result = {
                'country': country,
                'climate_zones': list(all_climate_zones),
                'total_days_analyzed': total_days,
                
                # Estadísticas básicas
                'precipitation_summary': {
                    'total_precipitation_mm': round(total_precip, 2),
                    'average_daily_precipitation': round(avg_precip, 2),
                    'median_daily_precipitation': round(median_precip, 2),
                    'max_daily_precipitation': round(max_precip, 2),
                    'humidity_classification': humidity_class
                },
                
                # Análisis de días
                'day_analysis': {
                    'total_rainy_days': total_rainy_days,
                    'total_dry_days': dry_days,
                    'rainy_days_percentage': round(rainy_percentage, 2),
                    'avg_precip_on_rainy_days': round(total_precip / total_rainy_days if total_rainy_days > 0 else 0, 2)
                },
                
                # Datos estacionales
                'seasonal_analysis': seasonal_final,
                'monthly_analysis': monthly_final,
                'yearly_analysis': yearly_final
            }
            
            yield (country, json.dumps(result, ensure_ascii=False))
            
        except Exception as e:
            logger.error(f"Error en reducer precipitación: {e}")
            yield ("ERROR", json.dumps({"error": str(e), "country": country}))

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
                mapper=self.mapper_extract_precipitation_data,
                combiner=self.combiner_aggregate_precipitation,
                reducer=self.reducer_calculate_precipitation_stats
            )
        ]

if __name__ == '__main__':
    PrecipitationAnalysisJob.run()