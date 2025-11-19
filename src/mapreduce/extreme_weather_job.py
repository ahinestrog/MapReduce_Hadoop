#!/usr/bin/env python3
"""
Job MapReduce #3: Análisis de Condiciones Climáticas Extremas
=============================================================

Identifica y analiza eventos climáticos extremos:
- Temperaturas extremas (olas de calor y frío)
- Precipitación extrema (sequías e inundaciones)
- Condiciones de viento extremo
- Índice de riesgo climático por ubicación

Características de Sistema Distribuido:
- Detección distribuida de anomalías
- Agregación de eventos por severidad
- Alertas tempranas automatizadas
"""

from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import sys
from statistics import mean, stdev
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ExtremeWeatherAnalysisJob(MRJob):
    """
    MapReduce Job para análisis de condiciones climáticas extremas.
    Detecta patrones anómalos y eventos de riesgo.
    """

    def configure_args(self):
        """Configurar argumentos del job."""
        super().configure_args()
        self.add_passthru_arg('--temp-extreme-threshold', type=float, default=2.0,
                            help='Desviaciones estándar para considerar temperatura extrema')
        self.add_passthru_arg('--precip-extreme-threshold', type=float, default=50.0,
                            help='Precipitación mínima (mm) para considerar evento extremo')
        self.add_passthru_arg('--wind-extreme-threshold', type=float, default=25.0,
                            help='Velocidad del viento (km/h) para considerar extremo')

    def mapper_detect_extreme_conditions(self, _, line):
        """
        MAPPER: Detecta condiciones extremas en cada registro.
        
        Input: Línea JSON con datos climáticos
        Output: (ubicación_zona, evento_extremo)
        """
        try:
            record = json.loads(line.strip())
            
            # Extraer campos necesarios
            location_key = record.get('location_key')
            climate_zone = record.get('climate_zone')
            country = record.get('country')
            date = record.get('date')
            
            temp_max = record.get('temperature_2m_max')
            temp_min = record.get('temperature_2m_min')
            temp_mean = record.get('temperature_2m_mean')
            precipitation = record.get('precipitation_sum')
            windspeed = record.get('windspeed_10m_max')
            humidity_max = record.get('humidity_2m_max')
            
            # Validación de datos esenciales
            if not all([location_key, climate_zone, country, date]):
                return
            
            # Convertir valores a numéricos con manejo de nulos
            try:
                temp_max = float(temp_max) if temp_max is not None else None
                temp_min = float(temp_min) if temp_min is not None else None
                temp_mean = float(temp_mean) if temp_mean is not None else None
                precipitation = float(precipitation) if precipitation is not None else 0.0
                windspeed = float(windspeed) if windspeed is not None else 0.0
                humidity_max = float(humidity_max) if humidity_max is not None else None
            except (ValueError, TypeError):
                logger.warning(f"Valores numéricos inválidos en: {date}")
                return
            
            # Datos base del evento
            base_event = {
                'location_key': location_key,
                'climate_zone': climate_zone,
                'country': country,
                'date': date,
                'temp_max': temp_max,
                'temp_min': temp_min,
                'temp_mean': temp_mean,
                'precipitation': precipitation,
                'windspeed': windspeed,
                'humidity': humidity_max
            }
            
            # Clave compuesta para agrupación
            composite_key = f"{location_key}_{climate_zone}"
            
            # 1. Detectar temperaturas extremas
            if temp_max is not None and temp_max > 40:  # Ola de calor severa
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'extreme_heat',
                    'severity': 'high' if temp_max > 45 else 'medium',
                    'value': temp_max,
                    'threshold': 40
                })
                yield (composite_key, json.dumps(extreme_event))
            
            if temp_min is not None and temp_min < 0:  # Frío extremo
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'extreme_cold',
                    'severity': 'high' if temp_min < -10 else 'medium',
                    'value': temp_min,
                    'threshold': 0
                })
                yield (composite_key, json.dumps(extreme_event))
            
            # 2. Detectar precipitación extrema
            if precipitation >= self.options.precip_extreme_threshold:
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'extreme_precipitation',
                    'severity': 'high' if precipitation > 100 else 'medium',
                    'value': precipitation,
                    'threshold': self.options.precip_extreme_threshold
                })
                yield (composite_key, json.dumps(extreme_event))
            
            # 3. Detectar viento extremo
            if windspeed >= self.options.wind_extreme_threshold:
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'extreme_wind',
                    'severity': 'high' if windspeed > 50 else 'medium',
                    'value': windspeed,
                    'threshold': self.options.wind_extreme_threshold
                })
                yield (composite_key, json.dumps(extreme_event))
            
            # 4. Detectar condiciones de sequía (precipitación muy baja)
            if precipitation == 0.0:
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'drought_day',
                    'severity': 'low',
                    'value': precipitation,
                    'threshold': 0
                })
                yield (composite_key, json.dumps(extreme_event))
            
            # 5. Detectar alta humedad extrema
            if humidity_max is not None and humidity_max > 95:
                extreme_event = base_event.copy()
                extreme_event.update({
                    'event_type': 'extreme_humidity',
                    'severity': 'medium',
                    'value': humidity_max,
                    'threshold': 95
                })
                yield (composite_key, json.dumps(extreme_event))
            
            # 6. Detectar día normal (para estadísticas base)
            if not any([
                temp_max and temp_max > 40,
                temp_min and temp_min < 0,
                precipitation >= self.options.precip_extreme_threshold,
                windspeed >= self.options.wind_extreme_threshold
            ]):
                normal_event = base_event.copy()
                normal_event.update({
                    'event_type': 'normal_day',
                    'severity': 'none',
                    'value': None,
                    'threshold': None
                })
                yield (composite_key, json.dumps(normal_event))
                
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
        except Exception as e:
            logger.error(f"Error inesperado en mapper extremos: {e}")

    def combiner_aggregate_extreme_events(self, composite_key, events):
        """
        COMBINER: Agrega eventos extremos por ubicación.
        """
        try:
            event_counts = {}
            severity_counts = {}
            extreme_values = {}
            normal_days = 0
            total_days = 0
            
            for event_json in events:
                event = json.loads(event_json)
                event_type = event['event_type']
                severity = event['severity']
                value = event['value']
                
                total_days += 1
                
                if event_type == 'normal_day':
                    normal_days += 1
                else:
                    # Contar eventos por tipo
                    event_counts[event_type] = event_counts.get(event_type, 0) + 1
                    
                    # Contar por severidad
                    severity_key = f"{event_type}_{severity}"
                    severity_counts[severity_key] = severity_counts.get(severity_key, 0) + 1
                    
                    # Rastrear valores extremos
                    if value is not None:
                        if event_type not in extreme_values:
                            extreme_values[event_type] = []
                        extreme_values[event_type].append(value)
            
            partial_result = {
                'composite_key': composite_key,
                'event_counts': event_counts,
                'severity_counts': severity_counts,
                'extreme_values': extreme_values,
                'normal_days': normal_days,
                'total_days': total_days,
                'is_partial': True
            }
            
            yield (composite_key, json.dumps(partial_result))
            
        except Exception as e:
            logger.error(f"Error en combiner extremos: {e}")
            # Re-emitir eventos originales
            for event in events:
                yield (composite_key, event)

    def reducer_analyze_extreme_patterns(self, composite_key, values):
        """
        REDUCER: Analiza patrones de eventos extremos por ubicación.
        """
        try:
            location_key, climate_zone = composite_key.split('_', 1)
            
            # Acumuladores finales
            final_event_counts = {}
            final_severity_counts = {}
            final_extreme_values = {}
            total_normal_days = 0
            total_days_analyzed = 0
            
            # Variables para detectar ubicación
            country = None
            
            # Procesar valores (eventos o agregaciones parciales)
            for value_json in values:
                data = json.loads(value_json)
                
                if data.get('is_partial', False):
                    # Datos del combiner
                    for event_type, count in data['event_counts'].items():
                        final_event_counts[event_type] = final_event_counts.get(event_type, 0) + count
                    
                    for severity_key, count in data['severity_counts'].items():
                        final_severity_counts[severity_key] = final_severity_counts.get(severity_key, 0) + count
                    
                    for event_type, values_list in data['extreme_values'].items():
                        if event_type not in final_extreme_values:
                            final_extreme_values[event_type] = []
                        final_extreme_values[event_type].extend(values_list)
                    
                    total_normal_days += data['normal_days']
                    total_days_analyzed += data['total_days']
                    
                else:
                    # Eventos originales
                    event_type = data['event_type']
                    severity = data['severity']
                    value = data['value']
                    country = data['country']  # Capturar país
                    
                    total_days_analyzed += 1
                    
                    if event_type == 'normal_day':
                        total_normal_days += 1
                    else:
                        final_event_counts[event_type] = final_event_counts.get(event_type, 0) + 1
                        
                        severity_key = f"{event_type}_{severity}"
                        final_severity_counts[severity_key] = final_severity_counts.get(severity_key, 0) + 1
                        
                        if value is not None:
                            if event_type not in final_extreme_values:
                                final_extreme_values[event_type] = []
                            final_extreme_values[event_type].append(value)
            
            # Calcular estadísticas finales
            total_extreme_events = sum(final_event_counts.values())
            extreme_days_percentage = (total_extreme_events / total_days_analyzed) * 100 if total_days_analyzed > 0 else 0
            
            # Calcular valores máximos por tipo de evento
            max_values = {}
            avg_values = {}
            for event_type, values_list in final_extreme_values.items():
                if values_list:
                    max_values[event_type] = max(values_list)
                    avg_values[event_type] = round(mean(values_list), 2)
            
            # Calcular índice de riesgo climático
            risk_score = self._calculate_risk_index(final_event_counts, final_severity_counts, total_days_analyzed)
            
            # Clasificar riesgo
            if risk_score > 7:
                risk_level = "muy_alto"
            elif risk_score > 5:
                risk_level = "alto"
            elif risk_score > 3:
                risk_level = "medio"
            elif risk_score > 1:
                risk_level = "bajo"
            else:
                risk_level = "muy_bajo"
            
            # Resultado final
            result = {
                'location_key': location_key,
                'climate_zone': climate_zone,
                'country': country,
                'analysis_period': {
                    'total_days_analyzed': total_days_analyzed,
                    'normal_days': total_normal_days,
                    'extreme_days': total_extreme_events,
                    'extreme_percentage': round(extreme_days_percentage, 2)
                },
                
                'extreme_events_summary': final_event_counts,
                'severity_breakdown': final_severity_counts,
                'extreme_values': {
                    'maximum_recorded': max_values,
                    'average_when_extreme': avg_values
                },
                
                'risk_assessment': {
                    'overall_risk_score': risk_score,
                    'risk_level': risk_level,
                    'primary_threats': self._identify_primary_threats(final_event_counts),
                    'recommendations': self._generate_recommendations(final_event_counts, risk_level)
                }
            }
            
            yield (location_key, json.dumps(result, ensure_ascii=False))
            
        except Exception as e:
            logger.error(f"Error en reducer extremos: {e}")
            yield ("ERROR", json.dumps({"error": str(e), "key": composite_key}))

    def _calculate_risk_index(self, event_counts, severity_counts, total_days):
        """
        Calcula un índice de riesgo climático basado en frecuencia y severidad.
        """
        risk_score = 0.0
        
        # Pesos por tipo de evento
        event_weights = {
            'extreme_heat': 2.0,
            'extreme_cold': 1.5,
            'extreme_precipitation': 2.5,
            'extreme_wind': 2.0,
            'extreme_humidity': 1.0,
            'drought_day': 0.1
        }
        
        # Multiplicadores por severidad
        severity_multipliers = {
            'high': 3.0,
            'medium': 2.0,
            'low': 1.0
        }
        
        for event_type, count in event_counts.items():
            base_weight = event_weights.get(event_type, 1.0)
            frequency_factor = (count / total_days) * 100  # Porcentaje
            
            # Agregar puntos por severidad
            for severity_key, severity_count in severity_counts.items():
                if severity_key.startswith(event_type):
                    severity = severity_key.split('_')[-1]
                    multiplier = severity_multipliers.get(severity, 1.0)
                    severity_factor = (severity_count / total_days) * 100
                    risk_score += base_weight * frequency_factor * multiplier * 0.01
        
        return round(risk_score, 2)

    def _identify_primary_threats(self, event_counts):
        """Identifica las principales amenazas climáticas."""
        if not event_counts:
            return []
        
        # Ordenar por frecuencia
        sorted_events = sorted(event_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Tomar los 3 más frecuentes
        primary_threats = []
        for event_type, count in sorted_events[:3]:
            if count > 0:
                primary_threats.append({
                    'threat_type': event_type,
                    'frequency': count,
                    'description': self._get_threat_description(event_type)
                })
        
        return primary_threats

    def _get_threat_description(self, event_type):
        """Devuelve descripción de la amenaza."""
        descriptions = {
            'extreme_heat': 'Temperaturas peligrosamente altas que pueden causar estrés térmico',
            'extreme_cold': 'Temperaturas bajo cero que pueden afectar agricultura y salud',
            'extreme_precipitation': 'Lluvias intensas con riesgo de inundaciones',
            'extreme_wind': 'Vientos fuertes que pueden causar daños estructurales',
            'extreme_humidity': 'Humedad muy alta que reduce confort y puede afectar salud',
            'drought_day': 'Días sin precipitación que contribuyen a sequías'
        }
        return descriptions.get(event_type, 'Evento climático extremo')

    def _generate_recommendations(self, event_counts, risk_level):
        """Genera recomendaciones basadas en los riesgos identificados."""
        recommendations = []
        
        if 'extreme_heat' in event_counts and event_counts['extreme_heat'] > 10:
            recommendations.append("Implementar sistemas de alerta temprana para olas de calor")
            recommendations.append("Desarrollar infraestructura de refrigeración en espacios públicos")
        
        if 'extreme_precipitation' in event_counts and event_counts['extreme_precipitation'] > 5:
            recommendations.append("Mejorar sistemas de drenaje y control de inundaciones")
            recommendations.append("Implementar alertas de precipitación extrema")
        
        if 'extreme_wind' in event_counts and event_counts['extreme_wind'] > 5:
            recommendations.append("Reforzar infraestructura crítica contra vientos fuertes")
            recommendations.append("Desarrollar protocolos de evacuación por vientos extremos")
        
        if risk_level in ['alto', 'muy_alto']:
            recommendations.append("Desarrollar plan integral de adaptación climática")
            recommendations.append("Implementar sistema de monitoreo climático continuo")
        
        return recommendations

    def steps(self):
        """Definir los pasos del job MapReduce."""
        return [
            MRStep(
                mapper=self.mapper_detect_extreme_conditions,
                combiner=self.combiner_aggregate_extreme_events,
                reducer=self.reducer_analyze_extreme_patterns
            )
        ]

if __name__ == '__main__':
    ExtremeWeatherAnalysisJob.run()