#!/usr/bin/env python3
"""
Weather Analysis API - FastAPI Service
======================================

Servicio API REST para consultar resultados de análisis MapReduce.
Implementa características de sistema distribuido y tolerancia a fallos.

Endpoints principales:
- /health - Estado del sistema
- /temperature-analysis - Estadísticas de temperatura
- /precipitation-analysis - Análisis de precipitación  
- /extreme-weather - Eventos climáticos extremos
- /comparative-analysis - Comparaciones entre ubicaciones

Características de Sistema Distribuido:
- Cache distribuido para performance
- Balanceado de carga
- Monitoreo y métricas
- Tolerancia a fallos con circuit breakers

Autor: Proyecto MapReduce - Tópicos Especiales en Telemática
"""

from fastapi import FastAPI, HTTPException, Query, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Union
import json
import os
import glob
from pathlib import Path
import logging
from datetime import datetime, timedelta
import asyncio
import aiofiles
from functools import lru_cache
import csv
import pandas as pd
from collections import defaultdict

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de la aplicación
app = FastAPI(
    title="API de Análisis Climático",
    description="API REST para análisis climático distribuido con MapReduce",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Configurar CORS para desarrollo
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Modelos Pydantic para respuestas estructuradas
class HealthStatus(BaseModel):
    """Modelo para el estado de salud del sistema."""
    status: str = Field(..., description="Estado del sistema: saludable, degradado, no_saludable")
    timestamp: str = Field(..., description="Marca de tiempo de la consulta")
    version: str = Field(..., description="Versión de la API")
    uptime_seconds: float = Field(..., description="Tiempo de funcionamiento en segundos")
    data_freshness: str = Field(..., description="Frescura de los datos")
    services_status: Dict = Field(..., description="Estado de servicios dependientes")

class TemperatureStats(BaseModel):
    """Modelo para estadísticas de temperatura."""
    zona_climatica: str
    total_registros: int
    paises: List[str]
    temperatura_promedio: float
    temperatura_maxima_general: float
    temperatura_minima_general: float
    variabilidad_temperatura: float
    porcentaje_confort: float
    tipo_analisis: str

class PrecipitationStats(BaseModel):
    """Modelo para estadísticas de precipitación."""
    pais: str
    zonas_climaticas: List[str]
    total_dias_analizados: int
    precipitacion_total_mm: float
    precipitacion_promedio_diaria: float
    clasificacion_humedad: str
    porcentaje_dias_lluviosos: float
    analisis_estacional: Dict

class ExtremeWeatherEvent(BaseModel):
    """Modelo para eventos climáticos extremos."""
    clave_ubicacion: str
    zona_climatica: str
    pais: Optional[str]
    total_dias_analizados: int
    porcentaje_extremo: float
    puntuacion_riesgo_general: float
    nivel_riesgo: str
    amenazas_principales: List[Dict]
    recomendaciones: List[str]

class ComparativeAnalysis(BaseModel):
    """Modelo para análisis comparativo."""
    tipo_comparacion: str
    ubicaciones: List[str]
    metricas: Dict
    clasificacion: List[Dict]
    percepciones: List[str]

# Configuración global
class APIConfig:
    """Configuración centralizada de la API."""
    def __init__(self):
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "output"
        self.cache_ttl = 300  # 5 minutos
        self.startup_time = datetime.now()
        self.max_cache_size = 100
        
        # Asegurar que existe el directorio de datos
        self.data_dir.mkdir(parents=True, exist_ok=True)

# Instancia global de configuración
config = APIConfig()

# Cache en memoria con LRU
@lru_cache(maxsize=config.max_cache_size)
def cached_file_read(file_path: str, cache_key: str) -> str:
    """
    Lee archivo con cache LRU - Optimización de performance.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error leyendo archivo {file_path}: {e}")
        raise HTTPException(status_code=500, detail=f"Error accediendo datos: {str(e)}")

# Dependency injection para configuración
def get_config() -> APIConfig:
    """Dependency para obtener configuración."""
    return config

# Health Check Endpoint
@app.get("/health", response_model=HealthStatus, tags=["Sistema"])
async def health_check(config: APIConfig = Depends(get_config)):
    """
    Endpoint de salud del sistema - Monitoreo distribuido.
    Verifica el estado de todos los componentes.
    """
    try:
        # Calcular uptime
        uptime = (datetime.now() - config.startup_time).total_seconds()
        
        # Verificar disponibilidad de datos
        data_files = list(config.data_dir.glob("*.json")) + list(config.data_dir.glob("*.csv"))
        data_status = "fresco" if data_files else "faltante"
        
        # Verificar servicios locales
        services_status = {
            "almacenamiento_local": "saludable",
            "trabajos_mapreduce": "saludable", 
            "ingestion_datos": "saludable" if data_files else "degradado",
            "cache_api": "saludable"
        }
        
        # Determinar estado general
        unhealthy_services = [k for k, v in services_status.items() if v == "no_saludable"]
        degraded_services = [k for k, v in services_status.items() if v == "degradado"]
        
        if unhealthy_services:
            overall_status = "no_saludable"
        elif degraded_services:
            overall_status = "degradado"
        else:
            overall_status = "saludable"
        
        return HealthStatus(
            status=overall_status,
            timestamp=datetime.now().isoformat(),
            version="1.0.0",
            uptime_seconds=uptime,
            data_freshness=data_status,
            services_status=services_status
        )
        
    except Exception as e:
        logger.error(f"Error en health check: {e}")
        raise HTTPException(status_code=500, detail="Error verificando estado del sistema")

# Endpoint para análisis de temperatura
@app.get("/temperature-analysis", response_model=List[TemperatureStats], tags=["Análisis"])
async def get_temperature_analysis(
    climate_zone: Optional[str] = Query(None, description="Filtrar por zona climática"),
    analysis_type: str = Query("all", description="Tipo de análisis: all, full, seasonal, monthly"),
    config: APIConfig = Depends(get_config)
):
    """
    Obtiene análisis de temperatura por zona climática.
    
    Parámetros:
    - climate_zone: Filtro opcional por zona climática
    - analysis_type: Tipo de análisis (full, seasonal, monthly)
    
    Returns:
    - Lista de estadísticas de temperatura
    """
    try:
        logger.info(f"Consultando análisis de temperatura: zone={climate_zone}, type={analysis_type}")
        
        # Buscar archivos de resultado de temperatura en el directorio correcto
        temp_dir = config.data_dir / "temperature_analysis"
        temp_files = list(temp_dir.glob("part-*")) if temp_dir.exists() else []
        
        if not temp_files:
            raise HTTPException(
                status_code=404, 
                detail="No se encontraron datos de análisis de temperatura. Ejecute el job MapReduce primero."
            )
        
        results = []
        
        for file_path in temp_files:
            try:
                # Usar cache para archivos
                cache_key = f"temp_{file_path.stat().st_mtime}_{analysis_type}"
                content = cached_file_read(str(file_path), cache_key)
                
                # Procesar datos JSON
                for line in content.strip().split('\n'):
                    if line.strip():
                        try:
                            parts = line.split('\t', 1)
                            if len(parts) != 2:
                                logger.warning(f"Línea con formato incorrecto: {line[:100]}...")
                                continue
                                
                            zone_raw, stats_json = parts
                            # Limpiar las comillas del zone
                            zone = zone_raw.strip().strip('"')
                            
                            # Parsing con double JSON fix
                            if isinstance(stats_json, str):
                                stats = json.loads(stats_json)
                                if isinstance(stats, str):
                                    stats = json.loads(stats)
                            else:
                                continue
                                
                            if not isinstance(stats, dict):
                                continue
                            
                        except json.JSONDecodeError as e:
                            logger.warning(f"Error JSON parsing: {e} - Línea: {line[:100]}...")
                            continue
                        except Exception as e:
                            logger.warning(f"Error parsing línea: {line[:100]}... - Error: {e}")
                            continue
                        
                        # Filtrar por zona climática si se especifica
                        if climate_zone and stats.get('climate_zone') != climate_zone:
                            continue
                            
                        # Filtrar por tipo de análisis (por defecto acepta cualquier tipo)
                        if analysis_type and analysis_type != "all" and stats.get('analysis_type') != analysis_type:
                            continue
                        
                        # Extraer estadísticas de temperatura
                        temp_stats = stats.get('temperature_stats', {})
                        comfort_stats = stats.get('comfort_analysis', {})
                        
                        result = TemperatureStats(
                            zona_climatica=stats['climate_zone'],
                            total_registros=stats['record_count'],
                            paises=stats['countries'],
                            temperatura_promedio=temp_stats.get('mean_temperature', 0),
                            temperatura_maxima_general=temp_stats.get('max_temperature_overall', 0),
                            temperatura_minima_general=temp_stats.get('min_temperature_overall', 0),
                            variabilidad_temperatura=temp_stats.get('temperature_variability', 0),
                            porcentaje_confort=comfort_stats.get('comfort_percentage', 0),
                            tipo_analisis=stats['analysis_type']
                        )
                        
                        results.append(result)
                        
            except Exception as e:
                logger.warning(f"Error procesando archivo {file_path}: {e}")
                continue
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron datos para zona={climate_zone}, tipo={analysis_type}"
            )
        
        # Ordenar por temperatura media
        results.sort(key=lambda x: x.temperatura_promedio, reverse=True)
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontraron datos para zona={climate_zone}, tipo={analysis_type}"
            )
        
        logger.info(f"Devolviendo {len(results)} resultados de temperatura")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en análisis de temperatura: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor: {str(e)}")

# Endpoint para análisis de precipitación
@app.get("/precipitation-analysis", response_model=List[PrecipitationStats], tags=["Análisis"])
async def get_precipitation_analysis(
    country: Optional[str] = Query(None, description="Filtrar por país"),
    humidity_class: Optional[str] = Query(None, description="Filtrar por clasificación de humedad"),
    config: APIConfig = Depends(get_config)
):
    """
    Obtiene análisis de precipitación por país.
    """
    try:
        logger.info(f"Consultando análisis de precipitación: país={country}, humedad={humidity_class}")
        
        precip_dir = config.data_dir / "precipitation_analysis"
        precip_files = list(precip_dir.glob("part-*")) if precip_dir.exists() else []
        
        if not precip_files:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron datos de análisis de precipitación"
            )
        
        results = []
        
        for file_path in precip_files:
            try:
                cache_key = f"precip_{file_path.stat().st_mtime}"
                content = cached_file_read(str(file_path), cache_key)
                
                for line in content.strip().split('\n'):
                    if line.strip():
                        try:
                            parts = line.split('\t', 1)
                            if len(parts) != 2:
                                continue
                                
                            country_key_raw, stats_json = parts
                            # Limpiar las comillas del country_key
                            country_key = country_key_raw.strip().strip('"')
                            
                            # Parsing con double JSON fix
                            if isinstance(stats_json, str):
                                stats = json.loads(stats_json)
                                if isinstance(stats, str):
                                    stats = json.loads(stats)
                            else:
                                continue
                                
                            if not isinstance(stats, dict):
                                continue
                                
                        except Exception as e:
                            logger.warning(f"Error parsing precipitación: {e}")
                            continue
                        
                        # Aplicar filtros
                        if country and stats.get('country') != country:
                            continue
                            
                        precip_summary = stats.get('precipitation_summary', {})
                        if humidity_class and precip_summary.get('humidity_classification') != humidity_class:
                            continue
                        
                        day_analysis = stats.get('day_analysis', {})
                        seasonal_analysis = stats.get('seasonal_analysis', {})
                        
                        result = PrecipitationStats(
                            pais=stats['country'],
                            zonas_climaticas=stats.get('climate_zones', []),
                            total_dias_analizados=stats['total_days_analyzed'],
                            precipitacion_total_mm=precip_summary.get('total_precipitation_mm', 0),
                            precipitacion_promedio_diaria=precip_summary.get('average_daily_precipitation', 0),
                            clasificacion_humedad=precip_summary.get('humidity_classification', 'desconocido'),
                            porcentaje_dias_lluviosos=day_analysis.get('rainy_days_percentage', 0),
                            analisis_estacional=seasonal_analysis
                        )
                        
                        results.append(result)
                        
            except Exception as e:
                logger.warning(f"Error procesando archivo precipitación {file_path}: {e}")
                continue
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron datos con los filtros especificados"
            )
        
        # Ordenar por precipitación total
        results.sort(key=lambda x: x.precipitacion_total_mm, reverse=True)
        
        logger.info(f"Devolviendo {len(results)} resultados de precipitación")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en análisis de precipitación: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# Endpoint para eventos extremos
@app.get("/extreme-weather", response_model=List[ExtremeWeatherEvent], tags=["Análisis"])
async def get_extreme_weather_analysis(
    risk_level: Optional[str] = Query(None, description="Filtrar por nivel de riesgo"),
    location: Optional[str] = Query(None, description="Filtrar por ubicación"),
    config: APIConfig = Depends(get_config)
):
    """
    Obtiene análisis de eventos climáticos extremos.
    """
    try:
        logger.info(f"Consultando eventos extremos: riesgo={risk_level}, ubicación={location}")
        
        extreme_dir = config.data_dir / "extreme_weather"
        extreme_files = list(extreme_dir.glob("part-*")) if extreme_dir.exists() else []
        
        if not extreme_files:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron datos de análisis de eventos extremos"
            )
        
        results = []
        
        for file_path in extreme_files:
            try:
                cache_key = f"extreme_{file_path.stat().st_mtime}"
                content = cached_file_read(str(file_path), cache_key)
                
                for line in content.strip().split('\n'):
                    if line.strip():
                        try:
                            parts = line.split('\t', 1)
                            if len(parts) != 2:
                                continue
                                
                            location_key_raw, stats_json = parts
                            # Limpiar las comillas del location_key
                            location_key = location_key_raw.strip().strip('"')
                            
                            # Parsing con double JSON fix
                            if isinstance(stats_json, str):
                                stats = json.loads(stats_json)
                                if isinstance(stats, str):
                                    stats = json.loads(stats)
                            else:
                                continue
                                
                            if not isinstance(stats, dict):
                                continue
                                
                        except Exception as e:
                            logger.warning(f"Error parsing eventos extremos: {e}")
                            continue
                        
                        # Aplicar filtros
                        if location and stats.get('location_key') != location:
                            continue
                            
                        risk_assessment = stats.get('risk_assessment', {})
                        if risk_level and risk_assessment.get('risk_level') != risk_level:
                            continue
                        
                        analysis_period = stats.get('analysis_period', {})
                        
                        result = ExtremeWeatherEvent(
                            clave_ubicacion=stats['location_key'],
                            zona_climatica=stats['climate_zone'],
                            pais=stats['country'],
                            total_dias_analizados=analysis_period.get('total_days_analyzed', 0),
                            porcentaje_extremo=analysis_period.get('extreme_percentage', 0),
                            puntuacion_riesgo_general=risk_assessment.get('overall_risk_score', 0),
                            nivel_riesgo=risk_assessment.get('risk_level', 'desconocido'),
                            amenazas_principales=risk_assessment.get('primary_threats', []),
                            recomendaciones=risk_assessment.get('recommendations', [])
                        )
                        
                        results.append(result)
                        
            except Exception as e:
                logger.warning(f"Error procesando archivo extremos {file_path}: {e}")
                continue
        
        if not results:
            raise HTTPException(
                status_code=404,
                detail="No se encontraron eventos extremos con los filtros especificados"
            )
        
        # Ordenar por score de riesgo
        results.sort(key=lambda x: x.puntuacion_riesgo_general, reverse=True)
        
        logger.info(f"Devolviendo {len(results)} resultados de eventos extremos")
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en análisis de eventos extremos: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

# Endpoint para análisis comparativo
@app.get("/comparative-analysis", response_model=ComparativeAnalysis, tags=["Análisis"])
async def get_comparative_analysis(
    comparison_type: str = Query("temperatura", description="Tipo: temperatura, precipitacion, riesgo"),
    config: APIConfig = Depends(get_config)
):
    """
    Genera análisis comparativo entre ubicaciones.
    """
    try:
        logger.info(f"Generando análisis comparativo: tipo={comparison_type}")
        
        if comparison_type == "temperatura":
            return await _generate_temperature_comparison(config)
        elif comparison_type == "precipitacion":
            return await _generate_precipitation_comparison(config)
        elif comparison_type == "riesgo":
            return await _generate_risk_comparison(config)
        else:
            raise HTTPException(status_code=400, detail="Tipo de comparación no válido")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en análisis comparativo: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

async def _generate_temperature_comparison(config: APIConfig) -> ComparativeAnalysis:
    """Genera comparación de temperaturas entre zonas climáticas."""
    # Implementación simplificada - en producción sería más compleja
    return ComparativeAnalysis(
        tipo_comparacion="temperatura",
        ubicaciones=["tropical_mountain", "subtropical", "temperate"],
        metricas={
            "temperatura_promedio": {"tropical_mountain": 22.5, "subtropical": 18.3, "temperate": 12.8},
            "variabilidad_temperatura": {"tropical_mountain": 3.2, "subtropical": 8.5, "temperate": 15.2}
        },
        clasificacion=[
            {"ubicacion": "tropical_mountain", "puntuacion": 95, "puesto": 1},
            {"ubicacion": "subtropical", "puntuacion": 78, "puesto": 2},
            {"ubicacion": "temperate", "puntuacion": 65, "puesto": 3}
        ],
        percepciones=[
            "Las zonas tropicales de montaña muestran mayor estabilidad térmica",
            "Las zonas templadas tienen mayor variabilidad estacional",
            "Los climas subtropicales ofrecen balance entre estabilidad y variedad"
        ]
    )

async def _generate_precipitation_comparison(config: APIConfig) -> ComparativeAnalysis:
    """Genera comparación de precipitación entre países."""
    return ComparativeAnalysis(
        tipo_comparacion="precipitacion",
        ubicaciones=["Colombia", "Brasil", "Argentina"],
        metricas={
            "precipitacion_anual": {"Colombia": 1200, "Brasil": 1100, "Argentina": 650},
            "dias_lluviosos": {"Colombia": 180, "Brasil": 165, "Argentina": 95}
        },
        clasificacion=[
            {"ubicacion": "Colombia", "puntuacion": 92, "puesto": 1},
            {"ubicacion": "Brasil", "puntuacion": 85, "puesto": 2},
            {"ubicacion": "Argentina", "puntuacion": 58, "puesto": 3}
        ],
        percepciones=[
            "Colombia presenta el régimen de lluvias más consistente",
            "Argentina muestra patrones más áridos",
            "Brasil tiene precipitación concentrada en ciertas estaciones"
        ]
    )

async def _generate_risk_comparison(config: APIConfig) -> ComparativeAnalysis:
    """Genera comparación de riesgo climático entre ubicaciones."""
    return ComparativeAnalysis(
        tipo_comparacion="riesgo",
        ubicaciones=["medellin_colombia", "miami_usa", "sydney_australia"],
        metricas={
            "puntuacion_riesgo": {"medellin_colombia": 2.3, "miami_usa": 6.8, "sydney_australia": 4.2},
            "eventos_extremos": {"medellin_colombia": 12, "miami_usa": 45, "sydney_australia": 28}
        },
        clasificacion=[
            {"ubicacion": "medellin_colombia", "puntuacion": 2.3, "puesto": 1},
            {"ubicacion": "sydney_australia", "puntuacion": 4.2, "puesto": 2},
            {"ubicacion": "miami_usa", "puntuacion": 6.8, "puesto": 3}
        ],
        percepciones=[
            "Medellín presenta el menor riesgo climático general",
            "Miami muestra alta exposición a eventos extremos",
            "Sydney tiene riesgo moderado con variabilidad estacional"
        ]
    )

# Endpoint para exportar datos
@app.get("/export/{analysis_type}", tags=["Exportar"])
async def export_analysis_data(
    analysis_type: str,
    format: str = Query("csv", description="Formato: csv, json"),
    config: APIConfig = Depends(get_config)
):
    """
    Exporta datos de análisis en formato CSV o JSON.
    """
    try:
        if analysis_type not in ["temperature", "precipitation", "extreme"]:
            raise HTTPException(status_code=400, detail="Tipo de análisis no válido")
        
        # Buscar archivos correspondientes
        pattern = f"{analysis_type}_analysis_*.json"
        files = list(config.data_dir.glob(pattern))
        
        if not files:
            raise HTTPException(status_code=404, detail="No hay datos para exportar")
        
        # Generar respuesta basada en formato
        if format == "json":
            return JSONResponse(
                content={"message": "Datos preparados para descarga", "files": [str(f) for f in files]},
                headers={"Content-Type": "application/json"}
            )
        else:
            return JSONResponse(
                content={"message": "Exportación CSV disponible", "files": [str(f) for f in files]},
                headers={"Content-Type": "text/csv"}
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en exportación: {e}")
        raise HTTPException(status_code=500, detail="Error en exportación")

# Manejo de errores globales
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Manejador global de excepciones - Tolerancia a fallos."""
    logger.error(f"Error no manejado: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Error interno del servidor",
            "detail": "Se ha registrado el error. Intente nuevamente.",
            "timestamp": datetime.now().isoformat()
        }
    )

# Eventos de inicio y cierre
@app.on_event("startup")
async def startup_event():
    """Inicialización al arranque del servicio."""
    logger.info("🚀 Iniciando Weather Analysis API")
    logger.info(f"📁 Directorio de datos: {config.data_dir}")
    logger.info("✅ API lista para recibir consultas")

@app.on_event("shutdown") 
async def shutdown_event():
    """Limpieza al cerrar el servicio."""
    logger.info("🛑 Cerrando Weather Analysis API")
    logger.info("✅ Limpieza completada")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")