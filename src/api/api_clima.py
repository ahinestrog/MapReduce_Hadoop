from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from typing import List, Dict, Optional
import json
import glob
from pathlib import Path
from datetime import datetime, timedelta
import csv
import io
from functools import lru_cache

app = FastAPI(title="API de Análisis Climático", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class HealthStatus(BaseModel):
    status: str = Field(..., description="Estado del sistema")
    timestamp: str
    version: str
    uptime_seconds: float
    data_freshness: str
    services_status: Dict

class EstadisticasTemperatura(BaseModel):
    zona_climatica: str
    total_registros: int
    paises: List[str]
    temperatura_promedio: float
    temperatura_maxima_general: float
    temperatura_minima_general: float
    variabilidad_temperatura: float
    porcentaje_confort: float
    tipo_analisis: str

class EstadisticasPrecipitacion(BaseModel):
    pais: str
    zonas_climaticas: List[str]
    total_dias_analizados: int
    precipitacion_total_mm: float
    precipitacion_promedio_diaria: float
    clasificacion_humedad: str
    porcentaje_dias_lluviosos: float
    analisis_estacional: Dict

class EventoExtremo(BaseModel):
    ubicacion: str
    zona_climatica: str
    pais: Optional[str]
    total_eventos: int
    total_dias_analizados: int
    porcentaje_extremo: float
    puntuacion_riesgo_general: float
    nivel_riesgo: str
    eventos_por_tipo: Dict

class APIConfig:
    def __init__(self):
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "output"
        self.startup_time = datetime.now()
        self.data_dir.mkdir(parents=True, exist_ok=True)

config = APIConfig()

def get_config() -> APIConfig:
    return config

@lru_cache(maxsize=100)
def cached_file_read(file_path: str, cache_key: str) -> str:
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def leer_resultados_mapreduce(patron: str) -> List[Dict]:
    archivos = list(config.data_dir.glob(patron))
    resultados = []
    for archivo in archivos:
        try:
            cache_key = f"{archivo.stem}_{archivo.stat().st_mtime}"
            contenido = cached_file_read(str(archivo), cache_key)
            for linea in contenido.strip().split('\n'):
                if linea.strip():
                    partes = linea.split('\t', 1)
                    if len(partes) == 2:
                        _, datos_json = partes
                        datos = json.loads(datos_json)
                        if isinstance(datos, str):
                            datos = json.loads(datos)
                        if isinstance(datos, dict):
                            resultados.append(datos)
        except Exception:
            continue
    return resultados

@app.get("/health", response_model=HealthStatus, tags=["Sistema"])
async def health_check(config: APIConfig = Depends(get_config)):
    uptime = (datetime.now() - config.startup_time).total_seconds()
    data_files = list(config.data_dir.glob("**/*part-*"))
    data_status = "fresco" if data_files else "faltante"
    
    services_status = {
        "almacenamiento_local": "saludable",
        "trabajos_mapreduce": "saludable",
        "ingestion_datos": "saludable" if data_files else "degradado",
        "cache_api": "saludable"
    }
    
    degraded = [k for k, v in services_status.items() if v == "degradado"]
    overall_status = "degradado" if degraded else "saludable"
    
    return HealthStatus(
        status=overall_status,
        timestamp=datetime.now().isoformat(),
        version="1.0.0",
        uptime_seconds=uptime,
        data_freshness=data_status,
        services_status=services_status
    )

@app.get("/temperature-analysis", response_model=List[EstadisticasTemperatura], tags=["Análisis"])
async def get_temperature_analysis(
    climate_zone: Optional[str] = Query(None, description="Filtrar por zona climática"),
    analysis_type: str = Query("all", description="Tipo de análisis"),
    config: APIConfig = Depends(get_config)
):
    try:
        datos = leer_resultados_mapreduce("analisis_temperatura/part-*")
        if not datos:
            raise HTTPException(status_code=404, detail="No se encontraron datos de análisis de temperatura. Ejecute el job MapReduce primero.")
        
        resultados = []
        for stats in datos:
            if climate_zone and stats.get('climate_zone') != climate_zone:
                continue
            if analysis_type != "all" and stats.get('analysis_type') != analysis_type:
                continue
            
            temp_stats = stats.get('temperature_stats', {})
            comfort_stats = stats.get('comfort_analysis', {})
            
            resultados.append(EstadisticasTemperatura(
                zona_climatica=stats['climate_zone'],
                total_registros=stats['record_count'],
                paises=stats['countries'],
                temperatura_promedio=temp_stats.get('mean_temperature', 0),
                temperatura_maxima_general=temp_stats.get('max_temperature_overall', 0),
                temperatura_minima_general=temp_stats.get('min_temperature_overall', 0),
                variabilidad_temperatura=temp_stats.get('temperature_variability', 0),
                porcentaje_confort=comfort_stats.get('comfort_percentage', 0),
                tipo_analisis=stats['analysis_type']
            ))
        
        if not resultados:
            raise HTTPException(status_code=404, detail=f"No se encontraron datos para zona={climate_zone}, tipo={analysis_type}")
        
        resultados.sort(key=lambda x: x.temperatura_promedio, reverse=True)
        return resultados
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.get("/precipitation-analysis", response_model=List[EstadisticasPrecipitacion], tags=["Análisis"])
async def get_precipitation_analysis(
    country: Optional[str] = Query(None, description="Filtrar por país"),
    humidity_class: Optional[str] = Query(None, description="Filtrar por clasificación de humedad"),
    config: APIConfig = Depends(get_config)
):
    try:
        datos = leer_resultados_mapreduce("analisis_precipitacion/part-*")
        if not datos:
            raise HTTPException(status_code=404, detail="No se encontraron datos de análisis de precipitación")
        
        resultados = []
        for stats in datos:
            if country and stats.get('country') != country:
                continue
            
            precip_summary = stats.get('precipitation_summary', {})
            if humidity_class and precip_summary.get('humidity_classification') != humidity_class:
                continue
            
            day_analysis = stats.get('day_analysis', {})
            seasonal_analysis = stats.get('seasonal_analysis', {})
            
            resultados.append(EstadisticasPrecipitacion(
                pais=stats['country'],
                zonas_climaticas=stats.get('climate_zones', []),
                total_dias_analizados=stats['total_days_analyzed'],
                precipitacion_total_mm=precip_summary.get('total_precipitation_mm', 0),
                precipitacion_promedio_diaria=precip_summary.get('average_daily_precipitation', 0),
                clasificacion_humedad=precip_summary.get('humidity_classification', 'desconocido'),
                porcentaje_dias_lluviosos=day_analysis.get('rainy_days_percentage', 0),
                analisis_estacional=seasonal_analysis
            ))
        
        return resultados
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/extreme-weather", response_model=List[EventoExtremo], tags=["Análisis"])
async def get_extreme_weather(
    location: Optional[str] = Query(None, description="Filtrar por ubicación"),
    risk_level: Optional[str] = Query(None, description="Filtrar por nivel de riesgo"),
    config: APIConfig = Depends(get_config)
):
    try:
        datos = leer_resultados_mapreduce("analisis_clima_extremo/part-*")
        if not datos:
            raise HTTPException(status_code=404, detail="No se encontraron datos de clima extremo")
        
        resultados = []
        for stats in datos:
            if location and stats.get('location_key') != location:
                continue
            
            risk_analysis = stats.get('risk_analysis', {})
            if risk_level and risk_analysis.get('risk_level') != risk_level:
                continue
            
            event_summary = stats.get('event_summary', {})
            
            resultados.append(EventoExtremo(
                ubicacion=stats['location_key'],
                zona_climatica=stats['climate_zone'],
                pais=stats.get('country'),
                total_eventos=event_summary.get('total_extreme_events', 0),
                total_dias_analizados=stats.get('total_days_analyzed', 0),
                porcentaje_extremo=event_summary.get('extreme_percentage', 0),
                puntuacion_riesgo_general=risk_analysis.get('overall_risk_score', 0),
                nivel_riesgo=risk_analysis.get('risk_level', 'bajo'),
                eventos_por_tipo=event_summary.get('events_by_type', {})
            ))
        
        return resultados
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/comparative-analysis", tags=["Análisis"])
async def get_comparative_analysis(config: APIConfig = Depends(get_config)):
    try:
        temp_data = leer_resultados_mapreduce("analisis_temperatura/part-*")
        precip_data = leer_resultados_mapreduce("analisis_precipitacion/part-*")
        extreme_data = leer_resultados_mapreduce("analisis_clima_extremo/part-*")
        
        return {
            "tipo_comparacion": "general",
            "ubicaciones_analizadas": len(temp_data) + len(precip_data) + len(extreme_data),
            "zonas_climaticas": len(temp_data),
            "paises": len(precip_data),
            "eventos_extremos_total": len(extreme_data),
            "resumen_temperatura": temp_data[:3] if temp_data else [],
            "resumen_precipitacion": precip_data[:3] if precip_data else [],
            "resumen_extremos": extreme_data[:3] if extreme_data else []
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/export/{analysis_type}", tags=["Exportar"])
async def export_analysis_data(
    analysis_type: str,
    formato: str = Query("json", description="Formato: json o csv"),
    config: APIConfig = Depends(get_config)
):
    tipo_map = {
        "temperatura": "analisis_temperatura",
        "precipitacion": "analisis_precipitacion",
        "extremos": "analisis_clima_extremo"
    }
    
    if analysis_type not in tipo_map:
        raise HTTPException(status_code=400, detail="Tipo de análisis inválido. Opciones: temperatura, precipitacion, extremos")
    
    try:
        datos = leer_resultados_mapreduce(f"{tipo_map[analysis_type]}/part-*")
        if not datos:
            raise HTTPException(status_code=404, detail="No hay datos disponibles")
        
        if formato == "csv":
            output = io.StringIO()
            if datos:
                writer = csv.DictWriter(output, fieldnames=datos[0].keys())
                writer.writeheader()
                writer.writerows(datos)
            
            return StreamingResponse(
                iter([output.getvalue()]),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={analysis_type}.csv"}
            )
        else:
            return StreamingResponse(
                iter([json.dumps(datos, indent=2)]),
                media_type="application/json",
                headers={"Content-Disposition": f"attachment; filename={analysis_type}.json"}
            )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
