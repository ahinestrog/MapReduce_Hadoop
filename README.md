# Weather Analysis with MapReduce
## Sistema de Análisis Climático Distribuido

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.68+-green.svg)](https://fastapi.tiangolo.com)
[![MRJob](https://img.shields.io/badge/MRJob-0.7+-orange.svg)](https://mrjob.readthedocs.io)

Un sistema completo de análisis de datos climáticos utilizando **MapReduce local** con procesamiento distribuido y API REST para consultas interactivas.

## 🌟 Características

- **🌦️ Extracción de Datos Reales**: Integración con Open-Meteo API
- **⚡ Procesamiento MapReduce**: Análisis distribuido con MRJob
- **🚀 API REST Completa**: FastAPI con documentación interactiva
- **📊 Análisis Multidimensional**: Temperatura, precipitación y eventos extremos
- **🔄 Sistema Local**: Sin dependencias de cloud (AWS/EMR)

## 📁 Estructura del Proyecto

```
MapReduce_Hadoop/
├── 📚 README.md                     # Este archivo
├── 📋 requirements.txt              # Dependencias Python
├── 🧹 cleanup_project.py            # Script de limpieza
├── 
├── 🏗️ src/                          # Código fuente principal
│   ├── 🌐 api/                      # API REST con FastAPI
│   │   └── weather_api.py           # Servidor API principal
│   ├── 📡 data_extraction/          # Extracción de datos
│   │   └── weather_extractor.py     # Extractor Open-Meteo
│   └── 🔄 mapreduce/                # Jobs MapReduce
│       ├── temperature_analysis_job.py    # Análisis temperatura
│       ├── precipitation_analysis_job.py  # Análisis precipitación
│       └── extreme_weather_job.py         # Eventos extremos
│
├── 📊 data/                         # Datos y resultados
│   ├── input/                       # Datos de entrada
│   │   ├── unified_weather_data.jsonl    # Dataset unificado
│   │   └── *.json                   # Datos por ciudad
│   └── output/                      # Resultados MapReduce
│       ├── temperature_analysis/    # Resultados temperatura
│       ├── precipitation_analysis/  # Resultados precipitación
│       └── extreme_weather/         # Resultados eventos extremos
│
└── 🧪 tests/                        # Scripts de testing
    ├── test_complete_system.py      # Test integración completa
    ├── test_mapreduce_quick.py      # Test jobs MapReduce
    ├── test_api_local.py            # Test API local
    └── setup_local_testing.py       # Setup entorno testing
```

## 🚀 Inicio Rápido

### 1. **Configurar Entorno**
```bash
# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o
venv\Scripts\activate     # Windows

# Instalar dependencias
pip install -r requirements.txt
```

### 2. **Extraer Datos Climáticos**
```bash
# Obtener datos reales de 8 ciudades globales
python src/data_extraction/weather_extractor.py
```

### 3. **Ejecutar Análisis MapReduce**
```bash
# Procesar todos los jobs de análisis
python test_mapreduce_quick.py
```

### 4. **Iniciar API REST**
```bash
# Levantar servidor API
python src/api/weather_api.py

# Acceder a documentación interactiva
# http://localhost:8000/docs
```

### 5. **Verificar Sistema Completo**
```bash
# Test de integración completa
python test_complete_system.py
```

## 🌍 Ciudades Analizadas

| Ciudad | País | Zona Climática | Coordenadas |
|--------|------|---------------|-------------|
| **Medellín** | Colombia | Tropical Mountain | 6.25°N, 75.56°W |
| **Buenos Aires** | Argentina | Temperate | 34.61°S, 58.38°W |
| **Madrid** | España | Continental Mediterranean | 40.42°N, 3.70°W |
| **Miami** | USA | Tropical Subtropical | 25.76°N, 80.19°W |
| **São Paulo** | Brasil | Humid Subtropical | 23.55°S, 46.64°W |
| **Tokyo** | Japón | Humid Subtropical | 35.68°N, 139.69°E |
| **Sydney** | Australia | Oceanic | 33.87°S, 151.21°E |
| **Ciudad de México** | México | Tropical Highland | 19.43°N, 99.13°W |

## 📊 Análisis Disponibles

### 🌡️ **Análisis de Temperatura**
- Estadísticas por zona climática
- Temperaturas promedio, máxima y mínima
- Análisis de confort térmico
- Distribución estacional

### 🌧️ **Análisis de Precipitación**
- Precipitación total y promedio
- Días lluviosos vs. secos
- Clasificación de humedad
- Tendencias estacionales

### ⚡ **Eventos Extremos**
- Detección de anomalías climáticas
- Evaluación de riesgos
- Análisis de sequías
- Recomendaciones de mitigación

## 🔗 API Endpoints

| Endpoint | Descripción | Ejemplo |
|----------|-------------|---------|
| `GET /health` | Estado del sistema | `curl localhost:8000/health` |
| `GET /temperature-analysis` | Análisis temperatura | `curl localhost:8000/temperature-analysis` |
| `GET /precipitation-analysis` | Análisis precipitación | `curl localhost:8000/precipitation-analysis` |
| `GET /extreme-weather` | Eventos extremos | `curl localhost:8000/extreme-weather` |
| `GET /docs` | Documentación interactiva | `http://localhost:8000/docs` |

### 🔍 **Filtros Disponibles**
```bash
# Por zona climática
curl "localhost:8000/temperature-analysis?climate_zone=tropical_mountain"

# Por país
curl "localhost:8000/precipitation-analysis?country=Spain"

# Por ubicación
curl "localhost:8000/extreme-weather?location=sydney"
```

## 🧪 Testing

### **Test Rápido de Jobs**
```bash
python test_mapreduce_quick.py
```

### **Test API Local**
```bash
python test_api_local.py
```

### **Test Sistema Completo**
```bash
python test_complete_system.py
```

## 🛠️ Mantenimiento

### **Limpiar Proyecto**
```bash
python cleanup_project.py
```

### **Regenerar Datos**
```bash
# Extraer nuevos datos
python src/data_extraction/weather_extractor.py

# Reprocesar análisis
python test_mapreduce_quick.py
```

## 📚 Tecnologías Utilizadas

- **[Python 3.8+](https://python.org)**: Lenguaje principal
- **[MRJob](https://mrjob.readthedocs.io)**: Framework MapReduce
- **[FastAPI](https://fastapi.tiangolo.com)**: API REST moderna
- **[Uvicorn](https://uvicorn.org)**: Servidor ASGI
- **[Requests](https://requests.readthedocs.io)**: Cliente HTTP
- **[Open-Meteo API](https://open-meteo.com)**: Datos climáticos

## 🎯 Casos de Uso

1. **Investigación Climática**: Análisis comparativo entre regiones
2. **Planificación Urbana**: Evaluación de riesgos climáticos
3. **Agricultura**: Análisis de patrones de precipitación
4. **Turismo**: Identificación de mejores épocas climáticas
5. **Educación**: Enseñanza de sistemas distribuidos

## 📈 Métricas del Sistema

- **🌍 8 ciudades** analizadas globalmente
- **📅 248 registros** de datos diarios procesados
- **🔄 3 jobs MapReduce** especializados
- **⚡ 4 endpoints API** completamente funcionales
- **📊 100% datos reales** (no sintéticos)

## 🏆 Logros del Proyecto

✅ **Sistema distribuido local** sin dependencias cloud  
✅ **API REST completa** con documentación interactiva  
✅ **Extracción de datos reales** desde fuentes externas  
✅ **Procesamiento MapReduce** totalmente funcional  
✅ **Análisis multidimensional** de datos climáticos  
✅ **Testing automatizado** de todo el sistema  

## 👥 Contribución

Este proyecto fue desarrollado como parte del curso **"Tópicos Especiales en Telemática"** en la **Universidad EAFIT**.

## 📄 Licencia

Proyecto académico - Universidad EAFIT

---

> 🌟 **Sistema de análisis climático distribuido completamente funcional con datos reales de 8 ciudades globales**