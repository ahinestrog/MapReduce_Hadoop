# Sistema de Análisis Climático con MapReduce

Sistema de análisis de datos climáticos usando MapReduce local y API REST.

## Estructura

```
MapReduce_Hadoop/
├── data/
│   ├── input/          # Datos de entrada
│   └── output/         # Resultados MapReduce
├── src/
│   ├── api/
│   │   └── api_clima.py
│   ├── extraccion/
│   │   └── extractor_clima.py
│   └── mapreduce/
│       ├── analisis_temperatura.py
│       ├── analisis_precipitacion.py
│       └── analisis_clima_extremo.py
├── ejecutar_mapreduce.py
└── requirements.txt
```

## Instalación

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Uso

### 1. Extraer datos

```bash
python src/extraccion/extractor_clima.py
```

### 2. Ejecutar análisis MapReduce

```bash
python ejecutar_mapreduce.py
```

O individualmente cada Job:

```bash
python src/mapreduce/analisis_temperatura.py data/input/unified_weather_data.jsonl --output-dir data/output/analisis_temperatura
python src/mapreduce/analisis_precipitacion.py data/input/unified_weather_data.jsonl --output-dir data/output/analisis_precipitacion
python src/mapreduce/analisis_clima_extremo.py data/input/unified_weather_data.jsonl --output-dir data/output/analisis_clima_extremo
```

### 3. Iniciar API

```bash
python -m uvicorn src.api.api_clima:app --reload --host 0.0.0.0 --port 8000
```

API disponible en: http://localhost:8000/docs -> En caso de que se este probando local

## Endpoints

- `GET /` - Información de la API
- `GET /salud` - Estado del sistema
- `GET /analisis-temperatura` - Estadísticas de temperatura por zona
- `GET /analisis-precipitacion` - Análisis de precipitación por país
- `GET /eventos-extremos` - Eventos climáticos extremos
- `GET /comparacion` - Comparación de datos

## Datos

El sistema analiza datos de 8 ciudades:
- Medellín (Colombia)
- São Paulo (Brasil)
- Buenos Aires (Argentina)
- Miami (USA)
- Ciudad de México (México)
- Madrid (España)
- Tokyo (Japón)
- Sydney (Australia)

Fuente: Open-Meteo Archive API

