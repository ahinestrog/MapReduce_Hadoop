# 🚀 **Setup Completo del Proyecto MapReduce**

## **Para el compañero: Ejecutar flujo completo desde cero**

### **1. Clonar y preparar entorno**
```bash
# Clonar repositorio
git clone [URL_DEL_REPO]
cd MapReduce_Hadoop

# Activar entorno virtual (ya incluido)
source venv/bin/activate

# Verificar dependencias
pip list
```

### **2. Ejecutar extracción de datos (OBLIGATORIO)**
```bash
# Obtener datos reales de 8 ciudades globales
python src/data_extraction/weather_extractor.py

# Verificar que se descargaron los datos
ls -la data/input/
# Deberías ver archivos como:
# - weather_medellin_colombia_2022-12-01_2022-12-31.json
# - weather_buenos_aires_argentina_2022-12-01_2022-12-31.json
# - unified_weather_data.jsonl
# - etc.
```

### **3. Ejecutar jobs MapReduce (OBLIGATORIO)**
```bash
# Procesar todos los análisis
python tests/test_mapreduce_quick.py

# O ejecutar jobs individuales:
# python src/mapreduce/temperature_analysis_job.py data/input/unified_weather_data.jsonl --output-dir data/output/temperature_analysis
# python src/mapreduce/precipitation_analysis_job.py data/input/unified_weather_data.jsonl --output-dir data/output/precipitation_analysis
# python src/mapreduce/extreme_weather_job.py data/input/unified_weather_data.jsonl --output-dir data/output/extreme_weather

# Verificar resultados generados
ls -la data/output/*/
```

### **4. Probar sistema completo**
```bash
# Test completo del sistema
python tests/test_complete_system.py

# Debería mostrar:
# 🎉 ¡SISTEMA COMPLETAMENTE FUNCIONAL!
# 📈 Tasa de éxito: 100.0%
```

### **5. Iniciar API y explorar resultados**
```bash
# Iniciar servidor
python src/api/weather_api.py

# Acceder a:
# - http://localhost:8000 (Aplicación)
# - http://localhost:8000/docs (Documentación API)
```

---

## **🎯 Flujo Completo Obligatorio**

### **Extracción → Procesamiento → Resultados → API**

1. **📡 Extracción**: `weather_extractor.py` obtiene datos reales
2. **🔄 MapReduce**: 3 jobs procesan y analizan datos
3. **📊 Resultados**: Archivos en `data/output/`
4. **🌐 API**: FastAPI sirve resultados procesados

---

## **📋 Verificación de Éxito**

### **Después de ejecutar todo, deberías tener:**

```bash
# Estructura de datos completa
data/
├── input/
│   ├── weather_medellin_colombia_2022-12-01_2022-12-31.json
│   ├── weather_buenos_aires_argentina_2022-12-01_2022-12-31.json
│   ├── weather_madrid_espana_2022-12-01_2022-12-31.json
│   ├── weather_miami_usa_2022-12-01_2022-12-31.json
│   ├── weather_sao_paulo_brasil_2022-12-01_2022-12-31.json
│   ├── weather_tokyo_japan_2022-12-01_2022-12-31.json
│   ├── weather_sydney_australia_2022-12-01_2022-12-31.json
│   ├── weather_ciudad_mexico_2022-12-01_2022-12-31.json
│   └── unified_weather_data.jsonl
└── output/
    ├── temperature_analysis/
    │   └── part-00000 (resultados temperatura)
    ├── precipitation_analysis/
    │   └── part-00000 (resultados precipitación)
    └── extreme_weather/
        └── part-00000 (resultados eventos extremos)
```

### **API funcionando con endpoints:**
- ✅ `/health` - Estado del sistema
- ✅ `/temperature-analysis` - Análisis térmico
- ✅ `/precipitation-analysis` - Análisis pluviométrico
- ✅ `/extreme-weather` - Eventos extremos

---

## **⚠️ Importante**

- **NO subir datos ni resultados al repositorio**
- **Cada miembro del equipo debe ejecutar el flujo completo**
- **Esto demuestra el funcionamiento real del sistema MapReduce**
- **Los datos son reales y actuales de Open-Meteo API**

## **🎥 Para el video de sustentación**

1. **Mostrar extracción de datos en tiempo real**
2. **Ejecutar jobs MapReduce paso a paso** 
3. **Demostrar resultados en la API**
4. **Explicar la arquitectura distribuida**

**Tiempo total de setup: ~5-10 minutos**
**Perfecto para demostración en vivo** ✨