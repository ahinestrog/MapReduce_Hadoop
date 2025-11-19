# **Trabajo 3 – Procesamiento Distribuido con MapReduce**  
### **ST0263 – Tópicos Especiales en Telemática, 2025-2 — Universidad EAFIT**

---

## **📅 Fecha de entrega**
**23 de noviembre de 2025**

---

## **📘 Descripción**

Este trabajo final consiste en construir una **arquitectura batch basada en Hadoop**, utilizando exclusivamente el modelo **MapReduce**, para simular un flujo real de procesamiento distribuido.

El proyecto permite experimentar con un flujo completo:

- Obtención de datos  
- Almacenamiento distribuido  
- Procesamiento paralelo  
- Entrega de resultados  

---

## **🎯 Objetivo General**

Implementar un flujo completo de procesamiento distribuido utilizando **HDFS y MapReduce**, comprendiendo los fundamentos del almacenamiento distribuido y el procesamiento batch.

---

## **🧩 Etapas del Proyecto**

### **1️⃣ Obtención de datos (manual)**
- Selección de una fuente de datos abierta.
- Descarga local de archivos en formato **CSV**, **JSON** o texto plano.  
- **No se requiere automatización.**

### **2️⃣ Carga a HDFS**
- Cargar los archivos al sistema distribuido (HDFS), por ejemplo en Amazon EMR.
- Puede hacerse manualmente o mediante un script.

### **3️⃣ Procesamiento con MapReduce**
- Implementación de uno o varios programas MapReduce en:
  - **Java (Hadoop nativo)**  
  - **Python (MRJob)**  
- Debe existir **al menos un job** que produzca resultados significativos:
  - agregación  
  - filtrado  
  - conteo  
  - análisis estadístico  

### **4️⃣ Salida y consulta de resultados**
- Guardar nuevamente los resultados en HDFS.
- Exportar a **CSV**.
- Servir los resultados mediante una **API (Flask o FastAPI)**.

---

## **📌 Alcance**

- Implementar y ejecutar programas MapReduce en Hadoop.
- Trabajar con archivos reales (estructurados o semi‑estructurados).
- Usar HDFS como almacenamiento principal.
- Mostrar el flujo completo: **carga → procesamiento → salida**.
- Visualización accesible mediante API.

---

## **📦 Entrega Final**

### **📁 Repositorio GitHub**
Debe incluir:

- Código MapReduce (.java o .py)  
- Script(s) de carga a HDFS (si aplica)  
- Archivos de entrada y salida de ejemplo  
- Código de la API para visualizar resultados  
- Instrucciones claras en `README.md`

---

### **🎥 Video de sustentación (máx. 10 minutos)**  
Debe explicar:

- Datos utilizados y justificación
- Método de carga al sistema
- Funcionamiento del programa MapReduce
- Resultados obtenidos

---

### **💬 Sustentación presencial (si se requiere)**
**Lunes 24 de noviembre de 2025**, 8:00 a.m. – 12:00 m.

---

## **🗂 Fuente de datos**

---

# **1. Datos del tiempo (clima)**

### **Open‑Meteo**  
- API gratuita sin autenticación  
- Datos actuales, pronósticos y registros históricos  
- Ideal para carga masiva en S3  
- Ejemplo:  
`https://archive-api.open-meteo.com/v1/archive?latitude=6.25&longitude=-75.56&start_date=2022-01-01&end_date=2022-12-31&daily=temperature_2m_max,precipitation_sum&timezone=America/Bogota`