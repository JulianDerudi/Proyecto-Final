# Proyecto Final – Ingeniería de Datos

### ETL desde API pública hacia Base de Datos MySQL

Este proyecto corresponde al **trabajo final del curso de Ingeniería de Datos (UTN)**.
Su objetivo principal es realizar un proceso **ETL completo (Extract – Transform – Load)** tomando datos desde una **API pública**, procesándolos y almacenándolos en una **base de datos relacional**.

---

## 🧩 Objetivo del Proyecto

El propósito fue construir un pipeline capaz de:

1. **Extraer** información desde una API externa.
2. **Transformar** y limpiar los datos (normalización, parseo de tipos, validaciones, eliminación de duplicados, etc.).
3. **Cargar** la información procesada en una base de datos **MySQL**, lista para ser consultada o utilizada por otros sistemas.

Este proyecto replica un flujo real utilizado en entornos de **Data Engineering**.

---

## 🛠️ Tecnologías Utilizadas

* **Python 3**
* **Requests** (consumo de API)
* **Pandas** (limpieza y transformación de datos)
* **MySQL / MySQL Connector**
* **dotenv** (manejo seguro de credenciales)
* **Jupyter Notebook** (desarrollo y documentación del proceso)

---

## 📡 Fuente de Datos (API)

El proyecto se conecta a una API pública (detallada en el notebook) desde donde se obtienen los datos crudos a procesar.

---

## 🔄 Flujo ETL Implementado

### **1. Extracción**

* Se realiza una solicitud HTTP a la API.
* Se validan respuestas, códigos de estado y estructura del JSON.
* Los datos crudos se convierten en un DataFrame inicial.

### **2. Transformación**

* Normalización de campos.
* Conversión de tipos numéricos y fechas.
* Limpieza de registros inválidos.
* Eliminación de duplicados.
* Estructuración final para ajustarse al modelo relacional de la base de datos.

### **3. Carga**

* Conexión a MySQL mediante credenciales protegidas por `.env`.
* Creación de tablas (si no existen).
* Inserción de datos transformados.

---

## 🧱 Estructura del Repositorio

```
├── notebooks/
│   └── Proyecto_Final.ipynb     # Notebook principal con todo el proceso ETL
├── src/
│   └── etl.py                   # Script modular del pipeline (si aplica)
├── .env.example                 # Ejemplo de variables de entorno
├── requirements.txt             # Dependencias del proyecto
└── README.md                    # Este archivo
```

---

## 📊 Modelo de Datos

El script/notebook genera una base de datos con tablas normalizadas según los datos obtenidos de la API.

*(Podés agregar aquí un diagrama ER si lo tenés.)*

---

## 🚀 Cómo Ejecutarlo

### **1. Clonar el repositorio**

```
git clone https://github.com/JulianDerudi/Proyecto-Final.git
cd Proyecto-Final
```

### **2. Crear y configurar el archivo `.env`**

```
DB_HOST=localhost
DB_USER=tu_usuario
DB_PASSWORD=tu_password
DB_NAME=nombre_bd
```

### **3. Instalar dependencias**

```
pip install -r requirements.txt
```

### **4. Ejecutar el ETL**

Podés hacerlo desde Jupyter Notebook o el script Python módulo ETL (si lo incluís).

---

## 📚 Aprendizajes del Proyecto

* Manejo de datos desde APIs reales.
* Construcción de pipelines ETL.
* Normalización y limpieza de datasets.
* Conexión y almacenamiento en MySQL.
* Buenas prácticas con entorno virtual y manejo de credenciales.

---

## 📞 Contacto

**Julián Derudi**
Portafolio: [https://julianderudi.github.io/Portafolio/](https://julianderudi.github.io/Portafolio/)
LinkedIn: [https://www.linkedin.com/in/julian-derudi-730ba8343/](https://www.linkedin.com/in/julian-derudi-730ba8343/)

---

Si te resulta útil o querés colaborar, ¡no dudes en abrir un issue!
