# UTA Mayor
El presente proyecto corresponde a codigo necesario para generar el motor de predicciones de variables climáticas para el proyecto UTA Mayor

# Estructura del proyecto
El proyecto se estructura de la siguiente forma:

```bash
├── oldVisualization(25-6)
│   └── visualizacoinAntes-25-6.html
├── scripts
│   ├── dataPreprocessed.csv
│   ├── pass
│   ├── predict
│   │   └── \*\*\* Predictions.py
│   ├── pres9-8
│   ├── pres9-8.ipynb
│   └── reTrain
│       ├── dataPreprocessing.py
│       └── \*\*\* Retrain.py
└── visualizacionDatosR
    ├── 200006_2021_ \*\*\* .csv
    ├── soloCodigo.R
    ├── visualization.html
    ├── visualization.Rmd
    └── weather.csv
```

Leyenda:
+ oldVisualization  : corresponde al archivo presentado en la primera visualización de datos del proyecto el 25 de junio
+ visualizacionDatosR : corresponde a los archivos de visualización presentados post-implementacion de la variable serverDate en la base de datos
  - *200006_2021_\*\*\*.csv*  : csv con datos DGAC a la fecha de presentacion
  - soloCodigo.R              : archivo ejecutable con opcion para conexion a DB, para obtener visualizacion de datos locales en cualquier momento
  - visualization.html        : archivo presentado un mes despues de la 1ra visualizacion
  - visualization.Rmd         : archivo R Markdown, generador de la última presentacion de datos del proyecto
  - weather.csv               : respaldo de datos locales
+ scripts           : carpeta con archivos ejecutables importantes para el funcionamiento del sistema de pronosticos UTA Mayor
  - **pass**                  : archivo con credenciales para acceso a base de datos UTA Mayor, debe crearse si no existe
  - pres9-8.ipynb             : notebook que contiene la logica e investigacion para la creacion de los modelos, tambien es ejecutable
  - predict                   : 
    + **Predictions.py**      : ejecutables que generan los pronosticos climaticos del proyecto, uno por modelo
  - **dataPreprocessed.csv**  : archivo con datos que alimenta los procesos de reentramiento, actualizado automaticamente 1 vez al mes
  - reTrain                   : 
    + **dataPreprocessing.py**: ejecutable que actualiza el *dataPreprocessed.csv* una vez al mes con datos locales nuevos
    + **reTrain.py**          : ejecutables que generan (o reentrenan) los modelos predictivos para el proyecto, uno por algoritmo


# Rmd 
El documento presentado el 25 de junio corresponde a un .html generado por un archivo R Markdown (.Rmd)
Este es un tipo de documento fuente que permite la escritura de texto markdown a la vez que  código R para su trabajo en conjunto
El código R es compilado al momento de exportar el documento, ya sea como .html .docx o .pdf
Para instalar la extensión ejecutar
install.packages("rmarkdown")
## Estado DB
Desde la fecha de presentacion la cantidad de registros de la base de datos ha de haber cambiado
Esto afecta la precisión de los gráficos y textos presentados el 25 de junio por lo tanto es probable que la ejecucion del archivo .Rmd de errores
Por favor leer y ejecutar "soloCodigo.R" en su reemplazo
## Actualizacion 19-07
ATENCION: El archivo visualization.Rmd va a compilar  **SOLO** si la contraseña de la DB es proveida como texto plano en vez de
```bash
.rs.askForPassword
```

# Sobre Scripts 
## De reentramiento
Fueron hechos para ser ejecutados en el servidor una vez al mes, generando 4 modelos, representando 4 algoritmos predictivos
Debido al peso de los modelos, estos no son adjuntados en la carpeta git, sino que se guardan en una carpeta "models" externa a la carpeta git y creada automaticamente por los scripts
Todos las rutinas obedecen la siguiente lógica:
> Carga Datos Preprocesados -> Preproceasmiento -> Entranamiento -> Guardado en "../models"
Nótese la falta de un proceso de evaluación y testeo, puesto que este no es un paso necesario para un servicio automatico
A pesar de esto, cada scripts viene con su proceso de testeo como comentarios en su código
Y mayor contextualización se provee meidnate el notebook pres9-8.ipynb
### Explicación de pasos
1. Carga Datos Preprocesados : Lectura de *"dataPreprocessed.csv"*. Este es un archivo csv que respalda los datos locales en formato horario, a través del script *dataPreprocessing*, se puede considerar esto último como un *"paso 0"* pues es necesario para evitar que cada proceso de reentramiento haga querys pesadas a la DB
2. Preproceasmiento : Depende de cada modelo, detalles en notebook y codigo de cada modelo
3. Entrenamiento : Depende de cada modelo, detalles en notebook y codigo de cada modelo
4. Guardado en "../models" : Como se dijo anteriormente, los modelos son guardados en una carpeta externa, la cual mas tarde es leída por los scripts de predicción
## De predicción
Fueron hechos para ser ejecutados en el servidor una vez al día, generando pronosticos a 3 días, todos obedecen la siguiente logica: 
> *Query* DB -> Carga modelo -> Generación pronósticos -> *Append* a DB 
1. Query : La cantidad de ultimos-registros a obtener desde la DB UTA Mayor para satisfacer los *inputs* de cada modelo
2. Carga : Desde "../models"
3. Generación de pronósticos : Cada 24 horas y para 3 días, pues más días dispara el error promedio en las predicciones 
4. *Append* : Se prefirio anexar predicciones para así contar con un "historial" de predicciones que pueden ser útiles para futuros estudios

