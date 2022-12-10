# Vuela Bonito y Barato
Proyecto final de la asignatura Cloud y Big Data - curso 2022/23

# Objetivo
Hemos analizado un conjunto de datos con información de un gran número de vuelos en EE.UU. con el fin de llegar a conclusiones interesantes como cuales son los viajes más económicos, cuales son los destinos más frecuentes y con ello en que estados o ciudades el tráfico aéreo es más intenso o que días de la semana son más frecuentes.

# Necesidad de utilizar Big Data
Analizar la gran cantidad de vuelos y los datos asociados a cada vuelo es bastante costoso. Además si queremos obtener resultados senciallamente y en un tiempo razonable es imprescindible aplicar la tecnología Big Data y 'large-scale parallel processing'.

# Datos utilizados para la obtención de resultados
El dataset ofrece gran variedad de datos acerca de cada vuelo pero para sacar conclusiones interesantes nos hemos enfocado en los siguientes:

- startingAirport (Aeropuerto origen)
- destinationAirport (Aeropuerto destino)
- flightDate (Fecha de vuelo)
- totalTravelDistance (Distancia recorrida en el vuelo contando escalas)
- totalFare (Precio del vuelo con impuestos añadidos)
- segmentsAirlineCode (Código de la aerolínea encargada del vuelo)
- seatsRemaining (Número de asientos libres en los vuelos)

Hemos utilizado como complemento otro archivo, 'USA_Covid_Data.csv', para poder relacionar el número de vuelos con el número de casos positivos oficiales por estado. Para este caso en concreto usamos el campo 'destinationAirport' de nuestro dataset original y los campos 'State', estado donde se observa la muestra, 'Active', casos positivos oficiales, y el campo 'Population' que representa el censo total del estado.

# Resultados gracias a los datos obtenidos 

- Promedio de tarifas por empresa
- Relación entre casos de covid y el número de vuelos
- Número de vuelos por día
- Relación entre distancia y tarifa
- Número de vuelos por aerolínea
- Evolución de la tarifa según los días
- Número de viajes sin escalas
- Vuelos por día
- Viajes más comunes

# Herramientas utilizadas
- Google Cloud: Servivio online para alojar las maquinas virutales y clusters utilizados para procesar los datos.
- Apache Spark: Motor multilenguaje utilizado para facilitarnos la ejecución de los datos.
- Python: Lenguaje de programación base para ejecutar spark.
- Github: Servicio online para alojar todo el material correspondiente al proyecto.

# Descripción del proyecto

## Archivo utilizados:
* 1 archivo .csv del que obtenemos los datos
* 9 scripts para el procesamiento de los datos y la obtención de los gráficos
* 12 imágenes que ilustran los gráficos del análisis de datos

## Cómo ejecutar el programa
Para poder ejecutar spark en modo **local** es necesario tener instalado python.
```
$ sudo apt-get install python
```

Una vez tengamos python instalado hay que instalar pyspark
```
$ curl -O https://archive.apache.org/dist/spark/spark-3.3.1/spark-3.3.1-bin-hadoop3.tgz
$ tar xvf spark-3.3.1-bin-hadoop3.tgz
$ sudo mv spark-3.3.1-bin-hadoop3 /usr/local/spark
```
Ahora hay que actualizar la variable PATH en el archivo ~/.profile
```
$ echo 'PATH="$PATH:/usr/local/spark/bin"' >> ~/.profile
$ source ~/.profile
```
Después de ejecutar todos los comandos anteriores ya podemos ejecutar los scripts especificando el archivo csv del que sacar los datos
```
$ spark-submit <script-name> <file.csv>
```
Ejemplo: $ spark-submit mergueDayStats.py itineraries_3GB.csv

Para poder ejecutaar todos los scripts es imprescindible ejecuatr los siguientes comandos:
```
$ pip install pandas
$ pip install matplotlib
```
Estos dos comandos instalan pandas, herramienta complementaria para tratar los datos, y la librería matplotlib para conseguirlos gráficos con el análisis de datos

# Página web
* [Página web](https://tripanalistycs.odoo.com/@/)

# Integrantes del grupo
* Francisco Calvo
* Daniel Gallego
* Rodrigo Quispe
* Pablo Regidor
