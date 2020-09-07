# Yahoo Finance

Link: https://finance.yahoo.com/

Yahoo Finance es parte de Verizon Media. Ofrece noticias financieras, datos y comentarios, incluye precios de acciones, reportajes de presa, reportes financieros y contenido original. También ofrece herramientas online para administración de finanzas personales.

### Descipción de la fuente de información

* Yahoo Finance obtiene precios de distintas bolsas financieras alrededor del mundo, en el caso de las acciones correspondientes a nuestro caso de estudio (acciones IPSA), estas son obtenidas originalmente de la Bolsa de Santiago.
* Yahoo Finance ofrece precios de acciones en “Near Real Time” con una frecuencia máxima de cada 2 minutos, aunque también se puede obtener el valor de cierre diario, valores ajustados por dividendo u obtener valores “Year to Date”, y por un rango de fechas a elegir que varía dependiendo de la acción.
* Esta fuente de datos es fundamental para lograr nuestro objetivo, dado que necesitamos analizar el movimiento de precios de las acciones del IPSA durante el periodo de análisis, e incluso agregar información de años anteriores para realizar comparativas de comportamiento versus el mismo periodo en distintos años.
* Para la extracción de datos usaremos la librería  de Python “yfinance” (https://github.com/ranaroussi/yfinance).
* Se utilizará una lista con los identificadores de Yahoo Finance con las acciones actuales del IPSA, para ir iterando en la extracción de los precios de cada una.
* Filtraremos la base de datos utilizando solo las columnas “close” y “volume”, ya que éstas se adaptan al objetivo del análisis.
* El formato de fecha “YYYY-MM-DD” en la columna “Date”, se adecúa a las necesidades del análisis.
* El formato de número float tanto en la columna “close” como “volumen” también se adapta a las necesidades del análisis.


#### Comando de ejecución script Bonobo

```
$ python bonobo_yahoo_stock.py
```

