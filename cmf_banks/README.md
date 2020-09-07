# CMF

<b>Link:</b> http://www.cmfchile.cl/


La Comisión para el Mercado Financiero, en adelante CMF, es la entidad regulador del Mercado Financiero Chileno, encargada de supervisar instituciones financieras como compañías emisores de acciones y deuda pública, corredores de bolsa, bancos, entre otros.

### Descripción de la fuente de información

* Emisores de acciones deben reportar a la CMF sus estados
financieros trimestrales cumpliendo con todos los
requisitos legales establecidos.

* El ejercicio con la web de la CMF persigue recolectar los
ingresos y utilidades de las compañías de oferta pública que
pertenecen al índice accionario IPSA para el periodo del 4to
trimestre calendario del año 2019 y compararlos con los
números del mismo periodo del año 2018 con el fin de
estudiar algún impacto generado por la crisis social de Chile
(https://tinyurl.com/yc62m9qj)

* La fuente original de los datos son las mismas compañías
reguladas siguiendo las directrices de la CMF, manteniendo
toda la información disponible en caso de cualquier consulta
o investigación.

* Importante destacar que la CMF es una entidad
relativamente nueva que se compone de las antiguas
entidades Superintendencia de Valores y Seguros (SVS) y la
Superintendencia de Bancos (SBIF), hecho que afectó el
proceso de extracción

* Para la extracción de datos, tendremos que usar el paquete de
Python “selenium” para poder interactuar dinámicamente con
el sitio web.

* Durante el ejercicio, descubrimos que las entidades bancarias
publican sus resultados en el sitio de la ex entidad reguladora
de bancos (SBIF), por lo que fue necesario crear otro script
para extraer esos datos.

* **Para el caso de bancos, la CMF mantiene vigente el sitio de la
Superintendencia de Bancos y publica los resultados
consolidados en un reporte para todos los bancos.**

#### Comando de ejecución script Bonobo
Obs: Para ejecutar el código utilizar versión 83 de Google Chrome
```
$ python bonobo_cmf_banks.py
```
