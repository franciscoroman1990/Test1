library(magrittr)

c("data.table", "lubridate", "bigrquery", "lattice", "ggplot2", "survival") %>% 
  sapply(require, character.only=T)

proyecto <- 'bi-global'

llamada1 <-
  'SELECT
  passengerID,
  first_boarded,
  last_boarded,
  city_name
FROM
  `presentation_dim.dim_passengers`
WHERE
  first_boarded > "2017-01-01 00:00:00"
  AND last_boarded < "2018-02-28 23:59:59"
  AND country_code="MX"'

llamada2 <-
  'SELECT
  driverID,
  first_boarded,
  last_boarded,
  city_name
FROM
  `presentation_dim.dim_drivers`
WHERE
  first_boarded > "2017-01-01 00:00:00"
  AND last_boarded < "2018-02-28 23:59:59"
  AND country_code="MX"'
# 
# 
llamadaConductoresInactivos <- 'SELECT
  rideID,
  driverID,
  passengerID,
  requested_at,
  taximeter_price,
  taximeter_distance,
  city_name,
  request_type
FROM
  `presentation_fact_rides.fact_rides201*`
WHERE
  bool_boarded
  AND country_code="MX"
  AND driverID IN (
  SELECT
    driverID
  FROM
    `presentation_dim.dim_drivers`
  WHERE
    first_boarded > "2017-01-01 00:00:00"
    AND last_boarded < "2018-02-28 23:59:59"
    AND country_code="MX" )
'

llamadaPasajerosInactivos <- 
  'SELECT
  rideID,
  driverID,
  passengerID,
  requested_at,
  taximeter_price,
  taximeter_distance,
  city_name
FROM
  `presentation_fact_rides.fact_rides201*`
WHERE
  bool_boarded
  AND country_code="MX"
  AND passengerID IN (
  SELECT
    passengerID
  FROM
    `presentation_dim.dim_passengers`
  WHERE
    first_boarded > "2017-01-01 00:00:00"
    AND last_boarded < "2018-02-28 23:59:59"
    AND country_code="MX" )'

# Analisis PCA
llamadaPasajerosPCA <-
  'SELECT
viajes_abordados/unique_requests AS unique_fullfillment,
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta,
unique_requests,
viajes_abordados
FROM(
SELECT
COUNT(rideID) AS viajes_abordados,
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta,
SUM(CASE
WHEN bool_unique_demand = TRUE THEN 1
ELSE 0 END) AS unique_requests
FROM
`presentation_fact_rides.fact_rides201*`
WHERE
bool_boarded
AND country_code="MX"
AND passengerID IN (
SELECT
passengerID
FROM
`presentation_dim.dim_passengers`
WHERE
first_boarded > "2017-01-01 00:00:00"
AND last_boarded < "2018-02-28 23:59:59"
AND country_code="MX" )
GROUP BY
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta
ORDER BY
viajes_abordados DESC
)
WHERE
unique_requests > 0
AND
taximeter_price >= 0
AND
taximeter_distance >= 0
AND
dispatching_distance >= 0
AND
dispatching_eta >= 0
AND
unique_requests >= 0
AND
viajes_abordados >= 0
ORDER BY
unique_fullfillment >= 0'



llamadaPasajerosPCACiudad <-
  'SELECT
viajes_abordados/unique_requests AS unique_fullfillment,
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta,
unique_requests,
viajes_abordados,
ciudad
FROM(
SELECT
COUNT(rideID) AS viajes_abordados,
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta,
SUM(CASE
WHEN bool_unique_demand = TRUE THEN 1
ELSE 0 END) AS unique_requests,
CASE 
WHEN city_name = "Mexico City" THEN "1"
WHEN city_name = "Puebla" THEN "2"
WHEN city_name = "Queretaro" THEN "3"
WHEN city_name = "Guadalajara" THEN "4"
WHEN city_name = "Cancun" THEN "5"
WHEN city_name = "Toluca" THEN "6"
WHEN city_name = "Mexicali" THEN "7"
WHEN city_name = "Villahermosa" THEN "8"
WHEN city_name = "Monterrey" THEN "9"
WHEN city_name = "Leon" THEN "10"
WHEN city_name = "Merida" THEN "11"
WHEN city_name = "Playa del Carmen" THEN "12" END AS ciudad
FROM
`presentation_fact_rides.fact_rides201*`
WHERE
bool_boarded
AND country_code="MX"
AND passengerID IN (
SELECT
passengerID
FROM
`presentation_dim.dim_passengers`
WHERE
first_boarded > "2017-01-01 00:00:00"
AND last_boarded < "2018-02-28 23:59:59"
AND country_code="MX" )
GROUP BY
taximeter_price,
taximeter_distance,
dispatching_distance,
dispatching_eta,
ciudad
ORDER BY
viajes_abordados DESC
)
WHERE
unique_requests > 0
AND
taximeter_price >= 0
AND
taximeter_distance >= 0
AND
dispatching_distance >= 0
AND
dispatching_eta >= 0
AND
unique_requests >= 0
AND
viajes_abordados >= 0
ORDER BY
unique_fullfillment DESC'
# 
datosPasajeros   <- query_exec(llamada1, max_pages = Inf, use_legacy_sql = F,
                               project = proyecto)

datosConductores <- query_exec(llamada2, max_pages = Inf, use_legacy_sql = F,
                               project = proyecto)
# 
# 
datosPasajerosInactivos <- query_exec(llamadaPasajerosInactivos,
                                      max_pages = Inf, use_legacy_sql = F,
                                      project = proyecto)

datosConductoresInactivos <- query_exec(llamadaConductoresInactivos,
                                        max_pages = Inf, use_legacy_sql = F,
                                        project = proyecto)


# Analisis PCA
PasajerosPCA <- query_exec(llamadaPasajerosPCA,
                                       max_pages = Inf, use_legacy_sql = F,
                                       project = proyecto)

# Analisis PCA Ciudad
PasajerosPCACiudad <- query_exec(llamadaPasajerosPCACiudad,
                                 max_pages = Inf, use_legacy_sql = F,
                                 project = proyecto)

# 
# 
# 
datosPasajeros %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosPasajeros.csv", row.names = F)
# 
datosConductores %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosConductores.csv", row.names = F)
# 
datosPasajerosInactivos %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosPasajerosInactivos.csv", row.names = F)
# 
# 
datosConductoresInactivos %>%
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosConductoresInactivos.csv", 
            row.names = F)


# Analisis PCA
PasajerosPCA %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosPCA.csv", row.names = F)


# Analisis PCACiudad
PasajerosPCACiudad %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosPCACiudad.csv", row.names = F)



setwd("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david")

datosPasajeros <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosPasajeros.csv",
                        header = T)

datosConductores <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosConductores.csv",
                          header = T)

datosPasajerosInactivos  <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosPasajerosInactivos.csv",
                                  header = T)

datosConductoresInactivos <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/temporaldatosConductoresInactivos.csv",
                                   header=T)

# Analisis PCA
PasajerosPCA <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosPCA.csv",
                                   header=T)


# Analisis PCACiudad
PasajerosPCACiudad <- fread("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosPCACiudad.csv",
                      header=T)







# data <- fread("~/local/temporales/datos15_17.csv", header = T) %>% 
#   .[, fecha := as.Date(requested_at)] %>% 
#   .[year(fecha)  > 2015]
#   
# data <- data %>%
#   .[, c("driverID", "passengerID", "city_name", "fecha",
#         "taximeter_price")]
# 
# data <- data %>% 
#   .[, anio := year(fecha)] %>% 
#   .[, mes  := month(fecha)] 



PasajerosG <-  datosPasajeros %>% 
  data.table %>% 
  .[, primerViaje    := as.POSIXct(first_boarded)] %>% 
  .[, ultimoViaje    := as.POSIXct(last_boarded)] %>% 
  .[, diasDiferencia := difftime(ultimoViaje, primerViaje, units="days")] %>% 
  .[, diasDiferencia := as.numeric(diasDiferencia)] %>% 
  .[year(primerViaje) > 2017] 


 # PasajerosG

PasajerosG %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosG.csv", row.names = F)
# 



pasajero <- PasajerosG %>% 
  .[as.Date(ultimoViaje) < as.Date("2018-02-28")]

# pasajero

pasajero %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/pasajero.csv", row.names = F)


ConductorG <-  datosConductores %>% 
  data.table %>% 
  .[, primerViaje    := as.POSIXct(first_boarded)] %>% 
  .[, ultimoViaje    := as.POSIXct(last_boarded)] %>% 
  .[, diasDiferencia := difftime(ultimoViaje, primerViaje, units="days")] %>% 
  .[, diasDiferencia := as.numeric(diasDiferencia)] %>% 
  .[year(primerViaje) > 2017] 

  # ConductorG

ConductorG %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/ConductorG.csv", row.names = F)



conductor <- ConductorG %>% 
  .[as.Date(ultimoViaje) < as.Date("2018-02-28")]

 # conductor


conductor %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/conductor.csv", row.names = F)



diferenciaNoviembre <- difftime(Sys.Date(), as.Date("2018-02-28"))
diferenciaNoviembre <- as.numeric(diferenciaNoviembre)


 # diferenciaNoviembre


PasajerosG <- PasajerosG %>%
  .[, anio := year(primerViaje)] %>% 
  .[, mes  := month(primerViaje)] %>% 
  .[, diferenciaHoy := difftime(Sys.Date(), ultimoViaje, units = "days")] %>% 
  .[, diferenciaHoy := as.numeric(diferenciaHoy)] %>% 
  .[, vigente := ifelse(diferenciaHoy <= diferenciaNoviembre + 1, 1, 0) ] %>% 
  .[anio %in% c(2017, 2018)]

 # PasajerosG

PasajerosG %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/PasajerosG2.csv", row.names = F)


ConductorG <- ConductorG %>%
  .[, anio := year(primerViaje)] %>% 
  .[, mes  := month(primerViaje)] %>% 
  .[, diferenciaHoy := difftime(Sys.Date(), ultimoViaje, units = "days")] %>% 
  .[, diferenciaHoy := as.numeric(diferenciaHoy)] %>% 
  .[, vigente := ifelse(diferenciaHoy <= diferenciaNoviembre + 1, 1, 0) ] %>% 
  .[anio %in% c(2017, 2018)]

 # ConductorG

ConductorG %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/ConductorG2.csv", row.names = F)




pasajerosInactivos <- datosPasajerosInactivos %>% 
  data.table %>% 
  .[, fecha := as.POSIXct(requested_at)] %>% 
  .[, anio  := year(fecha)] %>%
  .[, mes   := month(fecha)] %>% 
  .[ taximeter_price >= 35 & taximeter_price <= 1000]

 # pasajerosInactivos

pasajerosInactivos %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/pasajerosInactivos.csv", row.names = F)



conductoresInactivos <- datosConductoresInactivos %>% 
  data.table %>% 
  .[, fecha := as.POSIXct(requested_at)] %>% 
  .[, anio  := year(fecha)] %>%
  .[, mes   := month(fecha)] %>% 
  .[ taximeter_price >= 35 & taximeter_price <= 1000]


 # conductoresInactivos


conductoresInactivos %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/conductoresInactivos.csv", row.names = F)



pasajero$diasDiferencia %>%  mean(na.rm=T)
conductor$diasDiferencia %>%  mean(na.rm=T)



tablaPasajero <- pasajero %>%
  .[, .(mediaDuracion = mean(diasDiferencia)), by=city_name] %>% 
  .[, mediaDuracion   := round(mediaDuracion, 2)] %>% 
  .[order(mediaDuracion, decreasing = T)] %>% 
  data.frame

 # tablaPasajero

tablaPasajero %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/tablaPasajero.csv", row.names = F)


tablaConductor <- conductor %>%
  .[, .(mediaDuracion = mean(diasDiferencia)), by=city_name] %>% 
  .[, mediaDuracion   := round(mediaDuracion, 2)] %>% 
  .[order(mediaDuracion, decreasing = T)] %>% 
  data.frame

# tablaConductor


tablaConductor %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/tablaConductor.csv", row.names = F)



row.names(tablaPasajero)  <- NULL
row.names(tablaConductor) <- NULL


pasajerosInactivos %>% 
  .[, .(mediaPrecio = mean(taximeter_price, na.rm = T)), by=city_name] %>% 
  .[order(mediaPrecio, decreasing = T)]


pasajerosInactivos %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/pasajerosInactivos_mediaprecio.csv", row.names = F)


conductoresInactivos %>% 
  .[, .(mediaPrecio = mean(taximeter_price, na.rm = T)), by=city_name] %>% 
  .[order(mediaPrecio, decreasing = T)]


conductoresInactivos %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/conductoresInactivos_mediaprecio.csv", row.names = F)



sumaPasajeros  <- pasajerosInactivos %>% 
  .[, .(suma = sum(taximeter_price, na.rm=T),
        viajes= .N), by=list(passengerID)] 

  # sumaPasajeros

sumaPasajeros %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/sumaPasajeros.csv", row.names = F)



sumaConductores <- conductoresInactivos %>%  
  .[, ganancia := ifelse(request_type=="Regular", taximeter_price*0.88,
                         taximeter_price*0.8)] %>% 
  .[, .(suma = sum(ganancia, na.rm=T),
        viajes= .N), by=list(driverID)] 

 # sumaConductores


sumaConductores %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/sumaConductores.csv", row.names = F)



sumaConductores <- merge(sumaConductores, conductor, by="driverID") 

# sumaConductores

sumaConductores %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/sumaConductores_merge.csv", row.names = F)



sumaPasajeros <- merge(sumaPasajeros, pasajero, by="passengerID") 

# sumaPasajeros

sumaPasajeros %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/sumaPasajeros_merge.csv", row.names = F)



######################### Principal Component Analysis ######
# Utilizar Datos de la siguiente tabla que contiene el campo de "Ciudad"
# PasajerosPCACiudad


# Encabezado de lso datos
head(PasajerosPCA)

# Promedio de las variables
apply(X = PasajerosPCA, MARGIN = 2, FUN = mean)

# Varianza de las variables
apply(X = PasajerosPCA, MARGIN = 2, FUN = var)

# Centrar y estandarizar variables
pca <- prcomp(PasajerosPCA, scale = TRUE)

pca


names(pca)

# Media de las variables previa a la estandarización
pca$center

# Desviación típica de las variables
pca$scale

# Valor de los loadings para cada componente (eigenvector)
pca$rotation


# Calculo de los componentes principales para cada observación
head(pca$x)


# Dimensión de la matriz "X"
dim(pca$x)


# Representación bidimensional de las 2 primeras componentes
biplot(x = pca, scale = 0, cex = 0.6, col = c("blue4", "brown3"))
png(filename = "PCAgrafico1.png")
dev.off()


# Se invierte el signo de los loadings y de los PCAs
pca$rotation <- -pca$rotation
pca$x        <- -pca$x
biplot(x = pca, scale = 0, cex = 0.6, col = c("blue4", "brown3"))




# Varianza explicada por cada una de ellas, la proporción respecto al total y 
# la proporción de varianza acumulada
library(ggplot2)

pca$sdev^2


#
prop_varianza <- pca$sdev^2 / sum(pca$sdev^2)
prop_varianza

# Gráfico de la componente principal vs varianza proporcional explicada
ggplot(data = data.frame(prop_varianza, pc = 1:7),
       aes(x = pc, y = prop_varianza)) +
  geom_col(width = 0.3) +
  scale_y_continuous(limits = c(0,1)) +
  theme_bw() +
  labs(x = "Componente principal",
       y = "Prop. de varianza explicada")




prop_varianza_acum <- cumsum(prop_varianza)
prop_varianza_acum


# Gráfico de varianza explicada acumulada vs PCAs
ggplot(data = data.frame(prop_varianza_acum, pc = 1:7),
       aes(x = pc, y = prop_varianza_acum, group = 1)) +
  geom_point() +
  geom_line() +
  theme_bw() +
  labs(x = "Componente principal",
       y = "Prop. varianza explicada acumulada")


summary(PasajerosPCA)

boxplot(PasajerosPCA$unique_fullfillment)


boxplot(PasajerosPCA$unique_fullfillment, boxfill = "light gray", outpch = 21:25, outlty = 2,
        bg = "pink", lwd = 2,
        medcol = "dark blue", medcex = 2, medpch = 20)


require(ggplot2)
ggplot(data = PasajerosPCA, aes(x=variable, y=value)) + geom_boxplot(aes(fill=Label))

### End PCA

# Analizar cuáles son las componentes que explican el mayor porcentaje 
# de varianza observada en los datos.



tablaPasajerosSuma <- sumaPasajeros %>% 
  .[, .(promedioGasto = mean(suma), minimo = min(suma),
        maximo = max(suma)), by=year(primerViaje)]

 # tablaPasajerosSuma

tablaPasajerosSuma %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/tablaPasajerosSuma_gastos.csv", row.names = F)



names(tablaPasajerosSuma) <- c("año", "GastoPromedioTotal", "Gasto_mínimo",
                               "Gasto_máximo")

 # names(tablaPasajerosSuma)


tablaConductoresSuma <- sumaConductores %>% 
  .[, .(promedioGasto = mean(suma), minimo = min(suma),
        maximo = max(suma)), by=year(primerViaje)]


 # tablaConductoresSuma


tablaConductoresSuma %>% 
  write.csv("C:/Users/Francisco Roman/Documents/analisis_supervivencia_david/tablaConductoresSuma_gastos.csv", row.names = F)

names(tablaConductoresSuma) <- c("año", "GastoPromedioTotal", "Gasto_mínimo",
                                 "Gasto_máximo")


# names(tablaConductoresSuma)



## Análisis de supervivencia


## Cuando se van los pasajeros
supervivenciaDiaria <- survfit(Surv(diasDiferencia, vigente)~anio,
                               data= PasajerosG)

  # supervivenciaDiaria


GGally::ggsurv(supervivenciaDiaria, 
               ylab = "Supervivencia", xlab="Días", cens.size = 0)+
  scale_x_continuous(limits = c(0, 700), breaks = seq(0, 700, 50))+
  scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, 0.1))+
  theme_classic()+
  scale_color_manual(values = c("steelblue", "darkred"), name="")+
  scale_linetype_manual(values= 1:2, name="")

modelos <- c("extreme", "t", "gaussian")

  # modelos

minAkaike <- lapply(modelos, function(modelo){
  supervivencia <- Surv(PasajerosG$diasDiferencia, PasajerosG$vigente)
  y <- survreg(supervivencia~as.factor(anio), dist=modelo, data= PasajerosG)
  akaike <- extractAIC(y)[2]
  print(c(modelo, akaike))
  return(akaike)
})


   # minAkaike

tipo <- modelos[which.min(minAkaike)]

 # tipo

analisis <- survreg(Surv(PasajerosG$diasDiferencia, 
                         PasajerosG$vigente)~anio, dist = tipo, 
                    data=PasajerosG) 

 # analisis

test <- summary(analisis)


#  test


survreg(Surv(PasajerosG$diasDiferencia, 
             PasajerosG$vigente)~anio, dist = tipo, 
        data=PasajerosG) %>%  anova(test="Chisq") 




## Cuando se van los conductores
supervivenciaDiariaConductor <- survfit(Surv(diasDiferencia, vigente)~anio,
                                        data= ConductorG)

  # supervivenciaDiariaConductor


GGally::ggsurv(supervivenciaDiariaConductor, 
               ylab = "Supervivencia", xlab="Días", cens.size = 0) +
  scale_x_continuous(limits = c(0, 700), breaks = seq(0, 700, 50))+
  scale_y_continuous(limits = c(0, 1), breaks = seq(0, 1, 0.1))+
  theme_classic()+
  scale_color_manual(values = c("steelblue", "darkred"), name="")+
  scale_linetype_manual(values= 1:2, name="")

modelos <- c("extreme", "t", "gaussian")

 # modelos

minAkaike <- lapply(modelos, function(modelo){
  supervivencia <- Surv(ConductorG$diasDiferencia, ConductorG$vigente)
  y <- survreg(supervivencia~as.factor(anio), dist=modelo, data= ConductorG)
  akaike <- extractAIC(y)[2]
  print(c(modelo, akaike))
  return(akaike)
})


 # minAkaike
tipo <- modelos[which.min(minAkaike)]

 # tipo


survreg(Surv(ConductorG$diasDiferencia, 
             ConductorG$vigente)~anio, dist = tipo, 
        data=ConductorG) %>%  anova(test="Chisq") 

survreg(Surv(ConductorG$diasDiferencia, 
             ConductorG$vigente)~anio, dist = tipo, 
        data=ConductorG) %>%  summary
