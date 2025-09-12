# Verificar e instalar pacotes necessários
required_packages <- c("plumber", 
                        "SPEI", 
                        "jsonlite", 
                        "lubridate")

install_if_missing <- function(packages) {
  for (pkg in packages) {
    if (!require(pkg, character.only = TRUE)) {
      cat(paste("Instalando pacote:", pkg, "\n"))
      install.packages(pkg, dependencies = TRUE)
      library(pkg, character.only = TRUE)
    }
  }
}

install_if_missing(required_packages)

library(plumber)
library(SPEI)
library(jsonlite)
library(lubridate)


#* Endpoint Calcular o SPEI
#* @post /spei
#* @param tmin_data 
#* @param tmax_data
#* @param pr_data
#* @param lat
#* @param scales
function(req, res, tmin_data, tmax_data, pr_data, lat, scales) {
    
    # Calcular o PET mensal usando a média mensal de TMIN e TMAX
    PET_mensal <- hargreaves(Tmin= tmin_data, 
                            Tmax= tmax_data, 
                            lat= lat,
                            verbose = FALSE)

    # Calcular a precipitação líquida mensal (PREC - PET)
    NET_prec_mensal <- pr_data - PET_mensal

    kernel = list(type = "rectangular", shift=0)
    distribution = "log-Logistic"

    # Inicializa a lista para resultados
    spei_results <- list()

    # Calcular o SPEI nas séries mensais (scales parametro)
    # Itera sobre as escalas
    for (scale in scales) {
        spei_mensal <- spei(
            NET_prec_mensal,
            scale = scale,
            kernel = kernel,
            distribution = distribution,
            na.rm = TRUE,
            verbose = FALSE
        )
        # Adicionar ao resultado
        spei_results[[paste0("SPEI_", scale)]] <- as.numeric(spei_mensal$fitted)
    }

    return(spei_results)
}

#* @get /status
function(req, res) {
  list("status" = "OK")
}

# Função para iniciar o servidor
start_server <- function(host = "0.0.0.0", port = 8000) {
  cat("Iniciando API do SPEI...\n")
  cat(paste("Servidor disponível em: http://", host, ":", port, "\n", sep = ""))
  
  # Criar e executar a API
  # coloque o path absoluto do arquivo api.r no seu computador local
  api <- plumb("api.r")
  api$run(host = host, port = port, docs=FALSE)
}
