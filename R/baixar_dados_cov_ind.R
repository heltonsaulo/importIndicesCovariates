#' Baixar e consolidar séries temporais econômicas
#'
#' Esta função baixa e organiza dados de diversas fontes públicas.
#'
#' @param start_date Data inicial no formato "YYYY-MM-DD"
#' @param end_date Data final no formato "YYYY-MM-DD"
#' @return Uma lista com os objetos `responses_ts` e `covariates_ts`
#' @export


baixar_dados_cov_ind <- function(start_date = "2011-01-01", end_date = Sys.Date()){

################################################################################
## import_abcr
################################################################################

import_abcr_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(httr)
  library(rvest)
  library(readxl)
  library(dplyr)
  library(xml2)
  library(lubridate)

# Dados ABCR ------------------------------------------------------------------

# sCRAPING
url <- "https://melhoresrodovias.org.br/indice-abcr/#"
page <- read_html(url)
xpath <- '//*[@id="content"]/div/div/section[5]/div/div/div/div[1]/div/div/a'
link_node <- html_node(page, xpath = xpath)
file_url <- html_attr(link_node, "href")
file_url <- url_absolute(file_url, base = url)
destfile <- "abcr.xlsx"
download.file(file_url, destfile = destfile, mode = "wb")

start_date_l6 <- as.Date(start_date) %m-% months(6)


## ABCR Original ----------------------------------------------------------------
# Ler os dados corretamente
dados_abcr <- read_excel(destfile, sheet = "(C) Original", skip = 2)


# Selecionar a primeira e terceira colunas
dados_abcr_original <- dados_abcr[, c(1, 2, 3)]

# Converter a primeira coluna para o formato "YYYY-MM-DD"
dados_abcr_original[[1]] <- as.Date(dados_abcr_original[[1]])

# Renomear colunas e filtrar
colnames(dados_abcr_original) <- c("Data", "ABCR_LEVE", "ABCR_PESADOS")

dados_abcr_original <- dados_abcr_original %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))


## ABCR - Defasado -------------------------------------------------------

dados_abcr_defasado <- dados_abcr[, c(1, 2, 3)]

# Renomear colunas e filtrar
colnames(dados_abcr_defasado) <- c("Data", "ABCR_LEVE_L6", "ABCR_PESADOS_L6")

#Filtrando
dados_abcr_defasado <- dados_abcr_defasado %>%
  filter(Data >= as.Date(start_date_l6) & Data <= as.Date(end_date))

#Corrigindo datas
dados_abcr_defasado$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(dados_abcr_defasado),
  by = "1 month")

#COnsolidação
abcr_consolidado <- full_join(dados_abcr_original, dados_abcr_defasado, by ="Data")

return(abcr_consolidado)
}


################################################################################
## import_bcb_data
################################################################################

## quebrar em 10 anos
get_bcb_series_chunked <- function(id, start_date, end_date, format.data = "wide") {
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  # Gera sequências de datas com no máximo 10 anos (3652 dias)
  date_seq <- seq(from = start_date, to = end_date, by = "10 years")
  if (tail(date_seq, 1) < end_date) {
    date_seq <- c(date_seq, end_date + 1)
  }

  # Baixa e empilha os dados por faixa de 10 anos
  all_data <- purrr::map2_df(date_seq[-length(date_seq)], date_seq[-1] - 1, ~ {
    GetBCBData::gbcbd_get_series(
      id = id,
      first.date = .x,
      last.date = .y,
      format.data = format.data
    )
  })

  return(all_data)
}


import_bcb_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(GetBCBData)
  library(dplyr)
  library(lubridate)

  # Dados principais do Bacen
  dados_bacen <- GetBCBData::gbcbd_get_series(
    id = c(
      "IPCA_serv" = 10844, "IPCA_EX1" = 16121, "IPCA_MA_sem_suavização" = 11426,
      "IPCA_MA_suavizado" = 4466, "IPCA_EX0" = 11427, "IC_BR_agro" = 29041,
      "IBCR-CO" = 25381,"ibcr_go" = 25383, "IBC-BR" = 24363, "IC_BR_Metal_US$" = 29040,
      "IC_BR_energia_US$" = 29039, "IE" = 17662, "IEF" = 4395, "ICS" = 17660,
      "ICC" = 4393, "ICEA" = 4394, "Emprego_Formal_GO" = 21987,
      "Emprego_Formal" = 28763, "Emprego_Agro" = 28764, "Emprego_Ind_Ext" = 28765,
      "Exportação de bens - GO" = 13030,"Importação de bens - GO" = 13031,
      "Saldo da balança comercial - GO" = 13061,
      "Saldo das operações de crédito - GO - Pessoas físicas" = 14010,
      "Saldo das operações de crédito - GO - Pessoas jurídicas" = 14037,
      "Saldo das operações de crédito - GO - Total" = 14064,
      "Inadimplência das operações de crédito - GO - Pessoas físicas" = 15869,
      "Inadimplência das operações de crédito - GO - Pessoas jurídicas" = 15901,
      "Inadimplência das operações de crédito - GO - Total" = 15933
    ),
    first.date = as.Date(start_date),
    last.date = as.Date(end_date),
    format.data = "wide"
  ) %>% rename(Data = ref.date)

  # IC_BR_agro L12
  IC_BR_agro_L12 <- GetBCBData::gbcbd_get_series(
    id = c("IC_BR_agro_L12" = 29041),
    first.date = as.Date(start_date) - months(12),
    last.date = as.Date(end_date),
    format.data = "wide"
  ) %>%
    mutate(Data = seq.Date(from = as_date(start_date), length.out = nrow(.), by = "1 month")) %>%
    select(-1)

  # IC_BR_Metal L12
  IC_BR_Metal_L12 <- GetBCBData::gbcbd_get_series(
    id = c("IC_BR_Metal_L12" = 29040),
    first.date = as.Date(start_date) - months(12),
    last.date = as.Date(end_date),
    format.data = "wide"
  ) %>%
    mutate(Data = seq.Date(from = as_date(start_date), length.out = nrow(.), by = "1 month")) %>%
    select(-1)

  # SELIC
  # SELIC <- GetBCBData::gbcbd_get_series(
  #   id = c("SELIC" = 432),
  #   first.date = as.Date(start_date),
  #   last.date = as.Date(end_date),
  #   format.data = "wide"
  # ) %>%
  #   rename(Data = ref.date) %>%
  #   mutate(Data = as.Date(Data)) %>%
  #   group_by(ano_mes = format(Data, "%Y-%m")) %>%
  #   slice_max(Data) %>%
  #   ungroup() %>%
  #   select(-ano_mes) %>%
  #   mutate(Data = seq.Date(from = as_date(start_date), length.out = nrow(.), by = "1 month"))


  SELIC <- get_bcb_series_chunked(
  id = c("SELIC" = 432),
  start_date = as.Date(start_date),
  end_date = as.Date(end_date)
) %>%
  rename(Data = ref.date) %>%
  mutate(Data = as.Date(Data)) %>%
  group_by(ano_mes = format(Data, "%Y-%m")) %>%
  slice_max(Data) %>%
  ungroup() %>%
  select(-ano_mes) %>%
  mutate(Data = seq.Date(from = as_date(start_date), length.out = nrow(.), by = "1 month"))




  # Selic LA12
  # Selic_LA12 <- GetBCBData::gbcbd_get_series(
  #   id = c("Selic_LA12" = 432),
  #   first.date = as.Date(as.Date(start_date) - months(12)),
  #   last.date =  as.Date(end_date),
  #   format.data = "wide"
  # ) %>%

  Selic_LA12 <-  get_bcb_series_chunked(
  id = c("Selic_LA12" = 432),
  start_date = as.Date(as.Date(start_date) - months(12)),
  end_date = as.Date(end_date)
   ) %>%
    rename(Data = ref.date) %>%
    mutate(Data = as.Date(Data)) %>%
    group_by(ano_mes = format(Data, "%Y-%m")) %>%
    slice_max(Data) %>%
    ungroup() %>%
    select(-ano_mes) %>%
    mutate(Data = seq.Date(from = as_date(start_date), length.out = nrow(.), by = "1 month"))

  # Dólar médio mensal
  # Dolar_BRL <- GetBCBData::gbcbd_get_series(
  #   id = c("Dolar_BRL" = 1),
  #   first.date = as.Date(start_date),
  #   last.date = as.Date(end_date),
  #   format.data = "wide"
  # ) %>%
  Dolar_BRL <-  get_bcb_series_chunked(
  id = c("Dolar_BRL" = 1),
  start_date = as.Date(start_date),
  end_date = as.Date(end_date)
   ) %>%
    rename(Data = ref.date) %>%
    mutate(Data = format(Data, "%Y-%m")) %>%
    group_by(Data) %>%
    summarise(Dolar_BRL = mean(Dolar_BRL, na.rm = TRUE)) %>%
    ungroup() %>%
    mutate(Data = as.Date(paste0(Data, "-01")))

  # Join final de todos os datasets
  merged_data <- dados_bacen %>%
    full_join(IC_BR_agro_L12, by = "Data") %>%
    full_join(IC_BR_Metal_L12, by = "Data") %>%
    full_join(SELIC, by = "Data") %>%
    full_join(Selic_LA12, by = "Data") %>%
    full_join(Dolar_BRL, by = "Data") %>%
    arrange(Data)

  return(merged_data)
}


################################################################################
## import_commodities_data
################################################################################


import_commodities_data <- function(start_date = "2011-01-01", end_date = Sys.time()) {

  library(dplyr)
  library(lubridate)
  library(yahoofinancer)

  process_commodity <- function(ticker_symbol,name, start_date) {
  # Initialize the commodity object
  commodity <- Ticker$new(ticker_symbol)

  # Get historical data
  data <- commodity$get_history(start = as.Date(start_date), interval = '1d')

  # Filter out rows with invalid or missing adjusted close prices
  data <- dplyr::filter(data, adj_close != "NULL") # Adjust if NULL values are actual strings
  data$adj_close <- as.numeric(data$adj_close)

  if (any(is.na(data$adj_close))) {
    warning("NA values detected in adj_close. They will be removed.")
    data <- dplyr::filter(data, !is.na(adj_close))
  }

  # Ensure date column is in Date format
  if (!inherits(data$date, "Date")) {
    data$date <- as.Date(data$date)
  }

  # Add month and year columns
  data$mes <- lubridate::month(data$date)
  data$ano <- lubridate::year(data$date)

# Aggregate by month and year
aggregated_data <- data %>%
    dplyr::group_by(mes, ano) %>%
    dplyr::summarize(close = mean(adj_close, na.rm = TRUE), .groups = "drop")

colnames(aggregated_data) <- c("mes","ano",paste0("price_",name))

  return(aggregated_data)
}

# Process each commodity
soy <- process_commodity(ticker_symbol="ZS=F", name = "soy", start_date = start_date)
boi <- process_commodity(ticker_symbol="LE=F", name = "boi", start_date = start_date)
milho <- process_commodity(ticker_symbol="ZC=F", name = "milho", start_date = start_date)
acucar <- process_commodity(ticker_symbol="SB=F", name = "acucar", start_date = start_date)
oil <- process_commodity(ticker_symbol = "BZ=F", name = "oil", start_date = start_date)


# Juntar os dataframes soy, boi, milho, acucar e oil com base em mes e ano
commodities_combined <- full_join(soy, boi, by = c("mes","ano")) %>%
                        full_join(milho, by = c("mes","ano")) %>%
                        full_join(acucar, by = c("mes","ano")) %>%
                        full_join(oil, by = c("mes", "ano"))

commodities_combined$Data <- as.Date(paste(commodities_combined$mes,
                                           commodities_combined$ano,
                                           "01", sep = "/"), format = "%m/%Y/%d")
commodities_combined$mes <- NULL
commodities_combined$ano <- NULL

## Nomes dad colunas
colnames(commodities_combined) <- c("Soja_futuro","Boi_vivo_f","Milho_f","Açucar_f","Petroleo", "Data")

commodities_combined <- commodities_combined %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))

## Defasando "Soja_futuro","Boi_vivo_f","Milho_f","Açucar_f" ----------------------

process_commodity_lag <- function(ticker_symbol,name,start_date) {
  # Initialize the commodity object
  commodity <- Ticker$new(ticker_symbol)

  # Get historical data
  data <- commodity$get_history(start = as.Date(start_date), interval ='1d')

  # Filter out rows with invalid or missing adjusted close prices
  data <- dplyr::filter(data, adj_close != "NULL") # Adjust if NULL values are actual strings
  data$adj_close <- as.numeric(data$adj_close)

  if (any(is.na(data$adj_close))) {
    warning("NA values detected in adj_close. They will be removed.")
    data <- dplyr::filter(data, !is.na(adj_close))
  }

  # Ensure date column is in Date format
  if (!inherits(data$date, "Date")) {
    data$date <- as.Date(data$date)
  }

  # Add month and year columns
  data$mes <- lubridate::month(data$date)
  data$ano <- lubridate::year(data$date)

  # Aggregate by month and year
  aggregated_data <- data %>%
    dplyr::group_by(mes, ano) %>%
    dplyr::summarize(close = mean(adj_close, na.rm = TRUE), .groups = "drop")

  colnames(aggregated_data) <- c("mes","ano",paste0("price_",name))

  return(aggregated_data)
}

 # Criar defasagem (L12)
start_date_lag <- as.Date(start_date) %m-% months(12)

# Process each commodity
soy_lag <- process_commodity_lag(ticker_symbol="ZS=F", name="soy", start_date = start_date_lag)
boi_lag <- process_commodity_lag(ticker_symbol="LE=F", name="boi", start_date = start_date_lag)
milho_lag <- process_commodity_lag(ticker_symbol="ZC=F", name="milho", start_date = start_date_lag)
acucar_lag <- process_commodity_lag(ticker_symbol="SB=F", name="acucar", start_date = start_date_lag)



# Juntar os dataframes soy, boi, milho e acucar com base em mes e ano
commodities_combined_L12 <-
  full_join(soy_lag, boi_lag, by =c("mes","ano")) %>%
  full_join(milho_lag, by = c("mes","ano")) %>%
  full_join(acucar_lag, by = c("mes","ano"))

commodities_combined_L12$Data <- as.Date(paste(commodities_combined_L12$mes,
                                           commodities_combined_L12$ano,
                                           "01", sep = "/"), format = "%m/%Y/%d")
commodities_combined_L12$mes <- NULL
commodities_combined_L12$ano <- NULL


## Nomes da colunas
colnames(commodities_combined_L12) <- c("Soja_futuro_L12","Boi_vivo_f_L12","Milho_f_L12","Açucar_f_L12", "Data" )

# Ordenando por Data
commodities_combined_L12 <- commodities_combined_L12 %>%
  arrange(Data) %>%
  filter(Data >= as.Date(start_date_lag) & Data <= as.Date(end_date))

# Defasando as variáveis
commodities_combined_L12$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(commodities_combined_L12),
  by = "1 month"
)

## Combinando os dados originais e defasados
final_commodities <-
  full_join(commodities_combined, commodities_combined_L12, by="Data")

# Reordenar colunas: colocar "Data" na primeira posição
final_commodities <- final_commodities %>%
  dplyr::select(Data, everything()) %>%
  arrange(Data)

  return(final_commodities)
}


################################################################################
##
################################################################################

import_consumo_energia_go_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(httr)
  library(rvest)
  library(readxl)
  library(dplyr)
  library(xml2)

  # Testando outra forma de Scraping
url <- "https://www.epe.gov.br/pt/publicacoes-dados-abertos/dados-abertos/dados-do-consumo-mensal-de-energia-eletrica"
response <- GET(url)
page <- content(response, "text") %>%
    read_html()
download_link <- page %>%
    html_nodes("a[href$='.xlsx']") %>%
    html_attr("href")
file_url <- url_absolute(download_link[1], base = url)
destfile <- "Dados_abertos_Consumo_Mensal.xlsx"
download.file(file_url, destfile = destfile, mode = "wb")

# Ler a planilha específica
dados_energia <- read_excel(destfile, sheet = "CONSUMO E NUMCONS SAM UF")

# Filtrar os dados para UF == "GO"
dados_energia_filtrados <- subset(dados_energia, UF == "GO")

# Convertar para data
dados_energia_filtrados <- dados_energia_filtrados %>%
  mutate(Data = as.Date(DataExcel, origin = "2004-01-01")) # Para datas no formato serial Excel

# Somar o consumo por mês (considerando Dataexcel como data)
resultado_consumo <- dados_energia_filtrados %>%
  group_by(Data) %>%
  summarise(energia_consumo = sum(Consumo, na.rm = TRUE)) %>%
    filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))


  # 7. Retornar o resultado final
  return(resultado_consumo)
}




################################################################################
## import_ibge_data
################################################################################

import_ibge_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(sidrar)
  library(dplyr)


# Função reutilizável para importar dados SIDRA com coluna Data padronizada
import_sidra_ts <- function(table, variable, period, classific = NULL, category = NULL,
                             geo = "State", geo.filter = NULL) {
  df <- sidrar::get_sidra(x = table,
                  variable = variable,
                  period = period,
                  classific = classific,
                  category = category,
                  geo = geo,
                  geo.filter = geo.filter)

  start_date_parent <- get("start_date", envir = parent.frame())

  df$Data <- seq.Date(from = as.Date(start_date_parent),
                      length.out = nrow(df),
                      by = "1 month")
  return(df)
}


period <- paste0(format(as.Date(start_date), "%Y%m"), "-", format(as.Date(end_date), "%Y%m"))

start_date_l12 <- as.Date(start_date) %m-% months(12)

period_l12 <- paste0(format(start_date_l12, "%Y%m"), "-", format(as.Date(end_date), "%Y%m"))

start_date_f <- paste0(format(as.Date(start_date), "%Y%m"))

# === VARIÁVEIS DO PIPELINE SIDRA ===
algodao_lspa_go <- import_sidra_ts(6588, 35, period, "c48", list(39429), geo = "State", geo.filter = list("State" = 52))

feijao_lspa_go1 <- import_sidra_ts(6588, 35, period, "c48", list(39436), geo = "State", geo.filter = list("State" = 52))
feijao_lspa_go2 <- import_sidra_ts(6588, 35, period, "c48", list(39437), geo = "State", geo.filter = list("State" = 52))
feijao_lspa_go3 <- import_sidra_ts(6588, 35, period, "c48", list(39438), geo = "State", geo.filter = list("State" = 52))

feijao_lspa_go <- feijao_lspa_go1 %>% select(Data, Valor) %>% rename(Valor1 = Valor) %>%
  left_join(feijao_lspa_go2 %>% select(Data, Valor) %>% rename(Valor2 = Valor), by = "Data") %>%
  left_join(feijao_lspa_go3 %>% select(Data, Valor) %>% rename(Valor3 = Valor), by = "Data") %>%
  mutate(Valor = Valor1 + Valor2 + Valor3) %>% select(Data, Valor)

milho_lspa_go1 <- import_sidra_ts(6588, 35, period, "c48", list(39441), geo = "State", geo.filter = list("State" = 52))
milho_lspa_go2 <- import_sidra_ts(6588, 35, period, "c48", list(39442), geo = "State", geo.filter = list("State" = 52))

milho_lspa_go <- milho_lspa_go1 %>% select(Data, Valor) %>% rename(Valor1 = Valor) %>%
  left_join(milho_lspa_go2 %>% select(Data, Valor) %>% rename(Valor2 = Valor), by = "Data") %>%
  mutate(Valor = Valor1 + Valor2) %>% select(Data, Valor)

soja_lspa_go <- import_sidra_ts(6588, 35, period, "c48", list(39443), geo = "State", geo.filter = list("State" = 52))
sorgo_lspa_go <- import_sidra_ts(6588, 35, period, "c48", list(39444), geo = "State", geo.filter = list("State" = 52))
cana_lspa_go <- import_sidra_ts(6588, 35, period, "c48", list(39456), geo = "State", geo.filter = list("State" = 52))
tomate_lspa_go <- import_sidra_ts(6588, 35, period, "c48", list(39470), geo = "State", geo.filter = list("State" = 52))

pim_ind_geral_br <- import_sidra_ts(8888, 12606, period, "C544", list(129314), geo = "Brazil")
pim_ind_geral_go <- import_sidra_ts(8888, 12606, period, "C544", list(129314), geo = "State", geo.filter = list("State" = 52))
PIM_Ind_trans <- import_sidra_ts(8888, 12606, period, "C544", list(129316), geo = "Brazil")
PIM_ind_trans_go <- import_sidra_ts(8888, 12606, period, "C544", list(129316), geo = "State", geo.filter = list("State" = 52))

Fabricação_de_máquinas_aparelhos_materiais_elétricos <- import_sidra_ts(8888, 12606, period, "C544", list(129336), geo = "Brazil")
Fabricação_de_máquinas_equipamentos <- import_sidra_ts(8888, 12606, period, "C544", list(129337), geo = "Brazil")

Capital_pf <- import_sidra_ts(8887, 12606, period, "C543", list(129278), geo = "Brazil")
Capital_pf_L12 <- import_sidra_ts(8887, 12606, period_l12, "C543", list(129278), geo = "Brazil")

pms_servicos <- import_sidra_ts(5906, 7167, period, "C11046", list(56726), geo = "State", geo.filter = list("State" = 52))
#pms_serviços_A <- import_sidra_ts(5906, 7168, period, "C11046", list(56726), geo = "State", geo.filter = list("State" = 52))

PMC <- import_sidra_ts(8880, 7169, period, "C11046", list(56734), geo = "State", geo.filter = list("State" = 52))
pmc_varejo_amp <- import_sidra_ts(8881, 7169, period, "C11046", list(56736), geo = "State", geo.filter = list("State" = 52))

IPCA_acum_12_br <- import_sidra_ts(1737, 2265, period, geo = "Brazil")
IPCA <- import_sidra_ts(1737, 63, period, geo = "Brazil")

abate_bovino <- get_sidra(1092, variable = 284, period = "all", geo = "State", geo.filter = list("State" = 52), classific = c("C12716"), category = "all") %>%
  filter(!`Referência temporal` == "Total do trimestre") %>% group_by(`Trimestre (Código)`) %>% ungroup()
abate_bovino$Data <- seq.Date(from = as.Date(start_date), length.out = nrow(abate_bovino), by = "1 month")

abate_bovino <- abate_bovino %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))

abate_aves <- get_sidra(1094, variable = 284, period = "all", geo = "State", geo.filter = list("State" = 52), classific = c("C12716"), category = "all") %>%
  filter(!`Referência temporal` == "Total do trimestre") %>% group_by(`Trimestre (Código)`) %>% ungroup()
abate_aves$Data <- seq.Date(from = as.Date(start_date), length.out = nrow(abate_aves), by = "1 month")

abate_aves <- abate_aves %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))

# === MERGE DE TODAS AS VARIÁVEIS POR DATA ===
data_ibge <- algodao_lspa_go %>%
  select(Data, Valor) %>%
  rename(algodao_lspa_go = Valor) %>%
  full_join(feijao_lspa_go %>% select(Data, Valor) %>% rename(feijao_lspa_go = Valor), by = "Data") %>%
  full_join(milho_lspa_go %>% select(Data, Valor) %>% rename(milho_lspa_go = Valor), by = "Data") %>%
  full_join(soja_lspa_go %>% select(Data, Valor) %>% rename(soja_lspa_go = Valor), by = "Data") %>%
  full_join(sorgo_lspa_go %>% select(Data, Valor) %>% rename(sorgo_lspa_go = Valor), by = "Data") %>%
  full_join(cana_lspa_go %>% select(Data, Valor) %>% rename(cana_lspa_go = Valor), by = "Data") %>%
  full_join(tomate_lspa_go %>% select(Data, Valor) %>% rename(tomate_lspa_go = Valor), by = "Data") %>%
  full_join(pim_ind_geral_br %>% select(Data, Valor) %>% rename(pim_ind_geral_br = Valor), by = "Data") %>%
  full_join(pim_ind_geral_go %>% select(Data, Valor) %>% rename(pim_ind_geral_go = Valor), by = "Data") %>%
  full_join(PIM_Ind_trans %>% select(Data, Valor) %>% rename(PIM_Ind_trans = Valor), by = "Data") %>%
  full_join(PIM_ind_trans_go %>% select(Data, Valor) %>% rename(PIM_ind_trans_go = Valor), by = "Data") %>%
  full_join(Fabricação_de_máquinas_aparelhos_materiais_elétricos %>% select(Data, Valor) %>% rename(Fabricação_de_máquinas_aparelhos_materiais_elétricos = Valor), by = "Data") %>%
  full_join(Fabricação_de_máquinas_equipamentos %>% select(Data, Valor) %>% rename(Fabricação_de_máquinas_equipamentos = Valor), by = "Data") %>%
  full_join(Capital_pf %>% select(Data, Valor) %>% rename(Capital_pf = Valor), by = "Data") %>%
  full_join(Capital_pf_L12 %>% select(Data, Valor) %>% rename(Capital_pf_L12 = Valor), by = "Data") %>%
  full_join(pms_servicos %>% select(Data, Valor) %>% rename(pms_servicos = Valor), by = "Data") %>%
  # full_join(pms_serviços %>% select(Data, Valor) %>% rename(pms_serviços_A = Valor), by = "Data") %>%
  full_join(PMC %>% select(Data, Valor) %>% rename(PMC_A = Valor), by = "Data") %>%
  full_join(pmc_varejo_amp %>% select(Data, Valor) %>% rename(pmc_varejo_amp = Valor), by = "Data") %>%
  full_join(IPCA_acum_12_br %>% select(Data, Valor) %>% rename(IPCA_acum_12_br = Valor), by = "Data") %>%
  full_join(IPCA %>% select(Data, Valor) %>% rename(IPCA = Valor), by = "Data") %>%
  full_join(abate_bovino %>% select(Data, Valor) %>% rename(abate_bovino = Valor), by = "Data") %>%
  full_join(abate_aves %>% select(Data, Valor) %>% rename(abate_aves = Valor), by = "Data")

  return(data_ibge)
}




################################################################################
##
################################################################################

import_comex_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(httr)
  library(rvest)
  library(readxl)
  library(dplyr)
  library(xml2)
  library(data.table)
  library(lubridate)


## Scraping da Base de Importação ---------------------------------------------
url <- "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
page <- read_html(url)
xpath <- '/html/body/div[2]/div[1]/main/div[2]/div/div[5]/div/table[2]/tbody/tr[4]/td[1]/strong/a'
link_node <- html_node(page, xpath = xpath)
file_url <- html_attr(link_node, "href")
file_url <- url_absolute(file_url, base = url)
destfile <- "Importacao_Maquinas.csv"
  options(timeout = 500)
  download.file(file_url, destfile = destfile, mode = "wb")

#Data para criar lag
start_date_l12 <- as.Date(start_date) %m-% months(12)

# Usando fread para ler o arquivo CSV
importacao_data <- fread("Importacao_Maquinas.csv", sep = ";")

# Filtrando a Base para Goiás
importacao_data <- importacao_data %>%
  filter(SG_UF_NCM == "GO", CO_ANO >= format(as.Date(start_date_l12), "%Y") & CO_ANO <= format(as.Date(end_date))) %>%
  # Transformando a coluna CO_NCM em caracter para permitir o left_join mais a frente
  mutate(CO_NCM = as.character(CO_NCM)) %>%
  select(CO_ANO, CO_MES, CO_NCM,VL_FOB)


# Puxando a tabela de tradução NCM -> CUCI/CO_SH4
url <- "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
page <- read_html(url)
xpath <- '//*[@id="parent-fieldname-text"]/p[12]/strong/a'
link_node <- html_node(page, xpath = xpath)
file_url <- html_attr(link_node, "href")
file_url <- url_absolute(file_url, base = url)
destfile <- "Tabela_Tradução.xlsx"
options(timeout = 500)
download.file(file_url, destfile = destfile, mode = "wb")


## Importação Maquinas -------------------------------------------------------------------

# Lendo a planilha de tradução NCM -> CUCI
NCM_CUCI_MAQUINAS <- read_excel(destfile, sheet = "2")
NCM_CUCI_MAQUINAS <- NCM_CUCI_MAQUINAS [c (1,7)]

# Criando tabela base para calcular as bases normal e defasada
Importacao_Maquinas <- left_join(importacao_data, NCM_CUCI_MAQUINAS, by = "CO_NCM") %>%
  filter(CO_CUCI_GRUPO %in% c(695, 712, 713, 714, 716, 718, 721, 722, 724, 725,
                              726, 727, 728, 731, 733, 735, 737, 743, 744, 745,
                              746, 747, 748, 749, 751, 752, 759, 761, 771, 773,
                              776, 778, 782, 786, "793c")) %>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(VL_FOB = sum(VL_FOB, na.rm = TRUE), .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-"))) %>%
  select(Data, VL_FOB)

# Criando base com import_maquinas_go normal
Importacao_Maquinas_Soma <- Importacao_Maquinas %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  rename(Import_maquinas_go = VL_FOB)



### Maquinas Defasado ---------------------------------------------------------

# Cria base de máquinas com defasagem
Importacao_Maquinas_Soma_L12 <- Importacao_Maquinas %>%
  filter(Data >= as.Date(start_date_l12) & Data <= as.Date(end_date)) %>%
  rename(Import_maquinas_go_L12 = VL_FOB)

# Defasando
Importacao_Maquinas_Soma_L12$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(Importacao_Maquinas_Soma_L12),
  by = "1 month")


## Fertilizantes --------------------------------------------------------------

# Chamando a planilha para transformar NCM -> CO_SH4

NCM_COSH4_FERTILIZANTES <- read_excel(destfile, sheet = "1")
NCM_COSH4_FERTILIZANTES <- NCM_COSH4_FERTILIZANTES [c (1,7)]

# Criando tabela base para calcular as bases normal e defasada
Importacao_Fertilizantes <- left_join(importacao_data, NCM_COSH4_FERTILIZANTES, by = "CO_NCM") %>%
  filter(CO_SH4 %in% c(3101, 3102, 3103, 3104, 3105)) %>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(VL_FOB = sum(VL_FOB, na.rm = TRUE),  .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-"))) %>%
  select(Data, VL_FOB)

# Criando base com Importacao_Fertilizantes_soma normal
Importacao_Fertilizantes_soma <- Importacao_Fertilizantes %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  rename(Import_fertilizantes_go = VL_FOB)


### Fertilizantes Defasando ----------------------------------------------------

# Cria base de máquinas com defasagem
Importacao_Fertilizantes_Soma_L12 <- Importacao_Fertilizantes %>%
  filter(Data >= as.Date(start_date_l12) & Data <= as.Date(end_date)) %>%
  rename(Import_fertilizantes_go_L12 = VL_FOB)

# Defasando
Importacao_Fertilizantes_Soma_L12$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(Importacao_Fertilizantes_Soma_L12),
  by = "1 month")


# Juntar os resultados das máquinas e fertilizantes pela coluna Data
resultado_maquinas_fertilizantes <- full_join(Importacao_Maquinas_Soma, Importacao_Maquinas_Soma_L12, by = "Data") %>%
  full_join(Importacao_Fertilizantes_soma,by ="Data") %>%
  full_join(Importacao_Fertilizantes_Soma_L12, by="Data")

# Corrigir formatação das variaveis para numérico
resultado_maquinas_fertilizantes <- resultado_maquinas_fertilizantes %>%
  mutate(across(-Data, ~ as.numeric(.)))

## Limpar nao usado
rm(Importacao_Fertilizantes_Soma_L12, Importacao_Maquinas_Soma, Importacao_Maquinas_Soma_L12,
   importacao_data, NCM_COSH4_FERTILIZANTES, NCM_CUCI_MAQUINAS, link_node, page,
   url,xpath,Importacao_Fertilizantes,Importacao_Maquinas,
   Importacao_Fertilizantes_soma,destfile,file_url)

# Dados de Exportação - Soja e milho -------------------------------------------

url <- "https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta"
page <- read_html(url)
xpath <- '//*[@id="parent-fieldname-text"]/table[1]/tbody/tr[4]/td[1]/strong/a'
link_node <- html_node(page, xpath = xpath)
file_url <- html_attr(link_node, "href")
file_url <- url_absolute(file_url, base = url)
destfile <- "Exportação.csv"
  options(timeout = 500)
  download.file(file_url, destfile = destfile, mode = "wb")

# Usando fread para ler o arquivo CSV
Exportação_data <- fread("Exportação.csv", sep = ";", colClasses = c("KG_LIQUIDO" = "numeric"))

# Transformando a coluna CO_NCM em caracter para permitir o left_join mais a frente
Exportação_data <- Exportação_data %>%
  filter(CO_ANO >= format(as.Date(start_date_l12), "%Y") & CO_ANO <= format(as.Date(end_date), "%Y")) %>%
  # Transformando a coluna CO_NCM em caracter para permitir o left_join mais a frente
  mutate(CO_NCM = as.character(CO_NCM)) %>%
  select(CO_ANO, CO_MES, CO_NCM,VL_FOB, SG_UF_NCM, KG_LIQUIDO)


NCM_COSH4 <- read_excel("Tabela_Tradução.xlsx", sheet = "1")
NCM_COSH4 <- NCM_COSH4 [c (1,7)]

# Cruzando as tabelas
Exportação <- left_join(Exportação_data, NCM_COSH4, by = "CO_NCM") %>%
  group_by(CO_ANO, CO_MES, CO_SH4, SG_UF_NCM) %>%
  summarise(
    VL_FOB = sum(VL_FOB, na.rm = TRUE),
    KG_LIQUIDO = sum(KG_LIQUIDO, na.rm = TRUE),
    .groups = "drop") %>%
  filter(CO_ANO >= format(as.Date(start_date), "%Y"))


## Soja Brasil  -------------------------------------------------------------------

Exportação_Soja_BR <- Exportação %>%
  filter(CO_SH4 %in% c(1201, 1507, 2304)) %>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(soja_exBR_fob = sum(VL_FOB, na.rm = TRUE), .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-"))) %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  select(Data, soja_exBR_fob)

## Milho_Brasil -----------------------------------------------------------------

Exportação_Milho_BR <- Exportação %>%
  filter(CO_SH4 %in% c(1005)) %>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(milho_exBR_fob = sum(VL_FOB, na.rm = TRUE), .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-"))) %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  select(Data, milho_exBR_fob)

## Soja Goias - volume e valor -------------------------------------------------

Exportação_Soja_GO <- Exportação %>%
  filter(SG_UF_NCM == "GO",CO_SH4 %in% c(1201, 1507, 2304))%>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(soja_exgo_c_fob = sum(VL_FOB, na.rm = TRUE), soja_exgo_c_vol = sum(KG_LIQUIDO, na.rm = TRUE), .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-")))%>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  select(Data, soja_exgo_c_fob, soja_exgo_c_vol)

## Milho Goias - volume e valor ------------------------------------------------

# Filtrando a base para Goiás e para os SH4 desejados
Exportação_Milho_GO <- Exportação %>%
  filter(SG_UF_NCM == "GO", CO_SH4 %in% c(1005)) %>%
  group_by(CO_ANO, CO_MES) %>%
  summarise(milho_exgo_c_fob = sum(VL_FOB, na.rm = TRUE), milho_exgo_c_vol = sum(KG_LIQUIDO, na.rm = TRUE), .groups = "drop") %>%
  mutate(Data = as.Date(paste(CO_ANO, CO_MES, "01", sep = "-"))) %>%
  filter(Data >= as.Date(start_date) & Data <= as.Date(end_date)) %>%
  select(Data, milho_exgo_c_fob, milho_exgo_c_vol)

## Limpar nao usado
rm(Exportação, NCM_COSH4, Exportação_data,link_node, page,
   url,xpath,destfile,file_url)

# Junção dados -------------------------------------

resultado_Exp <- full_join(Exportação_Soja_BR, Exportação_Soja_GO, by ="Data") %>%
  full_join(Exportação_Milho_BR, by = "Data") %>%
  full_join(Exportação_Milho_GO, by = "Data")

resultado_Exp <- resultado_Exp %>%
  mutate(across(-Data, ~ as.numeric(.)))

comex_consolidado <- left_join(resultado_maquinas_fertilizantes,resultado_Exp,by = "Data")

return(comex_consolidado)

}




################################################################################
## import_receita_impostos_go_data
################################################################################

import_receita_impostos_go_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  library(data.table)
  library(dplyr)

  file_url <- "https://www.gov.br/receitafederal/dados/arrecadacao-estado.csv"
 destfile <- "arrecadacao_estado.csv"
 download.file(file_url, destfile = destfile, mode = "wb")


 # Ler o arquivo Excel
 data <- fread("arrecadacao_estado.csv", dec = ",")

# Mudando o nome das colunas
 colnames(data) <- c("Ano", "Mês", "UF", "IMPOSTO SOBRE IMPORTAÇÃO", "IMPOSTO SOBRE EXPORTAÇÃO", "IPI - FUMO",
                     "IPI - BEBIDAS", "IPI - AUTOMÓVEIS", "IPI - VINCULADO À IMPORTACAO", "IPI - OUTROS", "IRPF",
                     "IRPJ - ENTIDADES FINANCEIRAS", "IRPJ - DEMAIS EMPRESAS", "IRRF - RENDIMENTOS DO TRABALHO",
                     "IRRF - RENDIMENTOS DO CAPITAL", "IRRF - REMESSAS P/ EXTERIOR", "IRRF - OUTROS RENDIMENTOS",
                     "IMPOSTO S/ OPERAÇÕES FINANCEIRAS", "IMPOSTO TERRITORIAL RURAL", "IMPOSTO PROVIS.S/ MOVIMENT. FINANC. - IPMF",
                     "CPMF", "COFINS", "COFINS - FINANCEIRAS", "COFINS - DEMAIS", "CONTRIBUIÇÃO PARA O PIS/PASEP",
                     "CONTRIBUIÇÃO PARA O PIS/PASEP - FINANCEIRAS", "CONTRIBUIÇÃO PARA O PIS/PASEP - DEMAIS", "CSLL",
                     "CSLL - FINANCEIRAS", "CSLL - DEMAIS", "CIDE-COMBUSTÍVEIS (parc. não dedutível)", "CIDE-COMBUSTÍVEIS",
                     "CONTRIBUIÇÃO PLANO SEG. SOC. SERVIDORES", "CPSSS - Contrib. p/ o Plano de Segurid. Social Serv. Público",
                     "CONTRIBUICÕES PARA FUNDAF", "REFIS", "PAES", "RETENÇÃO NA FONTE - LEI 10.833, Art. 30", "PAGAMENTO UNIFICADO",
                     "OUTRAS RECEITAS ADMINISTRADAS", "DEMAIS RECEITAS", "RECEITA PREVIDENCIÁRIA", "RECEITA PREVIDENCIÁRIA - PRÓPRIA",
                     "RECEITA PREVIDENCIÁRIA - DEMAIS", "ADMINISTRADAS POR OUTROS ÓRGÃOS")

 # Filtrar os dados para "GO" e selecionar as colunas específicas
 data_filtrado <- data %>%
   filter(UF == "GO" & Ano >=  format(as.Date(start_date), "%Y") ) %>%
   select(Ano, Mês, UF, `IMPOSTO SOBRE IMPORTAÇÃO`, `IMPOSTO S/ OPERAÇÕES FINANCEIRAS`, contains("IPI"), contains("COFINS"), contains("IOF"))


 # Substituindo "," por "."
 data_filtrado[, 4:ncol(data_filtrado)] <- lapply(data_filtrado[, 4:ncol(data_filtrado)], function(x) gsub(",", ".", x))

 # Convertendo as colunas para númerico
 data_filtrado[, 4:ncol(data_filtrado)] <- lapply(data_filtrado[, 4:ncol(data_filtrado)], as.numeric)

 # Corrigindo a coluna de Mês
 data_filtrado$Mês <- iconv(data_filtrado$Mês, from = "UTF-8", to = "UTF-8") # Torna valores de Março em NA ?
 data_filtrado$Mês[is.na(data_filtrado$Mês)] <- "Março"

 # Criar uma nova coluna com a soma das colunas que contêm "IPI" e "COFINS"
 data_filtrado <- data_filtrado %>%
  mutate(SOMA_IPI = rowSums(select(., contains("IPI")), na.rm = TRUE)) %>%
  mutate(SOMA_COFINS = rowSums(select(., contains("COFINS")), na.rm = TRUE))

 # Filtrar colunas
 data_filtrado <-  data_filtrado %>%
   select(Ano, Mês, UF, `IMPOSTO SOBRE IMPORTAÇÃO`, `IMPOSTO S/ OPERAÇÕES FINANCEIRAS`, SOMA_IPI, SOMA_COFINS)

 #Criar um vetor de mapeamento para os meses
 month_mapping <- c(
   "Janeiro" = 1, "Fevereiro" = 2, "Março" = 3, "Abril" = 4, "Maio" = 5,
   "Junho" = 6, "Julho" = 7, "Agosto" = 8, "Setembro" = 9, "Outubro" = 10,
  "Novembro" = 11, "Dezembro" = 12
 )

 # Criar a coluna Data
   data_filtrado <- data_filtrado %>%
    mutate(Data = as.Date(paste(Ano, month_mapping[Mês], "01", sep = "-"))) %>%
    filter(Data >= as.Date(start_date) & Data <= as.Date(end_date))


 # Remover as colunas "Ano", "Mês", "UF" e reorganizar colocando "Data" como a primeira coluna
  data_imposto <- data_filtrado %>%
   select(Data, everything(), -Ano, -Mês, -UF)


 # renomear colunas
 colnames(data_imposto) <- c("Data", "IMPOSTO SOBRE IMPORTAÇÃO", "IOF", "IPI", "COFINS")

  # 10. Retornar o dataframe
  return(data_imposto)
}



################################################################################
## import_ipea_data
################################################################################

import_ipea_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

  require(ipeadatar)
  library(dplyr)

  list_data_mensal <- available_series(language = c("br"))

  seasList <- c('dessazonalizado','dessazonalizada')

  prefList <- c(
              'IBC-Br',
              'Preço médio',
              'Taxa de juros',
              'Massa de rendimento',
              'Vendas reais - varejo',
              'Sondagem Industrial',
              'Operações de crédito',
              'Nível da ocupação',
              'FipeZap',
              'Indicadores Industriais',
              'Energia elétrica',
              'Consumo nos lares',
              'Taxa de câmbio',
              'Base monetária',
              'Intenção de consumo das famílias',
              'Expectativas do empresário do comércio',
              'Indicadores Industriais - horas trabalhadas',
              'Índice de Confiança',
              'Indicadores Industriais',
              'taxa de desemprego',
              'Utilização da capacidade instalada',
              'Índice de confiança do consumidor',
              'Índice de condições econômicas atuais',
              'Índice de expectativas do consumidor',
              'FipeZap',
              'Vendas reais - varejo',
              'Nível da ocupação',
              'Salário mínimo real',
              'setor público',
              'importações',
              'exportações',
              'inflação',
              'Expectativa média de Inflação - IPCA'
  )


  # filtrar dados
  list_data_mensal <- list_data_mensal %>%
    subset( freq == "Mensal") %>%
    subset( theme == "Macroeconômico") %>%
    subset( status == "Ativa") %>%
    filter(!grepl(paste(seasList, collapse="|"), name)) %>%
    filter(grepl(paste(prefList, collapse="|"), name))

  # obtem os dados
  data_ipea <- ipeadata(code = list_data_mensal$code, language = "br")

  data_ipea$ano <- year(as.Date(data_ipea$date, format = "%Y-%m-%d"))

  data_ipea <- data_ipea %>%
  subset(start_date <= ano & ano <= end_date)

  ##Reshape data_ipea from long to wide format using dcast
  data_ipea$uname <- NULL
  data_ipea$tcode <- NULL
  data_ipea$ano <- NULL

  data_ipea_wide <- reshape2::dcast(data_ipea, date ~ code, value.var = "value")


  # Transformação em time-series

  data_ipea_wide <- as.data.frame(data_ipea_wide)
  data_ipea_wide$date <- NULL
  data_ipea_wide <- data_ipea_wide[!duplicated(as.list(data_ipea_wide))]

  start_year <- lubridate::year(start_date)
  start_month <- lubridate::month(start_date)

  data_ipea_ts <- ts(data_ipea_wide, start=c(start_year, start_month), frequency = 12)
  data_ipea_ts <- window(data_ipea_ts, start=c(start_year, start_month))

  ## remove unused dataframes
  rm(data_ipea_wide)

  ## Remove columns with more than 10% NA
  data_ipea_ts <- data_ipea_ts[, which(colMeans(!is.na(data_ipea_ts)) > 0.9)]

   data_ipea_df <- as.data.frame(data_ipea_ts)

#Corrigindo datas
data_ipea_df$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(data_ipea_df ),
  by = "1 month")

 # Movendo a coluna Data para a primeira posição
 data_ipea_df <- data_ipea_df[, c("Data", setdiff(names(data_ipea_df), "Data"))]

  #rm(seasList,prefList,list_data_mensal)

  return(data_ipea_df)
}


################################################################################
## dados_manuais covariáveis
################################################################################

# Link compartilhável do OneDrive

dados_manuais_covariates <- function(){

 library(httr)
 library(readxl)

  onedrive_url <- "https://goiasgovbr-my.sharepoint.com/personal/savio_oliveira_goias_gov_br/_layouts/15/download.aspx?share=EWTjhdVim89HgEqqIaAJmNEBFAUyDu6j3oux1nHlefUV6A"

# Baixar o arquivo temporariamente
temp_file <- tempfile(fileext = ".xlsx")
GET(onedrive_url, write_disk(temp_file, overwrite = TRUE))

# Ler o Excel
dados_manuais <- read_excel(temp_file)

return(dados_manuais)

}


################################################################################
## dados_manuais indices
################################################################################



indices_data <- function(start_date = "2011-01-01", end_date = Sys.Date()) {

## Rodar o código IPCA_INPC QUE ESTÁ NA MESMA PASTA ANTES DE RODAR ESTE

## Criação dos RDatas dos Indices

library(lubridate)
library(dplyr)
library(tidyr)
library(readxl)
library(sidrar)
library(rvest)
library(xml2)
library(xlsx)

###########################################

# Define URL
url <- "https://github.com/heltonsaulo/covariates_go/raw/99e4521e497fd27c37493010e1f7d162ad6906b3/Índices%20atividades%20PIB.xlsx"

# Nome temporário do arquivo
destfile <- tempfile(fileext = ".xlsx")

# Baixa o arquivo
download.file(url, destfile, mode = "wb")

# Lê o Excel
data_3 <- readxl::read_excel(destfile)

###########################################

# Link raw do GitHub
url <- "https://github.com/heltonsaulo/covariates_go/raw/99e4521e497fd27c37493010e1f7d162ad6906b3/Índices%20com%20e%20sem%20ajuste%20-%20dezembro.xlsx"

# Cria arquivo temporário
destfile <- tempfile(fileext = ".xlsx")

# Faz o download
download.file(url, destfile, mode = "wb")

# Lê o Excel
data_4 <- readxl::read_excel(destfile, skip = 1)



###########################################

# Lendo os arquivos
#data_3 = readxl::read_excel("Índices atividades PIB.xlsx")
#data_4 = readxl::read_excel("Índices com e sem ajuste - dezembro.xlsx", skip = 1)

#Tratando data_3
data_3 <- data_3 %>%
  rename(Data = Mês)
data_3 <- data_3[, c(1:12)]
data_3 <- data_3 %>%
    filter(Data >= as_date(as.Date(start_date)))

# Tratando data_4
data_4 <- data_4[, c(1,2,3,4,5,6,7,9, 10, 11, 12, 13,14)]
colnames(data_4) <- c("Data", "Agropecuária", "Indústria", "Serviços", "VA", "Impostos", "PIB","Agropecuária_a", "Indústria_a", "Serviços_a", "VA_a", "Impostos_a", "PIB_a")
  data_4 <- data_4 %>%
    filter(Data >= as_date(as.Date(start_date)))

  ######################## INPC_BR E IPCA_BR  ##########################################

  period <- paste0(format(as.Date(start_date), "%Y%m"), "-", format(as.Date(end_date), "%Y%m"))

  IPCA <- get_sidra(1737, variable = 2266 ,period = period, geo = "Brazil")
  IPCA$`Data` <- seq.Date(from = as_date(as.Date(start_date)),length.out = length(IPCA$Valor),by = "1 month")

  INPC <- get_sidra(1736, variable = 2289 ,period = period, geo = "Brazil")
  INPC$`Data` <- seq.Date(from = as_date(as.Date(start_date)),length.out = length(INPC$Valor),by = "1 month")

  IPCA <- IPCA %>%
    select(Valor, Data) %>%
    rename(IPCA_BR = Valor)

  INPC <- INPC %>%
    select(Valor, Data) %>%
    rename(INPC_BR = Valor)

  Indices_BR <- full_join(INPC, IPCA, by = "Data")
  Indices_BR <- Indices_BR %>%
    select(Data, everything())

  rm(INPC, IPCA)

  ################################################################################
  ######################## INPC_GO E IPCA_GO  ####################################
  ################################################################################

  ######################### INPC_GO ##############################################
  url <- "https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/Numeros_Indices/Numind_INPC_IPCA/"
  page <- read_html(url)
  links <- html_nodes(page, "a")
  hrefs <- html_attr(links, "href")
  # Filtra os links que contém .zip
  zip_links <- hrefs[grepl("\\.zip$", hrefs)]
  file_url <- zip_links[1] # Escolhe qual link você deseja
  file_url <- url_absolute(file_url, base = url)
  destfile <- "inpc.zip"
  download.file(file_url, destfile = destfile, mode = "wb")

  # Extraindo o zip
  caminho <- getwd()
  zip <- file.path(caminho, "inpc.zip")
  dest <- file.path(caminho)
  arquivo <- unzip(zip, exdir = dest)


  # INPC_GO
  INPC_GO <- read_excel(arquivo, sheet = "INPC N ÍNDICE ÁREAS", skip = 3)
  INPC_GO <- INPC_GO [, 12]
  INPC_GO <- na.omit(INPC_GO)
  colnames(INPC_GO) <- c("INPC_GO")
  INPC_GO$Data <- seq.Date(from = as_date("1993-12-01"),length.out = length(INPC_GO$INPC_GO),by = "1 month")
  INPC_GO <- INPC_GO %>%
    filter(Data >= as.Date(start_date))


  ############################## IPCA ############################################
  url <- "https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/Numeros_Indices/Numind_INPC_IPCA/"
  page <- read_html(url)
  links <- html_nodes(page, "a")
  hrefs <- html_attr(links, "href")
  # Filtra os links que contém .zip
  zip_links <- hrefs[grepl("\\.zip$", hrefs)]
  file_url <- zip_links[3] # Escolhe qual link você deseja
  file_url <- url_absolute(file_url, base = url)
  destfile <- "ipca.zip"
  download.file(file_url, destfile = destfile, mode = "wb")


  # Extraindo o zip
  caminho <- getwd()
  zip <- file.path(caminho, "ipca.zip")
  dest <- file.path(caminho)
  arquivo <- unzip(zip, exdir = dest)

  # IPCA_GO
  IPCA_GO <- read_excel(arquivo, sheet = "IPCA N ÍNDICE ÁREAS", skip = 3)
  IPCA_GO <- IPCA_GO [, 12]
  IPCA_GO <- na.omit(IPCA_GO)
  colnames(IPCA_GO) <- c("IPCA_GO")
  IPCA_GO$Data <- seq.Date(from = as_date("1993-12-01"),length.out = length(IPCA_GO$IPCA_GO), by = "1 month")
  IPCA_GO <- IPCA_GO %>%
    filter(Data >= as.Date(start_date))

  # Juntando as bases
  Indices <- full_join(Indices_BR, IPCA_GO, by = "Data") %>%
    full_join(INPC_GO, by = "Data")

  Indices <- Indices %>%
    select(Data, everything()) %>%
    filter(Data >= as.Date(start_date))

  ### Caso precise separar
  ### INDICES_BR é o IPCA E O INPC do BRASIL são puxados automaticamente
  ### INDICES_GO são puxados manualmente
  #
  # Indices_GO <- full_join(INPC_GO, IPCA_GO, by = "Data")
  # Indices_GO <- Indices_GO %>%
  #   select(Data, everything())

# Combinando os dados
data_combined = full_join(data_3, data_4, by = "Data") %>%
                full_join(Indices, by = "Data")
data_combined <- data_combined[, -c(1 ,19, 20, 21, 22, 23, 24)]

#Corrigindo datas
data_combined$Data <- seq.Date(
  from = as.Date(start_date),
  length.out = nrow(data_combined),
  by = "1 month")

# Movendo a coluna Data para a primeira posição
 data_combined <- data_combined[, c("Data", setdiff(names(data_combined), "Data"))]

return(data_combined)
}



################################################################################
## Baixar e juntar todos os dados - covariates
################################################################################


abcr_data        <- import_abcr_data(start_date = start_date, end_date = end_date)
bcb_data         <- import_bcb_data(start_date = start_date, end_date = end_date)
commodities_data <- import_commodities_data(start_date = start_date, end_date = end_date)
energia_data     <- import_consumo_energia_go_data(start_date = start_date, end_date = end_date)
ibge_data        <- import_ibge_data(start_date = start_date , end_date = end_date)
comex_data       <- import_comex_data(start_date = start_date, end_date = end_date)
receita_data     <- import_receita_impostos_go_data(start_date = start_date, end_date = end_date)
ipea_data        <- import_ipea_data(start_date = start_date, end_date = end_date)
manuais_data     <- dados_manuais_covariates()


# Juntar todos os dataframes
final_covariates <-  manuais_data %>%
  full_join(abcr_data, by ="Data") %>%
  full_join(bcb_data, by = "Data") %>%
  full_join(commodities_data, by = "Data") %>%
  full_join(energia_data, by = "Data") %>%
  full_join(ibge_data, by = "Data") %>%
  full_join(comex_data, by = "Data") %>%
  full_join(receita_data, by = "Data") %>%
  full_join(ipea_data, by ="Data")

## remover acentos e colocar minusculas
colnames(final_covariates) <- tolower(iconv(colnames(final_covariates),from="UTF-8",to="ASCII//TRANSLIT"))

## remove special characters in columnnames
colnames(final_covariates) <- make.names(colnames(final_covariates))

################################################################################
## Baixar e juntar todos os dados - indices
################################################################################

final_indices <- indices_data(start_date = start_date, end_date = end_date)

## remover acentos e colocar minusculas
colnames(final_indices) <- tolower(iconv(colnames(final_indices),from="UTF-8",to="ASCII//TRANSLIT"))

## remove special characters in columnnames
colnames(final_indices) <- make.names(colnames(final_indices))

################################################################################
## retornar xlsx e RData
################################################################################

responses_ts  <- final_indices
covariates_ts <- final_covariates

require(writexl)
writexl::write_xlsx(responses_ts, "responses_ts.xlsx")
writexl::write_xlsx(covariates_ts, "covariates_ts.xlsx")

start_vec <- c(
  as.integer(format(as.Date(start_date), "%Y")),
  as.integer(format(as.Date(start_date), "%m"))
)

responses_ts  <- ts(final_indices %>% select(-data), start = start_vec, frequency = 12)
covariates_ts <- ts(final_covariates %>% select(-data), start = start_vec, frequency = 12)

save(responses_ts,  file = "responses_ts.RData")
save(covariates_ts, file = "covariates_ts.RData")


return(list(responses_ts = responses_ts,
            covariates_ts = covariates_ts))
}


