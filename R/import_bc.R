#' @title
#' Importar arquivo da base geral de clientes do GBRABC
#'
#' @description
#' Importa o arquivo GBRABC para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#'  String. O caminho do arquivo a ser importado, incluindo a
#'  extensao (.txt).
#' @param metodo
#'  String. "readr" ou "vroom".
#' @param duckdb
#'  Logical.
#'  Se TRUE, entao o arquivo sera importado para o DuckDB.
#' @param db
#'  String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
#' @param tabela
#'  String. Nome da tabela a ser criada no DuckDB.
#'
#' @details
#'  Se for importar apenas para o R, lembre-se de nomear um objeto para
#'  o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#'  criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_bc("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_bc(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db"),
#' }
#'


import_bc = function(
  caminho,
  metodo = "vroom",
  duckdb = FALSE,
  db,
  tabela) {

  match.arg(
    arg = metodo,
    choices = c("vroom", "readr"),
    several.ok = FALSE
  )

  if (duckdb == TRUE && is.null(tabela)) {

    stop("tabela deve ser informada")

  }

  if (duckdb == TRUE && is.null(db)) {

    stop("caminho para DuckDb deve ser informado")

  }

  # conexao com DuckDB
  if (duckdb == TRUE) {

    con = DBI::dbConnect(
      duckdb::duckdb(),
      dbdir = db
    )
  }

  # especificacoes arquivo
  colunas = c(
   "tp_registro", "tp_cliente", "cpfcnpj", "dt_inicio_advertencia",
   "cod_advertencia", "cod_cliente", "nome", "cpf/cnpj_2",
   "n_doc_origem_advertencia", "cod_produto", "cod_sub_produto",
   "dt_fim_advertencia", "id_grau_responsabilidade",
   "dt_controle_processamento","cod_interno_dependencia",
   "cod_empresa_externa_informante","qtd_ocorrencia_advertencia",
   "dt_primeira_ocorrencia_advertencia", "dt_ult_ocorrencia_advertencia"
     )


  larguras = c(
  1, 1, 14, 8, 4, 9, 100, 14, 22, 4, 4, 8, 1, 8, 4, 10, 4, 8, 8
  )

  col_types = readr::cols(
    dt_inicio_advertencia = readr::col_date("%Y%m%d"),
    dt_fim_advertencia = readr::col_date("%Y%m%d"),
    dt_primeira_ocorrencia_advertencia = readr::col_date("%Y%m%d"),
    dt_ult_ocorrencia_advertencia = readr::col_date("%Y%m%d"),
    dt_controle_processamento = readr::col_date("%Y%m%d")

  )

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas,
      ),
      col_types = col_types,
      skip = 1
    )
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types,
      skip = 1
    )
  }

  # criar tabela no DuckDB
  if (duckdb == TRUE) {

    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))
    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    # desconectando
    DBI::dbDisconnect(con)

  } else {

    data

  }
}


