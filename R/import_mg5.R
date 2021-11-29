#' @title
#' Importar arquivo da base geral de clientes do MG5
#'
#' @description
#' Importa o arquivo MG5 para o R ou para o DuckDB.
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
#' data = import_mg5("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_mg5(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db")
#' }
#'


import_mg5 = function(
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
  "ano_pn", "n_pn", "dt_efetivacao", "produto_renegociacao",
  "sub_produto_renegociacao", "contrato_renegociacao","produto_renegociado",
  "sub_produto_renegociado", "contrato_renegociado", "valor_efetivamente_pg",
  "situacao_contrato", "valor_entrada", "class_contrato", "id_info_operacional",
  "valor_contabil", "valor_de_RA","saldo_contabil", "matrangariador")



  larguras = c(
   4, 7, 8, 4, 4, 20, 4, 4, 20, 17, 1, 12, 2, 1, 12, 12, 12, 9)

  col_types = readr::cols(
    dt_efetivacao = readr::col_date("%Y%m%d"),
    valor_efetivamente_pg = readr::col_double(),
    valor_entrada = readr::col_double(),



   )



  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(larguras,
                        colunas,),
      col_types = col_types,
      skip = 1
    )
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(larguras,
                        colunas),
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


