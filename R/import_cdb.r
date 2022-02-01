#' @title
#' Importar arquivo da base geral de clientes do CDB
#'
#' @description
#' Importa o arquivo CDB para o R ou para o DuckDB.
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
#' data = import_cdb ("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_cdb(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db")
#' }
#'


import_cdb = function(
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

    if (!is.null(tabela) && is.null(db)) {

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
    "conta", "pf_ou_pj", "lig_ou_nao", "cpfcnpj", "nome",
    "aplic", "c_cosif", "c_contabil", "dt_aplic", "dt_venc",
    "pre_ou_pos", "taxa", "ind",
    "porcentagem", "valor_aplic", "saldo_aplic",
    "jr_aprov", "agencia", "cid", "n_operador",
    "governo", "inst_fin", "invest_inst",
    "prefeitura", "cod_cetip", "municipio", "uf",
    "bloqueio", "vl_bacenjud"
    )



    larguras = c(
      10, 1, 3, 14, 30, 3, 8, 5, 8, 8, 3, 13, 3, 5, 16, 16, 16, 30, 4, 10, 3, 3, 3, 3, 11, 30, 2, 1, 16
      )

    col_types = readr::cols(
      dt_aplic = readr::col_date("%Y%m%d"),
      dt_venc = readr::col_date("%Y%m%d"),
      valor_aplic = readr::col_double(),
      saldo_aplic = readr::col_double(),
      vl_bacenjud = readr::col_double()




    )



    # importar arquivo para o R via {readr}
    if (metodo == "readr") {
      data = readr::read_fwf(
        caminho,
        readr::fwf_widths(larguras,
                          colunas, ),
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

