#' @title
#' Importar arquivo de folha de pagamento
#'
#' @description
#' Importa o arquivo X4 do sistema DPI para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#' String. O caminho do arquivo a ser importado, incluindo a
#' extensao (.txt).
#' @param metodo
#' String. "readr" ou "vroom".
#' @param duckdb
#' Logical.
#' Se TRUE, entao o arquivo sera importado para o DuckDB.
#' @param db
#' String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
#' @param tabela
#' String. Nome da tabela a ser criada no DuckDB.
#'
#' @details
#' Se for importar apenas para o R, lembre-se de nomear um objeto para
#' o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#' criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_x4("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_x4(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "t_201901"
#' }
#'


import_x4 = function(
  caminho,
  metodo = "vroom",
  duckdb = FALSE,
  db,
  tabela) {

  # evaluate args
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

  # especificacoes arquivo PF
  colunas = c(
      "sip", "empresa", "matricula",
      "tp_pgto", "vlr_pgto", "dt_credito",
      "float", "trf_cheque", "trf_recibo",
      "trf_cc", "trf_cartao_slr", "trf_cms",
      "vlr_trf_cheque", "vlr_trf_recibo",
      "vlr_trf_cc", "vlr_trf_cartao_slr", "vlr_trf_cms",
      "cpfcnpj_empresa", "ag_fav", "ag_empresa",
      "tp_empresa", "cc_empresa", "nr_remessa",
      "nome_fav", "cpf_fav", "cc_fav", "vlr_port"
    )

    larguras = c(
      5, 30, 9, 1, 11, 8, 2, 12, 12, 12, 12, 12,
      11, 11, 11, 11, 11, 14, 4, 4, 1, 11, 5, 100,
      14, 11, 21
    )

    col_types = readr::cols(
        vlr_pgto = readr::col_double(),
        trf_cheque = readr::col_double(),
        trf_recibo = readr::col_double(),
        trf_cc = readr::col_double(),
        trf_cartao_slr = readr::col_double(),
        trf_cms = readr::col_double(),
        vlr_trf_cheque = readr::col_double(),
        vlr_trf_recibo = readr::col_double(),
        vlr_trf_cc = readr::col_double(),
        vlr_trf_cartao_slr = readr::col_double(),
        vlr_trf_cms = readr::col_double(),
        vlr_port = readr::col_double(),
        dt_credito = readr::col_date("%Y%m%d"),
        cpfcnpj_empresa = readr::col_character(),
        ag_fav = readr::col_character(),
        ag_empresa = readr::col_character(),
        cpfcnpj_empresa = readr::col_character(),
        cpf_fav = readr::col_character(),
        cc_fav = readr::col_character()
    )

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(
        larguras,
        colunas
      ),
      col_types = col_types
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
      col_types = col_types
    )
  }

  # adicionando dicionário
  data = within(data, {
      dsc_tp_pgto = NA
      dsc_tp_empresa = NA
      dsc_tp_pgto[tp_pgto == 3] = "conta corrente"
      dsc_tp_pgto[tp_pgto == 6] = "conta meu salario"
      dsc_tp_pgto[tp_pgto == 7] = "conta salario eletronico"
      dsc_tp_empresa[tp_empresa %in% c("A", "B", "C", "D", "L")] = "estadual"
      dsc_tp_empresa[tp_empresa %in% c("E", "M")] = "municipal"
      dsc_tp_empresa[tp_empresa %in% c("F", "N")] = "privado"
      dsc_tp_empresa[tp_empresa %in% c("G")] = "federal"
      dsc_tp_empresa[tp_empresa %in% c("I")] = "pf"
      dsc_tp_empresa[tp_empresa %in% c("J")] = "consignacao"
  })

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
