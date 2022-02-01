#' @title
#' Importar arquivo da base geral de clientes do GOT
#'
#' @description
#' Importa o arquivo GOT para o R ou para o DuckDB.
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
#' data = import_bg("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_got(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db")
#' }
#'


import_got = function(
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
      "id_empresa", "cod_dependencia_interno", "dt_inicio_vigencia",
      "dt_fim_vigencia", "tp_dependencia", "digito_cid",
      "descricao_orgao", "descricao_resumida_orgao", "logradouro",
      "complemento_endereco", "bairro", "municipio", "uf", "cep",
      "complemento_cep", "ddd", "telefone", "telefone_2", "telefone_3",
      "telefone_4", "telefone_5", "item_tabelado_1", "item_tabelado_2",
      "item_tabelado_3", "item_tabelado_4", "item_tabelado_5",
      "item_tabelado_6", "filler", "item_tabelado_7", "item_tabelado_8",
      "item_tabelado_9", "item_tabelado_10", "item_tabelado_11",
      "item_tabelado_12", "id_hierarquico_orgao", "id_subcentro",
      "rota_entrega_relatorios", "matricula_cef", "identificador_cief",
      "descricao_carta_patente", "dependencia_contabilizacao",
      "agencia_subordinada", "transmite_arquivo", "id_banco_central",
      "digito_controle_id", "localizador_hierarquico", "orgao_imediato_sup",
      "classe_ag", "1_acatamento_2_novas_op_credito", "regiao_fiscal",
      "id_cid_cense", "status_ag_pioneira",
      "praca_compensacao", "id_orgao_adm", "id_orgao_controle",
      "id_gerencia_regional", "nivel_automocao_dependencia",
      "emissao_listao_papel", "sequencia_emissao_subcentro",
      "n_vias_relatorio_saldo", "agencia_contabil_subordinada",
      "codigo_localidade", "descricao_resumida_orgao_2",
      "superintendencia_regional", "id_regiao_cid_capital",
      "controle_de_adm_dados", "valor_acatamento",
      "id_dependencias_periferic")


    larguras = c(
      2, 4, 8, 8, 3, 2, 50, 20, 30, 30, 30, 30, 2, 5, 3, 4, 8, 8, 8, 8, 8, 4,
      4, 4, 4, 4, 4, 1, 6, 6, 6, 2, 2, 2, 12, 3, 2, 5, 8, 60, 4, 4, 1, 4, 2,
      30, 4, 3, 2, 2, 4, 1, 3, 1, 1, 5, 1, 1, 2, 2, 4, 6, 20, 4, 1, 4, 15, 1)

    col_types = readr::cols(
      dt_inicio_vigencia = readr::col_date("%Y%m%d"),
      dt_fim_vigencia = readr::col_date("%Y%m%d"),

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
