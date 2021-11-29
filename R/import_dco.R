#' @title
#' Importar arquivo da base geral de clientes do DCO
#'
#' @description
#' Importa o arquivo DCO para o R ou para o DuckDB.
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
#' data = import_dco("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_dco(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db")
#' }
#'


import_dco = function(
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
    "conta", "poupança", "id_pj_pf", "id_conta_gov", "cpfcgc_titular",
    "cod_faixa_conta", "cod_dependencia", "nome_titular",
    "desc_resumida_orgao", "saldo_fim_dia", "conta_contabil_faixa",
    "conta_contabil_transferencia",
    "conta_cosif_deposito",
    "conta_cosif_transferencia", "dt_abertura_conta",
    "dt_ult_mov_conta", "n_modalidade",
    "dia_cobrança_cesta_tarifa", "id_situação", "cod_situacao_conta",
    "tp_abertura_conta", "id_conta_bloqueada", "ind_aplicacao_automatica",
    "ind_resgate_automatico", "ind_emissao_aplicacoes_extra",
    "ind_bloq_movto_via_automaca", "ind_permissao_deposito",
    "cod_unidade_gestora", "cod_grupo_process", "cod_int_dependencia",
    "dt_ult_utilizacao_cartao", "ind_heranca_atributos_faixa",
    "ind_entrega_cheque", "ponto_reposicao_talao", "n_maximo_taloes",
    "ind_envio_correio", "quant_talonarios_enviados", "quant_folhas_ref",
    "limite_folhas_entrega_talao", "saldo_cheche", "n_cheque_poder_cliente",
    "porcent_cobranca_tarifa", "total_saldo_credor_mes",
    "total_saldo_devedor_mes"
  )



  larguras = c(
    10, 10, 1, 1, 14, 3, 4, 30, 30, 15, 5, 5, 11, 11, 8, 8, 2, 2, 1, 1, 1, 1,
    1, 1, 1, 1, 1, 11, 1, 4, 8, 1, 1, 2, 2, 1, 2, 4, 4, 3, 5, 4, 17, 17
  )

  col_types = readr::cols(
    dt_ult_mov_conta = readr::col_date("%Y%m%d"),
    dt_abertura_conta = readr::col_date("%Y%m%d"),
    dt_ult_utilizacao_cartao = readr::col_date("%Y%m%d")



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


