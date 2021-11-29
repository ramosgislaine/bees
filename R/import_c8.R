#' @title
#' Importar arquivo da base geral de clientes do GCRED – CFFAC8
#'
#' @description
#' Importa o arquivo c8 para o R ou para o DuckDB.
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
#' data = import_c8("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_c8(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db")
#' }
#'


import_c8 = function(
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
    "n_contrato", "nome", "conta", "agencia", "situacao",
    "n_conta_contabil", "dt_emissao", "dt_vencimento", "tx_juros",
    "indice_correcao", "qt_prestacoes", "valor_principal_aberto",
    "saldo_devedor", "rendas_a_apropriar", "saldo_contabil",
    "dt_venc_ult_prestacao_pg", "dt_pagamento_ult_prestacao_pg",
    "dt_venc_prim_prestacao_aberto", "garantias_prestadas",
    "prazo_total_dias", "total_dias_atraso", "situacao_atraso",
    "produto", "subproduto", "saldo_devedor_vencido", "cpfcnpj",
    "dt_transf_contabil", "dt_inclusao_contrato", "valor_cred_aberto",
    "class_contrato", "valor_aplicação", "empresa", "cod_beneficio",
    "tipo_cliente", "dt_nascimento", "sexo", "estado_civil", "profissao",
    "debito_conta", "valor_rendas_juros_mes", "cid",
    "valor_prestacao_original", "motivo_descaracterizacao",
    "receita_efetivada_correcao_monetaria",
    "renda_apropriar_renegociada", "rendas_efetivadas_mes",
    "n_versao_contrato_cff", "ano_origem_contrato",
    "n_pn_origem_contrato",
    "qtd_parcelas_pg", "canal_orig_contrato"
    )



    larguras = c(
      12, 50, 11, 4, 1, 5, 8, 8, 8, 4, 2, 12, 12, 12, 12, 8, 8, 8, 20, 4,
      4, 1, 4, 4, 12, 14, 8, 8, 14, 2, 16, 6, 20, 1, 8, 1, 10, 60, 1, 12,
      4, 12, 1, 15, 12, 12, 2, 4, 7, 3, 25
      )

    col_types = readr::cols(
      dt_emissao = readr::col_date("%Y%m%d"),
      dt_vencimento = readr::col_date("%Y%m%d"),
      dt_venc_ult_prestacao_pg = readr::col_date("%Y%m%d"),
      dt_pagamento_ult_prestacao_pg = readr::col_date("%Y%m%d"),
      dt_venc_prim_prestacao_aberto = readr::col_date("%Y%m%d"),
      dt_transf_contabil = readr::col_date("%Y%m%d"),
      dt_inclusao_contrato = readr::col_date("%Y%m%d"),
      dt_nascimento = readr::col_date("%Y%m%d"),
      qtd_parcelas_pg = readr::col_double(),
      valor_principal_aberto = readr::col_double(),
      saldo_devedor = readr::col_double(),
      saldo_devedor_vencido = readr::col_double(),
      canal_orig_contrato = readr::col_double(),
      rendas_a_apropriar = readr::col_double(),
      saldo_contabil = readr::col_double(),
      valor_cred_aberto = readr::col_double(),
      valor_prestacao_original = readr::col_double()




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


