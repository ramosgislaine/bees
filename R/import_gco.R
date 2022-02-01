#' @title
#' Importar arquivo de contratos de credito
#'
#' @description
#' Importa o arquivo GCOA53 para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho String. O caminho do arquivo a ser importado, incluindo a
#'   extensao (.txt).
#' @param duckdb Logical.
#'   Se TRUE, entao o arquivo sera importado para o DuckDB.
#' @param db String. Se duckdb = TRUE, o caminho do arquivo do banco de dados.
#' @param tabela String. Nome da tabela a ser criada no DuckDB.
#'
#' @details Se for importar apenas para o R, lembre-se de nomear um objeto para
#'   o resultado da funcao. Caso for importar para o DuckDB, nao e necessario
#'   criar um objeto. Note que a tabela sera sobrescrita se ja existir.
#'
#' @examples
#' \dontrun{
#' # para importar para o R:
#' data = import_gco("\\path\\to\\file.txt")
#'
#' # para importar para o DuckDB:
#' import_bg(
#'  "\\path\\to\\file.txt",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "bg_pf"
#' }
#'
#'
import_gco = function(caminho,
                      duckdb = FALSE,
                      db,
                      tabela,
                      metodo = c("vroom", "readr")) {

  # evaluate arg tabela
  if (duckdb == TRUE && is.null(tabela)) {
    stop("tabela deve ser informada")
  }

  # evaluate arg host
  if (duckdb == TRUE && is.null(user)) {
    stop("host deve ser informado")
  }

  # evaluate arg user
  if (duckdb == TRUE && is.null(user)) {
    stop("user deve ser informado")
  }

  # evaluate arg metodo
  match.arg(metodo)

  # conexao com DuckDB
  if (duckdb == TRUE) {
    DBI::dbConnect(duckdb::duckdb())
  }

  # importar arquivo para o R via {readr}
  if (metodo == "readr") {
    data = readr::read_fwf(
      caminho,
      readr::fwf_widths(
        c(
          6, 4, 4, 20, 8, 3, 4, 8, 8, 17, 17, 17, 4, 1, 2, 2, 2, 2, 5, 4, 14,
          30, 13, 1, 1, 2, 8, 20, 2, 1, 1, 13, 11, 2, 2, 2, 2, 17, 9, 1, 4, 1,
          2, 2, 2
        ),
        c(
          "processamento", "produto", "subproduto", "contrato", "clientegco",
          "sistema", "agencia", "efetivacao", "vencimento", "saldodevedor",
          "saldocontabil", "saldopdd", "diasatraso", "adiantamento",
          "classorigem", "class", "classinformada", "classfinal",
          "grupocontabil", "diasprazo", "cpfcnpj", "nome", "vlrutilizadopdd",
          "prejuizo", "tipocredito", "codinterno", "transfprejuizo",
          "tipoperda", "idempresa", "procadm", "procjud", "saldora", "cosif",
          "classggc", "classoperacional", "idpdd", "classrisco", "contratado",
          "txjuros", "tipotx", "indexador", "tipocli", "canalorigem",
          "canalatual", "classoficial"
        )
      ),
      col_types = readr::cols(
        tipocli = readr::col_character(),
        adiantamento = readr::col_character(),
        tipoperda = readr::col_character(),
        procadm = readr::col_character(),
        procjud = readr::col_character(),
        classoperacional = readr::col_character(),
        classoficial = readr::col_character(),
        txjuros = readr::col_double(),
        diasatraso = readr::col_double(),
        diasprazo = readr::col_double(),
        vlrutilizadopdd = readr::col_double(),
        contrato = readr::col_character(),
        saldodevedor = readr::col_double(),
        saldocontabil = readr::col_double(),
        saldora = readr::col_double(),
        contratado = readr::col_double(),
        saldopdd = readr::col_double(),
        processamento = readr::col_character(),
        vencimento = readr::col_date("%Y%m%d"),
        transfprejuizo = readr::col_date("%Y%m%d"),
        efetivacao = readr::col_date("%Y%m%d")
      )
    )
  }

  # importar arquivo para o R via {vroom}
  if (metodo == "vroom") {
    data = vroom::vroom_fwf(
      caminho,
      readr::fwf_widths(
        c(
          6, 4, 4, 20, 8, 3, 4, 8, 8, 17, 17, 17, 4, 1, 2, 2, 2, 2, 5, 4, 14,
          30, 13, 1, 1, 2, 8, 20, 2, 1, 1, 13, 11, 2, 2, 2, 2, 17, 9, 1, 4, 1,
          2, 2, 2
        ),
        c(
          "processamento", "produto", "subproduto", "contrato", "clientegco",
          "sistema", "agencia", "efetivacao", "vencimento", "saldodevedor",
          "saldocontabil", "saldopdd", "diasatraso", "adiantamento",
          "classorigem", "class", "classinformada", "classfinal",
          "grupocontabil", "diasprazo", "cpfcnpj", "nome", "vlrutilizadopdd",
          "prejuizo", "tipocredito", "codinterno", "transfprejuizo",
          "tipoperda", "idempresa", "procadm", "procjud", "saldora", "cosif",
          "classggc", "classoperacional", "idpdd", "classrisco", "contratado",
          "txjuros", "tipotx", "indexador", "tipocli", "canalorigem",
          "canalatual", "classoficial"
        )
      ),
      col_types = readr::cols(
        tipocli = readr::col_character(),
        adiantamento = readr::col_character(),
        tipoperda = readr::col_character(),
        procadm = readr::col_character(),
        procjud = readr::col_character(),
        classoperacional = readr::col_character(),
        classoficial = readr::col_character(),
        txjuros = readr::col_double(),
        diasatraso = readr::col_double(),
        diasprazo = readr::col_double(),
        vlrutilizadopdd = readr::col_double(),
        contrato = readr::col_character(),
        saldodevedor = readr::col_double(),
        saldocontabil = readr::col_double(),
        saldora = readr::col_double(),
        contratado = readr::col_double(),
        saldopdd = readr::col_double(),
        processamento = readr::col_character(),
        vencimento = readr::col_date("%Y%m%d"),
        transfprejuizo = readr::col_date("%Y%m%d"),
        efetivacao = readr::col_date("%Y%m%d")
      )
    )
  }

  # criar tabela no DuckDB
  if (duckdb == TRUE) {

    # dropar tabela
    DBI::dbSendQuery(con, paste("DROP TABLE IF EXISTS", tabela))

    # gravar tabela
    DBI::dbWriteTable(con, DBI::SQL(tabela), data)

    # gravar índices
    DBI::dbSendQuery(con, paste0("
    ALTER TABLE ", schema, ".", tabela, "
    ADD INDEX idx_contrato (contrato(20)),
    ADD INDEX idx_prejuizo (prejuizo(1)),
    ADD INDEX idx_cpfcnpj (cpfcnpj(14)),
    ADD INDEX idx_agencia (agencia(4)),
    ADD INDEX idx_produto (produto(4)),
    ADD INDEX idx_subproduto (subproduto(4)),
    ADD INDEX idx_tipocli (tipocli(1)),
    ADD INDEX idx_procjud (procjud(1)),
    ADD INDEX idx_diasatraso (diasatraso);
    "))

    # desconectando
    DBI::dbDisconnect(con)
  } else {
    data
  }
}