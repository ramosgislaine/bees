#' @title
#' Importar arquivo da base geral de clientes do GCB
#'
#' @description
#' Importa o arquivo BG para o R ou para o DuckDB.
#'
#' @export
#'
#' @param caminho
#'  String. O caminho do arquivo a ser importado, incluindo a
#'  extensao (.txt).
#' @param tipo
#'  String. "pf" para pessoa fisica ou "pj" para pessoa juridica.
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
#' data = import_bg("\\path\\to\\file.txt", "pf")
#'
#' # para importar para o DuckDB:
#' import_bg(
#'  "\\path\\to\\file.txt",
#'  tipo = "pf",
#'  duckdb = TRUE,
#'  db = "\\path\\to\\duck.db"),
#'  tabela = "bg_pf"
#' }
#'


import_bg = function(
  caminho,
  tipo,
  metodo = "vroom",
  duckdb = FALSE,
  db,
  tabela) {

  # evaluate args
  match.arg(
    arg = tipo,
    choices = c("pf", "pj"),
    several.ok = FALSE
  )

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
  if (tipo == "pf") {
    colunas = c(
      "tp_reg", "tp_cli", "cpfcnpj",
      "titularidade", "class_cliente", "nome",
      "nome_social", "ano_obito", "cid_origem",
      "canal_origem_cadastro", "cid_atual_cadastro", "canal_atual_cadastro",
      "cid_responsavel_doc", "dt_cadastro", "dt_vencimento",
      "cod_banco", "ag_banco", "dt_sis_fin",
      "id_comprovacao_doc", "dt_nascimento", "cidade_nascimento",
      "uf_nascimento", "sexo", "id_nacionalidade",
      "nome_mae", "nome_pai", "tp_documento",
      "documento", "org_emissor", "uf_org_emissor",
      "dt_emissao_doc", "estado_civil", "regime_casamento",
      "cof_conjuge", "nome_conjuge", "filhos",
      "grau_instrucao", "situacao_instrucao", "nome_curso",
      "dt_fim_curso", "dt_inclusao_curso", "empresa_fonte_renda",
      "cnpj_fonte_renda", "cargo", "cbo",
      "dt_admissao", "renda", "tp_renda",
      "dt_renda", "tp_fonte_renda", "tp_vinculo_fonte_renda",
      "renda_total", "tp_renda_total", "dt_renda_total",
      "logradouro", "logradouro_num", "logradouro_complemento",
      "bairro", "cep", "cidade",
      "uf", "ddd", "telefone",
      "ddd_celular", "celular", "email",
      "id_imoveis", "id_veiculos", "id_seguro",
      "patrimonio", "dt_ult_score", "hora_ult_score",
      "modelo", "score", "nivel_risco_calc",
      "class_calc", "score_final", "nivel_risco_final",
      "class_final", "saldo_contabil", "us_person",
      "outra_cidadania",
      "cod_pais_cidadania_1", "cod_pais_cidadania_2",
      "cod_pais_cidadania_3", "cod_pais_cidadania_4",
      "cod_pais_domicilio_1", "nif_1",
      "cod_pais_domicilio_2", "nif_2",
      "cod_pais_domicilio_3", "nif_3",
      "cod_pais_domicilio_4", "nif_4",
      "cod_pais_residencia", "endereco_exterior_numero",
      "endereco_exterior_logradouro", "endereco_exterior_cidade",
      "endereco_exterior_uf", "endereco_exterior_pais",
      "endereco_exterior_cod_postal", "tp_relacionamento", "tp_responsavel",
      "cpf_relacionamento", "nome_relacionamento", "logradouro_digital",
      "numero_logradouro_digital", "complemento_digital", "bairro_digital",
      "cep_digital", "cidade_digital", "uf_digital", "dt_ultima_alteracao"
    )

    larguras = c(
      1, 1, 14, 1, 2, 100, 150, 4, 4, 1, 4, 1, 4, 8, 8, 3, 4, 8, 1, 8, 30, 2, 1,
      4, 40, 40, 1, 15, 10, 2, 8, 1, 1, 14, 100, 2, 1, 1, 30, 6, 8, 60, 14, 30,
      4, 8, 11, 1, 8, 1, 1, 11, 1, 8, 30, 6, 30, 20, 8, 30, 2, 4, 8, 4, 10, 40,
      1, 1, 1, 12, 8, 6, 2, 10, 2, 2, 10, 2, 2, 17, 1, 1, 4, 4, 4, 4, 4, 11,
      4, 11, 4, 11, 4, 11, 4, 5, 100, 50, 50, 50, 10, 1, 1, 14, 100, 30, 6, 30,
      20, 8, 30, 2, 8
    )

    col_types = readr::cols(
      renda = readr::col_double(),
      renda_total = readr::col_double(),
      patrimonio = readr::col_double(),
      saldo_contabil = readr::col_double(),
      dt_cadastro = readr::col_date("%Y%m%d"),
      dt_vencimento = readr::col_date("%Y%m%d"),
      dt_sis_fin = readr::col_date("%Y%m%d"),
      dt_nascimento = readr::col_date("%Y%m%d"),
      dt_emissao_doc = readr::col_date("%Y%m%d"),
      dt_fim_curso = readr::col_date("%Y%m%d"),
      dt_inclusao_curso = readr::col_date("%Y%m%d"),
      dt_admissao = readr::col_date("%Y%m%d"),
      dt_renda = readr::col_date("%Y%m%d"),
      dt_renda_total = readr::col_date("%Y%m%d"),
      dt_ult_score = readr::col_date("%Y%m%d"),
      dt_ultima_alteracao = readr::col_date("%Y%m%d"),
      grau_instrucao = readr::col_character()
    )
  } else {
     # especificacoes arquivo PJ
    colunas = c(
      "tp_reg", "tp_cli", "cpfcnpj",
      "titularidade_cnpj", "razao_social", "nome_fantasia",
      "class_cli", "cid_origem_cadastro", "canal_origem_cadastro",
      "cid_atual_cadastro", "canal_atual_cadastro", "cid_resp_doc",
      "dt_cadastro", "dt_vencimento", "cod_banco", "ag_banco",
      "dt_sis_fin", "id_comprovacao_doc", "porte_class_sfb",
      "porte_class_rfb", "cod_ramo", "digito_ramo", "cod_subclass",
      "dt_inicio_atividade", "cod_natureza_juridica", "dig_natureza_juridica",
      "dt_constituicao", "faturamento", "dt_inicio_faturamento",
      "dt_fim_faturamento", "faturamento_anual",
      "sinal_patrimonio_liquido", "patrimonio_liquido", "doc_contabil",
      "dt_doc_contabil", "n_registro", "dt_registro", "descricao_orgao",
      "capital_social_registrado", "capital_social_integralizado",
      "forma_tributacao", "class_us_person", "percentual_capital_br",
      "percentual_capital_estrangeiro", "id_inter_global", "class_empresa",
      "part_grupo_economico", "cod_grupo_economico", "logradouro_comercial",
      "numero_logradouro_comercial", "complemento_comercial",
      "bairro_comercial", "cep_comercial", "descricao_local",
      "uf", "ddd", "telefone", "ramal",
      "ddd_cel_cliente", "tel_cel_cliente", "email",
      "id_imoveis", "id_veiculos", "id_seguro", "dt_ult_score",
      "hr_ult_score", "modelo", "valor_score_sistema", "nivel_risco_sistema",
      "class_sistema", "valor_score_final", "nivel_risco_final", "class_final",
      "nivel_risco_comite", "class_comite", "saldo_contabil",
      "dt_ultima_alteracao")

    larguras = c(
      1, 1, 14, 1, 100, 30, 2, 4, 1, 4, 1, 4, 8, 8, 3, 4, 8, 1, 1, 1, 4, 1,
      2, 8, 3, 1, 8, 14, 8, 8, 14, 1, 14, 1, 8, 15, 8, 30, 14, 14, 1, 1, 3, 3,
      19, 1, 1, 8, 30, 6, 30, 20, 8, 30, 2, 4, 8, 4, 4, 10, 40, 1, 1, 1, 8, 6,
      2, 10, 2, 2, 10, 2, 2, 2, 2, 17, 8)

    col_types = readr::cols(
      patrimonio_liquido = readr::col_double(),
      dt_cadastro = readr::col_date("%Y%m%d"),
      dt_vencimento = readr::col_date("%Y%m%d"),
      dt_sis_fin = readr::col_date("%Y%m%d"),
      dt_doc_contabil = readr::col_date("%Y%m%d"),
      dt_ult_score = readr::col_date("%Y%m%d"),
      dt_ultima_alteracao = readr::col_date("%Y%m%d"),
      dt_registro = readr::col_date("%Y%m%d"),
      dt_doc_contabil = readr::col_date("%Y%m%d"),
      dt_constituicao = readr::col_date("%Y%m%d"),
      dt_inicio_faturamento = readr::col_date("%Y%m%d"),
      dt_fim_faturamento = readr::col_date("%Y%m%d"),
      faturamento_anual = readr::col_double(),
      dt_inicio_atividade = readr::col_date("%Y%m%d"),
      saldo_contabil = readr::col_double(),
      capital_social_registrado = readr::col_double(),
      capital_social_integralizado = readr::col_double()

    )
  }

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
