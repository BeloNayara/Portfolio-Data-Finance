"""
============================================================
  Análise Financeira — Banco Itaú Unibanco
  Balanço Patrimonial + DRE (2019–2023)
  Análise Vertical, Horizontal e Dashboard HTML
============================================================
  Uso:
    cd ~/Downloads
    python3 itau_analise_v2.py
============================================================
"""

import io, zipfile, json, requests, pandas as pd, warnings
from pathlib import Path
from datetime import datetime

warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────────────────
#  CONFIGURAÇÕES
# ──────────────────────────────────────────────────────────
CD_CVM_ITAU = "019348"          # com zero à esquerda!
ANOS        = [2019, 2020, 2021, 2022, 2023]
OUTPUT_DIR  = Path.home() / "Downloads" / "itau_dados"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL    = "https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS"

# Códigos reais do Itaú na CVM (plano de contas BACEN)
CONTAS_BPA = {
    "1"           : "Ativo Total",
    "1.01"        : "Caixa e Equivalentes de Caixa",
    "1.02"        : "Ativos Financeiros",
    "1.02.03.04"  : "Operações de Crédito",
    "1.02.03.06"  : "(-) Provisão para Perda Esperada",
    "1.04"        : "Outros Ativos",
}

CONTAS_BPP = {
    "2"           : "Passivo Total",
    "2.01"        : "Passivo Financeiro",
    "2.01.01"     : "Depósitos",
    "2.01.02"     : "Captações no Mercado Aberto",
    "2.01.06"     : "Empréstimos e Repasses",
    "2.03"        : "Patrimônio Líquido",
}

CONTAS_DRE = {
    "3.01"        : "Receita de Intermediação Financeira",
    "3.02"        : "Despesa de Intermediação Financeira",
    "3.03"        : "Resultado Bruto de Intermediação",
    "3.04"        : "Outras Receitas/Despesas Operacionais",
    "3.05"        : "Resultado Operacional",
    "3.09"        : "Lucro Líquido",
}

# ──────────────────────────────────────────────────────────
#  EXTRAÇÃO
# ──────────────────────────────────────────────────────────

def baixar_zip(ano: int) -> dict:
    url = f"{BASE_URL}/dfp_cia_aberta_{ano}.zip"
    print(f"  ⬇  {ano}  →  {url}")
    try:
        r = requests.get(url, timeout=180)
        r.raise_for_status()
        csvs = {}
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            for nome in z.namelist():
                if nome.endswith(".csv"):
                    with z.open(nome) as f:
                        csvs[nome.lower()] = pd.read_csv(f, sep=";", encoding="latin1", dtype=str)
        print(f"     ✅ {len(csvs)} arquivos")
        return csvs
    except Exception as e:
        print(f"     ❌ {e}")
        return {}


def extrair_contas(csvs: dict, tipo: str, contas_map: dict, ano: int) -> pd.DataFrame:
    chave = next((k for k in csvs if f"_{tipo.lower()}_con_" in k), None)
    if chave is None:
        print(f"     ⚠  '{tipo}' não encontrado")
        return pd.DataFrame()

    df = csvs[chave]
    itau = df[
        (df["CD_CVM"].str.strip() == CD_CVM_ITAU) &
        (df["ORDEM_EXERC"].str.strip() == "ÚLTIMO")
    ].copy()

    if itau.empty:
        print(f"     ⚠  Nenhum dado do Itaú em {tipo}/{ano}")
        return pd.DataFrame()

    itau["VL_CONTA"] = pd.to_numeric(itau["VL_CONTA"], errors="coerce")

    rows = []
    for cod, label in contas_map.items():
        linha = itau[itau["CD_CONTA"].str.strip() == cod]
        val = linha["VL_CONTA"].iloc[0] if not linha.empty else None
        rows.append({"Conta": label, "Codigo": cod, str(ano): val})

    return pd.DataFrame(rows).set_index(["Conta", "Codigo"])


def montar_tabela(tipo: str, contas_map: dict) -> pd.DataFrame:
    frames = []
    for ano in ANOS:
        print(f"\n  [{tipo} {ano}]")
        csvs = baixar_zip(ano)
        if not csvs:
            continue
        df = extrair_contas(csvs, tipo, contas_map, ano)
        if not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    result = frames[0]
    for f in frames[1:]:
        result = result.join(f, how="outer")
    return result.reset_index()


# ──────────────────────────────────────────────────────────
#  ANÁLISE VERTICAL E HORIZONTAL
# ──────────────────────────────────────────────────────────

def analise_vertical(df: pd.DataFrame, base_label: str) -> pd.DataFrame:
    av = df.copy()
    cols_anos = [str(a) for a in ANOS if str(a) in df.columns]
    base = df[df["Conta"] == base_label]
    if base.empty:
        return pd.DataFrame()
    for col in cols_anos:
        base_val = pd.to_numeric(base[col].values[0], errors="coerce")
        col_serie = pd.to_numeric(av[col], errors="coerce")
        av[f"AV {col}"] = (col_serie / base_val * 100).round(2) if pd.notna(base_val) and base_val != 0 else None
    return av[["Conta"] + [f"AV {c}" for c in cols_anos]]


def analise_horizontal(df: pd.DataFrame, ano_base: int = 2019) -> pd.DataFrame:
    ah = df.copy()
    cols_anos = [str(a) for a in ANOS if str(a) in df.columns]
    base_col  = str(ano_base)
    if base_col not in cols_anos:
        return pd.DataFrame()
    base_serie = pd.to_numeric(ah[base_col], errors="coerce")
    for col in cols_anos:
        if col != base_col:
            col_serie = pd.to_numeric(ah[col], errors="coerce")
            ah[f"AH {col}"] = ((col_serie - base_serie) / base_serie.abs() * 100).round(2)
    ah[f"AH {base_col}"] = 0.0
    return ah[["Conta"] + [f"AH {c}" for c in cols_anos]]


# ──────────────────────────────────────────────────────────
#  DASHBOARD HTML
# ──────────────────────────────────────────────────────────

def df_to_json(df: pd.DataFrame) -> str:
    return df.to_json(orient="records", force_ascii=False)


def gerar_analise_html(df_bpa, df_bpp, df_dre) -> str:
    """Gera HTML com análise interpretativa respondendo as 5 perguntas."""

    def v(df, conta, ano):
        row = df[df["Conta"] == conta] if not df.empty else pd.DataFrame()
        if row.empty: return None
        col = str(ano)
        val = row[col].values[0] if col in row.columns else None
        return float(val) if pd.notna(val) and val is not None else None

    def pct_var(v1, v2):
        if v1 is None or v2 is None or v1 == 0: return None
        return (v2 - v1) / abs(v1) * 100

    def fmt(val, bilhoes=True):
        if val is None: return "N/D"
        av = abs(val)
        if bilhoes:
            if av >= 1e9: return f"R$ {val/1e9:.1f}T"
            if av >= 1e6: return f"R$ {val/1e6:.1f}B"
            if av >= 1e3: return f"R$ {val/1e3:.1f}M"
        return f"{val:.1f}%"

    def fmt_pct(v):
        if v is None: return "N/D"
        return f"{'▲ +' if v>=0 else '▼ '}{v:.1f}%"

    a19, a23 = 2019, 2023

    # Coleta valores-chave
    ativo19  = v(df_bpa, "Ativo Total", a19)
    ativo23  = v(df_bpa, "Ativo Total", a23)
    rec19    = v(df_dre, "Receita de Intermediação Financeira", a19)
    rec23    = v(df_dre, "Receita de Intermediação Financeira", a23)
    luc19    = v(df_dre, "Lucro Líquido", a19)
    luc23    = v(df_dre, "Lucro Líquido", a23)
    res_b19  = v(df_dre, "Resultado Bruto de Intermediação", a19)
    res_b23  = v(df_dre, "Resultado Bruto de Intermediação", a23)
    desp19   = v(df_dre, "Outras Receitas/Despesas Operacionais", a19)
    desp23   = v(df_dre, "Outras Receitas/Despesas Operacionais", a23)
    caixa19  = v(df_bpa, "Caixa e Equivalentes de Caixa", a19)
    caixa23  = v(df_bpa, "Caixa e Equivalentes de Caixa", a23)
    cred19   = v(df_bpa, "Operações de Crédito", a19)
    cred23   = v(df_bpa, "Operações de Crédito", a23)
    dep19    = v(df_bpp, "Depósitos", a19)
    dep23    = v(df_bpp, "Depósitos", a23)
    emp19    = v(df_bpp, "Empréstimos e Repasses", a19)
    emp23    = v(df_bpp, "Empréstimos e Repasses", a23)
    pl19     = v(df_bpp, "Patrimônio Líquido", a19)
    pl23     = v(df_bpp, "Patrimônio Líquido", a23)
    passivo19 = v(df_bpp, "Passivo Total", a19)
    passivo23 = v(df_bpp, "Passivo Total", a23)

    var_ativo  = pct_var(ativo19,  ativo23)
    var_rec    = pct_var(rec19,    rec23)
    var_luc    = pct_var(luc19,    luc23)
    var_res_b  = pct_var(res_b19,  res_b23)
    var_desp   = pct_var(desp19,   desp23)
    var_caixa  = pct_var(caixa19,  caixa23)
    var_cred   = pct_var(cred19,   cred23)
    var_dep    = pct_var(dep19,    dep23)
    var_emp    = pct_var(emp19,    emp23)
    var_pl     = pct_var(pl19,     pl23)

    marg19 = (luc19/rec19*100) if luc19 and rec19 else None
    marg23 = (luc23/rec23*100) if luc23 and rec23 else None

    # Alavancagem: Passivo/PL
    alav19 = (passivo19/pl19) if passivo19 and pl19 else None
    alav23 = (passivo23/pl23) if passivo23 and pl23 else None

    def card(num, cor, pergunta, corpo, metricas, veredicto, v_tipo):
        pills = "".join(f"<div class='metrica-pill'><span class='ml'>{m[0]}</span><span class='mv'>{m[1]}</span></div>" for m in metricas)
        return f"""
        <div class='analise-card {cor}'>
          <div class='analise-num'>Questão {num}</div>
          <div class='analise-pergunta'>{pergunta}</div>
          <div class='analise-metricas'>{pills}</div>
          <div class='analise-body'>{corpo}</div>
          <div class='veredicto {v_tipo}'>{veredicto}</div>
        </div>"""

    # ── Q1: Crescimento sustentável ──────────────────────
    eq1 = var_ativo is not None and var_rec is not None
    if eq1:
        diff = abs(var_ativo - var_rec)
        sustentavel = diff < 30 and var_ativo > 0 and var_rec > 0
        corpo1 = (f"Entre 2019 e 2023, o <strong>Ativo Total</strong> cresceu <strong>{fmt_pct(var_ativo)}</strong> "
                  f"({fmt(ativo19)} → {fmt(ativo23)}), enquanto a <strong>Receita de Intermediação Financeira</strong> "
                  f"variou <strong>{fmt_pct(var_rec)}</strong> ({fmt(rec19)} → {fmt(rec23)}). "
                  f"{'O crescimento foi equilibrado, com ativos e receita avançando em proporção similar, indicando expansão saudável.' if sustentavel else 'O crescimento dos ativos e da receita ocorreu em ritmos distintos — vale monitorar se a expansão patrimonial está sendo convertida em geração de receita de forma eficiente.'}")
        ver1 = ("✅ Crescimento equilibrado entre ativos e receita" if sustentavel
                else "⚠️ Assimetria entre crescimento de ativos e receita — análise adicional recomendada")
        vt1 = "pos" if sustentavel else "neu"
    else:
        corpo1 = "Dados insuficientes para esta análise."
        ver1, vt1 = "⚠️ Dados incompletos", "neu"

    c1 = card("01", "verde", "O crescimento foi sustentável? Ativos e receita avançaram de forma equilibrada?",
              corpo1,
              [("Ativo Total 2019", fmt(ativo19)), ("Ativo Total 2023", fmt(ativo23)),
               ("Variação Ativo", fmt_pct(var_ativo)), ("Variação Receita", fmt_pct(var_rec))],
              ver1, vt1)

    # ── Q2: Custos vs Receitas / Margem ─────────────────
    if rec19 and rec23 and res_b19 and res_b23:
        margem_bruta19 = res_b19/rec19*100
        margem_bruta23 = res_b23/rec23*100
        delta_mb = margem_bruta23 - margem_bruta19
        marg_ok = delta_mb >= 0
        corpo2 = (f"A margem bruta de intermediação saiu de <strong>{margem_bruta19:.1f}%</strong> em 2019 para "
                  f"<strong>{margem_bruta23:.1f}%</strong> em 2023 (<strong>{delta_mb:+.1f} p.p.</strong>). "
                  f"{'As despesas de intermediação cresceram proporcionalmente menos que a receita, preservando ou ampliando a margem — sinal positivo de eficiência.' if marg_ok else 'As despesas de intermediação cresceram mais que a receita, comprimindo a margem bruta. Isso pode refletir maior custo de captação ou aumento na provisão para perdas.'} "
                  f"A margem líquida {'melhorou' if marg23 and marg19 and marg23 > marg19 else 'recuou'} de "
                  f"<strong>{marg19:.1f}%</strong> para <strong>{marg23:.1f}%</strong>." if marg19 and marg23 else "")
        ver2 = (f"✅ Margem preservada ({margem_bruta23:.1f}%)" if marg_ok
                else f"⚠️ Compressão de margem ({delta_mb:.1f} p.p. no período)")
        vt2 = "pos" if marg_ok else "neg"
    else:
        corpo2 = "Dados insuficientes para esta análise."
        ver2, vt2 = "⚠️ Dados incompletos", "neu"

    c2 = card("02", "laranja", "Os custos cresceram proporcionalmente às receitas? Como isso impacta a margem?",
              corpo2,
              [("Margem Bruta 2019", f"{(res_b19/rec19*100):.1f}%" if res_b19 and rec19 else "N/D"),
               ("Margem Bruta 2023", f"{(res_b23/rec23*100):.1f}%" if res_b23 and rec23 else "N/D"),
               ("Margem Líquida 2019", f"{marg19:.1f}%" if marg19 else "N/D"),
               ("Margem Líquida 2023", f"{marg23:.1f}%" if marg23 else "N/D")],
              ver2, vt2)

    # ── Q3: Estoques (Crédito para bancos) ───────────────
    # Para bancos, "estoques" equivale à carteira de crédito
    if cred19 and cred23:
        cred_ok = var_cred and var_cred > 0
        corpo3 = (f"Para instituições financeiras como o Itaú, a carteira de <strong>Operações de Crédito</strong> "
                  f"equivale aos 'estoques' de uma empresa industrial — representa o principal ativo gerador de receita. "
                  f"A carteira {'cresceu' if cred_ok else 'recuou'} <strong>{fmt_pct(var_cred)}</strong> no período "
                  f"({fmt(cred19)} → {fmt(cred23)}). "
                  f"{'Expansão da carteira de crédito indica estratégia de crescimento, mas requer monitoramento da inadimplência e da provisão para perdas.' if cred_ok else 'A retração da carteira pode indicar postura conservadora, redução da demanda ou aumento da seletividade no crédito.'}")
        ver3 = ("✅ Expansão da carteira de crédito — crescimento ativo" if cred_ok
                else "⚠️ Carteira de crédito em retração — postura conservadora")
        vt3 = "pos" if cred_ok else "neu"
    else:
        corpo3 = "Dados de carteira de crédito não disponíveis."
        ver3, vt3 = "⚠️ Dados incompletos", "neu"

    c3 = card("03", "azul",
              "A empresa aumentou ou reduziu seus estoques/carteira de crédito? Eficiência ou risco?",
              corpo3,
              [("Crédito 2019", fmt(cred19)), ("Crédito 2023", fmt(cred23)),
               ("Variação", fmt_pct(var_cred)), ("% do Ativo 2023", f"{(cred23/ativo23*100):.1f}%" if cred23 and ativo23 else "N/D")],
              ver3, vt3)

    # ── Q4: Endividamento ────────────────────────────────
    if pl19 and pl23:
        pl_cresceu = var_pl and var_pl > 0
        dep_cresceu = var_dep and var_dep > 0
        corpo4 = (f"O <strong>Patrimônio Líquido</strong> variou <strong>{fmt_pct(var_pl)}</strong> "
                  f"({fmt(pl19)} → {fmt(pl23)}). "
                  f"Os <strong>Depósitos</strong> (principal fonte de funding) variaram <strong>{fmt_pct(var_dep)}</strong> "
                  f"e os <strong>Empréstimos e Repasses</strong> variaram <strong>{fmt_pct(var_emp)}</strong>. "
                  f"{'O crescimento do PL acima do endividamento indica que o banco financiou parte de sua expansão com capital próprio, fortalecendo sua base patrimonial.' if pl_cresceu else 'O PL recuou, sugerindo que o crescimento foi financiado predominantemente por capital de terceiros (depósitos e captações).'} "
                  f"A alavancagem (Passivo/PL) {'subiu' if alav23 and alav19 and alav23 > alav19 else 'caiu' if alav23 and alav19 else 'N/D'} "
                  f"de <strong>{f'{alav19:.1f}x' if alav19 else 'N/D'}</strong> para <strong>{f'{alav23:.1f}x' if alav23 else 'N/D'}</strong>.")
        ver4 = ("✅ PL cresceu — expansão com fortalecimento patrimonial" if pl_cresceu
                else "⚠️ PL recuou — maior dependência de capital de terceiros")
        vt4 = "pos" if pl_cresceu else "neg"
    else:
        corpo4 = "Dados de patrimônio insuficientes."
        ver4, vt4 = "⚠️ Dados incompletos", "neu"

    c4 = card("04", "vermelho",
              "O endividamento cresceu? O crescimento foi financiado com capital próprio ou dívida?",
              corpo4,
              [("PL 2019", fmt(pl19)), ("PL 2023", fmt(pl23)),
               ("Var. PL", fmt_pct(var_pl)), ("Alavancagem 2023", f"{alav23:.1f}x" if alav23 else "N/D")],
              ver4, vt4)

    # ── Q5: Liquidez ─────────────────────────────────────
    if caixa19 and caixa23:
        liq_ok = var_caixa and var_caixa > 0
        liq_rel19 = (caixa19/ativo19*100) if ativo19 else None
        liq_rel23 = (caixa23/ativo23*100) if ativo23 else None
        corpo5 = (f"O <strong>Caixa e Equivalentes</strong> variou <strong>{fmt_pct(var_caixa)}</strong> "
                  f"({fmt(caixa19)} → {fmt(caixa23)}). "
                  f"Em relação ao ativo total, o caixa representava <strong>{f'{liq_rel19:.1f}%' if liq_rel19 else 'N/D'}</strong> "
                  f"em 2019 e <strong>{f'{liq_rel23:.1f}%' if liq_rel23 else 'N/D'}</strong> em 2023. "
                  f"{'O aumento do caixa melhora a capacidade de honrar obrigações de curto prazo e indica postura conservadora de gestão de liquidez.' if liq_ok else 'A redução do caixa merece atenção: pode indicar uso dos recursos para expansão (positivo) ou pressão de liquidez (negativo). É importante analisar em conjunto com os depósitos e captações de curto prazo.'} "
                  f"Para bancos, a liquidez também é regulada pelo BACEN através dos índices LCR e NSFR, não capturados nesta análise.")
        ver5 = ("✅ Posição de caixa fortalecida no período" if liq_ok
                else "⚠️ Redução do caixa — avaliar contexto de uso dos recursos")
        vt5 = "pos" if liq_ok else "neu"
    else:
        corpo5 = "Dados de caixa insuficientes."
        ver5, vt5 = "⚠️ Dados incompletos", "neu"

    c5 = card("05", "ouro",
              "A liquidez melhorou ou piorou? Caixa e recebíveis cobrem as obrigações?",
              corpo5,
              [("Caixa 2019", fmt(caixa19)), ("Caixa 2023", fmt(caixa23)),
               ("Variação Caixa", fmt_pct(var_caixa)),
               ("Caixa/Ativo 2023", f"{(caixa23/ativo23*100):.1f}%" if caixa23 and ativo23 else "N/D")],
              ver5, vt5)

    return f"<div class='analise-grid'>{c1}{c2}{c3}{c4}{c5}</div>"


def gerar_dashboard(df_bpa, df_bpp, df_dre, av_bpa, ah_bpa, av_dre, ah_dre) -> str:
    anos_js = json.dumps(ANOS)
    cols_anos = [str(a) for a in ANOS]

    # Une BPA + BPP para exibição do Balanço completo
    df_bal = pd.concat([df_bpa, df_bpp], ignore_index=True) if not df_bpa.empty and not df_bpp.empty else df_bpa

    # Pré-computa renomes fora da f-string
    av_bpa_r = av_bpa.rename(columns={"AV " + str(a): str(a) for a in ANOS}) if not av_bpa.empty else av_bpa
    ah_bpa_r = ah_bpa.rename(columns={"AH " + str(a): str(a) for a in ANOS}) if not ah_bpa.empty else ah_bpa
    av_dre_r = av_dre.rename(columns={"AV " + str(a): str(a) for a in ANOS}) if not av_dre.empty else av_dre
    ah_dre_r = ah_dre.rename(columns={"AH " + str(a): str(a) for a in ANOS}) if not ah_dre.empty else ah_dre
    df_bal_r  = df_bal[["Conta"] + [a for a in cols_anos if a in df_bal.columns]] if not df_bal.empty else df_bal
    df_dre_r  = df_dre[["Conta"] + [a for a in cols_anos if a in df_dre.columns]] if not df_dre.empty else df_dre
    analise_html = gerar_analise_html(df_bpa, df_bpp, df_dre)

    def get_serie(df, conta):
        row = df[df["Conta"] == conta]
        if row.empty:
            return [None] * len(ANOS)
        vals = []
        for a in ANOS:
            col = str(a)
            v = row[col].values[0] if col in row.columns else None
            vals.append(round(float(v), 2) if pd.notna(v) and v is not None else None)
        return vals

    def fmt_val(v):
        if v is None:
            return "—"
        av = abs(v)
        if av >= 1e9:  return f"R$ {v/1e9:.1f}T"
        if av >= 1e6:  return f"R$ {v/1e6:.1f}B"
        if av >= 1e3:  return f"R$ {v/1e3:.1f}M"
        return f"R$ {v:.0f}"

    def table_html(df, title, pct=False):
        if df.empty:
            return f"<div class='table-block'><h3>{title}</h3><p style='color:var(--muted)'>Sem dados</p></div>"
        cols = [c for c in df.columns if c not in ("Codigo",)]
        headers = "".join(f"<th>{c}</th>" for c in cols)
        rows_html = ""
        for _, row in df.iterrows():
            cells = f"<td class='label-cell'>{row['Conta']}</td>"
            for col in cols[1:]:
                v = row[col]
                try:
                    fv = float(v)
                    if pct:
                        color = "pos" if fv >= 0 else "neg"
                        cells += f"<td class='{color}'>{fv:+.1f}%</td>"
                    else:
                        cells += f"<td>{fv:,.0f}</td>"
                except:
                    cells += "<td>—</td>"
            rows_html += f"<tr>{cells}</tr>"
        return f"""
        <div class='table-block'>
          <h3>{title}</h3>
          <div class='table-wrap'>
            <table><thead><tr>{headers}</tr></thead><tbody>{rows_html}</tbody></table>
          </div>
        </div>"""

    # KPIs
    ativo   = get_serie(df_bpa, "Ativo Total")
    pl      = get_serie(df_bpp, "Patrimônio Líquido")
    receita = get_serie(df_dre, "Receita de Intermediação Financeira")
    lucro   = get_serie(df_dre, "Lucro Líquido")
    credito = get_serie(df_bpa, "Operações de Crédito")
    depositos = get_serie(df_bpp, "Depósitos")
    res_bruto = get_serie(df_dre, "Resultado Bruto de Intermediação")
    res_op    = get_serie(df_dre, "Resultado Operacional")

    def kpi_change(vals):
        if not vals or vals[0] is None or vals[-1] is None:
            return ("—", "")
        chg = (vals[-1] - vals[0]) / abs(vals[0]) * 100
        return (f"{'▲ +' if chg>=0 else '▼ '}{chg:.1f}% vs 2019", "pos" if chg >= 0 else "neg")

    kpis = [
        ("Ativo Total 2023",      ativo,    "orange"),
        ("Receita Fin. 2023",     receita,  "blue"),
        ("Lucro Líquido 2023",    lucro,    "green"),
        ("Patrimônio Líq. 2023",  pl,       "gold"),
    ]

    kpi_html = ""
    for label, vals, color in kpis:
        v = vals[-1] if vals else None
        chg_txt, chg_cls = kpi_change(vals)
        kpi_html += f"""
        <div class='kpi-card {color}'>
          <div class='kpi-label'>{label}</div>
          <div class='kpi-value'>{fmt_val(v)}</div>
          <div class='kpi-change {chg_cls}'>{chg_txt}</div>
        </div>"""

    return f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Análise Financeira — Itaú Unibanco 2019–2023</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;600;700;800&family=DM+Mono:wght@300;400;500&display=swap');
  :root {{
    --bg:#0a0e17; --surface:#111827; --border:#1f2d45;
    --accent:#f97316; --accent2:#3b82f6; --accent3:#10b981;
    --neg:#ef4444; --text:#e2e8f0; --muted:#64748b; --gold:#f59e0b;
  }}
  * {{ box-sizing:border-box; margin:0; padding:0; }}
  body {{ background:var(--bg); color:var(--text); font-family:'DM Mono',monospace; font-size:13px; }}
  .hero {{ background:linear-gradient(135deg,#0d1b2e,#0a0e17); border-bottom:1px solid var(--border); padding:48px 40px 36px; position:relative; overflow:hidden; }}
  .hero::before {{ content:''; position:absolute; top:-80px; right:-80px; width:360px; height:360px; background:radial-gradient(circle,rgba(249,115,22,.18),transparent 70%); }}
  .hero-tag {{ font-size:10px; letter-spacing:.18em; color:var(--accent); text-transform:uppercase; margin-bottom:10px; }}
  .hero h1 {{ font-family:'Syne',sans-serif; font-size:clamp(28px,4vw,48px); font-weight:800; color:#fff; }}
  .hero h1 span {{ color:var(--accent); }}
  .hero-sub {{ margin-top:10px; color:var(--muted); font-size:12px; }}
  .hero-meta {{ margin-top:20px; display:flex; gap:16px; flex-wrap:wrap; }}
  .meta-pill {{ background:rgba(255,255,255,.04); border:1px solid var(--border); border-radius:6px; padding:6px 14px; font-size:11px; color:var(--muted); }}
  .meta-pill strong {{ color:var(--text); }}
  .tabs {{ display:flex; background:var(--surface); border-bottom:1px solid var(--border); padding:0 40px; overflow-x:auto; }}
  .tab-btn {{ padding:14px 22px; background:none; border:none; border-bottom:2px solid transparent; color:var(--muted); font-family:'DM Mono',monospace; font-size:12px; cursor:pointer; white-space:nowrap; transition:all .2s; }}
  .tab-btn:hover {{ color:var(--text); }}
  .tab-btn.active {{ color:var(--accent); border-bottom-color:var(--accent); }}
  .container {{ padding:32px 40px; max-width:1400px; margin:0 auto; }}
  .section {{ display:none; }}
  .section.active {{ display:block; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(200px,1fr)); gap:16px; margin-bottom:36px; }}
  .kpi-card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:20px 22px; position:relative; overflow:hidden; }}
  .kpi-card::before {{ content:''; position:absolute; top:0; left:0; right:0; height:2px; }}
  .kpi-card.orange::before {{ background:var(--accent); }}
  .kpi-card.blue::before   {{ background:var(--accent2); }}
  .kpi-card.green::before  {{ background:var(--accent3); }}
  .kpi-card.gold::before   {{ background:var(--gold); }}
  .kpi-label {{ font-size:10px; letter-spacing:.12em; color:var(--muted); text-transform:uppercase; margin-bottom:8px; }}
  .kpi-value {{ font-family:'Syne',sans-serif; font-size:22px; font-weight:700; color:#fff; }}
  .kpi-change {{ margin-top:6px; font-size:11px; }}
  .kpi-change.pos {{ color:var(--accent3); }}
  .kpi-change.neg {{ color:var(--neg); }}
  .charts-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(440px,1fr)); gap:20px; margin-bottom:32px; }}
  .chart-card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:24px; }}
  .chart-card h3 {{ font-family:'Syne',sans-serif; font-size:13px; font-weight:700; margin-bottom:20px; }}
  .chart-card canvas {{ max-height:260px; }}
  .table-block {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:24px; margin-bottom:20px; }}
  .table-block h3 {{ font-family:'Syne',sans-serif; font-size:13px; font-weight:700; margin-bottom:16px; }}
  .table-wrap {{ overflow-x:auto; }}
  table {{ width:100%; border-collapse:collapse; font-size:12px; }}
  th {{ background:rgba(255,255,255,.04); padding:10px 14px; text-align:right; font-size:10px; letter-spacing:.1em; color:var(--muted); text-transform:uppercase; border-bottom:1px solid var(--border); white-space:nowrap; }}
  th:first-child {{ text-align:left; }}
  td {{ padding:9px 14px; text-align:right; border-bottom:1px solid rgba(31,45,69,.5); }}
  td.label-cell {{ text-align:left; color:var(--muted); font-size:11px; }}
  tr:hover td {{ background:rgba(255,255,255,.02); }}
  td.pos {{ color:var(--accent3); }}
  td.neg {{ color:var(--neg); }}
  .section-title {{ font-family:'Syne',sans-serif; font-size:18px; font-weight:700; color:#fff; margin-bottom:8px; }}
  .section-desc {{ color:var(--muted); font-size:11px; margin-bottom:28px; }}
  footer {{ text-align:center; padding:32px; color:var(--muted); font-size:10px; border-top:1px solid var(--border); margin-top:40px; }}
  .analise-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(580px,1fr)); gap:20px; margin-bottom:24px; }}
  .analise-card {{ background:var(--surface); border:1px solid var(--border); border-radius:12px; padding:28px; position:relative; overflow:hidden; }}
  .analise-card::before {{ content:''; position:absolute; top:0; left:0; bottom:0; width:3px; }}
  .analise-card.verde::before  {{ background:var(--accent3); }}
  .analise-card.laranja::before {{ background:var(--accent); }}
  .analise-card.azul::before   {{ background:var(--accent2); }}
  .analise-card.vermelho::before {{ background:var(--neg); }}
  .analise-card.ouro::before   {{ background:var(--gold); }}
  .analise-num {{ font-family:'Syne',sans-serif; font-size:11px; font-weight:700; letter-spacing:.15em; text-transform:uppercase; margin-bottom:8px; }}
  .analise-card.verde  .analise-num {{ color:var(--accent3); }}
  .analise-card.laranja .analise-num {{ color:var(--accent); }}
  .analise-card.azul   .analise-num {{ color:var(--accent2); }}
  .analise-card.vermelho .analise-num {{ color:var(--neg); }}
  .analise-card.ouro   .analise-num {{ color:var(--gold); }}
  .analise-pergunta {{ font-family:'Syne',sans-serif; font-size:15px; font-weight:700; color:#fff; margin-bottom:16px; line-height:1.4; }}
  .analise-body {{ color:#94a3b8; font-size:12px; line-height:1.9; }}
  .analise-body strong {{ color:var(--text); }}
  .analise-metricas {{ display:flex; gap:12px; flex-wrap:wrap; margin:16px 0; }}
  .metrica-pill {{ background:rgba(255,255,255,.05); border:1px solid var(--border); border-radius:8px; padding:8px 14px; font-size:11px; }}
  .metrica-pill .ml {{ color:var(--muted); display:block; font-size:10px; margin-bottom:2px; }}
  .metrica-pill .mv {{ color:#fff; font-weight:600; }}
  .veredicto {{ margin-top:16px; padding:12px 16px; border-radius:8px; font-size:12px; font-weight:500; }}
  .veredicto.pos {{ background:rgba(16,185,129,.1); border:1px solid rgba(16,185,129,.25); color:var(--accent3); }}
  .veredicto.neg {{ background:rgba(239,68,68,.1); border:1px solid rgba(239,68,68,.25); color:var(--neg); }}
  .veredicto.neu {{ background:rgba(245,158,11,.1); border:1px solid rgba(245,158,11,.25); color:var(--gold); }}
</style>
</head>
<body>

<div class="hero">
  <div class="hero-tag">Análise Financeira · Dados CVM</div>
  <h1>Itaú Unibanco<br><span>2019 — 2023</span></h1>
  <p class="hero-sub">Balanço Patrimonial · DRE · Análise Vertical & Horizontal</p>
  <div class="hero-meta">
    <div class="meta-pill">Ticker: <strong>ITUB4</strong></div>
    <div class="meta-pill">CD CVM: <strong>019348</strong></div>
    <div class="meta-pill">Fonte: <strong>dados.cvm.gov.br</strong></div>
    <div class="meta-pill">Gerado em: <strong>{datetime.now().strftime('%d/%m/%Y')}</strong></div>
  </div>
</div>

<div class="tabs">
  <button class="tab-btn active" onclick="showTab('visao',this)">Visão Geral</button>
  <button class="tab-btn" onclick="showTab('balanco',this)">Balanço Patrimonial</button>
  <button class="tab-btn" onclick="showTab('dre',this)">DRE</button>
  <button class="tab-btn" onclick="showTab('av',this)">Análise Vertical</button>
  <button class="tab-btn" onclick="showTab('ah',this)">Análise Horizontal</button>
  <button class="tab-btn" onclick="showTab('analise',this)">📋 Análise Interpretativa</button>
</div>

<div class="container">

<!-- VISÃO GERAL -->
<div id="tab-visao" class="section active">
  <div class="kpi-grid">{kpi_html}</div>
  <div class="charts-grid">
    <div class="chart-card"><h3>Ativo Total vs Patrimônio Líquido (R$ mil)</h3><canvas id="cAtivoPL"></canvas></div>
    <div class="chart-card"><h3>Receita vs Resultado Bruto vs Lucro Líquido (R$ mil)</h3><canvas id="cDRE"></canvas></div>
    <div class="chart-card"><h3>Operações de Crédito vs Depósitos (R$ mil)</h3><canvas id="cCredDep"></canvas></div>
    <div class="chart-card"><h3>Margem Líquida (Lucro / Receita %)</h3><canvas id="cMargem"></canvas></div>
  </div>
</div>

<!-- BALANÇO -->
<div id="tab-balanco" class="section">
  <p class="section-title">Balanço Patrimonial</p>
  <p class="section-desc">Valores em R$ mil · Consolidado · Fonte: CVM/DFP</p>
  {table_html(df_bal_r, "Ativo")}
  {table_html(df_dre_r[df_dre_r["Conta"].isin(list(CONTAS_BPP.values()))] if not df_dre_r.empty else pd.DataFrame(), "Passivo e Patrimônio Líquido")}
</div>

<!-- DRE -->
<div id="tab-dre" class="section">
  <p class="section-title">Demonstração do Resultado</p>
  <p class="section-desc">Valores em R$ mil · Consolidado · Fonte: CVM/DFP</p>
  {table_html(df_dre_r, "DRE — Valores (R$ mil)")}
  <div class="charts-grid" style="margin-top:20px">
    <div class="chart-card"><h3>Receita vs Resultado Bruto (R$ mil)</h3><canvas id="cDRE2"></canvas></div>
    <div class="chart-card"><h3>Resultado Operacional vs Lucro Líquido (R$ mil)</h3><canvas id="cDRE3"></canvas></div>
  </div>
</div>

<!-- AV -->
<div id="tab-av" class="section">
  <p class="section-title">Análise Vertical</p>
  <p class="section-desc">BPA: % do Ativo Total · DRE: % da Receita de Intermediação Financeira</p>
  {table_html(av_bpa_r, "AV — Balanço Ativo (% do Ativo Total)", pct=False)}
  {table_html(av_dre_r, "AV — DRE (% da Receita de Intermediação)", pct=False)}
</div>

<!-- AH -->
<div id="tab-ah" class="section">
  <p class="section-title">Análise Horizontal</p>
  <p class="section-desc">Variação % acumulada em relação ao ano-base 2019</p>
  {table_html(ah_bpa_r, "AH — Balanço Ativo (variação % vs 2019)", pct=True)}
  {table_html(ah_dre_r, "AH — DRE (variação % vs 2019)", pct=True)}
</div>

<!-- ANÁLISE INTERPRETATIVA -->
<div id="tab-analise" class="section">
  <p class="section-title">Análise Interpretativa</p>
  <p class="section-desc">Respostas automáticas baseadas nos dados reais extraídos da CVM · 2019–2023</p>
  {analise_html}
</div>

</div><!-- /container -->

<footer>ITAÚ UNIBANCO HOLDING S.A. · CD CVM 019348 · Dados: dados.cvm.gov.br · Uso analítico</footer>

<script>
const ANOS    = {anos_js};
const BAL     = {df_to_json(df_bal)};
const DRE_DAT = {df_to_json(df_dre)};

function get(data, conta) {{
  const row = data.find(r => r["Conta"] === conta);
  if (!row) return ANOS.map(() => null);
  return ANOS.map(a => {{ const v = row[String(a)]; return (v != null && !isNaN(v)) ? v : null; }});
}}

const CD = {{
  plugins: {{ legend: {{ labels: {{ color:"#94a3b8", font:{{ family:"DM Mono", size:11 }} }} }} }},
  scales: {{
    x: {{ ticks:{{ color:"#64748b" }}, grid:{{ color:"rgba(255,255,255,.04)" }} }},
    y: {{ ticks:{{ color:"#64748b" }}, grid:{{ color:"rgba(255,255,255,.06)" }} }}
  }},
  responsive: true
}};

function mkLine(id, datasets) {{
  new Chart(document.getElementById(id), {{ type:"line",
    data:{{ labels:ANOS, datasets }}, options:{{ ...CD }} }});
}}
function mkBar(id, datasets) {{
  new Chart(document.getElementById(id), {{ type:"bar",
    data:{{ labels:ANOS, datasets }}, options:{{ ...CD, scales:{{ ...CD.scales,
      x:{{ ...CD.scales.x, stacked:false }}, y:{{ ...CD.scales.y, stacked:false }} }} }} }});
}}

window.addEventListener("DOMContentLoaded", () => {{
  const ativo   = get(BAL,     "Ativo Total");
  const pl      = get(BAL,     "Patrimônio Líquido");
  const receita = get(DRE_DAT, "Receita de Intermediação Financeira");
  const resBrut = get(DRE_DAT, "Resultado Bruto de Intermediação");
  const lucro   = get(DRE_DAT, "Lucro Líquido");
  const resOp   = get(DRE_DAT, "Resultado Operacional");
  const credito = get(BAL,     "Operações de Crédito");
  const deposit = get(BAL,     "Depósitos");

  const sc = v => v != null ? v/1e3 : null; // R$ mil → R$ milhões para legibilidade

  mkBar("cAtivoPL", [
    {{ label:"Ativo Total",        data:ativo.map(sc),   backgroundColor:"rgba(249,115,22,.75)", borderRadius:4 }},
    {{ label:"Patrimônio Líquido", data:pl.map(sc),      backgroundColor:"rgba(59,130,246,.75)",  borderRadius:4 }}
  ]);

  mkBar("cDRE", [
    {{ label:"Receita",         data:receita.map(sc), backgroundColor:"rgba(249,115,22,.75)", borderRadius:4 }},
    {{ label:"Resultado Bruto", data:resBrut.map(sc), backgroundColor:"rgba(59,130,246,.75)",  borderRadius:4 }},
    {{ label:"Lucro Líquido",   data:lucro.map(sc),   backgroundColor:"rgba(16,185,129,.75)",  borderRadius:4 }}
  ]);

  mkBar("cCredDep", [
    {{ label:"Operações de Crédito", data:credito.map(sc), backgroundColor:"rgba(245,158,11,.75)", borderRadius:4 }},
    {{ label:"Depósitos",            data:deposit.map(sc), backgroundColor:"rgba(139,92,246,.75)",  borderRadius:4 }}
  ]);

  // Margem líquida
  const margem = receita.map((r,i) => r && lucro[i] ? parseFloat((lucro[i]/r*100).toFixed(2)) : null);
  mkLine("cMargem", [{{
    label:"Margem Líquida (%)", data:margem,
    borderColor:"#f59e0b", backgroundColor:"rgba(245,158,11,.1)",
    fill:true, tension:.4, pointRadius:6, pointBackgroundColor:"#f59e0b"
  }}]);

  mkBar("cDRE2", [
    {{ label:"Receita",         data:receita.map(sc), backgroundColor:"rgba(249,115,22,.75)", borderRadius:4 }},
    {{ label:"Resultado Bruto", data:resBrut.map(sc), backgroundColor:"rgba(59,130,246,.75)",  borderRadius:4 }}
  ]);

  mkLine("cDRE3", [
    {{ label:"Resultado Operacional", data:resOp.map(sc),
       borderColor:"#3b82f6", backgroundColor:"rgba(59,130,246,.1)", fill:true, tension:.4, pointRadius:5 }},
    {{ label:"Lucro Líquido", data:lucro.map(sc),
       borderColor:"#10b981", backgroundColor:"rgba(16,185,129,.1)", fill:true, tension:.4, pointRadius:5 }}
  ]);
}});

function showTab(id, btn) {{
  document.querySelectorAll(".section").forEach(s => s.classList.remove("active"));
  document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
  document.getElementById("tab-" + id).classList.add("active");
  btn.classList.add("active");
}}
</script>
</body>
</html>"""


# ──────────────────────────────────────────────────────────
#  EXCEL
# ──────────────────────────────────────────────────────────

def exportar_excel(df_bpa, df_bpp, df_dre, av_bpa, ah_bpa, av_dre, ah_dre):
    path = OUTPUT_DIR / "itau_analise_2019_2023.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        for df, nome in [
            (df_bpa, "Balanço Ativo"),
            (df_bpp, "Balanço Passivo+PL"),
            (df_dre, "DRE"),
            (av_bpa.rename(columns={"AV "+str(a): str(a) for a in ANOS}), "AV Balanço"),
            (av_dre.rename(columns={"AV "+str(a): str(a) for a in ANOS}), "AV DRE"),
            (ah_bpa.rename(columns={"AH "+str(a): str(a) for a in ANOS}), "AH Balanço"),
            (ah_dre.rename(columns={"AH "+str(a): str(a) for a in ANOS}), "AH DRE"),
        ]:
            if df.empty:
                continue
            df.to_excel(w, sheet_name=nome, index=False)
            ws = w.sheets[nome]
            ws.column_dimensions["A"].width = 40
            for i in range(2, 10):
                ws.column_dimensions[chr(64+i)].width = 18
    print(f"  📊 Excel: {path}")


# ──────────────────────────────────────────────────────────
#  MAIN
# ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("="*60)
    print("  ITAÚ UNIBANCO — Análise Financeira 2019–2023")
    print(f"  {datetime.now().strftime('%d/%m/%Y %H:%M')}")
    print("="*60)

    print("\n[1/4] Baixando Balanço Ativo (BPA)...")
    df_bpa = montar_tabela("BPA", CONTAS_BPA)

    print("\n[2/4] Baixando Balanço Passivo (BPP)...")
    df_bpp = montar_tabela("BPP", CONTAS_BPP)

    print("\n[3/4] Baixando DRE...")
    df_dre = montar_tabela("DRE", CONTAS_DRE)

    if df_bpa.empty and df_dre.empty:
        print("\n❌ Nenhum dado. Verifique conexão.")
        exit(1)

    print("\n[4/4] Calculando análises e gerando arquivos...")

    av_bpa = analise_vertical(df_bpa, "Ativo Total")     if not df_bpa.empty else pd.DataFrame()
    ah_bpa = analise_horizontal(df_bpa)                   if not df_bpa.empty else pd.DataFrame()
    av_dre = analise_vertical(df_dre, "Receita de Intermediação Financeira") if not df_dre.empty else pd.DataFrame()
    ah_dre = analise_horizontal(df_dre)                   if not df_dre.empty else pd.DataFrame()

    exportar_excel(df_bpa, df_bpp, df_dre, av_bpa, ah_bpa, av_dre, ah_dre)

    html = gerar_dashboard(df_bpa, df_bpp, df_dre, av_bpa, ah_bpa, av_dre, ah_dre)
    dash = OUTPUT_DIR / "dashboard_itau.html"
    dash.write_text(html, encoding="utf-8")
    print(f"  🌐 Dashboard: {dash}")

    print(f"\n{'='*60}")
    print(f"  ✅ Concluído! Pasta: {OUTPUT_DIR}")
    print(f"  👉 Abra dashboard_itau.html no navegador")
    print(f"{'='*60}")
