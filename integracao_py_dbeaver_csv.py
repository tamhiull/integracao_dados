import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import chardet
from datetime import datetime
import re

# 📌 Conexão com o banco: AQUI VOCÊ COLOCA AS SUAS CREDENCIAIS
conn = psycopg2.connect(
    host="endereço da rede",
    port="porta",
    dbname="nome no banco de dados",
    user="usuario",
    password="senha"
)
cur = conn.cursor()

# 📂 Pasta dos arquivos
pasta = r"C:\Base_de_dados\455_averbacao"

# 📌 Primeiro, vamos criar uma tabela com TODAS as coluna. 
def criar_tabela_completa_2025():

    sql = """
    CREATE TABLE IF NOT EXISTS seguro.averbacao (
        serie_numero_ctrc TEXT,
        data_autorizacao DATE,
        codigo_correios TEXT,
        numero_arquivo_averbacao TEXT,
        retorno_averbacao TEXT,
        data_retorno_averbacao DATE,
        numero_averbacao TEXT,
        valor_enviado NUMERIC(18,2)
    );
    """
    
    try:
        cur.execute(sql)
        conn.commit()
        print("✅ Tabela averbacao criada com sucesso!")
    except Exception as e:
        print(f"❌ Erro ao criar tabela: {e}")

# 📌 Aqui eu resolvi um problema para filtra uma data de uma primeira linha do arquivo que não era o cabeçalho. 
def extrair_periodo_robusto(primeira_linha):
    """Extrai período de autorização de forma robusta"""
    periodo_inicio, periodo_fim = None, None
    
    if "PERIODO DE AUTORIZ" in primeira_linha:
        try:
            trecho = primeira_linha.split("PERIODO DE AUTORIZ:")[1].strip()
            print(f"🔍 Trecho original: '{trecho}'")
            
            # Remove tudo depois do primeiro ; (se houver)
            trecho = trecho.split(';')[0].strip()
            print(f"🔍 Trecho após remover ;: '{trecho}'")
            
            # Usa regex para encontrar padrões de data
            padrao_data = r'(\d{1,2}/\d{1,2}/\d{2,4})'
            datas = re.findall(padrao_data, trecho)
            
            if len(datas) >= 2:
                data_inicio_str = datas[0]
                data_fim_str = datas[1]
                
                print(f"🔍 Data início encontrada: '{data_inicio_str}'")
                print(f"🔍 Data fim encontrada: '{data_fim_str}'")
                
                # Converte as datas
                periodo_inicio = pd.to_datetime(data_inicio_str, format="%d/%m/%y", errors="coerce")
                periodo_fim = pd.to_datetime(data_fim_str, format="%d/%m/%y", errors="coerce")
                
                periodo_inicio = periodo_inicio.date() if not pd.isna(periodo_inicio) else None
                periodo_fim = periodo_fim.date() if not pd.isna(periodo_fim) else None
                
                print(f"📅 Período extraído: {periodo_inicio} a {periodo_fim}")
            else:
                print(f"⚠️ Não encontrei datas no padrão em: '{trecho}'")
                print(f"🔍 Datas encontradas: {datas}")
                
                # Tenta método alternativo: dividir por "A" ou "a", pois tinha algumas tabelas que tinha A e a. 
                if ' A ' in trecho:
                    datas_alt = trecho.split(' A ')
                elif ' a ' in trecho:
                    datas_alt = trecho.split(' a ')
                else:
                    datas_alt = trecho.split('A')
                
                if len(datas_alt) >= 2:
                    data_inicio_str = datas_alt[0].strip()
                    data_fim_str = datas_alt[1].strip()
                    
                    # Remove qualquer ; que possa ter sobrado
                    data_inicio_str = data_inicio_str.rstrip(';')
                    data_fim_str = data_fim_str.rstrip(';')
                    
                    print(f"🔍 Data início (método alternativo): '{data_inicio_str}'")
                    print(f"🔍 Data fim (método alternativo): '{data_fim_str}'")
                    
                    periodo_inicio = pd.to_datetime(data_inicio_str, format="%d/%m/%y", errors="coerce")
                    periodo_fim = pd.to_datetime(data_fim_str, format="%d/%m/%y", errors="coerce")
                    
                    periodo_inicio = periodo_inicio.date() if not pd.isna(periodo_inicio) else None
                    periodo_fim = periodo_fim.date() if not pd.isna(periodo_fim) else None
                    
                    print(f"📅 Período extraído (alternativo): {periodo_inicio} a {periodo_fim}")
                
        except Exception as e:
            print(f"⚠️ Erro ao extrair período: {e}")
            import traceback
            traceback.print_exc()
    
    return periodo_inicio, periodo_fim

# 📌 Função para processar o arquivo com TODAS as colunas, caso tiver a mais que as colunas esperadas. 
def processar_arquivo_2025_completo(caminho, arquivo):
    """Processa arquivos de 2025 com todas as colunas"""
    print(f"🔧 Processando arquivo 2025 completo: {arquivo}")
    
    try:
        # Extrair período com método robusto
        with open(caminho, "r", encoding="latin-1") as f:
            primeira_linha = f.readline().strip()

        periodo_inicio, periodo_fim = extrair_periodo_robusto(primeira_linha)

        # Lê TODAS as colunas do arquivo
        df = pd.read_csv(caminho, sep=";", skiprows=1, dtype=str, encoding="latin-1", on_bad_lines='skip')
        
        if df.empty:
            print("⚠️ Arquivo vazio")
            return []

        print(f"📊 Arquivo tem {len(df.columns)} colunas")
        
        # Adiciona coluna numero
        df['numero'] = df.index + 1 #funciona como se fosse um id de coluna. 
        
        # Lista de todas as colunas que vamos mapear
        colunas_mapeamento = [
            'Serie/Numero CTRC','Codigo dos Correios',
            'Numero do Arquivo de Averbacao', 'Retorno Averbacao', 'Data Retorno Averbacao',
            'Numero de Averbacao', 'Valor Enviado'
        ]
        
        # Prepara os dados para inserção
        registros = []
        linhas_com_erro = 0
        
        for idx, row in df.iterrows():
            try:
                registro = []
                
                # Adiciona numero
                registro.append(idx + 1)
                
                # Processa cada coluna do mapeamento, pula alguma coluna se for necessario.
                for coluna in colunas_mapeamento[1:]:  
                        valor = row[coluna]
                    else:
                        valor = None
                    
                    # Limpeza básica
                    if pd.isna(valor) or str(valor).strip() in ['', 'nan', 'NaN', 'NULL', 'None']:
                        registro.append(None)
                    else:
                        valor_limpo = str(valor).strip()
                        
                        # Limpa CNPJs
                        if 'CNPJ' in coluna:
                            valor_limpo = ''.join(filter(str.isdigit, valor_limpo))
                            if len(valor_limpo) == 14:
                                try:
                                    registro.append(int(valor_limpo))
                                except:
                                    registro.append(None)
                            else:
                                registro.append(None)
                        else:
                            registro.append(valor_limpo)
                
                # Adiciona períodos. 
                registro.append(periodo_inicio)
                registro.append(periodo_fim)
                
                registros.append(tuple(registro))
                
            except Exception as e:
                linhas_com_erro += 1
                # Continua processando as demais linhas
                continue
        
        if linhas_com_erro > 0:
            print(f"⚠️ {linhas_com_erro} linhas com erro foram ignoradas")
        
        print(f"✅ Arquivo processado: {len(registros)} registros válidos")
        return registros
        
    except Exception as e:
        print(f"❌ Erro ao processar arquivo: {e}")
        import traceback
        traceback.print_exc()
        return []

# 📌 Cria a tabela completa
criar_tabela_completa_2025()

# 📌 Lista de colunas para a inserção, caso tiver colunas a mais. 
colunas_insercao = [
     'serie_numero_ctrc','codigo_correios', 'numero_arquivo_averbacao',
    'retorno_averbacao', 'data_retorno_averbacao', 'numero_averbacao', 'valor_enviado'
]

# 📌 Processa os arquivos de 2025
for arquivo in os.listdir(pasta):
    if arquivo.endswith(".csv") and '2025' in arquivo:
        caminho = os.path.join(pasta, arquivo)
        print(f"\n📂 Processando: {arquivo}")
        
        registros = processar_arquivo_2025_completo(caminho, arquivo)
        
        if registros:
            # SQL de inserção com TODAS as colunas
            placeholders = ', '.join(['%s'] * len(colunas_insercao))
            sql = f"INSERT INTO seguro.averbacao_2025 ({', '.join(colunas_insercao)}) VALUES ({placeholders})"
            
            # Insere em lotes menores para melhor acompanhamento no Dbeaver. 
            batch_size = 50
            total_inserido = 0
            
            for i in range(0, len(registros), batch_size):
                batch = registros[i:i + batch_size]
                try:
                    execute_batch(cur, sql, batch, page_size=batch_size)
                    conn.commit()
                    total_inserido += len(batch)
                    print(f"✅ Lote {i//batch_size + 1}: {len(batch)} linhas")
                except Exception as e:
                    print(f"❌ Erro no lote: {e}")
                    conn.rollback()
            
            print(f"🎯 {arquivo}: {total_inserido} linhas inseridas")
        else:
            print(f"⚠️ Nenhum registro válido encontrado em {arquivo}")

cur.close()
conn.close()
print("🎉 Processamento concluído!")