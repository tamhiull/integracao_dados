import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime


# 📌 Conexão com o banco: AQUI VOCÊ COLOCA AS SUAS CREDENCIAIS
conn = psycopg2.connect(
    host="endereço da rede",
    port="porta",
    dbname="nome no banco de dados",
    user="usuario",
    password="senha"
)


cur = conn.cursor()

# Lê o Excel
df = pd.read_excel(r"c:\base_de_dados\arquivos_auxiliares\clt_seguro.xlsx", dtype=str)

registros = []  #ARMAZENA CADA LINHA LIMPA E PROCESSADDAAAA

## VAI EM CADA LINHA DO DATAFRAME 
for _, row in df.iterrows():
    # Processa cada campo individualmente
    cnpj = str(row.get('CNPJ', '')).strip().replace('.', '').replace('/', '').replace('-', '')
    cnpj = int(cnpj) if cnpj and cnpj != 'nan' and cnpj != '' else None
    
    cliente = str(row.get('CLIENTE', '')).strip()
    cliente = cliente if cliente != 'nan' and cliente != '' else None
    
    numero_apolice = str(row.get('N° Apolice', '')).strip()
    numero_apolice = numero_apolice if numero_apolice != 'nan' and numero_apolice != '' else None
    
    seguradora = str(row.get('Seguradora', '')).strip()
    seguradora = seguradora if seguradora != 'nan' and seguradora != '' else None
    
    # CNPJ Seguradora
    cnpj_seguradora = str(row.get('CNPJ Seguradora', '')).strip()
    if cnpj_seguradora and cnpj_seguradora != 'nan' and cnpj_seguradora != '':
        cnpj_seguradora = cnpj_seguradora.replace('.', '').replace('/', '').replace('-', '')
        cnpj_seguradora = int(cnpj_seguradora) if cnpj_seguradora else None
    else:
        cnpj_seguradora = None
    
    # 🔥 CORREÇÃO PRINCIPAL: Tratamento robusto para data
    vigencia_str = str(row.get('Vigencia', '')).strip()
    vigencia = None  # Valor padrão como None
    
    if vigencia_str and vigencia_str != 'nan' and vigencia_str != '':
        try:
            # Tenta converter para data - vários formatos possíveis
            if '/' in vigencia_str:
                # Formato brasileiro: DD/MM/YYYY
                vigencia = datetime.strptime(vigencia_str, '%d/%m/%Y').date()
            elif '-' in vigencia_str:
                # Formato internacional: YYYY-MM-DD
                vigencia = datetime.strptime(vigencia_str, '%Y-%m-%d').date()
            else:
                # Tenta outros formatos ou usa como string e deixa o PostgreSQL converter
                vigencia = vigencia_str
        except (ValueError, TypeError):
            # Se não conseguir converter, mantém como None
            vigencia = None
    
    # Percentuais
    rc_fdc = str(row.get('RCFDC', '0')).strip().replace('%', '').replace(',', '.')
    rc_fdc = float(rc_fdc) / 100 if rc_fdc and rc_fdc != 'nan' and rc_fdc != '' else 0.0
    
    rc_trc = str(row.get('RCTRC', '0')).strip().replace('%', '').replace(',', '.')
    rc_trc = float(rc_trc) / 100 if rc_trc and rc_trc != 'nan' and rc_trc != '' else 0.0
    
    tipo_seguro = str(row.get('TIPO DE SEGURO', '')).strip()
    tipo_seguro = tipo_seguro if tipo_seguro != 'nan' and tipo_seguro != '' else None
    
    registro = (cnpj, cliente, numero_apolice, seguradora, cnpj_seguradora, vigencia, rc_fdc, rc_trc, tipo_seguro)
    registros.append(registro)

sql = """
INSERT INTO seguro.clt_seguro (cnpj, cliente, numero_apolice, seguradora, 
                              cnpj_seguradora, vigencia, rc_fdc, rc_trc, tipo_seguro)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

try:
    execute_batch(cur, sql, registros, page_size=100)
    conn.commit()
    print(f"✅ Dados inseridos com sucesso! Linhas: {len(registros)}")
    
except Exception as e:
    print(f"❌ Erro: {e}")
    conn.rollback()

finally:
    cur.close()
    conn.close()