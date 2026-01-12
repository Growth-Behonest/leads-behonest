"""
Script para remover leads com motivo_perda = DUPLICADO ou Teste
"""
import pandas as pd

# Carregar o CSV com encoding UTF-8
df = pd.read_csv('leads-dashboard/public/leads_sults_consolidado.csv', sep=';', encoding='utf-8-sig')

print(f"Total de leads antes da limpeza: {len(df)}")
print()

# Verificar valores únicos de motivo_perda (case insensitive)
print("Valores de motivo_perda que serão removidos:")
mask_duplicado = df['motivo_perda'].str.upper().str.strip() == 'DUPLICADO'
mask_teste = df['motivo_perda'].str.upper().str.strip() == 'TESTE'

duplicados = df[mask_duplicado]
testes = df[mask_teste]

print(f"  - DUPLICADO: {len(duplicados)} registros")
print(f"  - Teste: {len(testes)} registros")
print(f"  Total a remover: {len(duplicados) + len(testes)} registros")
print()

# Remover os registros
df_limpo = df[~(mask_duplicado | mask_teste)]

print(f"Total de leads após a limpeza: {len(df_limpo)}")
print(f"Leads removidos: {len(df) - len(df_limpo)}")

# Salvar o arquivo atualizado
df_limpo.to_csv('leads-dashboard/public/leads_sults_consolidado.csv', sep=';', index=False, encoding='utf-8-sig')

print()
print("✅ Arquivo 'leads-dashboard/public/leads_sults_consolidado.csv' atualizado com sucesso!")
