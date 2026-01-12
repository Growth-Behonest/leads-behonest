"""
Script para substituir origem 'Facebook' por 'Meta Ads'
"""
import pandas as pd

# Carregar o CSV
df = pd.read_csv('leads_sults_consolidado.csv', sep=';', encoding='utf-8-sig')

print(f"Total de leads: {len(df)}")

# Contar leads com origem = Facebook
facebook_count = len(df[df['origem'] == 'Facebook'])
print(f"Leads com origem 'Facebook': {facebook_count}")

# Substituir Facebook por Meta Ads
df['origem'] = df['origem'].replace('Facebook', 'Meta Ads')

# Verificar
meta_ads_count = len(df[df['origem'] == 'Meta Ads'])
print(f"Leads com origem 'Meta Ads' após substituição: {meta_ads_count}")

# Salvar
df.to_csv('leads_sults_consolidado.csv', sep=';', index=False, encoding='utf-8-sig')

print("\n✅ Arquivo atualizado com sucesso!")
