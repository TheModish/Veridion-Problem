import pandas as pd
import numpy as np
from rapidfuzz import process, fuzz

# definire cailor pentru fisiere
input_file = "veridion_product_deduplication_challenge.snappy.parquet"
output_file = "produse.xlsx"

# incarcare baza date
df = pd.read_parquet(input_file)

# conversie excel
df.to_excel(output_file, index=False)

# identificarea coloanelor in mod dinamic
numerical_cols = df.select_dtypes(include=[np.number]).columns.tolist()
text_cols = df.select_dtypes(exclude=[np.number]).columns.tolist()
text_cols.remove("product_name") # Asigură că "product_name" este utilizat doar pentru grupare

def duplicate(df, group_by_col, threshold=90):


    def combina_valori_text(series):
        """Combină valorile textuale unice eficient, separându-le cu '|'"""
        return ' | '.join(series.dropna().astype(str).unique())

    def combina_valori_numerice(series):
        """Gestionarea valorilor numerice prin calculul mediei, ignorând valorile NaN."""
        return series.mean(skipna=True)

    # Pasul 1: Eliminarea duplicatelor prin potrivire exactă
    merged_df = df.groupby(group_by_col, as_index=False).agg({
        **{col: combina_valori_text for col in text_cols},
        **{col: combina_valori_numerice for col in numerical_cols}
    })

    # Pasul 2: Potrivire fuzzy pentru nume similare
    unique_names = merged_df[group_by_col].tolist() # Extrage lista numelor unice ale produselor
    merged_pairs = {}# Dicționar pentru a stoca perechile de potrivire

    for name in unique_names:
        match_data = process.extractOne(name, unique_names, scorer=fuzz.ratio)
        if match_data and match_data[1] > threshold and name != match_data[0]:
            merged_pairs[name] = match_data[0]  # Stochează potrivirea

    # Aplicarea combinării fuzzy
    for name, match in merged_pairs.items():
        merged_df.loc[merged_df[group_by_col] == match, text_cols] = merged_df.loc[
            merged_df[group_by_col] == match, text_cols
        ].applymap(combina_valori_text)

    merged_df.drop_duplicates(subset=[group_by_col], keep="first", inplace=True) # Elimină rândurile duplicate

    return merged_df

# Aplică deduplicarea îmbunătățită
df_duplicat = duplicate(df, "produse")

# Salvarea dataset-ului curățat
output_file = "produse_unice.xlsx"
df_duplicat.to_excel(output_file, index=False)

print(f"Operatie completa. Salvata ca '{output_file}'.")