"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    import os
    import zipfile
    import pandas as pd
    from glob import glob

    # Create output directory if it doesn't exist
    os.makedirs("files/output", exist_ok=True)

    # Read all compressed CSV files
    input_files = glob("files/input/bank-marketing-campaing-*.csv.zip")
    
    dataframes = []
    for file in sorted(input_files):
        with zipfile.ZipFile(file, 'r') as z:
            # Get the name of the CSV file inside the zip
            csv_filename = z.namelist()[0]
            with z.open(csv_filename) as f:
                df = pd.read_csv(f)
                dataframes.append(df)
    
    # Concatenate all dataframes
    data = pd.concat(dataframes, ignore_index=True)
    
    # Reset client_id
    data['client_id'] = range(len(data))
    
    # ===== CLIENT.CSV =====
    client = pd.DataFrame()
    client['client_id'] = data['client_id']
    client['age'] = data['age']
    
    # Clean job: replace "." with "" and "-" with "_"
    client['job'] = data['job'].str.replace('.', '', regex=False).str.replace('-', '_')
    
    client['marital'] = data['marital']
    
    # Clean education: replace "." with "_" and "unknown" with pd.NA
    client['education'] = data['education'].str.replace('.', '_', regex=False)
    client['education'] = client['education'].replace('unknown', pd.NA)
    
    # credit_default: "yes" -> 1, others -> 0
    client['credit_default'] = (data['credit_default'] == 'yes').astype(int)
    
    # mortgage: "yes" -> 1, others -> 0
    client['mortgage'] = (data['mortgage'] == 'yes').astype(int)
    
    # ===== CAMPAIGN.CSV =====
    campaign = pd.DataFrame()
    campaign['client_id'] = data['client_id']
    campaign['number_contacts'] = data['number_contacts']
    campaign['contact_duration'] = data['contact_duration']
    campaign['previous_campaign_contacts'] = data['previous_campaign_contacts']
    
    # previous_outcome: "success" -> 1, others -> 0
    campaign['previous_outcome'] = (data['previous_outcome'] == 'success').astype(int)
    
    # campaign_outcome: "yes" -> 1, others -> 0
    campaign['campaign_outcome'] = (data['campaign_outcome'] == 'yes').astype(int)
    
    # Create last_contact_date in format "YYYY-MM-DD"
    month_map = {
        'jan': '01', 'feb': '02', 'mar': '03', 'apr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'aug': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dec': '12'
    }
    
    campaign['last_contact_date'] = data.apply(
        lambda row: f"2022-{month_map[row['month']]}-{str(row['day']).zfill(2)}",
        axis=1
    )
    
    # ===== ECONOMICS.CSV =====
    economics = pd.DataFrame()
    economics['client_id'] = data['client_id']
    economics['cons_price_idx'] = data['cons_price_idx']
    economics['euribor_three_months'] = data['euribor_three_months']
    
    # Save files
    client.to_csv("files/output/client.csv", index=False)
    campaign.to_csv("files/output/campaign.csv", index=False)
    economics.to_csv("files/output/economics.csv", index=False)
    
    return


if __name__ == "__main__":
    clean_campaign_data()