import boto3
from bs4 import BeautifulSoup

# Inicializar cliente S3
s3 = boto3.client('s3')

def detect_newspaper(key, soup):
    """
    Detecta el periódico basado en el nombre del archivo o la estructura del HTML.
    """
    if "portafolio" in key:
        return "portafolio"
    elif "eltiempo" in key:
        return "eltiempo"

    # Heurística basada en estructuras HTML
    if soup.find('article', class_='c-article'):  # El Tiempo
        return "eltiempo"
    elif soup.find('article'):  # Portafolio
        return "portafolio"
    else:
        return "desconocido"

def extract_headlines_portafolio(soup):
    """
    Extrae titulares, categorías y enlaces del sitio web de Portafolio.
    """
    headlines = []
    for article in soup.find_all('article'):
        category = article.get('data-category', None)
        if not category:
            category_tag = article.find('p', class_='tarjeta__categoria')
            category = category_tag.get_text(strip=True) if category_tag else "Sin categoria"

        title = article.get('data-name', None)
        if not title:
            title_tag = article.find('p', class_='tarjeta__titulo')
            title = title_tag.get_text(strip=True) if title_tag else "Sin titular"

        title = '"' + title.replace('"', '""') + '"'

        link = "Sin enlace"
        link_tag = article.find('a', href=True)
        if link_tag:
            link = "https://www.portafolio.co" + link_tag['href'] if link_tag['href'].startswith("/") else link_tag['href']

        headlines.append((category, title, link))
    return headlines

def extract_headlines_eltiempo(soup):
    """
    Extrae titulares, categorías y enlaces del sitio web de El Tiempo.
    """
    headlines = []
    for article in soup.find_all('article', class_='c-article'):
        category = article.get('data-category', 'Sin categoría')

        title_tag = None
        title = article.get('data-name', None)
        if not title:
            title_tag = article.find('h3', class_='c-article__title')
            title = title_tag.get_text(strip=True) if title_tag else "Sin titular"

        title = '"' + title.replace('"', '""') + '"'

        link = "Sin enlace"
        if title_tag:
            link_tag = title_tag.find('a')
            if link_tag and 'href' in link_tag.attrs:
                link = link_tag['href']

        if link == "Sin enlace":
            secondary_link_tag = article.find('a', href=True)
            if secondary_link_tag:
                link = secondary_link_tag['href']

        headlines.append((category, title, link))
    return headlines

def process_and_store(bucket_name, key):
    try:
        print(f"Procesando archivo: {key} desde el bucket: {bucket_name}")

        response = s3.get_object(Bucket=bucket_name, Key=key)
        html_content = response['Body'].read().decode('utf-8')

        soup = BeautifulSoup(html_content, 'html.parser')

        newspaper = detect_newspaper(key, soup)
        print(f"Periódico detectado: {newspaper}")

        if newspaper == "portafolio":
            headlines = extract_headlines_portafolio(soup)
        elif newspaper == "eltiempo":
            headlines = extract_headlines_eltiempo(soup)
        else:
            print(f"No se pudo determinar el periódico para el archivo {key}")
            return

        if key.startswith("headlines/raw/"):
            filename = key.replace("headlines/raw/", "").replace(".html", "")
            parts = filename.split("-")

            print(f"Partes extraídas del nombre del archivo: {parts}")

            if len(parts) >= 4:  # Validar el formato del nombre
                periodico = parts[0]
                year, month, day = parts[1], parts[2], parts[3]
                csv_key = f"headlines/final/periodico={periodico}/year={year}/month={month}/day={day}/headlines.csv"
                print(f"Ruta generada para el archivo CSV: {csv_key}")

                csv_buffer = "Categoría,Titular,Enlace\n"
                for row in headlines:
                    csv_buffer += ",".join(row) + "\n"

                s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer, ContentType='text/csv')
                print(f"Archivo CSV guardado exitosamente en {csv_key}")
            else:
                print(f"El nombre del archivo {filename} no tiene el formato esperado.")

        else:
            print(f"El archivo {key} no está en la carpeta 'raw/'.")
    except Exception as e:
        print(f"Error al procesar {key}: {e}")

def lambda_recive(event, context):
    try:
        print(f"Evento recibido: {event}")

        if "Records" in event and event["Records"]:
            for record in event['Records']:
                bucket_name = record['s3']['bucket']['name']
                key = record['s3']['object']['key']

                print(f"Registro recibido: Bucket={bucket_name}, Key={key}")

                if key.startswith("headlines/raw/") and key.endswith(".html"):
                    print(f"El archivo {key} está en la carpeta 'raw/' y es un archivo HTML.")
                    process_and_store(bucket_name, key)
                else:
                    print(f"El archivo {key} no pertenece a la carpeta 'raw/' o no es un archivo HTML.")
        else:
            print("Evento no soportado o no relacionado con S3.")
    except Exception as e:
        print(f"Error general en Lambda: {e}")