from time import sleep
import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
import random
from unidecode import unidecode
from datetime import datetime
import pytz
import os
import time
from destinos import Destinos
import pprint
from sendemail import SendEmail


class ETL:
    def __init__(self) -> None:
        self.DATE = self.getDate()
        self.URL ='https://www.turismocity.com.ar/vuelos-baratos-a-'
        self.FROM = '?from=BUE'
        self.porcentaje = 0.90
        self.ejecuciones_diarias = 4
        self.dias_atras = 30

    def getDate(self) -> str:
        argentina_timezone = pytz.timezone('America/Argentina/Buenos_Aires')
        utc_now = datetime.utcnow()
        argentina_now = utc_now.replace(tzinfo=pytz.utc).astimezone(argentina_timezone)
        formatted_date = argentina_now.strftime('%Y-%m-%d %H:%M:%S')
        return formatted_date
    
    
    def parseToFloatOrInt(self, data) -> any:
        if data != '' and '.' in data:
            return float(data)
        elif data != '' and '.' not in data:
            return int(data)
        else:
            return None

    def getSoup(self, url):
        user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:58.0) Gecko/20100101 Firefox/58.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/13.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36 Edg/83.0.478.37",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.102 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.96 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:93.0) Gecko/20100101 Firefox/93.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:92.0) Gecko/20100101 Firefox/92.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Version/15.0.1 Safari/605.1.15",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36 Edg/89.0.774.57",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.164 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.54 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:94.0) Gecko/20100101 Firefox/94.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36"
        ]


        headers = {
            "User-Agent": random.choice(user_agents),
            "Accept-Language": "en-US,en;q=0.9",
        }

        sleep(random.randint(1, 2))
        response = self.getRequests(url, headers)
        soup = BS(response.content, 'html.parser')
        return soup

    def getRequests(self, url, headers):
        delay = 2
        retry = 0
        limit = 5
        while retry < limit:
            try:
                res = requests.get(url,headers=headers)
                if res.status_code == 200:
                    return res
                else:
                    print("Error: la request fue: " + str(res.status_code)+ " se sigue reintentando..")
                    time.sleep(delay * retry)
            except ConnectionError:
                print("No se puedo conectar")
                retry += 1
                continue
            break
        if retry==limit:
            raise(f"status code {str(res.status_code)}")


    def run(self):
        destinos = Destinos()
        destinos = destinos.getDestinos()    

        datos = []

        for destino in destinos:
            url = self.URL + destino.get('code') + self.FROM
            soup = self.getSoup(url)
            importe_span = soup.find('div', class_='container-wrapper')
            importe_text = importe_span.find('span', class_='landing-baratos-tabs active loading-placeholder loading')
            importe = importe_text.text.split('$')[1].strip().replace('.','')
            data = {
                        'pais': destino.get('pais'),
                        'aeropuerto' : destino.get('code'),
                        'importe' : int(importe),
                        'date' : self.DATE
                    }
            
            datos.append(data)

        df = pd.DataFrame(datos)

        file_name = os.path.join('data', "datatravel.csv")
        if os.path.exists(file_name):
            existing_df = pd.read_csv(file_name)
            validation = self.DATE in existing_df['date'].values
            if validation == False:
                existing_df = pd.concat([existing_df, df], ignore_index=True)
                existing_df = existing_df.drop_duplicates()
                existing_df.sort_values(by='date').to_csv(file_name, index=False)
        else:
            df.sort_values(by='date').to_csv(file_name, index=False)

        
        df = pd.read_csv(file_name)
        ultimos_importes = df.groupby('pais')['importe'].last()
        df_sin_ultimos_importes = df.iloc[:-len(destinos)]

        datos = []

        for pais, ultimo_importe in ultimos_importes.items():
            try:
                df_filtrado = df_sin_ultimos_importes[df_sin_ultimos_importes['pais'] == pais].tail(self.ejecuciones_diarias * self.dias_atras)
                promedio_importe = int(df_filtrado['importe'].mean())
                ultimo_importe = int(ultimo_importe)
                condition_descuento = self.porcentaje * promedio_importe
                condition_aumento = ((1-self.porcentaje ) +1) * promedio_importe
            
                if ultimo_importe<= condition_descuento:

                    data_rebajas = {
                        'pais' : pais,
                        'importe' : ultimo_importe,
                        'precio_anterior' : round(promedio_importe),
                        'variacion' : round(((ultimo_importe - promedio_importe) / promedio_importe) *100)
                    }
                    datos.append(data_rebajas)

                if ultimo_importe > condition_aumento:
                    data_aumentos = {
                        'pais' : pais,
                        'importe' : ultimo_importe,
                        'precio_anterior' : round(promedio_importe),
                        'variacion' : round(((ultimo_importe - promedio_importe) / promedio_importe) *100)
                    }
                    datos.append(data_aumentos)
            except:
                continue

        if len(datos) > 0:
            correo = os.environ.get('EMAIL')
            pss = os.environ.get('PSS')
            email = SendEmail(datos, correo, pss)
            email.run()

            
if __name__ == "__main__":
    app = ETL()
    app.run()
