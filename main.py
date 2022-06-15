
from sysconfig import get_makefile_filename
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver import Chrome, ChromeOptions
from selenium.common.exceptions import StaleElementReferenceException

import pandas as pd
import schedule
import keyboard
from time import sleep
import psutil
import smtplib, email.message
from datetime import datetime
import traceback

import mysql.connector as database
from mysql.connector.errors import ProgrammingError

pd.set_option('mode.chained_assignment', None)

#Credenciais de e-mail para receber atualizações de erros.
USERNAME = "your username"
PASSWORD = 'your password'
RECIEVER = 'e-mail that will receive updates'
PORT = 587

class SeleniumSession:
    """
    Context manager para que a navegador feche automaticamente em caso de algum erro.
    """
    def __init__(self, browser):
        self.browser = browser
    def __enter__(self):
        return self.browser
    def __exit__(self, type, value, traceback):
        self.browser.quit()    
        
class RetrieveData:
    
    def __init__(self, browser):
        self.browser = browser
        
        url = 'https://pt.surebet.com/surebets'
        self.browser.get(url)
        
        button_path = '//li[@class="nav-item"]//child::a[starts-with(@class, "autoupdate-link")]'
        self.browser.find_element(By.XPATH, button_path).click()
                                  
    def parse_information(self):

        data = self.browser.find_element(By.XPATH, '//table[@class="app-table app-wide"]').get_attribute('outerHTML') 
    
        df = pd.read_html(data)[0]
        df= df[df.columns[1:7]]
        df = df.drop(df.index[2::3])
        
        if len(df):
        
            game = self.browser.find_elements(By.XPATH, '//table[@class="app-table app-wide"]//child::td[@class="booker"]//span[@class="minor"]')
            games = [value.get_attribute("textContent") for value in game]
                
            partida = self.browser.find_elements(By.XPATH, '//table[@class="app-table app-wide"]//child::td[starts-with(@class,"event")]//span')
            partidas = [value.get_attribute("textContent") for value in partida]    
            
            lucro_column = df[tuple(df.columns)[0]].str.split('  ', expand=True)
            lucro_column.rename(columns={0: 'Porcentagem Lucro', 1: 'Tempo de aposta'}, inplace=True)
            
            links = self.browser.find_elements(By.XPATH, '//table[@class="app-table app-wide"]//child::td[starts-with(@class,"event")]//a')
            links = [value.get_attribute("href") for value in links]
            
            info = self.browser.find_elements(By.XPATH, '//table[@class="app-table app-wide"]//child::td[@class="coeff"]//abbr')
            info = [value.get_attribute("title") for value in info]
            
            games_new = []
            casa_jogos = []
            for casa, game in zip(df[tuple(df.columns)[1]], games):
                if game in casa:
                    casa = casa.replace(game, '')
                    games_new.append(game)
                else:
                    games_new.append("Esporte não Informado")
                    
                casa_jogos.append(casa)
                
            casa_jogos = {old: new for old, new in zip(df[tuple(df.columns)[1]], casa_jogos)}
            df[tuple(df.columns)[1]].replace(casa_jogos, inplace=True)
            
            new_tempo = []
            for value in df[tuple(df.columns)[2]]:
                value_list = list(value)
                value_list.insert(5, "-")
                value = "".join(value_list)
                new_tempo.append(value)

            tempo_values = {old: new for old, new in zip(df[tuple(df.columns)[2]], new_tempo)}
            df[tuple(df.columns)[2]].replace(tempo_values, inplace=True)
            tempo_column = df[tuple(df.columns)[2]].str.split('-', expand=True)
            tempo_column.rename(columns={0: 'Dia', 1: 'Hora'}, inplace=True)
            
            eventos = []
            partidas_new = []
            for evento, partida in zip(df[tuple(df.columns)[3]], partidas):
                if partida in evento:
                    evento = evento.replace(partida, '')
                    partidas_new.append(partida)
                else:
                    partidas_new.append("Evento info sem nome")
                eventos.append(evento)
                
                
            eventos = {old: new for old, new in zip(df[tuple(df.columns)[3]], eventos)}
            df[tuple(df.columns)[3]].replace(eventos, inplace=True)
            
            column_names = [tuple(df.columns)[0], tuple(df.columns)[2], tuple(df.columns)[1]] + [tuple(df.columns)[i] for i in range(3, len(tuple(df.columns))) ]

            df = df.reindex(columns=column_names)
            
            df.insert(2, 'Esporte', games_new)
            df.insert(5, 'Evento info', partidas_new)

            
            df.drop([tuple(df.columns)[0], tuple(df.columns)[1]], axis=1, inplace=True)
            
            bigdata = pd.concat([lucro_column, tempo_column,  df], axis=1)
            
            lucro_column = {value: str(value)[:4].replace(",",".") for value in bigdata[tuple(bigdata.columns)[0]]}
            bigdata[tuple(bigdata.columns)[0]].replace(lucro_column, inplace=True)

            prob_column = {value: str(value)[:4] for value in bigdata[tuple(bigdata.columns)[-1]]}
            bigdata[tuple(bigdata.columns)[-1]].replace(prob_column, inplace=True)
            
            new_bigdata_1 = bigdata.iloc[0::2]
            new_bigdata_1.rename(columns={"Casa de aposta": 'Casa de aposta 1', "Evento": 'Evento 1', "Evento info": 'Evento 1 info', "Mercado": 'Mercado 1', "Probabilidade": 'Probabilidade 1'}, inplace=True)
            new_bigdata_1.reset_index(drop=True, inplace=True)
            
            new_bigdata_2 = bigdata.iloc[1::2][["Casa de aposta", "Evento", "Evento info", "Mercado", "Probabilidade"]]
            new_bigdata_2.rename(columns={'Casa de aposta': 'Casa de aposta 2', "Evento": 'Evento 2', "Evento info": 'Evento 2 info', "Mercado": 'Mercado 2', "Probabilidade": 'Probabilidade 2'}, inplace=True)
            new_bigdata_2.reset_index(drop=True, inplace=True)
            
            bigdata = pd.concat([new_bigdata_1, new_bigdata_2], axis=1)
            
            bigdata.insert(8, 'Evento 1 link', links[::2])
            bigdata.insert(10, 'Mercado 1 info', info[::2])
            bigdata.insert(14, 'Evento 2 link', links[1::2])
            bigdata.insert(17, 'Mercado 2 info', info[1::2])
            
            path_apostas_segura = '//table[@id="surebets-table"]//child::td[@class="extra"]//a'
            apostas_seguras = browser.find_elements(By.XPATH, path_apostas_segura)
            links = [item.get_attribute('href') for item in apostas_seguras]
            
            return bigdata, links
        
        else:
            
            bigdata, links = [], []
            
            return bigdata, links           

                     
    def parse_next(self, data):
        
        old_data, links = data
        
        if len(links) > 0 and len(old_data) > 0:
            for link in links:
                
                browser.switch_to.new_window()
                browser.get(link)  
                new_data = self.parse_information()[0]
                
                if len(new_data) > 0:
                    
                    complete_data = pd.concat([old_data, new_data], axis=0)
            
                    old_data = complete_data
                    
                browser.close()
                browser.switch_to.window(browser.window_handles[0])
                    
            complete_data = old_data 
            complete_data.reset_index(drop=True, inplace=True)
    
            return complete_data 
        else:
            return old_data    
           
class DataBase:
    
    def __init__(self):
        
        self.data_base = database.connect(
            host ="", 
            user = ",
            passwd = "",
            database = ""
        ) 
        
        self.cursor = self.data_base.cursor()
        
    def save(self, data):
        
        head = [value.replace(" ", "_").upper() for value in tuple(data.columns)]
        info = f"""CREATE TABLE if not exists SUREBET (
                        ID  int not null,
                        {head[0]}  varchar(5),
                        {head[1]}  VARCHAR(10),
                        {head[2]}  VARCHAR(10),
                        {head[3]}  VARCHAR(10),
                        {head[4]}  VARCHAR(50),
                        {head[5]}  VARCHAR(100),
                        {head[6]}  VARCHAR(100),
                        {head[7]}  VARCHAR(100),
                        {head[8]}  VARCHAR(100),
                        {head[9]}  varchar(100),
                        {head[10]}  varchar(100),
                        {head[11]}  varchar(100),
                        {head[12]}  varchar(100),
                        {head[13]}  varchar(100),
                        {head[14]}  varchar(100),
                        {head[15]}  varchar(100),
                        {head[16]}  varchar(100),
                        {head[17]}  varchar(100),
                        {head[18]}  varchar(100),
                        primary key (ID)
                        ) default charset = utf8;"""
                        
        self.cursor.execute(info)
        
        for item in data.itertuples():
            sql = f"INSERT INTO SUREBET (ID , {head[0]} , {head[1]} , {head[2]} , {head[3]} , {head[4]} , {head[5]} , \
            {head[6]} , {head[7]} , {head[8]}, {head[9]} , {head[10]} , {head[11]} , {head[12]}, {head[13]}, {head[14]}, \
            {head[15]}, {head[16]}, {head[17]}, {head[18]})\
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val =  tuple(item)
            
            self.cursor.execute(sql, val)
            self.data_base.commit()
            
    def delete(self):
         
        delete = "DELETE FROM SUREBET;"
        self.cursor.execute(delete)
        self.data_base.commit()  
         
    def quit(self):
        
        self.data_base.close()
        
if __name__ == '__main__':
    
    path_chrome_driver = r'.\Drivers & Cookies\chromedriver.exe'
    options = ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_argument(f"--user-data-dir=C:\\Users\\{psutil.users()[0][0]}\\AppData\\Local\\Google\\Chrome\\User Data")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    title = "PROGRAMA PARA SALVAR DADOS DO SUREBET"
    size = len(title)
    
    print()
    print(f'{size*"_"}')
    print(title)
    print(f'{size*"_"}')
    print()
    
    with SeleniumSession(Chrome(service=Service(path_chrome_driver), options=options)) as browser:
        
    # browser = Chrome(service=Service(path_chrome_driver), options=options)
    
        data_base = DataBase()
                
        init = RetrieveData(browser)

        print("\nSelecione as opções que deseja como filtro no navegador")
        input('Depois de selecionado, pressione "ENTER" para continuar...')

        #def job(): 
            #data = init.parse_information()
            #SavaExcel(data)
            
        def job(): 
            try:
                data_base.delete()
            except ProgrammingError:
                pass
            finally:
                
                while True:
                    try:
                        information = init.parse_information()
                        data = init.parse_next(information)
                        break
                    except StaleElementReferenceException:
                        print("\nStaleElementReferenceException.Programa Irá tentar conectar novamente.")
                    except ValueError:
                        print("\nValueError. Programa Irá tentar conectar novamente.")    
                
                if len(data) > 0:
                    data_base.save(data)
        print("\nPrograma ficará em looping até usuário pressionar 'Esc' por alguns segundos para sair")

        schedule.every(2).minutes.do(job)
        i = 0
        while True:
            try:
                schedule.run_pending()  
                sleep(1)
                if keyboard.is_pressed('Esc'):
                    data_base.quit()
                    print('\nPrograma está encerrando operação...')
                    sleep(3)
                    break 
                i = 0         
            except Exception as e:
                
                if i == 10:
                    message = f"""
<meta charset="utf-8">  
<p>Programa parou de funcionar</p>
<p>Dia/Horário: {datetime.now()}</p>      
<p>Conteúdo da mensagem de erro: {traceback.format_exc()}</p>
    
Mensagem enviada automaticamente pelo "PROGRAMA PARA SALVAR DADOS DO SUREBET".<p>
"""
                    msg = email.message.Message()
                    msg["Subject"] = "Programa falhou devido a um erro"
                    msg["From"] = USERNAME
                    msg["To"] = RECIEVER
                    msg.add_header("Content-Type", "text/html")
                    msg.set_payload(message) 
                    
                    with smtplib.SMTP('smtp.gmail.com', PORT) as server:
                        server.starttls()
                        server.login(USERNAME, PASSWORD)
                        server.sendmail(msg["From"], msg["To"], msg.as_string().encode("utf-8")) 
                    print(f"O programa deu um erro: um e-mail foi enviado, mas o programa tentará rodar novamente por cerca de {i + 1}/10")
                    break
                i +=1
                
                
