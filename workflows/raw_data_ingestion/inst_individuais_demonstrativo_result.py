import time
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

download_dir = "/home/daiane/projetos/finance_data/data/demonstracao_resultados_individuais"  
if not os.path.exists(download_dir):
    os.makedirs(download_dir)

options = webdriver.ChromeOptions()
prefs = {"download.default_directory": download_dir,
         "download.prompt_for_download": False,
         "safebrowsing.enabled": True}
options.add_experimental_option("prefs", prefs)


driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

url = "https://www3.bcb.gov.br/ifdata/#"
driver.get(url)

datas = ["03/2024", "12/2023", "09/2023", "06/2023", "03/2023", "12/2022", "09/2022", "06/2022", "03/2022", "12/2021", "09/2021", "06/2021", "03/2021", "12/2020", "09/2020", "06/2020", "03/2020", "12/2019", "09/2019", "06/2019", "03/2019", "12/2018", "09/2018", "06/2018", "03/2018", "12/2017", "09/2017", "06/2017", "03/2017", "12/2016", "09/2016", "06/2016", "03/2016", "12/2015", "09/2015", "06/2015", "03/2015", "12/2014", "09/2014", "06/2014", "03/2014", "12/2013", "09/2013", "06/2013", "03/2013", "12/2012", "09/2012", "06/2012", "03/2012", "12/2011", "09/2011", "06/2011", "03/2011", "12/2010", "09/2010", "06/2010", "03/2010", "12/2009", "09/2009", "06/2009", "03/2009", "12/2008", "09/2008", "06/2008", "03/2008", "12/2007", "09/2007", "06/2007", "03/2007", "12/2006", "09/2006", "06/2006", "03/2006", "12/2005", "09/2005", "06/2005", "03/2005", "12/2004", "09/2004", "06/2004", "03/2004", "12/2003", "09/2003", "06/2003", "03/2003", "12/2002", "09/2002", "06/2002", "03/2002", "12/2001", "09/2001", "06/2001", "03/2001", "12/2000", "09/2000", "06/2000", "03/2000"] 


def aguardar_elemento_clicavel(driver, by, selector):
    return WebDriverWait(driver, 10).until(EC.element_to_be_clickable((by, selector)))

def selecionar_opcao_por_texto(driver, xpath, texto):
    option = aguardar_elemento_clicavel(driver, By.XPATH, xpath)
    driver.execute_script("arguments[0].click();", option)

for data in datas:
    print(f"Processando data: {data}")

    try:
        aguardar_elemento_clicavel(driver, By.ID, "btnDataBase").click()
        time.sleep(20)
        selecionar_opcao_por_texto(driver, f"//a[contains(text(), '{data}')]", data)
        print(f"Data-base selecionada: {data}")
        time.sleep(20)

        aguardar_elemento_clicavel(driver, By.ID, "btnTipoInst").click()
        time.sleep(20)
        selecionar_opcao_por_texto(driver, "//a[contains(text(), 'Instituições Individuais')]", "Instituições Individuais")
        print("Tipo de Instituição selecionado: Instituições Individuais")
        time.sleep(20)

        aguardar_elemento_clicavel(driver, By.ID, "btnRelatorio").click()
        time.sleep(20)
        selecionar_opcao_por_texto(driver, "//a[contains(text(), 'Demonstração de Resultado')]", "Demonstração de Resultado")
        print("Tipo de Relatório selecionado: Demonstração de Resultado")
        time.sleep(20)

        aguardar_elemento_clicavel(driver, By.LINK_TEXT, "CSV").click()
        print(f"Download iniciado para a data: {data}")
        time.sleep(10)

        latest_file = max([os.path.join(download_dir, f) for f in os.listdir(download_dir)], key=os.path.getctime)
        new_name = f"dados_{data.replace('/', '-')}.csv"
        os.rename(latest_file, os.path.join(download_dir, new_name))
        print(f"Arquivo renomeado para: {new_name}")

    except Exception as e:
        print(f"Erro ao processar a data {data}: {e}")

driver.quit()
