from selenium.webdriver import Firefox
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

class FirefoxBrowser:
    
    def start_session(self, headless: bool) -> Firefox:
        """
        Inicia uma instância do firefox

        Args:
            headless (bool): True or False

        Returns:
            browser (Firefox): Webdriver Selenium Firefox or None se a coneção não for estabelecida
        """
        
        #Garante que o webdriver irá encontrar os drivers certos para as versões certas do windows
        path_geckodriver_32bit = r'.\Drivers & Cookies\geckodriver_32bit.exe'
        path_geckodriver_64bit = r'.\Drivers & Cookies\geckodriver_64bit.exe'
        
        options = FirefoxOptions()
        options.add_argument("window-size=1920,1080")
        options.headless = headless   

        try:
            browser = Firefox(service=Service(path_geckodriver_32bit), options=options)
        except:
            browser = Firefox(service=Service(path_geckodriver_64bit), options=options)

        return browser

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


if __name__ == '__main__':
    
    browser = FirefoxBrowser()
    driver = browser.start_session(False)
    driver.quit()
    
   
    