import pandas as pd
import xml.etree.cElementTree as ET

from utils import setup_logger

logger = setup_logger(__name__)

class WikipediaAbstractETL:

    def __init__(self, wikiabstract_path: str):
        self.parser = ET.iterparse(wikiabstract_path)
        self.dict_list = []
    
    def parse_data(self, urls: set):
        self.run_parser(urls)
        return pd.DataFrame(self.dict_list)

    def run_parser(self, urls: set):
        for event, elem in self.parser:
            if elem.tag == "doc" and elem.findall('title')[0].text:
                url = elem.findall('url')[0].text
                if url in urls:
                    title = elem.findall('title')[0].text[10:]
                    abstract = elem.findall('abstract')[0].text
                    self.dict_list.append({
                        'wikipedia_title': title,
                        'url': url,
                        'abstract': abstract
                    })
                elem.clear()