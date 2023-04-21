import requests
import pandas as pd
import json
import csv
from os import remove
from time import sleep, time
from datetime import date
from tqdm import tqdm
from bs4 import BeautifulSoup


class Parser:
    def __init__(self, json_path, output_path):
        current_date = str(date.today())
        self.html = self.read_json_config(json_path)
        self.output_path = '{}/cian-{}.csv'.format(output_path, current_date)

    @staticmethod
    def _counter(page_num):
        with open('.temp.csv', 'a') as infile:
            writer = csv.writer(infile)
            writer.writerow([page_num])

    @staticmethod
    def read_counter():
        with open('.temp.csv', 'r') as infile:
            reader = csv.reader(infile)
            *_, last = reader
        return int(last[0])

    @staticmethod
    def read_json_config(json_path):
        with open(json_path) as json_file:
            data = json.load(json_file)
        config_dict = {
            'url': data[0],
            'section': data[1],
            'page': data[2],
            'title': data[3],
            'main_inf': data[4],
            'gen_inf': data[5],
            'contact': data[6],
            'price': data[7]
        }
        return config_dict

    @staticmethod
    def extract_info_block(info_block):
        info = []

        for entry in info_block:
            for tag in entry:
                info.append(tag.text)

        keys = [key for i, key in enumerate(info, 1) if i % 2 == 0]
        values = [value for i, value in enumerate(info, 1) if i % 2 == 1]
        raw_info = {key: value for key, value in zip(keys, values)}

        return raw_info

    def extract_title(self, soup):
        title = soup.find(self.html['title']['flag'],
                          class_=self.html['title']['class']).text
        if title.split()[0] == 'Студия,':
            rooms_count = 0
            all_area = title.split()[1].replace(',', '.')
        else:
            rooms_count = title.split()[0][0]
            all_area = title.split()[2].replace(',', '.')
        data = {
            'RoomCount': rooms_count,
            'AllArea': all_area
        }
        return pd.Series(data=data)

    def extract_main_inf(self, soup):
        main_inf = soup.find_all(self.html['main_inf']['flag'],
                                 class_=self.html['main_inf']['class'])
        data = {}
        raw_info = self.extract_info_block(main_inf)

        for item in raw_info.items():
            if item[0] == 'Жилая':
                data['LivingArea'] = item[1].split()[0].replace(',', '.')
            elif item[0] == 'Кухня':
                data['KitchenArea'] = item[1].split()[0].replace(',', '.')
            elif item[0] == 'Этаж':
                data['Floor'] = item[1].split()[0]
                data['FloorsCount'] = item[1].split()[2]
            elif item[0] == 'Построен':
                data['FondationYear'] = item[1].split()[0]

        return pd.Series(data=data)

    def extract_gen_inf(self, soup):
        gen_inf = soup.find(self.html['gen_inf']['flag'],
                            class_=self.html['gen_inf']['class'])
        data = {}
        raw_info = self.extract_info_block(gen_inf)

        for item in raw_info.items():
            if item[1] == 'Тип жилья':
                data['HousingType'] = item[0]
            elif item[1] == 'Высота потолков':
                data['CeilingHeight'] = item[0].split()[0].replace(',', '.')
            elif item[1] == 'Санузел':
                data['Restroom'] = item[0]
            elif item[1] == 'Балкон/лоджия':
                data['Balcony/Loggia'] = item[0]
            elif item[1] == 'Ремонт':
                data['RenovationType'] = item[0]
            elif item[1] == 'Вид из окон':
                data['WindowView'] = item[0]

        return pd.Series(data=data)

    def extract_price(self, soup):
        price = soup.find(self.html['price']['flag'],
                          class_=self.html['price']['class']).text
        for char in ['\xa0', '₽', ' ']:
            price = price.replace(char, '')
        data = {'Price': price}

        return pd.Series(data=data)

    def extract_contact(self, section):
        contact = section.find(self.html['contact']['flag'],
                               class_=self.html['contact']['class'])
        entries = []
        for tag in contact:
            for entry in tag:
                entries.append(entry.text)
        try:
            contact_type = entries[0]
            contact_name = entries[1]
        except IndexError:
            contact_type = 'Собственник'
            contact_name = entries[0]
        data = {
            'ContactType': contact_type,
            'ContactName': contact_name
        }
        return pd.Series(data=data)

    def form_offer(self, offer_url, section):
        response = requests.get(offer_url)
        soup = BeautifulSoup(response.text, 'lxml')
        title = self.extract_title(soup)
        main_inf = self.extract_main_inf(soup)
        gen_inf = self.extract_gen_inf(soup)
        price = self.extract_price(soup)
        contact = self.extract_contact(section)
        offer = pd.concat([title, main_inf, gen_inf, contact, price])

        return offer

    def parse(self):
        start_time = time()
        params = {'page': 1}
        pages = 2  # To start iteration.
        df_columns = ['RoomCount', 'AllArea',
                      'LivingArea', 'KitchenArea',
                      'Floor', 'FloorsCount',
                      'ContactType', 'ContactName',
                      'FondationYear', 'HousingType',
                      'CeilingHeight', 'Restroom',
                      'Balcony/Loggia', 'RenovationType',
                      'WindowView', 'Price']
        df = pd.DataFrame(columns=df_columns)
        df.to_csv(self.output_path, index=False)

        while params['page'] <= pages: 
            if params['page'] % 3 == 0:  # To avoid ip ban.
                sleep(60)
            print(f'Page number: {params["page"]}')  # Debug.
            self._counter(params['page'])
            response = requests.get(self.html['url'], params=params)
            soup = BeautifulSoup(response.text, 'lxml')
            sections = soup.find_all(self.html['section']['flag'],
                                     class_=self.html['section']['class'])
            pages_list = soup.find_all(self.html['page']['flag'],
                                       class_=self.html['page']['class'])
            try:
                for section in tqdm(sections):  # Debug - Progress bar.
                    offer_url = section.find(href=True)['href']
                    offer = self.form_offer(offer_url, section).to_frame().T
                    offer = pd.concat([df, offer], ignore_index=True)
                    offer.to_csv(self.output_path, mode='a',
                                 header=False, index=False)
                last_page_num = int(pages_list[-2].text)
                pages = last_page_num if pages < last_page_num else pages
                params['page'] += 1
            except AttributeError:
                print('Выпал в ошибку на странице {}'.format(params['page']))
                sleep(30)
                last_processed_page = self.read_counter()
                params['page'] = last_processed_page
            finally:
                remove('.temp.csv')

        finish_time = round((time() - start_time) / 60, 2)  # Minutes.
        return finish_time
