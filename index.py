import requests
import sys
import csv
import chardet
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from db_func import *
from config import *
from sql import sql_create_links_table
import os
import nltk
nltk.download("stopwords")
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer


stopwords_arm = ["այդ","այլ","այն","այս","դու","դուք","եմ","են","ենք","ես","եք","է","էի","էին","էինք","էիր","էիք","էր","ըստ","թ","ի","ին","իսկ","իր","կամ","համար","հետ","հետո","մենք","մեջ","մի","ն","նա","նաև","նրա","նրանք","որ","որը","որոնք","որպես","ու","ում","պիտի","վրա","և", 'մեզ', 'չի', 'չէր', 'ոչ', 'այո']
tag_coefficients_in_rating = {'html': 0.4, 'head': 0.4, 'title': 0.4, 'body': 0.1, 'nav': 0.2, 'div': 0.1, 'a': 0.3, 'small': 0.1,
                        'button': 0.2, 'ul': 0.1, 'li': 0.1, 'section': 0.1, 'span': 0.1, 'h1': 0.5, 'p': 0.1, 'h3': 0.3, 'h2': 0.4, 'footer': 0.3, 'br': 0.1, 'form': 0.2}


def get_content(url):
    try:
        data = requests.get(url)
        if data.status_code == 200:
            soup = BeautifulSoup(data.text, 'html.parser')
        else:
            print(f"Something went wrong. Status code - {data.status_code}")
            return None
    except requests.ConnectionError:
        sys.exit('Connection failed')
    return soup


def get_full_url(obj):
    return obj['protocol'] + obj['domain'] + obj['path']


def get_page_links(soup):
    links = []
    for link in soup.find_all('a'):
        href = link.get('href')
        o = urlparse(href)
        if o.path:
            flag = False
            if o.netloc == SITE_NAME or not o.netloc:
                flag = True
                if len(links):
                    if any([data.get('path') == o.path for data in links]):
                            flag = False
                else:
                    flag = True
            if flag:
                links.append({'protocol': SITE_PROTOCOL, 'domain': SITE_NAME, 'path': o.path})
    return links


all_links = []
frontier = []


def get_all_links(link):
    global all_links
    global frontier
    frontier.append(link)
    all_links.append(link)
    with open('main.html', "w") as f:
        f.write(requests.get(get_full_url(link)).text)

    while frontier:
        soup = get_content(get_full_url(frontier[0]))
        if soup is not None:
            page_links = get_page_links(soup)
            for link in page_links:
                if link not in all_links and link not in frontier:
                    if link['path'] == "void(0)":
                        continue
                    print(len(all_links))
                    frontier.append(link)
                    all_links.append(link)

            del frontier[0]
        else:
            continue
    print("The End!")
    return None


def importing_html_files():
    directory_path = f'/home/ruben/PycharmProjects/WebPagesAnalysis/{SITE_NAME}'
    try:
        os.mkdir(directory_path)
    except FileExistsError:
        pass
    conn = create_connection(DATABASE)
    if conn is not None:
        db_links = select_all_links(conn)
        for link in db_links:
            full_url = link[3] + link[2] + link[1]
            with open(f'foodtime.am/web_page_{link[0]}.html', 'w') as f:
                f.write(requests.get(full_url).text)


def write_csv(i, words_values):
    directory_path = '/home/ruben/PycharmProjects/WebPagesAnalysis/word_rankings'
    try:
        os.mkdir(directory_path)
    except FileExistsError:
        pass
    with open(f'word_rankings/word_ranking_for_page_{i}.csv', 'w') as f:
        csv_writer = csv.writer(f, delimiter=",")
        csv_writer.writerow(['word', 'rating'])
        for key in words_values.keys():
            csv_writer.writerow([key, words_values[key]])


def add_rankings_in_sql(i, words_values):
    conn = create_connection(DATABASE)
    if conn is not None:
        sql_create_ratings_table = f"""
            CREATE TABLE IF NOT EXISTS rating_for_page_{i} (
                id integer PRIMARY KEY,
                word text NOT NULL,
                rating text
            )
        """
        create_table(conn, sql_create_ratings_table)
        for key in words_values.keys():
            create_word_row(conn, (i, key, words_values[key]))


def analyse_html_page():
    conn = create_connection(DATABASE)
    if conn is not None:
        for i in range(1, len(select_all_links(conn)) + 1):
            with open(f'foodtime.am/web_page_{i}.html', 'rb') as f:
                text = f.read()
                if chardet.detect(text)['encoding'] != 'utf-8':
                    continue
                soup = BeautifulSoup(text, 'html.parser')
                for script in soup(["script", "style"]):
                    script.extract()  # rip it out
                tag_texts = []
                for tag in soup.find_all():
                    new_text = ""
                    for character in tag.text:
                        if character.isalpha() or character == " ":
                            new_text += character
                    if new_text.strip() != "":
                        new_text = new_text.split(" ")
                        new_text = [e for e in new_text if e != "" and e != ""]
                        tag_texts.append((tag.name, new_text))
                tag_text_dict = {}
                porter = PorterStemmer()
                for (tag, text) in tag_texts:
                    text = [porter.stem(word) for word in text if word not in stopwords_arm and word not in stopwords.words('english')]
                    try:
                        tag_text_dict[tag].extend(text)
                    except KeyError:
                        tag_text_dict[tag] = text
                words_values = {}
                for key in tag_text_dict.keys():
                    for word in tag_text_dict[key]:
                        try:
                            words_values[word] += tag_coefficients_in_rating[key]
                        except KeyError:
                            words_values[word] = tag_coefficients_in_rating[key]
                add_rankings_in_sql(i, words_values)


def main():
    # conn = create_connection(DATABASE)
    # if conn is not None:
    #     create_table(conn, sql_create_links_table)
    #
    #     get_all_links({"protocol": SITE_PROTOCOL, "domain": SITE_NAME, "path": "/"})
    #
    #     print("Starting DB process")
    #
    #     for data in all_links:
    #         link_data = (data.get('path'), data.get('domain'), data.get('protocol'))
    #         lastrowid = create_link(conn, link_data)
    #         print(lastrowid)
    # importing_html_files()
    analyse_html_page()


if __name__ == "__main__":
    main()