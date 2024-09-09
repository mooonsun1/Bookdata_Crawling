import time
import os
import requests
import pandas as pd
import asyncio
import aiohttp
import os
import random
from datetime import datetime
from bs4 import BeautifulSoup
import random


user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'

# best_seller_main = 'https://www.yes24.com/Product/Category/BestSeller?categoryNumber=001&pageSize=25'
# month_seller_main = 'https://www.yes24.com/Product/Category/MonthWeekBestSeller?categoryNumber=001&pageSize=25'
# steady_seller_main = 'https://www.yes24.com/Product/Category/SteadySeller?categoryNumber=001&pageSize=25'

book_selector = '#yesBestList > li > div > div.item_info > div.info_row.info_name > a.gd_name' # 책고유 ID를 찾기위한 selector
rank_selector = '#yesBestList > li > div > div.item_img > div.img_canvas > div > em' # 순위 selector
book_name_selector= "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > div > h2" # 책 제목 selector
auth_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_auth" # 저자 selector
publish_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_pub" # 출판사 selector
date_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoTop > span.gd_pubArea > span.gd_date" # 출판일 selector
price_selector = "#yDetailTopWrap > div.topColRgt > div.gd_infoBot > div.gd_infoTbArea > div > table > tbody > tr > td > span > em" # 가격selector
category_selector = '#infoset_goodsCate > div.infoSetCont_wrap > dl:nth-child(1) > dd > ul' # 카테고리selector
introduce_selector = '#infoset_introduce > div.infoSetCont_wrap' # 책소개 selector

cover_selector = "#yesBestList > li > div > div.item_img > div.img_canvas > span > span > a > em > img" #19금 자료를 제외하기 위한 이미지주소 selector

def get_book_url(links):  # links 의 값에 best_page, month_page, steady_page 값이 들어온다
    result_list = [] # result_list 라는 빈리스트 생성 
    for l in links: # links 리스트 순회하면서 링크 요청 
        res = requests.get(l, headers={"user-agent":user_agent}) #requests.get 사용해서 링크내용 가져온다 
        if res.status_code == 200: # 응답이 정상일때 돌아가는 코드 
            soup = BeautifulSoup(res.text, "lxml")  # BeautifulSoup을 사용해서 res.text값을 soup에 넣는다.
            cover_list = soup.select(cover_selector) # soup에 cover_selector 을 넣어서 cover_list를 뽑아온다. 이미지 값을 뽑아온다
            book_list = soup.select(book_selector) # soup에 book_selector을 넣어서 book_list를 뽑아온다. 책의 고유주소를 뽑아온다
            rank_list = soup.select(rank_selector)# soup에 rank_selector을 넣어서 rank_list를 뽑아온다. 책 순위를 뽑아온다.
            for book, ranks, cover in zip(book_list, rank_list, cover_list): # zip을 사용해서 3개의 리스트를 for문으로 돌린다.
                if cover.get("data-original") == "https://image.yes24.com/momo/PD_19_L.gif": # data-original값이 성인제한 이미지로 나오면 다음 작업으로 넘어간다.
                    continue
                link = book.get("href")  # href 속성 값 (책 소개페이지 링크)
                rank = ranks.get_text() # 순위 
                result_list.append(('https://www.yes24.com/'+link,int(rank)))

        else:
            raise Exception(f"요청 실패. 응답코드: {res.status_code}")
    return result_list


async def get_book_info(url, session): 
    async with session.get(url[0]) as res: # result_list = ('https://www.yes24.com/'+link,int(rank))  url 값만 가져오기 위해서 index [0]값을 호출
        if res.status == 200:
            pk = url[0].split('/')[-1] # 책 고유 id를 pk에 저장. 
            html = await res.text() # 리턴이 있는 코루틴 호출. 변수 
            soup = BeautifulSoup(html, "lxml") 
            rank = url[1] # 순위를 rank에 저장.

            book_name = soup.select(book_name_selector)[0].get_text() # 책이름을 book_name에 저장

            auth_datas = soup.select(auth_selector) #작가를 auth_datas에 저장 
            auth_list = [] 
            for a in auth_datas: 
                auth_list.append(a.get_text().strip().replace("/n",''))

            publish = soup.select(publish_selector)[0].get_text() # 출판사를 publish에 저장

            date = soup.select(date_selector)[0].get_text().replace(' ','-').replace('월','').replace('일','').replace('년','') #출판일을 date에 저장

            price_list = soup.select(price_selector)#가격을 price_list에 저장
            pricese = []
            for p in price_list:
                pricese.append(p.get_text().replace(',','').replace('원',''))

            category_list = soup.select(category_selector)#카테고리를 category_list에 저장
            category_datas = ['']*4 #빈리스트를 4개 생성
            for cl in category_list:
                remove_list = ['\xa0\n','\n','\r'] # list안에 포함된 특정문자열 제거 
                category_data = cl.get_text().strip()# cl.get_text()를 호출하여 요소의 텍스트를 가져오고, .strip() 메소드를 사용하여 양쪽 끝의 공백을 제거
                for r in remove_list:#remove_list에 있는 각 문자열(r)에 대해 반복하면서, category_data에서 해당 문자열을 빈 문자열로 대체
                    category_data = category_data.replace(r,'')
                temp_c = category_data.split('국내도서>')# category_data를 '국내도서' 기준으로 분할하여 temp_c 리스트에 저장
                cd_i = 0
                for cd in temp_c:
                    if cd_i == 4: # cd_i가 4일 경우, continue를 사용하여 다음 반복으로 넘어감.
                        continue
                    if len(cd) > 1:
                        category_datas[cd_i]=cd.strip().split('>')[0] #cd의 길이가 1보다 큰경우 cd의 양쪽 끝 공백을 제거하고,
                                                                    #'>' 문자열을 기준으로 분할한 후 첫 번째 요소를 category_datas의 cd_i 인덱스에 저장
                        try:
                            category_datas[cd_i+1]=cd.strip().split('>')[1] # cd를 '>' 문자열을 기준으로 분할한 후 두 번째 요소를 category_datas의 cd_i+1 인덱스에 저장

                        except:
                            cd_i += 2 #예외가 발생하면 cd_i에 2를 추가합니다
                            continue
                        cd_i += 2 

            introduce_list = soup.select(introduce_selector)#책소개를 introduce_list에 저장
            introduce_datas = []
            for il in introduce_list:
                remove_list = ['\xa0','\n','\r','책의 일부 내용을 미리 읽어보실 수 있습니다. 미리보기','MD 한마디'] #제거하고싶은 리스트
                introduce_data = il.get_text() # li에서 get_text 꺼내옴
                for r in remove_list:
                    introduce_data = introduce_data.replace(r,'') # remove_list 안에 있는 값이 있으면 ''로 변환
                introduce_datas.append(introduce_data.strip())
            if introduce_datas == []: # introduce_datas가 없으면 ''로 반환(빈리스트)
                introduce_datas = [''] 

            result_list = [pk, rank, book_name, auth_list[0], publish, date, int(pricese[0]), int(pricese[1]), *category_datas, introduce_datas[0]]
            # 결과값 = 책ID, 순위, 책제목, 작가, 출판사, 출판일, 정가, 판매가, 카테고리1-1, 카테고리1-2, 카테고리2-1, 카테고리2-2, 책소개
            print("처리완료 : ", pk) 
            return result_list 
        else:
            raise Exception(f"요청 실패. 응답코드: {res.status_code}")

async def main(links):
    async with aiohttp.ClientSession(headers={"user-agent":user_agent}) as session: #1. ClientSession 생성. 2. ClientSession을 이용해서 http요청.
                                                                                    #3. http요청, ClientSession 연결을 닫기.
        result = await asyncio.gather(*[get_book_info(url, session) for url in links])# 여러개의 코루틴을 비동기적으로 실행함
    return result


if __name__ == '__main__': 

    os.chdir(r'C:\Projects\DA35 2nd project\DA35-2nd-SMJ-OBC\OBC') #경로설정
    os.makedirs('Datas/best_seller_datas', exist_ok=True) # 폴더생성
    os.makedirs('Datas/month_seller_datas', exist_ok=True)
    os.makedirs('Datas/steady_seller_datas', exist_ok=True)   

    t = time.time() # 실행시간
    print("작업 시작") 
    
    best_pages = ['https://www.yes24.com/Product/Category/BestSeller?categoryNumber=001&pageNumber='+str(x)+'&pageSize=25' for x in range(1,21)]# best_pages의url
    best_seller_links = get_book_url(best_pages) # get_book_url 함수안에 best_pages라는 url을 넣어서 나온값이다
    best_seller_datas = asyncio.run(main(best_seller_links)) #best_seller_links를 main함수에 넣어서 나온값
    best_df = pd.DataFrame(best_seller_datas)# dataFrame으로 변환

    month_pages = ['https://www.yes24.com/Product/Category/MonthWeekBestSeller?categoryNumber=001&pageNumber='+str(x)+'&pageSize=25' for x in range(1,21)] 
    month_seller_links = get_book_url(month_pages)
    month_seller_datas = asyncio.run(main(month_seller_links))
    month_df = pd.DataFrame(month_seller_datas)

    steady_pages = ['https://www.yes24.com/Product/Category/SteadySeller?categoryNumber=001&pageNumber='+str(x)+'&pageSize=25' for x in range(1,21)]
    steady_seller_links = get_book_url(steady_pages)
    steady_seller_datas = asyncio.run(main(steady_seller_links))
    steady_df = pd.DataFrame(steady_seller_datas)

    e = time.time()
    print("작업 완료, 소요 시간: ",e-t) 


    d = datetime.now().strftime("%Y-%m-%d-%H-%M-%S") #실행일시

    best_file_path = f"Datas/best_seller_datas/{d}.csv" # 파일 이름,저장방식
    best_df.to_csv(best_file_path, index=False) # df파일을 저장

    month_file_path = f"Datas/month_seller_datas/{d}.csv"
    month_df.to_csv(month_file_path, index=False)

    steady_file_path = f"Datas/steady_seller_datas/{d}.csv"
    steady_df.to_csv(steady_file_path, index=False)

    if input("DB갱신: Y입력") == 'Y': 
        from sqlalchemy import create_engine

        db_connection_str = 'mysql+pymysql://playdata:1111@127.0.0.1:3306/obc'# sql서버 접속 port 번호 db접속 
        db_connection = create_engine(db_connection_str) # db 정의
        conn = db_connection.connect() #  db연결
        columns_name = ['책ID','순위','책제목','작가','출판사','출판일','정가','판매가','카테고리1_1','카테고리1_2','카테고리2_1','카테고리2_2','책소개']
        best_df.columns = columns_name # columns 이름 
        best_df.to_sql(name='best_obc', con=db_connection, if_exists='replace',index=False ) #obc/best_obc 저장 할때마다 갱신
        month_df.columns = columns_name
        month_df.to_sql(name='month_obc', con=db_connection, if_exists='replace',index=False )
        steady_df.columns = columns_name
        steady_df.to_sql(name='steady_obc', con=db_connection, if_exists='replace',index=False )


