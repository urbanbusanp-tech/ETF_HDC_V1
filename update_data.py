import requests
import pandas as pd
import json
import os
from datetime import datetime
import FinanceDataReader as fdr
import time

# 구글 블로그 API 연동을 위한 라이브러리
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_equity_etfs():
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df = pd.DataFrame(data['result']['etfItemList'])
    
    target_codes = [1, 2, 4]
    equity_df = df[df['etfTabCode'].isin(target_codes)].copy()
    
    exclude_keywords = ['채권', '국고채', '금리', '원유', '골드', '금선물', '은선물', '달러', '인버스', '레버리지', 'TR']
    pattern = '|'.join(exclude_keywords)
    equity_df = equity_df[~equity_df['itemname'].str.contains(pattern)]
    
    equity_df = equity_df[['itemcode', 'itemname', 'nowVal', 'quant']]
    return equity_df

def calculate_minervini_rs(equity_df):
    end_date = datetime.today()
    start_date = end_date - pd.DateOffset(years=1)
    
    benchmark_data = fdr.DataReader('069500', start_date, end_date)
    if len(benchmark_data) >= 240: 
        benchmark_now = float(benchmark_data['Close'].iloc[-1])
        benchmark_21d = float(benchmark_data['Close'].iloc[-21])
        benchmark_63d = float(benchmark_data['Close'].iloc[-63])
        benchmark_1y = float(benchmark_data['Close'].iloc[0])    
        
        benchmark_1m_ret = (benchmark_now / benchmark_21d) - 1
        benchmark_3m_ret = (benchmark_now / benchmark_63d) - 1
        benchmark_1y_ret = (benchmark_now / benchmark_1y) - 1
    else:
        benchmark_1m_ret, benchmark_3m_ret, benchmark_1y_ret = 0, 0, 0

    scores = []
    codes = equity_df['itemcode'].tolist()
    total = len(codes)
    
    for i, code in enumerate(codes):
        if i % 50 == 0 and i > 0:
            time.sleep(0.5) 
            
        try:
            df_hist = fdr.DataReader(code, start_date, end_date)
            if len(df_hist) < 240:
                scores.append({'itemcode': code, 'weighted_return': None, '1m_ret': None, '3m_ret': None, '1y_ret': None})
                continue
                
            close = df_hist['Close']
            price_now = float(close.iloc[-1])
            price_21d = float(close.iloc[-21])   
            price_63d = float(close.iloc[-63])   
            price_126d = float(close.iloc[-126]) 
            price_189d = float(close.iloc[-189]) 
            price_240d = float(close.iloc[-240]) 
            
            weighted_ret = ((price_now/price_63d - 1) * 0.4 + 
                            (price_now/price_126d - 1) * 0.2 + 
                            (price_now/price_189d - 1) * 0.2 + 
                            (price_now/price_240d - 1) * 0.2)
            
            ret_1m = (price_now/price_21d) - 1
            ret_3m = (price_now/price_63d) - 1
            ret_1y = (price_now/price_240d) - 1
            
            scores.append({
                'itemcode': code, 
                'weighted_return': weighted_ret,
                '1m_ret': ret_1m,
                '3m_ret': ret_3m,
                '1y_ret': ret_1y
            })
        except Exception as e:
            scores.append({'itemcode': code, 'weighted_return': None, '1m_ret': None, '3m_ret': None, '1y_ret': None})
            
    scores_df = pd.DataFrame(scores)
    
    valid_scores = scores_df.dropna(subset=['weighted_return']).copy()
    valid_scores['RS_Rating'] = valid_scores['weighted_return'].rank(pct=True) * 99
    valid_scores['RS_Rating'] = valid_scores['RS_Rating'].apply(lambda x: int(round(x)))
    
    result_df = pd.merge(equity_df, valid_scores[['itemcode', '1m_ret', '3m_ret', '1y_ret', 'RS_Rating']], on='itemcode', how='inner')
    result_df = result_df.sort_values(by='RS_Rating', ascending=False)
    
    result_df.columns = ['종목코드', '종목명', '현재가(원)', '거래량', '1개월', '3개월', '1년', '상대강도']
    
    return result_df, benchmark_1m_ret, benchmark_3m_ret, benchmark_1y_ret

def post_to_blogger(title, html_content):
    """
    환경 변수에 등록된 인증 정보를 바탕으로 구글 블로그에 자동 포스팅합니다.
    개발 단계(인증 정보 없음)에서는 포스팅을 건너뜁니다.
    """
    blog_id = os.environ.get('BLOGGER_BLOG_ID')
    client_id = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([blog_id, client_id, client_secret, refresh_token]):
        print("💡 [개발 모드] Blogger API 인증 정보가 없어 자동 포스팅 로직은 건너뜁니다.")
        return

    print("🚀 [배포 모드] 구글 블로그(Blogger) 자동 포스팅을 시작합니다...")
    try:
        # OAuth 2.0 자격 증명 생성
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri='https://oauth2.googleapis.com/token',
            client_id=client_id,
            client_secret=client_secret
        )

        service = build('blogger', 'v3', credentials=creds)
        body = {
            "kind": "blogger#post",
            "title": title,
            "content": html_content
        }
        
        # 블로그에 글 게시 (isDraft=False 로 설정하여 즉시 발행)
        posts = service.posts()
        res = posts.insert(blogId=blog_id, body=body, isDraft=False).execute()
        print(f"✅ 구글 블로그 포스팅 성공! 링크: {res.get('url')}")
    except Exception as e:
        print(f"❌ 구글 블로그 포스팅 실패: {e}")

def export_data(df, bm_1m, bm_3m, bm_1y):
    # 1. Streamlit 연동을 위한 CSV 저장
    df.to_csv('etf_data.csv', index=False, encoding='utf-8-sig')

    # 2. 구글 블로그 포스팅용 HTML 생성
    html_df = df.copy()
    html_df['1개월'] = (html_df['1개월'] * 100).round(2).astype(str) + '%'
    html_df['3개월'] = (html_df['3개월'] * 100).round(2).astype(str) + '%'
    html_df['1년'] = (html_df['1년'] * 100).round(2).astype(str) + '%'
    
    html_df['종목코드'] = html_df['종목코드'].apply(
        lambda x: f'<a href="https://finance.naver.com/item/fchart.naver?code={x}" target="_blank" style="color: #3498db; text-decoration: none; font-weight: bold;">{x}</a>'
    )
    
    html_df['상대강도'] = html_df['상대강도'].apply(
        lambda x: f'<span style="color: #e74c3c; font-weight: bold;">{x}</span>' if x >= 80 else str(x)
    )

    today_date = datetime.now().strftime('%Y-%m-%d')
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    table_html = html_df.to_html(index=False, classes='etf-table', border=0, escape=False)
    post_title = f"주식형 ETF 상대강도 모멘텀 랭킹({today_date})"
    
    # 💡 SEO 최적화를 위해 시맨틱 태그 구조를 갖춘 포스팅용 HTML 본문
    html_content = f"""
    <div class="etf-container" style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; max-width: 100%; overflow-x: auto; margin-bottom: 30px;">
        <h2 style="color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px; font-size: 1.5em;">📊 {post_title}</h2>
        <div class="description" style="font-size: 0.95em; color: #7f8c8d; margin-bottom: 15px; padding: 15px; background-color: #f8f9fa; border-radius: 5px; border-left: 4px solid #3498db;">
            <strong>💡 마크 미너비니 상대강도 (IBD RS Rating)</strong><br>
            최근 1년간의 가중 수익률(최근 3개월 40% 비중)을 전체 ETF 내에서 1~99점의 백분위 순위로 매긴 값입니다. (80점 이상 붉은색 강조 처리)<br><br>
            * <strong>업데이트 일시:</strong> {current_time} (분석 종목: {len(df)}개)<br>
            * <strong>벤치마크(KODEX 200):</strong> 1개월({bm_1m*100:.2f}%), 3개월({bm_3m*100:.2f}%), 1년({bm_1y*100:.2f}%)
        </div>
        {table_html}
    </div>
    """

    # 개발 단계 확인용 로컬 파일 저장
    with open('minervini_rs_etf_list.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
        
    # 3. 구글 블로그 API로 전송
    post_to_blogger(post_title, html_content)

if __name__ == "__main__":
    equity_df = get_equity_etfs()
    rs_df, bm_1m, bm_3m, bm_1y = calculate_minervini_rs(equity_df)
    export_data(rs_df, bm_1m, bm_3m, bm_1y)