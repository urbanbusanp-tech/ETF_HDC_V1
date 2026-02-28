import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta, timezone
import FinanceDataReader as fdr
import time

# 구글 블로그 API 연동을 위한 라이브러리
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

def get_equity_etfs():
    """네이버 금융 API를 통해 국내 상장 주식형 ETF 목록을 수집합니다."""
    url = "https://finance.naver.com/api/sise/etfItemList.nhn"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    df = pd.DataFrame(data['result']['etfItemList'])
    
    # 주식형 관련 탭 코드 필터링
    target_codes = [1, 2, 4]
    equity_df = df[df['etfTabCode'].isin(target_codes)].copy()
    
    # 자산배분 전략 수립을 위해 주식형이 아닌 종목 제외
    exclude_keywords = ['채권', '국고채', '금리', '원유', '골드', '금선물', '은선물', '달러', '인버스', '레버리지', 'TR']
    pattern = '|'.join(exclude_keywords)
    equity_df = equity_df[~equity_df['itemname'].str.contains(pattern)]
    
    equity_df = equity_df[['itemcode', 'itemname', 'nowVal', 'quant']]
    return equity_df

def calculate_minervini_rs(equity_df):
    """최근 3, 6, 9, 12개월 수익률에 가중치를 부여하여 상대강도를 계산합니다."""
    end_date = datetime.now()
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
    
    for i, code in enumerate(codes):
        if i % 50 == 0 and i > 0:
            time.sleep(0.5) 
            
        try:
            df_hist = fdr.DataReader(code, start_date, end_date)
            if len(df_hist) < 240:
                scores.append({'itemcode': code, 'weighted_return': None, '1m_ret': None, '3m_ret': None, '1y_ret': None})
                continue
                
            close = df_hist['Close']
            p0 = float(close.iloc[-1])
            p63 = float(close.iloc[-63])   
            p126 = float(close.iloc[-126]) 
            p189 = float(close.iloc[-189]) 
            p240 = float(close.iloc[-240]) 
            
            # 가중 수익률 계산
            weighted_ret = ((p0/p63 - 1) * 0.4 + (p0/p126 - 1) * 0.2 + (p0/p189 - 1) * 0.2 + (p0/p240 - 1) * 0.2)
            
            scores.append({
                'itemcode': code, 
                'weighted_return': weighted_ret,
                '1m_ret': (p0/float(close.iloc[-21])) - 1,
                '3m_ret': (p0/p63) - 1,
                '1y_ret': (p0/p240) - 1
            })
        except Exception:
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
    """Blogger API를 사용하여 글을 게시합니다."""
    blog_id = os.environ.get('BLOGGER_BLOG_ID')
    client_id = os.environ.get('BLOGGER_CLIENT_ID')
    client_secret = os.environ.get('BLOGGER_CLIENT_SECRET')
    refresh_token = os.environ.get('BLOGGER_REFRESH_TOKEN')

    if not all([blog_id, client_id, client_secret, refresh_token]):
        print("💡 Blogger API 인증 정보가 없어 포스팅을 건너뜁니다.")
        return

    try:
        creds = Credentials(token=None, refresh_token=refresh_token, token_uri='https://oauth2.googleapis.com/token', client_id=client_id, client_secret=client_secret)
        service = build('blogger', 'v3', credentials=creds)
        body = {"kind": "blogger#post", "title": title, "content": html_content}
        service.posts().insert(blogId=blog_id, body=body, isDraft=False).execute()
        print("✅ 구글 블로그 포스팅 성공!")
    except Exception as e:
        print(f"❌ 구글 블로그 포스팅 실패: {e}")

def export_data(df, bm_1m, bm_3m, bm_1y):
    """데이터 가공 및 SEO 최적화된 HTML 리포트를 생성합니다."""
    df.to_csv('etf_data.csv', index=False, encoding='utf-8-sig')

    html_df = df.copy()
    html_df['현재가(원)'] = html_df['현재가(원)'].apply(lambda x: f"{x:,}")
    html_df['거래량'] = html_df['거래량'].apply(lambda x: f"{x:,}")
    html_df['1개월'] = (html_df['1개월'] * 100).round(2).astype(str) + '%'
    html_df['3개월'] = (html_df['3개월'] * 100).round(2).astype(str) + '%'
    html_df['1년'] = (html_df['1년'] * 100).round(2).astype(str) + '%'
    
    html_df['종목코드'] = html_df['종목코드'].apply(lambda x: f'<a href="https://finance.naver.com/item/fchart.naver?code={x}" target="_blank" style="color: #3498db; text-decoration: none; font-weight: bold;">{x}</a>')
    html_df['상대강도'] = html_df['상대강도'].apply(lambda x: f'<span style="color: #c0392b; font-weight: bold;">{x}</span>' if x >= 80 else str(x))

    kst = timezone(timedelta(hours=9))
    now_kst = datetime.now(kst)
    today_date = now_kst.strftime('%Y-%m-%d')
    current_time = now_kst.strftime('%Y-%m-%d %H:%M')
    
    table_html = html_df.to_html(index=False, classes='etf-table', border=0, escape=False, justify='center')
    post_title = f"🚀 주식형 ETF 상대강도 모멘텀 랭킹({today_date})"
    
    # [디자인 수정] 표 중앙 정렬 및 여백 밸런스 조정
    html_content = f"""
    <div class="etf-container" style="font-family: 'Helvetica Neue', Arial, sans-serif; line-height: 1.6; color: #333; width: 100%; max-width: 1000px; margin: 0 auto 30px auto; padding: 0 10px; box-sizing: border-box;">
        <style>
            .etf-container h3 {{ color: #2c3e50; padding-left: 10px; margin-top: 25px; margin-bottom: 10px; }}
            .etf-container .content-block {{ padding: 10px 12px 10px 15px; border-left: 4px solid #ccc; margin-bottom: 20px; }}
            /* 표 전용 섹션: 구분선 제거 및 양옆 여백 동일화를 위한 설정 */
            .etf-container .table-section {{ border-left: none; padding: 0; margin-top: 15px; width: 100%; overflow-x: auto; }}
            .etf-container .content-block p {{ font-size: 0.95em; color: #444; margin: 0 0 10px 0; }}
            .technical-list {{ list-style: none; padding: 0; margin: 0; font-size: 0.9em; color: #666; }}
            .technical-list li {{ margin-bottom: 5px; }}
            
            /* [표 중앙 정렬 및 너비 최적화] */
            .etf-table {{ 
                width: 100%; 
                max-width: 100%;
                border-collapse: collapse; 
                background-color: #ffffff; 
                font-size: 0.9em; 
                border: 1px solid #e0e0e0; 
                margin: 0 auto; /* 표 자체를 중앙으로 */
            }}
            .etf-table th {{ background-color: #f8f9fa; color: #2c3e50; font-weight: 600; padding: 10px; border: 1px solid #e0e0e0; }}
            .etf-table td {{ padding: 10px; border: 1px solid #e0e0e0; text-align: center; vertical-align: middle; }}
            .etf-table td:nth-child(2) {{ text-align: left; }}
            .etf-table tr:hover {{ background-color: #f1f4f8; }}
        </style>
        
        <h3>💡 개요: 시장 주도주를 찾는 모멘텀 분석</h3>
        <div class="content-block">
            <p>본 리포트는 대한민국 상장 주식형 ETF 중 현재 가장 강력한 상승 에너지를 보여주는 종목을 선별합니다. 단순 가격 상승률을 넘어 시장 대비 초과 수익을 내는 '진짜 주도주'를 확인하여 전략적 자산배분의 기초 자료로 활용해 보세요.</p>
        </div>

        <h3>📈 상대강도(Relative Strength)란?</h3>
        <div class="content-block">
            <p>마크 미너비니의 <b>RS Rating</b>은 특정 종목의 퍼포먼스를 시장 전체와 비교하여 1~99점으로 환산한 지표입니다.</p>
            <p>- <b>계산 방식:</b> 최근 <b>3개월(40%)</b>, 6개월(20%), 9개월(20%), 12개월(20%) 가중치를 부여하여 최신 트렌드를 강조합니다.<br>
               - <b>해석 방법:</b> 점수가 80점 이상인 종목은 현재 시장 상위 20% 이내의 주도주군임을 의미합니다.</p>
        </div>

        <h3>📊 분석 상세 정보</h3>
        <div class="content-block">
            <ul class="technical-list">
                <li>📅 <b>업데이트 일시:</b> {current_time} (KST 기준)</li>
                <li>🔍 <b>분석 대상:</b> 국내 상장 주식형 ETF {len(df)}개 (상장 1년 미만 제외)</li>
                <li>📉 <b>벤치마크(KODEX 200) 성과:</b> 1개월({bm_1m*100:.2f}%), 3개월({bm_3m*100:.2f}%), 1년({bm_1y*100:.2f}%)</li>
            </ul>
        </div>

        <h3>📋 주식형 ETF 상대강도 순위 TOP 리스트</h3>
        <div class="table-section">
            {table_html}
        </div>
    </div>
    """

    with open('minervini_rs_etf_list.html', 'w', encoding='utf-8') as f:
        f.write(html_content)
        
    post_to_blogger(post_title, html_content)

if __name__ == "__main__":
    equity_df = get_equity_etfs()
    rs_df, bm_1m, bm_3m, bm_1y = calculate_minervini_rs(equity_df)
    export_data(rs_df, bm_1m, bm_3m, bm_1y)

