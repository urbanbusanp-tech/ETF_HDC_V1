import streamlit as st
import pandas as pd
import os

# 페이지 기본 설정
st.set_page_config(page_title="ETF 상대강도 대시보드", page_icon="📈", layout="wide")

st.title("📊 대한민국 상장 주식형 ETF 모멘텀 대시보드")
st.markdown("""
마크 미너비니의 상대강도를 기준으로 국내 상장 주식형 ETF의 추세를 분석한 결과입니다. 
데이터는 **매일 장 마감 후 자동으로 업데이트** 됩니다.
""")

csv_path = 'etf_data.csv'

if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)
    
    # 스트림릿에서 %로 예쁘게 보여주기 위해 100을 곱해줍니다.
    df['1개월'] = df['1개월'] * 100
    df['3개월'] = df['3개월'] * 100
    df['1년'] = df['1년'] * 100
    
    # 종목코드를 클릭 가능한 네이버 금융 링크로 변환
    df['네이버 차트'] = "https://finance.naver.com/item/fchart.naver?code=" + df['종목코드'].astype(str).str.zfill(6)
    
    # 표 렌더링
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_order=['종목코드', '종목명', '현재가(원)', '거래량', '1개월', '3개월', '1년', '상대강도', '네이버 차트'],
        column_config={
            "종목코드": st.column_config.TextColumn("코드"),
            "1개월": st.column_config.NumberColumn("1개월", format="%.2f%%"),
            "3개월": st.column_config.NumberColumn("3개월", format="%.2f%%"),
            "1년": st.column_config.NumberColumn("1년", format="%.2f%%"),
            "상대강도": st.column_config.NumberColumn(
                "상대강도",
                help="1~99점. 80 이상이면 강력한 추세",
                format="%d"
            ),
            "네이버 차트": st.column_config.LinkColumn("차트 보기", display_text="📈 네이버 금융")
        }
    )
else:
    st.warning("데이터 파일이 아직 생성되지 않았습니다. GitHub Actions 백엔드 업데이트를 기다려 주세요.")